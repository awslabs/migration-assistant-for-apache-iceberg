# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""On-demand validation API routes.

Thin HTTP surface over core.validator (run) and ValidationRepo (read). All
validation logic lives in the engine; this module only marshals requests and
serializes stored results. Fully net-new — it does not touch the reconcile
loop, state machine, or any existing route.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from ...db import BatchRepo, ValidationRepo
from ...core.validator import run_validation

log = logging.getLogger(__name__)

router = APIRouter(prefix="/validation", tags=["validation"])
repo = ValidationRepo()
batches = BatchRepo()


@router.post("/run")
def start_validation(body: dict):
    """Kick off validation for one or more pipelines.

    Body: {"pipeline_ids": [...], "tier": "basic"|"advanced",
           "triggered_by": "<user>"}
    Returns immediately: {"started": [{pipeline_id, run_id, engine}],
                          "errors": [{pipeline_id, error}]}.
    """
    pipeline_ids = body.get("pipeline_ids") or []
    tier = (body.get("tier") or "basic").lower()
    triggered_by = body.get("triggered_by") or ""
    # Optional Athena overrides supplied by the app (from its Settings), so the
    # query uses the user-configured region/workgroup/output location rather
    # than the engine's deploy-time env defaults.
    athena_config = body.get("athena_config") or {}

    if not pipeline_ids:
        raise HTTPException(400, "pipeline_ids is required and must be non-empty")
    if tier not in ("basic", "advanced"):
        raise HTTPException(400, "tier must be 'basic' or 'advanced'")

    log.info("Validation requested: %d pipeline(s), tier=%s, by=%s",
             len(pipeline_ids), tier, triggered_by or "?")
    result = run_validation(pipeline_ids, tier, triggered_by=triggered_by,
                            athena_config=athena_config)
    log.info("Validation started: %d run(s), %d error(s)",
             len(result.get("started", [])), len(result.get("errors", [])))
    return result


def _parse_ts(value) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _last_batch_activity(pipeline_id: str) -> datetime | None:
    """Most recent batch create/start/complete time for a pipeline."""
    latest = None
    for b in batches.list_by_pipeline(pipeline_id):
        for field in ("created_at", "started_at", "completed_at"):
            ts = _parse_ts(b.get(field))
            if ts and (latest is None or ts > latest):
                latest = ts
    return latest


def _validation_state(pipeline_id: str) -> dict:
    """Summarize validation for one pipeline.

    state: none (never run / still running), passed, failed, or stale (a batch
    ran after the last validation started, so the result no longer reflects the
    target).
    """
    latest = repo.get_latest(pipeline_id)
    if not latest or latest.get("overall_status") == "running":
        return {"pipeline_id": pipeline_id, "state": "none",
                "validated_at": (latest or {}).get("run_at")}
    state = "passed" if latest.get("overall_status") == "passed" else "failed"
    run_at = _parse_ts(latest.get("run_at"))
    last_batch = _last_batch_activity(pipeline_id)
    if run_at and last_batch and last_batch > run_at:
        state = "stale"
    return {"pipeline_id": pipeline_id, "state": state,
            "validated_at": latest.get("run_at"),
            "last_batch_at": last_batch.isoformat() if last_batch else None}


@router.post("/status")
def get_validation_status(body: dict):
    """Validation state for many pipelines at once.

    Body: {"pipeline_ids": [...]}. Returns {pipeline_id: {state, validated_at,
    last_batch_at}} where state is none | passed | failed | stale.
    """
    pipeline_ids = body.get("pipeline_ids") or []
    return {pid: _validation_state(pid) for pid in pipeline_ids}


@router.get("/{pipeline_id}")
def get_latest_validation(pipeline_id: str):
    """Latest validation run for a pipeline (or null if never validated)."""
    latest = repo.get_latest(pipeline_id)
    return latest or {}


@router.get("/{pipeline_id}/history")
def get_validation_history(pipeline_id: str):
    """All validation runs for a pipeline, newest first."""
    return repo.list_history(pipeline_id)


@router.get("/run/{run_id}")
def get_validation_run(run_id: str, pipeline_id: str = ""):
    """A single validation run by id.

    pipeline_id is optional but recommended: the DynamoDB backend keys on
    (pipeline_id, run_id), so passing it avoids a table scan.
    """
    run = repo.get_run(run_id, pipeline_id)
    if not run:
        raise HTTPException(404, "Validation run not found")
    return run
