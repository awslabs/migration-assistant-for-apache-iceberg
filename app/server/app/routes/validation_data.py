# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""On-demand data-validation proxy routes.

Distinct from routes/validation.py (which serves pre-migration *eligibility*
rules). This module fronts the engine's data-validation API
(source-vs-target checks run after migration) and adds plan-aware eligibility
enumeration + MANAGE authorization.

All endpoints live under /validation/data/... so they never collide with the
existing /validation/{rules,eligible,table,batch} eligibility endpoints. Fully
net-new; nothing here modifies existing routes or services.
"""

import logging

import httpx
from app import engine_client
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.database import get_conn, _fetchall
from app.deps import require_plan_manage, require_plan_view, require_pipeline_view
from app.services.plan_service import _get_live_statuses_batch

log = logging.getLogger(__name__)

router = APIRouter(prefix="/validation/data", tags=["validation-data"])

# Table statuses for which validation makes sense: the target exists and is
# current. Everything else (mid-load, drifted, failed, not yet executed) is
# skipped with a reason so the UI can explain why. Managed Migration never
# reaches in_sync — its post-load states (initial_load_complete / sync_complete)
# are the points at which its S3 Tables target is present and current, so they
# are eligible too.
_ELIGIBLE_STATUSES = {"completed", "in_sync",
                      "initial_load_complete", "sync_complete"}

# Human-readable skip reasons per ineligible status.
_SKIP_REASONS = {
    "out_of_sync": "target has drifted from source (out of sync)",
    "failed": "migration failed — resolve before validating",
    "executing": "migration is still running",
    "initial_load": "initial load in progress",
    "staged": "migration not started yet",
    "paused": "migration is paused",
    "cancelled": "migration was cancelled",
}


def _get_engine_url():
    from app.services.settings_service import get_setting
    return get_setting("ENGINE_URL") or "http://localhost:8001"


def _plan_pipeline_rows(plan_id: str) -> list[dict]:
    """Rows in the plan that have a pipeline (i.e. were actually migrated)."""
    conn = get_conn()
    rows = _fetchall(conn,
        "SELECT source_database, source_table, migration_type, status, "
        "pipeline_id FROM plan_tables "
        "WHERE plan_id=? AND pipeline_id IS NOT NULL AND pipeline_id != ''",
        (plan_id,))
    conn.close()
    return [dict(r) for r in rows]


def _partition_eligible(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split plan rows into (eligible, skipped) using live engine status.

    Live status is authoritative (the app DB can lag the engine), so we refresh
    each row's status from the engine before deciding.
    """
    live = _get_live_statuses_batch(rows)
    eligible, skipped = [], []
    for r in rows:
        pid = r["pipeline_id"]
        status = live.get(pid, r.get("status", ""))
        entry = {
            "pipeline_id": pid,
            "source_database": r.get("source_database"),
            "source_table": r.get("source_table"),
            "status": status,
        }
        if status in _ELIGIBLE_STATUSES:
            eligible.append(entry)
        else:
            entry["reason"] = _SKIP_REASONS.get(
                status, f"not eligible in status '{status}'")
            skipped.append(entry)
    return eligible, skipped


def _proxy_run(pipeline_ids: list[str], tier: str, triggered_by: str) -> dict:
    """Forward a validation run request to the engine.

    Passes the user-configured Athena settings (region/workgroup/output
    location) so the engine's validation query uses them instead of its
    deploy-time env defaults — otherwise a Settings-only configuration (e.g. a
    different region than the engine was deployed with) never reaches the
    Athena client and the query fails with "No output location provided".
    """
    from app.services.settings_service import get_setting
    athena_config = {
        "region": get_setting("ENGINE_ATHENA_REGION") or "",
        "workgroup": get_setting("ENGINE_ATHENA_WORKGROUP") or "",
        "output_location": get_setting("ENGINE_ATHENA_OUTPUT_LOCATION") or "",
    }
    try:
        resp = engine_client.post(
            f"{_get_engine_url()}/api/validation/run",
            json={"pipeline_ids": pipeline_ids, "tier": tier,
                  "triggered_by": triggered_by,
                  "athena_config": athena_config},
            timeout=30.0)
        if resp.status_code >= 400:
            raise HTTPException(resp.status_code, resp.text[:300])
        return resp.json()
    except httpx.RequestError as e:
        log.warning("Failed to reach the engine to start validation: %s", e)
        raise HTTPException(503, "Engine unavailable")


class TablesRunRequest(BaseModel):
    pipeline_ids: list[str]
    tier: str = "basic"


@router.post("/plan/{plan_id}/run")
def run_plan_validation(plan_id: str, tier: str = "basic",
                        username: str = Depends(require_plan_manage)):
    """Validate every eligible table in a plan. Requires MANAGE on the plan.

    Returns {started, errors, skipped}. Ineligible tables are reported in
    `skipped` with a reason rather than silently dropped.
    """
    if tier not in ("basic", "advanced"):
        raise HTTPException(400, "tier must be 'basic' or 'advanced'")

    rows = _plan_pipeline_rows(plan_id)
    eligible, skipped = _partition_eligible(rows)
    if not eligible:
        return {"started": [], "errors": [], "skipped": skipped}

    result = _proxy_run([e["pipeline_id"] for e in eligible], tier, username)
    result["skipped"] = skipped
    log.info("Plan %s validation: %d started, %d skipped (by %s)",
             plan_id, len(result.get("started", [])), len(skipped), username)
    return result


@router.post("/plan/{plan_id}/tables/run")
def run_tables_validation(plan_id: str, body: TablesRunRequest,
                          username: str = Depends(require_plan_manage)):
    """Validate a specific subset of a plan's tables. Requires MANAGE.

    The submitted pipeline_ids must belong to the plan (prevents validating
    another plan's pipelines through this plan's manage grant). Ineligible
    selected tables are reported in `skipped`.
    """
    if body.tier not in ("basic", "advanced"):
        raise HTTPException(400, "tier must be 'basic' or 'advanced'")
    if not body.pipeline_ids:
        raise HTTPException(400, "pipeline_ids is required")

    rows = _plan_pipeline_rows(plan_id)
    by_pid = {r["pipeline_id"]: r for r in rows}
    requested = set(body.pipeline_ids)
    foreign = requested - set(by_pid)
    if foreign:
        raise HTTPException(
            400, f"pipeline(s) not in plan {plan_id}: {', '.join(sorted(foreign))}")

    selected_rows = [by_pid[pid] for pid in body.pipeline_ids]
    eligible, skipped = _partition_eligible(selected_rows)
    if not eligible:
        return {"started": [], "errors": [], "skipped": skipped}

    result = _proxy_run([e["pipeline_id"] for e in eligible], body.tier, username)
    result["skipped"] = skipped
    log.info("Plan %s table validation: %d started, %d skipped (by %s)",
             plan_id, len(result.get("started", [])), len(skipped), username)
    return result


@router.get("/plan/{plan_id}/status", dependencies=[Depends(require_plan_view)])
def get_plan_validation_status(plan_id: str):
    """Validation state for every migrated table in a plan, keyed by pipeline id.

    state is one of none | passed | failed | stale (a batch ran after the last
    validation). Requires VIEW on the plan.
    """
    pipeline_ids = [r["pipeline_id"] for r in _plan_pipeline_rows(plan_id)]
    if not pipeline_ids:
        return {}
    try:
        resp = engine_client.post(
            f"{_get_engine_url()}/api/validation/status",
            json={"pipeline_ids": pipeline_ids}, timeout=30.0)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, e.response.text[:200])
    except httpx.RequestError:
        return {}


@router.get("/pipeline/{pipeline_id}", dependencies=[Depends(require_pipeline_view)])
def get_pipeline_validation(pipeline_id: str):
    """Latest validation run for a pipeline. Requires VIEW on the owning plan."""
    try:
        resp = engine_client.get(
            f"{_get_engine_url()}/api/validation/{pipeline_id}", timeout=15.0)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, e.response.text[:200])
    except httpx.RequestError:
        return {}


@router.get("/pipeline/{pipeline_id}/history",
            dependencies=[Depends(require_pipeline_view)])
def get_pipeline_validation_history(pipeline_id: str):
    """Full validation history for a pipeline. Requires VIEW on the owning plan."""
    try:
        resp = engine_client.get(
            f"{_get_engine_url()}/api/validation/{pipeline_id}/history",
            timeout=15.0)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, e.response.text[:200])
    except httpx.RequestError:
        return []
