# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pipeline proxy routes — forwards requests to the engine."""

import logging

import httpx
from app import engine_client
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.deps import require_plan_view, require_pipeline_view, require_pipeline_manage

log = logging.getLogger(__name__)


def _get_engine_url():
    from app.services.settings_service import get_setting
    return get_setting("ENGINE_URL") or "http://localhost:8001"


router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.get("/plan/{plan_id}/costs", dependencies=[Depends(require_plan_view)])
def get_plan_costs(plan_id: str):
    """Get aggregated compute costs for a plan. Requires VIEW access to the plan."""
    from app.database import get_conn, _fetchall
    conn = get_conn()
    rows = _fetchall(conn,
        "SELECT pipeline_id FROM plan_tables WHERE plan_id=? AND pipeline_id IS NOT NULL AND pipeline_id != ''",
        (plan_id,))
    conn.close()
    pipeline_ids = [r["pipeline_id"] for r in rows]
    if not pipeline_ids:
        return {"plan_id": plan_id, "athena_bytes_scanned": 0,
                "athena_query_count": 0, "athena_cost_usd": 0,
                "emr_vcpu_hours": 0, "emr_cost_usd": 0, "total_cost_usd": 0}
    try:
        resp = engine_client.post(f"{_get_engine_url()}/api/pipeline/costs/plan",
                          json=pipeline_ids, timeout=10.0)
        resp.raise_for_status()
        result = resp.json()
        result["plan_id"] = plan_id
        return result
    except httpx.RequestError:
        return {"plan_id": plan_id, "total_cost_usd": 0}


# The routes below are keyed by pipeline id (not plan id). Per-plan ownership is
# enforced by resolving pipeline_id -> owning plan via plan_tables in
# require_pipeline_view, so a user cannot read/act on another group's
# pipeline just because it is authenticated traffic.
@router.get("/{pipeline_id}", dependencies=[Depends(require_pipeline_view)])
def get_pipeline(pipeline_id: str):
    """Proxy pipeline detail from the engine. Requires VIEW on the owning plan."""
    try:
        resp = engine_client.get(f"{_get_engine_url()}/api/pipeline/{pipeline_id}", timeout=30.0)
        if resp.status_code == 404:
            raise HTTPException(404, "Pipeline not found")
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, str(e))
    except httpx.RequestError as e:
        log.warning("Failed to reach the engine for pipeline %s: %s", pipeline_id[:8], e)
        raise HTTPException(503, "Engine unavailable")


@router.get("/{pipeline_id}/batches", dependencies=[Depends(require_pipeline_view)])
def get_pipeline_batches(pipeline_id: str):
    """Proxy batch list from the engine. Requires VIEW on the owning plan."""
    try:
        resp = engine_client.get(
            f"{_get_engine_url()}/api/pipeline/{pipeline_id}/batches", timeout=30.0)
        if resp.status_code == 404:
            raise HTTPException(404, "Pipeline not found")
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, str(e))
    except httpx.RequestError as e:
        log.warning("Failed to reach the engine for batches %s: %s", pipeline_id[:8], e)
        raise HTTPException(503, "Engine unavailable")


@router.get("/{pipeline_id}/costs", dependencies=[Depends(require_pipeline_view)])
def get_pipeline_costs(pipeline_id: str):
    """Get compute costs for a pipeline. Requires VIEW on the owning plan."""
    try:
        resp = engine_client.get(f"{_get_engine_url()}/api/pipeline/{pipeline_id}/costs", timeout=10.0)
        if resp.status_code == 404:
            return {"pipeline_id": pipeline_id, "total_cost_usd": 0}
        resp.raise_for_status()
        return resp.json()
    except httpx.RequestError:
        return {"pipeline_id": pipeline_id, "total_cost_usd": 0}


@router.post("/{pipeline_id}/sync-now", dependencies=[Depends(require_pipeline_view)])
def trigger_sync(pipeline_id: str):
    """Trigger an immediate sync for a pipeline. Requires VIEW on the owning plan."""
    try:
        resp = engine_client.post(f"{_get_engine_url()}/api/pipeline/{pipeline_id}/sync-now", timeout=10.0)
        if resp.status_code == 404:
            raise HTTPException(404, "Pipeline not found")
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, e.response.text[:200])
    except httpx.RequestError as e:
        raise HTTPException(503, f"Engine unavailable: {e}")


@router.post("/{pipeline_id}/approve-migration", dependencies=[Depends(require_pipeline_manage)])
def approve_migration(pipeline_id: str):
    """Approve a Managed Migration plan-review pause (resume the migration). Requires MANAGE.

    Proxies to the engine, which validates the pipeline is a Managed Migration
    at awaiting_review and issues update_table_migration(queued).
    """
    try:
        resp = engine_client.post(
            f"{_get_engine_url()}/api/pipeline/{pipeline_id}/approve-migration",
            timeout=30.0)
        if resp.status_code == 404:
            raise HTTPException(404, "Pipeline not found")
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, e.response.text[:200])
    except httpx.RequestError as e:
        raise HTTPException(503, f"Engine unavailable: {e}")


class UpdateMigrationRequestBody(BaseModel):
    payload: dict


@router.put("/{pipeline_id}/migration-request", dependencies=[Depends(require_pipeline_manage)])
def update_migration_request(pipeline_id: str, body: UpdateMigrationRequestBody):
    """Update the stored Managed Migration request payload. Requires MANAGE.

    Proxies to the engine, which stores the payload under
    spec_json._metadata.start_request. This records/overrides the stored copy
    for inspection; it does not re-invoke the AWS StartTableMigration API.
    """
    try:
        resp = engine_client.put(
            f"{_get_engine_url()}/api/pipeline/{pipeline_id}/migration-request",
            json={"payload": body.payload},
            timeout=30.0)
        if resp.status_code == 404:
            raise HTTPException(404, "Pipeline not found")
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, e.response.text[:200])
    except httpx.RequestError as e:
        raise HTTPException(503, f"Engine unavailable: {e}")
