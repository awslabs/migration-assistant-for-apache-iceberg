# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Migration plan endpoints."""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from app.deps import get_current_user
from app.models import AddToPlanRequest, CreatePlanRequest, MigrationPlan
from app.services import plan_service
from app.services import pipeline_sync
from app.services.permissions import (
    filter_plans_for_user, get_plan_access, check_manage_access, get_manageable_groups,
    get_user_role,
)

router = APIRouter(prefix="/plans", tags=["plans"])


def _require_manage(plan_id: str, username: str):
    """Check that the user has manage access to the plan. Raises 403 if not."""
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    if not check_manage_access(username, plan.owners):
        raise HTTPException(status_code=403, detail="You need manage access to this plan's group")


def _require_view(plan_id: str, username: str):
    """Check that the user has at least view access to the plan. Raises 403 if not."""
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    if get_plan_access(username, plan.owners) is None:
        raise HTTPException(status_code=403, detail="You do not have access to this plan")


@router.get("", response_model=list[MigrationPlan])
def list_plans(username: str = Depends(get_current_user)):
    """List migration plans visible to the current user."""
    plans = plan_service.list_plans()
    return filter_plans_for_user(username, plans)


@router.delete("/{plan_id}")
def delete_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Delete a plan and release all its tables."""
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    if not check_manage_access(username, plan.owners):
        raise HTTPException(status_code=403, detail="You need manage access to this plan's group")
    if not plan_service.delete_plan(plan_id):
        raise HTTPException(status_code=404, detail="Plan not found")
    return {"detail": "Plan deleted"}


@router.get("/meta/tables-in-plans")
def tables_in_plans(username: str = Depends(get_current_user)):
    """Get mapping of table keys to plan names, restricted to plans the caller can see."""
    import json as _json
    visible_plan_ids = {p.id for p in filter_plans_for_user(username, plan_service.list_plans())}
    mapping = plan_service.get_tables_in_plans()
    filtered = {}
    for key, value in mapping.items():
        try:
            info = _json.loads(value)
        except (ValueError, TypeError):
            continue
        if info.get("id") in visible_plan_ids:
            filtered[key] = value
    return filtered


@router.get("/{plan_id}", response_model=MigrationPlan)
def get_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Get a specific migration plan."""
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    access = get_plan_access(username, plan.owners)
    if access is None:
        raise HTTPException(status_code=403, detail="You do not have access to this plan")
    return plan


@router.post("", response_model=MigrationPlan, status_code=201)
def create_plan(request: CreatePlanRequest, username: str = Depends(get_current_user)):
    """Create a new migration plan."""
    # Viewers cannot create plans — editor or admin required
    if get_user_role(username) == "viewer":
        raise HTTPException(status_code=403, detail="Editor or admin access required to create plans")
    # Non-admins can only assign ownership to groups they manage
    if request.owners:
        manageable = get_manageable_groups(username)
        invalid = [g for g in request.owners if g not in manageable]
        if invalid:
            raise HTTPException(
                status_code=403,
                detail=f"You can only assign ownership to groups you manage. Invalid: {', '.join(invalid)}")
    try:
        return plan_service.create_plan(request, username)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{plan_id}/tables", response_model=MigrationPlan)
def add_table_to_plan(plan_id: str, request: AddToPlanRequest, username: str = Depends(get_current_user)):
    """Add a table to an existing plan."""
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    if not check_manage_access(username, plan.owners):
        raise HTTPException(status_code=403, detail="You need manage access to this plan's group")
    try:
        plan = plan_service.add_table_to_plan(plan_id, request.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan


class BatchTableItem(BaseModel):
    source_database: str = Field(min_length=1)
    source_table: str = Field(min_length=1)
    target_database: str = Field(min_length=1)
    target_table: str = Field(min_length=1)
    # Client-resolved migration type. Falls back to the plan's compatible
    # default server-side when omitted or "full_table".
    migration_type: str = "full_table"


class AddTablesBatchRequest(BaseModel):
    tables: list[BatchTableItem] = Field(min_length=1)


@router.post("/{plan_id}/tables/batch")
def add_tables_to_plan_batch(plan_id: str, request: AddTablesBatchRequest,
                             username: str = Depends(get_current_user)):
    """Add many tables to a plan in a single transaction.

    Returns a minimal summary — {"added": int, "skipped": [{table, reason}]} —
    rather than the hydrated plan, so adding hundreds of tables is one cheap
    request instead of 2N plan-reloading requests. The client refetches the plan
    once after this returns.
    """
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    if not check_manage_access(username, plan.owners):
        raise HTTPException(status_code=403, detail="You need manage access to this plan's group")
    result = plan_service.add_tables_to_plan_batch(
        plan_id, [t.model_dump() for t in request.tables])
    if result is None:
        raise HTTPException(status_code=404, detail="Plan not found")
    return result


@router.put("/{plan_id}", response_model=MigrationPlan)
def update_plan(plan_id: str, request: CreatePlanRequest, username: str = Depends(get_current_user)):
    """Update a migration plan."""
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    if not check_manage_access(username, plan.owners):
        raise HTTPException(status_code=403, detail="You need manage access to this plan's group")
    # Validate new owners assignment
    if request.owners:
        manageable = get_manageable_groups(username)
        invalid = [g for g in request.owners if g not in manageable]
        if invalid:
            raise HTTPException(status_code=403, detail=f"Cannot assign ownership to groups you don't manage: {', '.join(invalid)}")
    try:
        plan = plan_service.update_plan(plan_id, request)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan


@router.post("/{plan_id}/finalize", response_model=MigrationPlan)
def finalize_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Finalize a plan, moving it to staged status."""
    _require_manage(plan_id, username)
    plan = plan_service.finalize_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan


@router.post("/{plan_id}/execute", response_model=MigrationPlan)
def execute_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Execute a staged plan, moving it to executing status."""
    _require_manage(plan_id, username)
    plan = plan_service.execute_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found or not staged")
    return plan


@router.post("/{plan_id}/finalize-migration", response_model=MigrationPlan)
def finalize_migration(plan_id: str, username: str = Depends(get_current_user)):
    """Finalize an in-progress migration: start all rewrite pipelines (cutover)."""
    _require_manage(plan_id, username)
    plan = plan_service.finalize_migration(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found or not in_progress")
    return plan


@router.post("/{plan_id}/archive", response_model=MigrationPlan)
def archive_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Archive a completed migration."""
    _require_manage(plan_id, username)
    plan = plan_service.archive_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found or not completed")
    return plan


@router.post("/{plan_id}/cancel", response_model=MigrationPlan)
def cancel_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Cancel a staged or executing migration."""
    _require_manage(plan_id, username)
    plan = plan_service.cancel_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found or cannot be cancelled")
    return plan


@router.post("/{plan_id}/replan", response_model=MigrationPlan)
def replan(plan_id: str, username: str = Depends(get_current_user)):
    """Move a cancelled migration back to draft for re-planning."""
    _require_manage(plan_id, username)
    plan = plan_service.replan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found or not cancelled")
    return plan


@router.post("/{plan_id}/pause", response_model=MigrationPlan)
def pause_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Pause an executing migration."""
    _require_manage(plan_id, username)
    plan = plan_service.pause_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found or not executing")
    return plan


@router.post("/{plan_id}/resume", response_model=MigrationPlan)
def resume_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Resume a paused migration."""
    _require_manage(plan_id, username)
    plan = plan_service.resume_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found or not paused")
    return plan


@router.delete("/{plan_id}/tables/{source_db}/{source_table}", response_model=MigrationPlan)
def remove_table_from_plan(plan_id: str, source_db: str, source_table: str, username: str = Depends(get_current_user)):
    """Remove a table from a plan."""
    _require_manage(plan_id, username)
    plan = plan_service.remove_table_from_plan(plan_id, source_db, source_table)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan or table not found")
    return plan


@router.post("/{plan_id}/apply-recommendations", response_model=MigrationPlan)
def apply_recommendations(plan_id: str, username: str = Depends(get_current_user)):
    """Apply agent-recommended migration types to all tables in the plan."""
    _require_manage(plan_id, username)
    plan_service.apply_recommendations(plan_id)
    plan = plan_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan


class UpdateTableTypeRequest(BaseModel):
    migration_type: str

class UpdateTableSpecRequest(BaseModel):
    spec_json: dict

@router.put("/{plan_id}/tables/{source_db}/{source_table}/type", response_model=MigrationPlan)
def update_table_type(plan_id: str, source_db: str, source_table: str, request: UpdateTableTypeRequest, username: str = Depends(get_current_user)):
    """Update a table's migration type within a plan."""
    _require_manage(plan_id, username)
    plan = plan_service.update_table_migration_type(plan_id, source_db, source_table, request.migration_type)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan or table not found")
    return plan


@router.put("/{plan_id}/tables/{source_db}/{source_table}/spec", response_model=MigrationPlan)
def update_table_spec(plan_id: str, source_db: str, source_table: str, request: UpdateTableSpecRequest, username: str = Depends(get_current_user)):
    """Update a table's spec JSON within a plan."""
    _require_manage(plan_id, username)
    try:
        plan = plan_service.update_table_spec(plan_id, source_db, source_table, request.spec_json)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not plan:
        raise HTTPException(status_code=404, detail="Plan or table not found")
    return plan


@router.post("/{plan_id}/tables/{source_db}/{source_table}/pause", response_model=MigrationPlan)
def pause_table(plan_id: str, source_db: str, source_table: str, username: str = Depends(get_current_user)):
    """Pause an individual table."""
    _require_manage(plan_id, username)
    return plan_service.pause_table(plan_id, source_db, source_table)


@router.post("/{plan_id}/tables/{source_db}/{source_table}/resume", response_model=MigrationPlan)
def resume_table(plan_id: str, source_db: str, source_table: str, username: str = Depends(get_current_user)):
    """Resume a paused table."""
    _require_manage(plan_id, username)
    return plan_service.resume_table(plan_id, source_db, source_table)


@router.post("/{plan_id}/tables/{source_db}/{source_table}/cancel", response_model=MigrationPlan)
def cancel_table(plan_id: str, source_db: str, source_table: str, username: str = Depends(get_current_user)):
    """Cancel an individual table."""
    _require_manage(plan_id, username)
    return plan_service.cancel_table(plan_id, source_db, source_table)


@router.post("/{plan_id}/tables/{source_db}/{source_table}/retry", response_model=MigrationPlan)
def retry_table(plan_id: str, source_db: str, source_table: str, username: str = Depends(get_current_user)):
    """Retry a failed table by re-submitting and starting its pipeline."""
    _require_manage(plan_id, username)
    try:
        plan = plan_service.retry_table(plan_id, source_db, source_table)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not plan:
        raise HTTPException(status_code=404, detail="Table not found or not in failed state")
    return plan


@router.post("/{plan_id}/tables/{source_db}/{source_table}/resume-from-failure", response_model=MigrationPlan)
def resume_table_from_failure(plan_id: str, source_db: str, source_table: str, username: str = Depends(get_current_user)):
    """Resume a failed table from its failed batch (incremental/morph only)."""
    _require_manage(plan_id, username)
    try:
        plan = plan_service.resume_table_from_failure(plan_id, source_db, source_table)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not plan:
        raise HTTPException(status_code=404, detail="Table not found or not in failed state")
    return plan


@router.put("/{plan_id}/tables/{source_db}/{source_table}/analysis")
def update_table_analysis(plan_id: str, source_db: str, source_table: str, analysis: dict, username: str = Depends(get_current_user)):
    """Update source analysis fields for a table (format, location, partitioning, etc.)."""
    _require_manage(plan_id, username)
    plan_service.store_table_analysis(plan_id, source_db, source_table, analysis)
    return {"detail": "Analysis updated"}


@router.post("/{plan_id}/sync")
def sync_plan(plan_id: str, username: str = Depends(get_current_user)):
    """Sync pipeline statuses from the engine for this plan."""
    _require_view(plan_id, username)
    return pipeline_sync.sync_plan_pipelines(plan_id)


@router.get("/{plan_id}/tables/{source_db}/{source_table}/spec")
def get_table_spec(plan_id: str, source_db: str, source_table: str, username: str = Depends(get_current_user)):
    """Get the built spec for a table as the engine would receive it."""
    _require_view(plan_id, username)
    from app.database import get_conn, _fetchone
    conn = get_conn()
    p = _fetchone(conn, "SELECT * FROM migration_plans WHERE id=?", (plan_id,))
    if not p:
        conn.close()
        raise HTTPException(status_code=404, detail="Plan not found")
    t = _fetchone(conn,
        "SELECT * FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?",
        (plan_id, source_db, source_table))
    conn.close()
    if not t:
        raise HTTPException(status_code=404, detail="Table not found in plan")
    from app.services.plan_service import _build_spec
    return _build_spec(t, p)
