# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Sync pipeline status from the engine."""

import json
import logging
from app import engine_client
from app.database import get_conn, _fetchall, _run_sql, _commit, _fetchone
from app.services import notification_service

log = logging.getLogger(__name__)


def _load_plan_meta(conn, plan_id: str) -> dict:
    """Fetch minimal plan metadata (id, name, owners) for notifications."""
    row = _fetchone(conn, "SELECT id, name, owners FROM migration_plans WHERE id=?", (plan_id,))
    if not row:
        return {}
    try:
        owners = json.loads(row.get("owners") or "[]")
    except (ValueError, TypeError):
        owners = []
    return {"id": row["id"], "name": row.get("name", ""), "owners": owners}


def _get_engine_url():
    from app.services.settings_service import get_setting
    return get_setting("ENGINE_URL") or "http://localhost:8001"


def _map_status(engine_status: str, migration_type: str, current_status: str,
                sync_status: str = "") -> str:
    """Map the engine pipeline status to plan_tables status based on strategy."""
    if migration_type == "s3tables_managed":
        # Managed Migration is a polling adapter over the AWS S3 Tables
        # migration API. Because we no longer track source drift, we surface
        # WHAT phase completed rather than a generic in_sync. Its ONLY valid app
        # states are: initial_load, syncing, awaiting_review,
        # initial_load_complete, sync_complete, sync_failed, paused, cancelled,
        # failed, completed. out_of_sync is NOT valid and must never be produced.
        if engine_status == "running":
            if sync_status == "awaiting_review":
                return "awaiting_review"
            elif sync_status == "initial_load_complete":
                # Initial load finished — migrated, awaiting finalize/cutover.
                return "initial_load_complete"
            elif sync_status == "sync_complete":
                # A sync run finished — migrated, awaiting finalize/cutover.
                return "sync_complete"
            elif sync_status == "syncing":
                # A sync_table_migration run is in flight.
                return "syncing"
            elif sync_status == "sync_failed":
                # The LAST SYNC failed, but the table is already migrated (the
                # initial load produced it). Still finalizable; the sync retries
                # on cadence. Distinct from a true (initial-load) failure.
                return "sync_failed"
            elif sync_status == "failed":
                return "failed"
            # initial_load, pending, queued, empty, or any other transient
            # state → still doing the initial load.
            return "initial_load"
        # Non-running engine states. Note pending/queued map to initial_load
        # (the migration is being started), NOT out_of_sync.
        return {
            "pending": "initial_load",
            "queued": "initial_load",
            "completed": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
            "paused": "paused",
        }.get(engine_status, current_status)

    if migration_type == "rewrite":
        # rewrite: pending->staged, running->executing, completed/failed/cancelled as-is
        return {
            "pending": "staged",
            "running": "executing",
            "completed": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
            "paused": "paused",
        }.get(engine_status, current_status)
    else:
        # incremental/morph: status depends on sync_status
        if engine_status == "running":
            if sync_status == "in_sync":
                return "in_sync"
            elif sync_status == "out_of_sync":
                return "out_of_sync"
            elif sync_status == "syncing":
                return "executing"
            elif sync_status == "awaiting_review":
                # Managed Migration paused for compatibility-report approval.
                return "awaiting_review"
            elif sync_status == "failed":
                return "failed"
            else:
                # pending sync_status during initial load
                return "initial_load"
        return {
            "pending": "out_of_sync",
            # A completed incremental/morph/add_files pipeline has been finalized
            # (cutover done + tracker destroyed) — it is terminally completed,
            # not merely in_sync. Mapping it back to in_sync would revert the
            # table status set during finalize.
            "completed": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
            "paused": "paused",
        }.get(engine_status, current_status)


def sync_plan_pipelines(plan_id: str) -> dict:
    """Poll the engine for each table's pipeline status and update plan_tables."""
    conn = get_conn()
    tables = _fetchall(conn,
        "SELECT source_database, source_table, target_database, target_table, "
        "pipeline_id, status, migration_type "
        "FROM plan_tables WHERE plan_id=? AND pipeline_id IS NOT NULL "
        "AND pipeline_id != ''", (plan_id,))

    updated = 0
    for t in tables:
        pid = t["pipeline_id"]
        old_status = t["status"]
        migration_type = t.get("migration_type", "rewrite")
        # Skip truly terminal states. 'failed' is NOT terminal — a pipeline can
        # recover after an operator fixes the issue (e.g. missing tracker role),
        # and the engine will move it back to running/in_sync. If we skip
        # failed, the app DB stays frozen at 'failed' while the engine has
        # recovered, and finalize silently no-ops because it reads the app DB.
        if old_status in ("completed", "cancelled"):
            continue
        try:
            resp = engine_client.get(
                f"{_get_engine_url()}/api/pipeline/{pid}",
                timeout=10.0)
            if resp.status_code == 404:
                # Pipeline was deleted from the engine — mark as failed
                _run_sql(conn,
                    "UPDATE plan_tables SET status='failed', error_message=? "
                    "WHERE plan_id=? AND source_database=? AND source_table=?",
                    ("Pipeline not found in engine (deleted)", plan_id,
                     t["source_database"], t["source_table"]))
                updated += 1
                log.warning("Pipeline %s (%s.%s): not found in the engine, marking failed",
                            pid, t["source_database"], t["source_table"])
                continue
            if resp.status_code != 200:
                continue
            pipeline = resp.json()
            engine_status = pipeline.get("status", "")
            sync_status = pipeline.get("sync_status", "")
            new_status = _map_status(engine_status, migration_type, old_status, sync_status)
            if new_status != old_status:
                error_msg = pipeline.get("error_message", "") if new_status == "failed" else None
                if error_msg:
                    _run_sql(conn,
                        "UPDATE plan_tables SET status=?, error_message=? "
                        "WHERE plan_id=? AND source_database=? AND source_table=?",
                        (new_status, error_msg, plan_id,
                         t["source_database"], t["source_table"]))
                else:
                    _run_sql(conn,
                        "UPDATE plan_tables SET status=?, error_message=NULL "
                        "WHERE plan_id=? AND source_database=? AND source_table=?",
                        (new_status, plan_id,
                         t["source_database"], t["source_table"]))

                # For rewrite tables that just completed via Athena engine,
                # trigger table swap if enabled for this plan.
                if new_status == "completed" and migration_type == "rewrite":
                    # Check if swap is enabled for this plan
                    plan_row = _fetchone(conn, "SELECT swap_tables FROM migration_plans WHERE id=?", (plan_id,))
                    swap_enabled = bool(plan_row.get("swap_tables", 0)) if plan_row else False

                    if not swap_enabled:
                        log.info("Table swap disabled for plan %s — skipping %s.%s",
                                 plan_id[:8], t["source_database"], t["source_table"])
                    else:
                        # Check if this was an Athena pipeline (EMR does swap in-job)
                        try:
                            pipeline_resp = engine_client.get(
                                f"{_get_engine_url()}/api/pipeline/{pid}", timeout=10.0)
                            if pipeline_resp.status_code == 200:
                                pipeline_data = pipeline_resp.json()
                                engine = pipeline_data.get("execution_engine", "athena")
                                if engine == "athena":
                                    swap_resp = engine_client.post(
                                        f"{_get_engine_url()}/api/pipeline/{pid}/swap-tables",
                                        timeout=60.0)
                                    if swap_resp.status_code in (200, 201):
                                        log.info("Table swap completed for %s.%s (pipeline %s)",
                                                 t["source_database"], t["source_table"], pid[:8])
                                    else:
                                        err = f"Table swap failed: {swap_resp.text[:200]}"
                                        log.error("Table swap failed for %s.%s: %s",
                                                  t["source_database"], t["source_table"],
                                                  swap_resp.text[:200])
                                        _run_sql(conn,
                                            "UPDATE plan_tables SET error_message=? "
                                            "WHERE plan_id=? AND source_database=? AND source_table=?",
                                            (err, plan_id, t["source_database"], t["source_table"]))
                                else:
                                    log.info("Table swap skipped for %s.%s — handled by EMR job",
                                             t["source_database"], t["source_table"])
                        except Exception as e:
                            err = f"Table swap error: {str(e)[:200]}"
                            log.error("Table swap error for %s.%s: %s",
                                      t["source_database"], t["source_table"], e)
                            _run_sql(conn,
                                "UPDATE plan_tables SET error_message=? "
                                "WHERE plan_id=? AND source_database=? AND source_table=?",
                                (err, plan_id, t["source_database"], t["source_table"]))

                updated += 1
                log.info("Pipeline %s (%s.%s): %s -> %s",
                         pid, t["source_database"], t["source_table"],
                         old_status, new_status)

                # Emit a notification event for this status transition (best-effort)
                if notification_service.event_type_for_status(new_status):
                    plan_meta = _load_plan_meta(conn, plan_id)
                    notification_service.emit_pipeline_event(
                        plan=plan_meta,
                        table={
                            "source_database": t["source_database"],
                            "source_table": t["source_table"],
                            "target_database": t.get("target_database", ""),
                            "target_table": t.get("target_table", ""),
                            "migration_type": migration_type,
                        },
                        old_status=old_status,
                        new_status=new_status,
                        error_message=(error_msg or ""))

            # Update progress info for active pipelines
            p_completed = pipeline.get("partitions_completed", 0)
            p_total = pipeline.get("partitions_total", 0)
            log.info("Pipeline %s progress: p_completed=%s, p_total=%s, new_status=%s",
                     pid[:8], p_completed, p_total, new_status)
            if new_status == "initial_load":
                if p_total > 0 and p_completed < p_total:
                    progress_msg = f"{p_completed}/{p_total}"
                else:
                    progress_msg = ""
                _run_sql(conn,
                    "UPDATE plan_tables SET error_message=? "
                    "WHERE plan_id=? AND source_database=? AND source_table=? "
                    "AND status NOT IN ('failed','cancelled','in_sync','completed',"
                    "'initial_load_complete','sync_complete','sync_failed')",
                    (progress_msg, plan_id, t["source_database"], t["source_table"]))
            elif new_status in ("in_sync", "completed",
                                "initial_load_complete", "sync_complete"):
                # Clear progress message when done
                _run_sql(conn,
                    "UPDATE plan_tables SET error_message=NULL "
                    "WHERE plan_id=? AND source_database=? AND source_table=? AND status NOT IN ('failed')",
                    (plan_id, t["source_database"], t["source_table"]))
        except Exception as e:
            log.warning("Failed to poll pipeline %s: %s", pid, e)

    # Update plan-level status based on table statuses (always check,
    # not just when individual tables were updated — handles the case where
    # all tables reached a terminal state via a different path, e.g. finalize
    # + remove-failed-table, and the plan needs to transition to completed).
    _update_plan_status(conn, plan_id)
    _commit(conn)

    conn.close()
    return {"plan_id": plan_id, "tables_checked": len(tables),
            "tables_updated": updated}


def _update_plan_status(conn, plan_id: str):
    """Derive plan status from aggregate table statuses.

    Rules:
    - Plan stays in_progress as long as there's work to do or failed tables to retry
    - Plan only moves to completed when ALL tables are in a success state (completed/in_sync)
    - Plan NEVER moves to failed — individual tables fail, plan stays in_progress
    """
    rows = _fetchall(conn,
        "SELECT status, COUNT(*) as cnt FROM plan_tables "
        "WHERE plan_id=? GROUP BY status", (plan_id,))
    counts = {r["status"]: r["cnt"] for r in rows}
    total = sum(counts.values())

    success_count = counts.get("completed", 0)

    if success_count == total:
        new = "completed"
    elif counts.get("cancelled", 0) == total:
        new = "cancelled"
    elif counts.get("paused", 0) > 0 and counts.get("executing", 0) == 0 and counts.get("out_of_sync", 0) == 0 and counts.get("initial_load", 0) == 0 and counts.get("syncing", 0) == 0:
        new = "paused"
    else:
        new = "in_progress"

    # Detect the transition into 'completed' so we only emit once
    prev = _fetchone(conn, "SELECT status FROM migration_plans WHERE id=?", (plan_id,))
    prev_status = prev["status"] if prev else None

    _run_sql(conn, "UPDATE migration_plans SET status=? WHERE id=?",
             (new, plan_id))

    if new == "completed" and prev_status != "completed":
        plan_meta = _load_plan_meta(conn, plan_id)
        notification_service.emit_plan_completed_event(plan_meta)


def sync_all_active_plans() -> list[dict]:
    """Sync all plans that are in_progress, paused, or failed.

    'failed' is included deliberately: a plan-level 'failed' is NOT terminal.
    Individual tables can recover (e.g. after an operator fixes a missing role,
    or the failed table is removed), and _update_plan_status re-derives the plan
    back to in_progress/completed. If failed plans were excluded here, such a
    plan would stay frozen at 'failed' forever — hiding finalize and other
    actions — even though its remaining tables are healthy.
    """
    conn = get_conn()
    plans = _fetchall(conn,
        "SELECT id FROM migration_plans WHERE status IN ('in_progress', 'paused', 'failed')")
    conn.close()
    results = []
    for p in plans:
        try:
            r = sync_plan_pipelines(p["id"])
            results.append(r)
        except Exception as e:
            log.exception("Sync failed for plan %s", p["id"])
            results.append({"plan_id": p["id"], "error": str(e)})
    return results
