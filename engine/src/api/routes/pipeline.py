# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pipeline API routes."""

import logging

from fastapi import APIRouter, HTTPException

from ...db import PipelineRepo
from ...core.pipeline_locks import with_pipeline_lock
from ...core.engine import MigrationEngine
from ...utils.sql_builder import athena_drop_table, athena_alter_rename
from ...utils.polling import poll_until_complete

log = logging.getLogger(__name__)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])
repo = PipelineRepo()
engine = MigrationEngine()


@router.get("")
def list_pipelines(status: str = None):
    pipelines = repo.list_all(status=status)
    log.info("Listed %d pipeline(s)%s",
             len(pipelines),
             f" (filter: status={status})" if status else "")
    for p in pipelines:
        log.debug("  ├─ %s | %s.%s | status=%s | type=%s | created=%s",
                  p["id"][:8], p["source_database"], p["source_table"],
                  p["status"], p["migration_type"], p["created_at"])
    return pipelines


@router.get("/{pipeline_id}")
def get_pipeline(pipeline_id: str):
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    log.info("Pipeline %s detail: %s.%s → %s.%s | status=%s | sync=%s | "
             "type=%s | engine=%s | partitions=%s/%s | bytes=%s/%s",
             pipeline_id[:8],
             p["source_database"], p["source_table"],
             p.get("target_database", ""), p.get("target_table", ""),
             p["status"], p.get("sync_status", ""),
             p["migration_type"], p.get("execution_engine", ""),
             p.get("partitions_completed", 0), p.get("partitions_total", 0),
             p.get("bytes_processed", 0), p.get("bytes_total", 0))
    return p


@router.get("/{pipeline_id}/batches")
def get_pipeline_batches(pipeline_id: str):
    """Get all batches for a pipeline with summary."""
    from ...db import BatchRepo
    batches_repo = BatchRepo()

    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")

    batches = batches_repo.list_by_pipeline(pipeline_id)
    summary = batches_repo.get_summary(pipeline_id)

    return {
        "pipeline_id": pipeline_id,
        "summary": summary,
        "batches": batches,
    }


@router.post("/submit")
def submit_spec(spec: dict):
    """Submit a spec to create a pipeline."""
    try:
        from ...core.spec import MigrationSpec
        parsed = MigrationSpec.from_dict(spec)
        spec_dict = parsed.to_dict()
        temp = {
            "source_location": spec_dict["source"]["location"],
            "source_format": spec_dict["source"]["format"],
            "source_partitioned": spec_dict["source"].get("partitioned", False),
            "target_type": spec_dict["target"]["type"],
            # Managed Migration validate() needs the target bucket ARN and the
            # migration role; harmless extra keys for the other strategies.
            "target_bucket_arn": spec_dict["target"].get("table_bucket_arn", ""),
            "iam_role_arn": spec_dict["strategy"].get("iam_role_arn", ""),
            "migration_type": spec_dict["strategy"]["type"],
        }
        log.info("Validating spec: source_format=%s, migration_type=%s, target_type=%s",
                 temp["source_format"], temp["migration_type"], temp["target_type"])
        ok, reason = engine.validate(temp)
        if not ok:
            log.warning("Spec validation failed: %s", reason)
            raise HTTPException(400, f"Validation failed: {reason}")
        pipeline = repo.create_from_spec(spec_dict)
        log.info("Pipeline %s created: %s.%s → %s.%s | type=%s | engine=%s | "
                 "format=%s | partitioned=%s",
                 pipeline["id"][:8],
                 pipeline["source_database"], pipeline["source_table"],
                 pipeline.get("target_database", ""),
                 pipeline.get("target_table", ""),
                 pipeline["migration_type"],
                 pipeline.get("execution_engine", ""),
                 pipeline.get("source_format", ""),
                 pipeline.get("source_partitioned", False))
        return pipeline
    except ValueError as e:
        log.warning("Pipeline creation conflict: %s", e)
        raise HTTPException(409, str(e))
    except KeyError as e:
        log.warning("Pipeline spec missing field: %s", e)
        raise HTTPException(400, f"Missing required field: {e}")


@router.post("/{pipeline_id}/start")
@with_pipeline_lock
def start_pipeline(pipeline_id: str):
    """Start a pipeline — submits the job, or queues it if at the concurrency cap."""
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] not in ("pending", "paused"):
        raise HTTPException(
            400, f"Cannot start pipeline in '{p['status']}' status")

    # Concurrency governor (Phase 3.1 / T16): if we're at the running ceiling,
    # park the pipeline as 'queued' instead of submitting. The reconcile loop
    # promotes queued pipelines to running as slots free up. This protects EMR
    # Serverless / Athena concurrency quotas and bounds compute cost.
    from ...core.governor import has_capacity, QUEUED_STATUS
    if not has_capacity(repo):
        log.info("Pipeline %s: at concurrency cap, queued", pipeline_id[:8])
        return repo.update_status(pipeline_id, QUEUED_STATUS)

    try:
        log.info("Starting pipeline %s: %s.%s → %s.%s | type=%s | engine=%s",
                 pipeline_id[:8],
                 p["source_database"], p["source_table"],
                 p.get("target_database", ""), p.get("target_table", ""),
                 p["migration_type"], p.get("execution_engine", ""))
        updated = engine.submit(p)
        log.info("Pipeline %s submitted successfully, status=%s, emr_job=%s",
                 pipeline_id[:8], updated.get("status"),
                 updated.get("emr_job_run_id", "n/a"))
        return updated
    except NotImplementedError as e:
        raise HTTPException(501, str(e))
    except Exception as e:
        log.exception("Failed to start pipeline %s", pipeline_id)
        raise HTTPException(500, f"Failed to start pipeline: {e}")


@router.post("/{pipeline_id}/pause")
@with_pipeline_lock
def pause_pipeline(pipeline_id: str):
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] != "running":
        raise HTTPException(400, "Can only pause running pipelines")
    log.info("Pausing pipeline %s: %s.%s | was running since %s",
             pipeline_id[:8], p["source_database"], p["source_table"],
             p.get("started_at", "unknown"))
    return repo.update_status(pipeline_id, "paused")


@router.post("/{pipeline_id}/resume")
@with_pipeline_lock
def resume_pipeline(pipeline_id: str):
    """Resume a paused/pending pipeline — resets failed batches and restarts reconcile."""
    from ...core.state_machine import PipelineStateMachine

    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] not in ("paused", "pending"):
        raise HTTPException(400, f"Can only resume paused/pending pipelines, current status: {p['status']}")

    log.info("Resuming pipeline %s: %s.%s → %s.%s | status=%s",
             pipeline_id[:8],
             p["source_database"], p["source_table"],
             p.get("target_database", ""), p.get("target_table", ""),
             p["status"])

    # Managed Migration is a polling adapter over the AWS migration API — it has
    # no internal batches/state machine. Delegate to the strategy so resume
    # re-derives the true API state instead of inventing initial_load.
    if p.get("migration_type") == "s3tables_managed":
        strategy = engine.get_strategy(p["migration_type"], p.get("source_format", ""))
        result = strategy.resume(p)
        log.info("Pipeline %s (managed) resumed; state re-projected from API", pipeline_id[:8])
        return result

    sm = PipelineStateMachine()
    result = sm.resume(pipeline_id)
    log.info("Pipeline %s resumed, status=running, sync_status=initial_load", pipeline_id[:8])
    return result


@router.post("/{pipeline_id}/sync-now")
@with_pipeline_lock
def sync_now(pipeline_id: str):
    """Trigger an immediate sync for an incremental/morph pipeline.

    Bypasses the sync interval — useful when out_of_sync and you don't want to wait.
    """
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] != "running":
        raise HTTPException(400, f"Can only sync running pipelines (current: {p['status']})")
    sync_status = p.get("sync_status", "")

    # Managed Migration syncs via the S3 Tables sync_table_migration API, not
    # the incremental interval mechanism. Its post-load states are
    # initial_load_complete / sync_complete (never out_of_sync), and its
    # reconcile() only polls — it does not trigger execute_sync. So invoke the
    # strategy's execute_sync directly here.
    if p.get("migration_type") == "s3tables_managed":
        if sync_status not in ("initial_load_complete", "sync_complete", "failed"):
            raise HTTPException(
                400,
                "Managed Migration can only sync after the initial load or a "
                f"prior sync has completed (current: '{sync_status}')")
        try:
            strategy = engine.get_strategy(p["migration_type"],
                                           p.get("source_format", ""))
            strategy.execute_sync(p)
            log.info("Managed migration %s: manual sync triggered", pipeline_id[:8])
            return {"detail": "Sync started", "pipeline_id": pipeline_id}
        except Exception as e:
            log.exception("Managed migration sync failed for %s", pipeline_id)
            raise HTTPException(500, f"Sync failed: {e}")

    if sync_status not in ("out_of_sync", "in_sync", "failed"):
        raise HTTPException(400, f"Cannot sync in '{sync_status}' state")

    # Set sync_status to out_of_sync and clear updated_at to bypass interval check
    repo.update_sync_status(pipeline_id, "out_of_sync")
    # Force updated_at far in the past so interval check passes immediately
    repo.update_status(pipeline_id, "running")  # re-trigger sets updated_at
    # Now manually override updated_at to bypass interval
    from ...db import PipelineRepo
    _repo = PipelineRepo()
    # Use the repo's native update mechanism to set updated_at in the past
    import boto3, os
    _db_backend = os.getenv("ENGINE_DB_BACKEND", "sql")
    if _db_backend == "dynamodb":
        _region = os.getenv("ENGINE_DYNAMODB_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        _table_name = os.getenv("ENGINE_PIPELINE_TABLE", "ima-pipelines")
        dynamo = boto3.resource("dynamodb", region_name=_region)
        table = dynamo.Table(_table_name)
        table.update_item(
            Key={"id": pipeline_id},
            UpdateExpression="SET updated_at = :t, sync_status = :s",
            ExpressionAttributeValues={":t": "2000-01-01T00:00:00", ":s": "out_of_sync"})
    else:
        from ...db.connection import get_conn, run_sql, commit
        conn = get_conn()
        run_sql(conn, "UPDATE migration_pipeline SET updated_at='2000-01-01T00:00:00', sync_status='out_of_sync' WHERE id=?",
                (pipeline_id,))
        commit(conn)
        conn.close()

    log.info("Pipeline %s: manual sync triggered", pipeline_id[:8])
    return {"detail": "Sync triggered — will execute on next reconcile cycle",
            "pipeline_id": pipeline_id}


@router.post("/{pipeline_id}/cancel")
@with_pipeline_lock
def cancel_pipeline(pipeline_id: str):
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] in ("completed", "cancelled"):
        raise HTTPException(
            400, f"Cannot cancel '{p['status']}' pipeline")
    try:
        log.info("Cancelling pipeline %s: %s.%s | current_status=%s | "
                 "progress: partitions=%s/%s, bytes=%s/%s",
                 pipeline_id[:8],
                 p["source_database"], p["source_table"],
                 p["status"],
                 p.get("partitions_completed", 0),
                 p.get("partitions_total", 0),
                 p.get("bytes_processed", 0),
                 p.get("bytes_total", 0))
        result = engine.cancel(p)
        log.info("Pipeline %s cancelled", pipeline_id[:8])
        return result
    except NotImplementedError as e:
        raise HTTPException(501, str(e))
    except Exception as e:
        log.exception("Failed to cancel pipeline %s", pipeline_id)
        raise HTTPException(500, f"Failed to cancel pipeline: {e}")


@router.delete("/{pipeline_id}")
@with_pipeline_lock
def delete_pipeline(pipeline_id: str):
    """Delete a pipeline record. Only allowed for failed or cancelled pipelines."""
    from ...tracking import TrackingManager

    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] not in ("failed", "cancelled", "pending"):
        raise HTTPException(
            400, f"Cannot delete pipeline in '{p['status']}' status. "
                 "Only failed, cancelled, or pending pipelines can be deleted.")

    # Destroy the S3 event tracker if one exists
    source_location = p.get("source_location", "")
    if source_location and p.get("migration_type") in ("incremental", "morph", "add_files"):
        try:
            tracker = TrackingManager()
            tracker.destroy_tracker(pipeline_id, source_location)
        except Exception as e:
            log.warning("Failed to destroy tracker for pipeline %s: %s", pipeline_id[:8], e)

    log.info("Deleting pipeline %s: %s.%s | status=%s",
             pipeline_id[:8], p["source_database"], p["source_table"],
             p["status"])
    repo.delete(pipeline_id)
    return {"detail": "Pipeline deleted", "id": pipeline_id}


@router.post("/{pipeline_id}/resume-from-failure")
@with_pipeline_lock
def resume_from_failure(pipeline_id: str):
    """Resume a failed pipeline from the failed batch.

    Resets failed batches to pending and sets pipeline back to running.
    If no failed batches exist (e.g., sync failure), just resets pipeline to running.
    """
    from ...db import BatchRepo
    batches = BatchRepo()

    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] != "failed" and not (
        p["status"] == "running" and p.get("sync_status") in ("failed", "sync_failed")
    ):
        raise HTTPException(
            400, f"Cannot resume pipeline in '{p['status']}' status (sync_status='{p.get('sync_status', '')}')."
                 " Only failed pipelines can be resumed.")

    # Managed Migration has no batches to reset, and a failed AWS migration
    # cannot be resumed in place — the strategy starts a fresh migration.
    if p.get("migration_type") == "s3tables_managed":
        strategy = engine.get_strategy(p["migration_type"], p.get("source_format", ""))
        result = strategy.resume_from_failure(p)
        log.info("Pipeline %s (managed) resumed from failure: new migration started",
                 pipeline_id[:8])
        return result

    # Reset failed batches to pending (if any)
    reset_count = batches.reset_failed_to_pending(pipeline_id)

    # Clear error and set pipeline back to running
    updated = repo.update_status(pipeline_id, "running", error_message="")
    repo.update_sync_status(pipeline_id, "pending")
    log.info("Resuming pipeline %s from failure: %d batch(es) reset, pipeline set to running",
             pipeline_id[:8], reset_count)
    return updated


@router.post("/{pipeline_id}/complete")
@with_pipeline_lock
def complete_pipeline(pipeline_id: str):
    """Mark a pipeline as completed (called by backend on finalize).
    Also destroys the S3 event tracker if one exists.
    """
    from ...tracking import TrackingManager

    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] not in ("running", "pending"):
        raise HTTPException(
            400, f"Cannot complete pipeline in '{p['status']}' status")

    # Managed Migration cutover goes through the API (complete_table_migration),
    # not a plain status flip. The strategy releases the target table to
    # read-write and marks the pipeline completed.
    if p.get("migration_type") == "s3tables_managed":
        try:
            result = engine.complete(p)
            log.info("Managed migration cutover complete for pipeline %s", pipeline_id[:8])
            return result
        except Exception as e:
            log.exception("Managed migration cutover failed for %s", pipeline_id)
            raise HTTPException(500, f"Cutover failed: {e}")

    # Destroy the S3 event tracker
    source_location = p.get("source_location", "")
    if source_location and p.get("migration_type") in ("incremental", "morph", "add_files"):
        try:
            tracker = TrackingManager()
            tracker.destroy_tracker(pipeline_id, source_location)
        except Exception as e:
            log.warning("Failed to destroy tracker for pipeline %s: %s", pipeline_id[:8], e)

    log.info("Marking pipeline %s as completed: %s.%s",
             pipeline_id[:8], p["source_database"], p["source_table"])
    return repo.update_status(pipeline_id, "completed")


@router.post("/{pipeline_id}/approve-migration")
@with_pipeline_lock
def approve_migration(pipeline_id: str):
    """Approve a Managed Migration plan-review pause (resume → queued).

    Only valid for s3tables_managed pipelines currently at
    sync_status='awaiting_review'.
    """
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p.get("migration_type") != "s3tables_managed":
        raise HTTPException(400, "Approve is only valid for Managed Migration pipelines")
    if p.get("sync_status") != "awaiting_review":
        raise HTTPException(
            400, f"Migration is not awaiting review (sync_status='{p.get('sync_status')}')")
    try:
        result = engine.approve(p)
        log.info("Managed migration approved for pipeline %s", pipeline_id[:8])
        return result
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.exception("Managed migration approve failed for %s", pipeline_id)
        raise HTTPException(500, f"Approve failed: {e}")


@router.put("/{pipeline_id}/migration-request")
@with_pipeline_lock
def update_migration_request(pipeline_id: str, body: dict):
    """Store an edited Managed Migration request payload on the pipeline.

    Only valid for s3tables_managed pipelines. The payload is persisted under
    spec_json._metadata.start_request. This records/overrides the stored copy
    for inspection — it does NOT re-invoke the AWS StartTableMigration API
    (the migration has already been submitted). The request body is the JSON
    object to store, either bare or wrapped as {"payload": {...}}.
    """
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p.get("migration_type") != "s3tables_managed":
        raise HTTPException(
            400, "Migration request is only available for Managed Migration pipelines")
    payload = body.get("payload") if isinstance(body, dict) and "payload" in body else body
    if not isinstance(payload, dict):
        raise HTTPException(400, "Request payload must be a JSON object")
    try:
        import json as _json
        repo.update_metadata(pipeline_id, "start_request", _json.dumps(payload))
        log.info("Updated stored migration request for pipeline %s", pipeline_id[:8])
        return repo.get(pipeline_id)
    except Exception as e:
        log.exception("Failed to update migration request for %s", pipeline_id)
        raise HTTPException(500, f"Failed to update migration request: {e}")


@router.get("/{pipeline_id}/costs")
def get_pipeline_costs(pipeline_id: str):
    """Get compute cost metrics for a pipeline."""
    from ...db import CostRepo
    costs = CostRepo()
    record = costs.get(pipeline_id)
    if not record:
        return {"pipeline_id": pipeline_id, "athena_bytes_scanned": 0,
                "athena_query_count": 0, "athena_cost_usd": 0,
                "emr_vcpu_hours": 0, "emr_cost_usd": 0, "total_cost_usd": 0}
    return record


@router.post("/costs/plan")
def get_plan_costs(pipeline_ids: list[str]):
    """Aggregate costs across multiple pipelines (plan level)."""
    from ...db import CostRepo
    costs = CostRepo()
    return costs.get_plan_costs(pipeline_ids)


@router.post("/{pipeline_id}/swap-tables")
@with_pipeline_lock
def swap_tables(pipeline_id: str):
    """Swap tables: drop the original and rename the Iceberg table to the original name.

    This is the cutover step — the new Iceberg table takes the place of the old table.
    Waits for DROP to complete before issuing the RENAME.
    """
    from ...engines.athena import AthenaClient

    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")

    source_db = p["source_database"]
    source_table = p["source_table"]
    target_db = p.get("target_database", source_db)
    target_table = p.get("target_table", "")
    target_type = p.get("target_type", "")

    # For S3 Tables with same name: just drop the old Glue catalog table (no rename needed)
    if target_type == "s3_tables" and target_table == source_table:
        athena = AthenaClient.from_pipeline(p)
        drop_sql = athena_drop_table(source_db, source_table)
        try:
            drop_qid = athena.submit_query(
                drop_sql, database=source_db,
                description=f"IMA-swap-drop-{source_db}.{source_table}")
            log.info("S3 Tables swap: dropped old Glue table %s.%s (qid=%s)",
                     source_db, source_table, drop_qid)
        except Exception as e:
            log.error("Failed to drop original table %s.%s: %s", source_db, source_table, e)
            raise HTTPException(500, f"Failed to drop original table: {e}")
        return {"detail": "Table swap completed (S3 Tables — drop only)", "id": pipeline_id,
                "drop_query_id": drop_qid}

    if not target_table or target_table == source_table:
        return {"detail": "No swap needed — source and target are the same", "id": pipeline_id}

    athena = AthenaClient.from_pipeline(p)

    # Step 1: Drop the original table
    drop_sql = athena_drop_table(source_db, source_table)
    try:
        drop_qid = athena.submit_query(
            drop_sql, database=source_db,
            description=f"IMA-swap-drop-{source_db}.{source_table}")
    except Exception as e:
        log.error("Failed to drop original table %s.%s: %s", source_db, source_table, e)
        raise HTTPException(500, f"Failed to drop original table: {e}")

    # Wait for DROP to complete before renaming
    def _check_drop():
        try:
            return athena.get_query_status(drop_qid)
        except Exception:
            return {"state": "PENDING"}

    status = poll_until_complete(
        check=_check_drop,
        is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"),
        interval=1.0, max_attempts=60)
    if status["state"] in ("FAILED", "CANCELLED"):
        reason = status.get("state_reason", "DROP query failed")
        log.error("DROP TABLE failed for %s.%s: %s", source_db, source_table, reason)
        raise HTTPException(500, f"DROP TABLE failed: {reason}")
    elif status["state"] != "SUCCEEDED":
        raise HTTPException(500, "DROP TABLE timed out after 60 seconds")

    # Step 2: Rename the new Iceberg table to the original name
    rename_sql = athena_alter_rename(target_db, target_table, source_table)
    try:
        rename_qid = athena.submit_query(
            rename_sql, database=target_db,
            description=f"IMA-swap-rename-{target_db}.{target_table}-to-{source_table}")
    except Exception as e:
        log.error("Failed to rename table %s.%s to %s: %s",
                  target_db, target_table, source_table, e)
        raise HTTPException(500, f"Failed to rename table: {e}")

    log.info("Table swap completed for pipeline %s: dropped %s.%s, "
             "renamed %s.%s → %s (drop_qid=%s, rename_qid=%s)",
             pipeline_id[:8], source_db, source_table,
             target_db, target_table, source_table,
             drop_qid, rename_qid)

    return {
        "detail": "Table swap completed",
        "id": pipeline_id,
        "drop_query_id": drop_qid,
        "rename_query_id": rename_qid,
    }

@router.post("/{pipeline_id}/retry")
@with_pipeline_lock
def retry_pipeline(pipeline_id: str):
    """Retry a failed pipeline from scratch while preserving batch history.

    Resets pipeline status to pending, clears error, and re-submits.
    Previous batch records (including failed ones) are preserved for history.
    """
    p = repo.get(pipeline_id)
    if not p:
        raise HTTPException(404, "Pipeline not found")
    if p["status"] != "failed":
        raise HTTPException(
            400, f"Cannot retry pipeline in '{p['status']}' status. "
                 "Only failed pipelines can be retried.")

    # Reset pipeline status to pending (preserves batch history)
    repo.update_status(pipeline_id, "pending", error_message="")
    repo.update_sync_status(pipeline_id, "pending")

    # Start the pipeline (submits fresh work)
    p = repo.get(pipeline_id)
    try:
        updated = engine.submit(p)
        log.info("Pipeline %s retried: status=%s", pipeline_id[:8],
                 updated.get("status"))
        return updated
    except Exception as e:
        log.exception("Pipeline %s retry failed", pipeline_id)
        raise HTTPException(500, f"Retry failed: {e}")
