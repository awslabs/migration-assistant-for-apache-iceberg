# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Iceberg Incremental strategy — snapshot-based migration from Iceberg on S3 to S3 Tables.

Uses Iceberg snapshot metadata to track changes instead of S3 event notifications.

Phases:
  0. Preparation: Record source snapshot, create target table, discover partitions
  1. Initial Load: Batched INSERT from source at snapshot_before
  2. First Catch-Up: Sync changes that occurred during initial load
  3. Iterative Convergence: Repeat catch-up until delta is small
  4. Keeping in Sync: Periodic sync using snapshot cursor

Key difference from Hive incremental:
  - No Lambda/S3 notifications needed
  - Change detection via Iceberg snapshot diffs
  - INSERT OVERWRITE for affected partitions (atomic)
  - Source reads at specific snapshot IDs for consistency
"""

import json
import logging

from .base import MigrationStrategy
from .common import (
    detect_date_partitioning, generate_date_range_batches,
    build_date_filter_sql, s3tables_spark_props,
    NamespaceError, ensure_s3tables_namespace,
)
from ..engines.athena import AthenaClient
from ..engines.emr import EmrServerlessClient
from ..iceberg.metadata import IcebergMetadataReader
from ..db import PipelineRepo, BatchRepo
from ..db.snapshot_tracking import SnapshotTrackingRepo
from ..utils.sql_builder import (
    athena_table_ref, athena_ctas, athena_drop_table, athena_insert_into,
    athena_delete, athena_select,
)
from ..utils.polling import poll_until_complete

log = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 5
_MAX_CONVERGENCE_ITERATIONS = 10  # Force to in_sync after this many iterations


class IcebergIncrementalStrategy(MigrationStrategy):
    """Snapshot-based incremental migration from Iceberg on S3 to S3 Tables."""

    def __init__(self):
        self._athena = AthenaClient()
        self._iceberg = IcebergMetadataReader()
        self._repo = PipelineRepo()
        self._batches = BatchRepo()
        self._snapshots = SnapshotTrackingRepo()
        from ..engines.emr import EmrServerlessClient
        self._emr = EmrServerlessClient()

    def _engine(self, pipeline: dict) -> str:
        return pipeline.get("execution_engine", "athena")

    def validate(self, pipeline: dict) -> tuple[bool, str]:
        if not pipeline.get("source_location"):
            return False, "Source table has no S3 location"
        fmt = pipeline.get("source_format", "")
        if fmt != "iceberg":
            return False, (
                "Iceberg incremental strategy requires an Iceberg source table. "
                f"Got format: '{fmt}'")
        if pipeline.get("target_type") != "s3_tables":
            return False, (
                "Iceberg incremental strategy only supports S3 Tables as target")
        return True, "OK"

    # ── Phase 0: Preparation ─────────────────────────────────────

    def submit(self, pipeline: dict) -> dict:
        """Phase 0: Record snapshot, create target, discover partitions."""
        pid = pipeline["id"]
        # Use the pipeline's Athena/EMR config (from Settings) not env defaults.
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        ok, reason = self.validate(pipeline)
        if not ok:
            return self._repo.update_status(pid, "failed",
                                            error_message=reason)

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        src = spec_json.get("source", {})
        tgt = spec_json.get("target", {})
        table_bucket_arn = pipeline.get("target_bucket_arn", "")

        # Step 1: Get current snapshot ID from source via PyIceberg
        snap_info = self._iceberg.get_latest_snapshot(src["database"], src["table_name"])
        if not snap_info:
            return self._repo.update_status(
                pid, "failed", error_message="No snapshots found in source table")
        snapshot_before = snap_info["snapshot_id"]
        committed_at_before = str(snap_info.get("committed_at", ""))
        log.info("Pipeline %s: source snapshot_before = %s (at %s)",
                 pid[:8], snapshot_before, committed_at_before)

        # Store snapshot tracking with timestamp
        self._snapshots.create(pid, snapshot_before, committed_at_before)

        # Route to EMR if engine is not Athena
        engine = self._engine(pipeline)
        if engine != "athena":
            return self._submit_emr_setup(pipeline, spec_json, snapshot_before)

        # Step 2: Create namespace + target table (Athena path)
        # Create namespace
        try:
            ensure_s3tables_namespace(table_bucket_arn, tgt["database"])
        except NamespaceError as e:
            log.error("Pipeline %s: %s", pid[:8], e)
            return self._repo.update_status(
                pid, "failed", error_message=str(e)[:500])
        # Create target table with source schema and partition spec
        # Discover partition spec via PyIceberg (fast, no Athena query needed)
        ip = spec_json.get("iceberg_properties", {})
        partition_spec = ip.get("partition_spec")

        if not partition_spec:
            log.info("Pipeline %s: discovering partition spec via PyIceberg", pid[:8])
            partition_spec = self._iceberg.get_partition_spec(src["database"], src["table_name"])
            if partition_spec:
                log.info("Pipeline %s: discovered partition spec: %s", pid[:8], partition_spec)
            else:
                log.info("Pipeline %s: no partition spec (unpartitioned table)", pid[:8])

        # Build CTAS properties dict
        ctas_properties = {"format": "PARQUET"}
        if partition_spec:
            if isinstance(partition_spec, list) and partition_spec:
                if isinstance(partition_spec[0], dict):
                    part_exprs = []
                    for ps in partition_spec:
                        col = ps.get("source_column", "")
                        transform = ps.get("transform", "identity")
                        if transform == "identity":
                            part_exprs.append(f"'{col}'")
                        else:
                            part_exprs.append(f"'{transform}({col})'")
                    ctas_properties["partitioning"] = f"ARRAY[{', '.join(part_exprs)}]"
                elif isinstance(partition_spec[0], str):
                    part_exprs = [f"'{p}'" for p in partition_spec]
                    ctas_properties["partitioning"] = f"ARRAY[{', '.join(part_exprs)}]"

        create_sql = athena_ctas(
            target_db=tgt["database"], target_table=tgt["table_name"],
            source_db=src["database"], source_table=src["table_name"],
            select_cols="*",
            where="1=0",
            properties=ctas_properties,
            target_type="s3_tables",
            table_bucket_arn=table_bucket_arn,
        )

        log.info("Pipeline %s: CREATE TABLE SQL:\n%s", pid[:8], create_sql)

        # Drop if exists first (handles retries)
        drop_sql = athena_drop_table(
            tgt["database"], tgt["table_name"], "s3_tables", table_bucket_arn)
        try:
            drop_qid = self._athena.submit_query(
                drop_sql, database=src["database"],
                description=f"IMA-iceberg-inc-drop-{pid[:8]}")
            poll_until_complete(
                check=lambda: self._athena.get_query_status(drop_qid),
                is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED"),
                interval=1.0, max_attempts=30)
        except Exception as e:
            # Best-effort cleanup of any stale target table before (re)creating
            # it; a failure here (e.g. table does not exist) is non-fatal.
            log.debug("Pre-create DROP for pipeline %s failed (ignored): %s", pid[:8], e)

        try:
            create_qid = self._athena.submit_query(
                create_sql, database=src["database"],
                description=f"IMA-iceberg-inc-create-table-{pid[:8]}")
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"CREATE TABLE failed: {e}")

        # Create batch 0 (table creation) and batch 1 (discovery)
        self._batches.create(pid, batch_number=0, partitions=["__create_table__"])
        self._batches.mark_running(0, f"athena:{create_qid}", pipeline_id=pid)

        # Determine batching strategy using discovered partition_spec
        # Convert partition_spec to partition_keys format for date detection
        partition_keys = src.get("partition_keys", [])
        if not partition_keys and partition_spec:
            # Build partition_keys from discovered partition_spec
            partition_keys = [{"name": ps["source_column"], "type": "date"}
                             for ps in partition_spec
                             if isinstance(ps, dict)]
            log.info("Pipeline %s: using discovered partition keys: %s",
                     pid[:8], [pk["name"] for pk in partition_keys])

            # Store partition info back on the pipeline for later phases
            self._repo._table.update_item(
                Key={"id": pid},
                UpdateExpression="SET source_partitioned = :sp, source_partition_keys = :pk, partition_spec = :ps",
                ExpressionAttributeValues={
                    ":sp": 1,
                    ":pk": json.dumps(partition_keys),
                    ":ps": json.dumps(partition_spec),
                },
            ) if hasattr(self._repo, '_table') else None

        date_info = detect_date_partitioning(partition_keys)

        if date_info:
            cols_str = ",".join(c["name"] for c in date_info.columns)
            self._batches.create(pid, batch_number=1,
                                 partitions=[f"__discover_date_range__{date_info.mode}__{cols_str}__"])
        else:
            self._batches.create(pid, batch_number=1,
                                 partitions=["__full_insert__"])

        self._snapshots.update_phase(pid, "initial_load")
        self._repo.update_emr_job_run_id(pid, f"athena:{create_qid}")
        return self._repo.update_status(pid, "running")

    # ═══════════════════════════════════════════════════════════════
    # STATE MACHINE HOOKS
    # ═══════════════════════════════════════════════════════════════

    def submit_batch(self, pipeline: dict, batch: dict) -> dict:
        """Submit the next pending batch."""
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        return self._submit_next_batch(pipeline, [batch])

    def on_job_success(self, pipeline: dict, batch=None) -> dict:
        """Handle job completion — called by state machine after batch is marked completed.

        The state machine already marked the batch completed and cleared emr_job_run_id.
        We receive the completed batch object directly.
        """
        job_ref = pipeline.get("emr_job_run_id", "")
        # Use the batch passed by the state machine (it was already marked completed)
        return self._on_job_success_v2(pipeline, job_ref, batch)

    def on_initial_load_complete(self, pipeline: dict) -> dict:
        """All initial load batches done — start catch-up phase."""
        pid = pipeline["id"]
        snap = self._snapshots.get(pid)
        if snap:
            return self._start_catch_up(pipeline, snap)
        # No snapshot tracking — just mark in_sync
        self._repo.update_sync_status(pid, "in_sync")
        return self._repo.get(pid)

    def detect_changes(self, pipeline: dict) -> bool:
        """Check if source Iceberg table has new snapshots."""
        pid = pipeline["id"]
        snap = self._snapshots.get(pid)
        if not snap:
            return False
        cursor = snap.get("snapshot_cursor", "")
        if not cursor:
            return False

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)
        src = (spec_json or {}).get("source", {})

        snap_info = self._iceberg.get_latest_snapshot(
            src.get("database", ""), src.get("table_name", ""))
        if snap_info and snap_info["snapshot_id"] != cursor:
            log.info("Pipeline %s: new snapshot detected (%s != cursor %s)",
                     pid[:8], snap_info["snapshot_id"][:12], cursor[:12])
            return True
        return False

    def execute_sync(self, pipeline: dict, batch_number: int) -> dict:
        """Execute ongoing sync — uses convergence loop."""
        pid = pipeline["id"]
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        snap = self._snapshots.get(pid)
        if not snap:
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)
        # Use the convergence mechanism (handles iteration counting, diff queries)
        return self._reconcile_convergence(pipeline, snap)

    def _on_job_success_v2(self, pipeline: dict, job_ref: str, completed_batch=None) -> dict:
        """Handle successful job — uses the batch passed by the state machine.

        The state machine already marked the batch completed and cleared emr_job_run_id.
        We use the batch object directly instead of re-querying get_running().
        """
        pid = pipeline["id"]

        if completed_batch:
            batch_partitions = completed_batch.get("partitions", "[]")
            if isinstance(batch_partitions, str):
                batch_partitions = json.loads(batch_partitions)

            # Handle discovery batch: create date-range batches from results
            if (batch_partitions and len(batch_partitions) == 1
                    and batch_partitions[0].startswith("__discover_date_range__")):
                marker = batch_partitions[0].replace(
                    "__discover_date_range__", "", 1).strip("_")
                marker_parts = marker.split("__")
                mode = marker_parts[0]
                cols_str = marker_parts[1] if len(marker_parts) > 1 else ""
                col_names = cols_str.split(",")

                # Get the query ID from the batch's job ref
                batch_job_ref = completed_batch.get("emr_job_run_id", "") or job_ref
                qid = batch_job_ref.replace("athena:", "") if batch_job_ref.startswith("athena:") else batch_job_ref
                try:
                    results = self._athena.get_query_results(qid)
                    if results:
                        min_val = results[0].get("min_val", "")
                        max_val = results[0].get("max_val", "")
                        spec_json = pipeline.get("spec_json")
                        if isinstance(spec_json, str):
                            spec_json = json.loads(spec_json)
                        batch_count = (spec_json or {}).get(
                            "strategy", {}).get("batch_size", _DEFAULT_BATCH_SIZE)

                        partition_keys = (spec_json or {}).get(
                            "source", {}).get("partition_keys", [])
                        if not partition_keys:
                            pk_str = pipeline.get("source_partition_keys", "")
                            if pk_str and isinstance(pk_str, str):
                                try:
                                    partition_keys = json.loads(pk_str)
                                except (json.JSONDecodeError, TypeError):
                                    pass
                            elif isinstance(pk_str, list):
                                partition_keys = pk_str
                        columns = [pk for pk in partition_keys
                                   if pk["name"] in col_names]
                        if not columns:
                            columns = [{"name": n, "type": "string"}
                                       for n in col_names]

                        date_batches = generate_date_range_batches(
                            min_val, max_val, col_names[0],
                            batch_count, mode=mode, columns=columns)

                        if date_batches:
                            for i, batch_marker in enumerate(date_batches):
                                self._batches.create(
                                    pid,
                                    batch_number=completed_batch["batch_number"] + 1 + i,
                                    partitions=[batch_marker])
                            log.info("Pipeline %s: created %d date-range batches "
                                     "(%s: %s to %s, mode=%s)",
                                     pid[:8], len(date_batches), cols_str,
                                     min_val, max_val, mode)
                        else:
                            self._batches.create(
                                pid,
                                batch_number=completed_batch["batch_number"] + 1,
                                partitions=["__full_insert__"])
                            log.warning("Pipeline %s: could not generate date ranges, "
                                        "falling back to full insert", pid[:8])
                    else:
                        self._batches.create(
                            pid,
                            batch_number=completed_batch["batch_number"] + 1,
                            partitions=["__full_insert__"])
                        log.warning("Pipeline %s: no discovery results, "
                                    "falling back to full insert", pid[:8])
                except Exception as e:
                    log.error("Pipeline %s: failed to read discovery results: %s",
                              pid[:8], e)
                    self._batches.create(
                        pid,
                        batch_number=completed_batch["batch_number"] + 1,
                        partitions=["__full_insert__"])

            # Batch 0 post-setup (sort order)
            if completed_batch.get("batch_number") == 0:
                self._post_batch0_setup(pipeline)

        # Check for more pending batches
        pending = self._batches.list_by_pipeline(pid, status="pending")
        log.debug("Pipeline %s: pending batches after completion: %d",
                  pid[:8], len(pending))
        if pending:
            try:
                return self._submit_next_batch(pipeline, pending)
            except Exception as e:
                log.exception("Failed to submit next batch for pipeline %s", pid[:8])
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"Batch submission error: {str(e)[:200]}")

        # No more pending batches — phase transition
        snap = self._snapshots.get(pid)
        phase = snap.get("phase", "initial_load") if snap else "initial_load"

        if phase == "initial_load":
            return self._start_catch_up(pipeline, snap)
        elif phase in ("catch_up", "converging"):
            batch_job_ref = (completed_batch or {}).get("emr_job_run_id", "") or job_ref
            qid = batch_job_ref.replace("athena:", "") if batch_job_ref.startswith("athena:") else batch_job_ref
            try:
                results = self._athena.get_query_results(qid, max_results=1000)
                if results and "pk_val" in results[0]:
                    return self._process_diff_results(pipeline, snap, results)
            except Exception:
                pass

            self._repo.update_sync_status(pid, "in_sync")
            self._snapshots.update_cursor(
                pid, snap.get("snapshot_cursor", ""), phase="in_sync")
            self._snapshots.reset_convergence(pid)
            log.info("Pipeline %s: converged, entering sync mode", pid[:8])
            return self._repo.get(pid)

        return self._repo.get(pid)

    def _post_batch0_setup(self, pipeline: dict):
        """Post batch 0 completion: apply sort order."""
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)
        if not spec_json:
            return

        ip = spec_json.get("iceberg_properties", {})
        sort_order = ip.get("sort_order")
        if sort_order:
            tgt = spec_json.get("target", {})
            target_type = pipeline.get("target_type", "")
            bucket_arn = pipeline.get("target_bucket_arn", "")
            alter_sql = self._athena.build_alter_write_order_sql(
                tgt.get("database", ""), tgt.get("table_name", ""),
                sort_order, target_type=target_type, table_bucket_arn=bucket_arn)
            if alter_sql:
                try:
                    self._athena.submit_query(
                        alter_sql, database=tgt.get("database", ""),
                        description=f"IMA-sort-order-{tgt.get('table_name', '')}")
                    log.info("Pipeline %s: submitted ALTER TABLE for sort order", pid[:8])
                except Exception as e:
                    log.warning("Pipeline %s: sort order ALTER failed (non-fatal): %s", pid[:8], e)

    def _process_diff_results(self, pipeline: dict, snap: dict,
                              results: list[dict]) -> dict:
        """Process diff query results: DELETE + INSERT for affected partitions.
        
        Fix #4: Handles partition archival correctly — DELETE removes stale data,
        INSERT re-reads from source (returns 0 rows for archived partitions).
        """
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        src = spec_json.get("source", {})
        tgt = spec_json.get("target", {})
        table_bucket_arn = pipeline.get("target_bucket_arn", "")
        partition_keys = src.get("partition_keys", [])
        # Fallback: read from pipeline if spec doesn't have it
        if not partition_keys:
            pk_str = pipeline.get("source_partition_keys", "")
            if pk_str and isinstance(pk_str, str):
                try:
                    partition_keys = json.loads(pk_str)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(pk_str, list):
                partition_keys = pk_str
        pk_col = partition_keys[0]["name"] if partition_keys else ""


        # Extract affected partition values
        affected = [r.get("pk_val", "") for r in results if r.get("pk_val")]
        if not affected:
            # No affected partitions — converged
            self._snapshots.update_cursor(
                pid, snap.get("snapshot_cursor", ""), phase="in_sync")
            self._snapshots.reset_convergence(pid)
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        log.info("Pipeline %s: %d affected partition(s) to sync", pid[:8], len(affected))

        # Build partition filter
        from .common import build_partition_where
        pk_filters = [f"{pk_col}={v}" for v in affected]
        where = build_partition_where(pk_filters)

        if not where:
            self._snapshots.update_cursor(
                pid, snap.get("snapshot_cursor", ""), phase="in_sync")
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        # DELETE affected partitions from target, then INSERT from source
        delete_sql = athena_delete(
            tgt["database"], tgt["table_name"],
            where=where, target_type="s3_tables", table_bucket_arn=table_bucket_arn)
        insert_sql = athena_insert_into(
            target_db=tgt["database"], target_table=tgt["table_name"],
            source_db=src["database"], source_table=src["table_name"],
            where=where, target_type="s3_tables", table_bucket_arn=table_bucket_arn)

        # Submit DELETE and wait
        try:
            del_qid = self._athena.submit_query(
                delete_sql, database=src["database"],
                description=f"IMA-iceberg-inc-sync-delete-{pid[:8]}")
            s = poll_until_complete(
                check=lambda: self._athena.get_query_status(del_qid),
                is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"),
                interval=1.0, max_attempts=120)
            if s["state"] in ("FAILED", "CANCELLED"):
                reason = s.get("state_reason", "DELETE failed")
                return self._repo.update_status(
                    pid, "failed", error_message=f"Sync DELETE failed: {reason}")
            elif s["state"] != "SUCCEEDED":
                return self._repo.update_status(
                    pid, "failed", error_message="Sync DELETE timed out")
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"Sync DELETE error: {e}")

        # Submit INSERT
        try:
            ins_qid = self._athena.submit_query(
                insert_sql, database=src["database"],
                description=f"IMA-iceberg-inc-sync-insert-{pid[:8]}")
            self._repo.update_emr_job_run_id(pid, f"athena:{ins_qid}")
            log.info("Pipeline %s: submitted sync INSERT for %d partitions (qid=%s)",
                     pid[:8], len(affected), ins_qid)
            return self._repo.get(pid)
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"Sync INSERT error: {e}")

    # ── Phase transitions ────────────────────────────────────────

    def _start_catch_up(self, pipeline: dict, snap: dict) -> dict:
        """Transition from initial load to catch-up phase."""
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)
        src = spec_json.get("source", {})
        # Get current snapshot via PyIceberg
        snap_info = self._iceberg.get_latest_snapshot(src["database"], src["table_name"])
        if not snap_info:
            return self._repo.update_status(
                pid, "failed", error_message="Failed to get current snapshot")
        snapshot_after = snap_info["snapshot_id"]
        committed_at_after = str(snap_info.get("committed_at", ""))
        snapshot_before = snap.get("snapshot_before", "")
        if snapshot_after == snapshot_before:
            # No changes during initial load — go straight to in_sync
            self._snapshots.update_cursor(pid, snapshot_after, phase="in_sync",
                                          committed_at_cursor=committed_at_after)
            self._snapshots.reset_convergence(pid)
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: no changes during load, in_sync", pid[:8])
            return self._repo.get(pid)
        # Changes occurred — need to catch up
        self._snapshots.set_snapshot_after_load(pid, snapshot_after,
                                                committed_at_after_load=committed_at_after)
        log.info("Pipeline %s: catch-up needed (before=%s, after=%s)",
                 pid[:8], snapshot_before[:12], snapshot_after[:12])
        # Submit catch-up job
        return self._submit_snapshot_diff_sync(pipeline, snap, snapshot_after)

    def _reconcile_convergence(self, pipeline: dict, snap: dict) -> dict:
        """Check if we've converged (no more changes to sync).
        
        Fix #13: Forces to in_sync after MAX_CONVERGENCE_ITERATIONS.
        Uses PyIceberg for snapshot lookup (no Athena query needed).
        """
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)
        src = spec_json.get("source", {})
        cursor = snap.get("snapshot_cursor", "")

        # Fix #13: Check convergence iteration limit
        iterations = int(snap.get("convergence_iterations", 0))
        if iterations >= _MAX_CONVERGENCE_ITERATIONS:
            log.warning("Pipeline %s: max convergence iterations (%d) reached, "
                        "forcing to in_sync", pid[:8], iterations)
            self._snapshots.update_cursor(pid, cursor, phase="in_sync")
            self._snapshots.reset_convergence(pid)
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        # Get latest snapshot via PyIceberg (fast, no Athena query)
        snap_info = self._iceberg.get_latest_snapshot(src["database"], src["table_name"])
        if not snap_info:
            return pipeline

        latest = snap_info["snapshot_id"]
        latest_committed_at = str(snap_info.get("committed_at", ""))

        if latest == cursor:
            # Converged — no new changes
            self._snapshots.update_cursor(pid, latest, phase="in_sync",
                                          committed_at_cursor=latest_committed_at)
            self._snapshots.reset_convergence(pid)
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: converged at snapshot %s", pid[:8], latest[:12])
            return self._repo.get(pid)

        # More changes — increment counter and submit sync
        self._snapshots.increment_convergence(pid)
        self._snapshots.update_phase(pid, "converging")
        return self._submit_snapshot_diff_sync(pipeline, snap, latest)

    # ── Snapshot diff sync ───────────────────────────────────────

    def _submit_snapshot_diff_sync(self, pipeline: dict,
                                   snap: dict, to_snapshot: str) -> dict:
        """Sync changed partitions between cursor and to_snapshot.
        
        Uses PyIceberg for change detection (append-only check, affected partitions).
        Uses Athena or EMR for the actual data movement (DELETE + INSERT).
        """
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        src = spec_json.get("source", {})
        tgt = spec_json.get("target", {})
        table_bucket_arn = pipeline.get("target_bucket_arn", "")

        source_ref = athena_table_ref(src["database"], src["table_name"]).sql(dialect="trino")
        target_ref = self._athena.format_table_ref(
            tgt["database"], tgt["table_name"], "s3_tables", table_bucket_arn)

        # Get cursor info from snapshot tracking
        from_snapshot = snap.get("snapshot_cursor", "") or snap.get("snapshot_before", "")

        # Get partition columns
        partition_keys = src.get("partition_keys", [])
        # Fallback: read from pipeline if spec doesn't have it
        if not partition_keys:
            pk_str = pipeline.get("source_partition_keys", "")
            if pk_str and isinstance(pk_str, str):
                try:
                    partition_keys = json.loads(pk_str)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(pk_str, list):
                partition_keys = pk_str
        if not partition_keys:
            # Unpartitioned — full rewrite
            engine = self._engine(pipeline)
            if engine != "athena":
                return self._submit_sync_emr(
                    pipeline, "full_sync", "", to_snapshot, spec_json)

            delete_sql = athena_delete(
                tgt["database"], tgt["table_name"],
                where="1=1", target_type="s3_tables", table_bucket_arn=table_bucket_arn)
            insert_sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                target_type="s3_tables", table_bucket_arn=table_bucket_arn)
            try:
                # Submit DELETE, wait, then INSERT
                del_qid = self._athena.submit_query(
                    delete_sql, database=src["database"],
                    description=f"IMA-iceberg-inc-sync-delete-full-{pid[:8]}")
                poll_until_complete(
                    check=lambda: self._athena.get_query_status(del_qid),
                    is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"),
                    interval=1.0, max_attempts=120)
                qid = self._athena.submit_query(
                    insert_sql, database=src["database"],
                    description=f"IMA-iceberg-inc-sync-insert-full-{pid[:8]}")
                self._repo.update_emr_job_run_id(pid, f"athena:{qid}")
                self._snapshots.update_cursor(pid, to_snapshot, phase="converging")

                # Record sync as a batch
                existing_batches = self._batches.list_by_pipeline(pid)
                next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
                self._batches.create(pid, batch_number=next_batch_num,
                                     partitions=[f"__sync__full_table__{to_snapshot[:12]}__"])
                self._batches.mark_running(next_batch_num, f"athena:{qid}", pipeline_id=pid)

                return self._repo.get(pid)
            except Exception as e:
                return self._repo.update_status(
                    pid, "failed", error_message=f"Sync failed: {e}")

        pk_col = partition_keys[0]["name"]

        # Use PyIceberg to detect changes (avoids Athena $metadata table issues)
        # Validate snapshot IDs before converting
        if not from_snapshot or not to_snapshot:
            log.warning("Pipeline %s: missing snapshot IDs (from=%s, to=%s), "
                        "falling back to full partition sync",
                        pid[:8], from_snapshot, to_snapshot)
            return self._submit_full_partition_sync(
                pipeline, from_snapshot, to_snapshot, src, tgt,
                pk_col, source_ref, target_ref, table_bucket_arn)

        try:
            from_snap_int = int(from_snapshot)
            to_snap_int = int(to_snapshot)
        except (ValueError, TypeError):
            log.warning("Pipeline %s: invalid snapshot IDs (from=%s, to=%s), "
                        "falling back to full partition sync",
                        pid[:8], from_snapshot, to_snapshot)
            return self._submit_full_partition_sync(
                pipeline, from_snapshot, to_snapshot, src, tgt,
                pk_col, source_ref, target_ref, table_bucket_arn)

        # Check if changes are append-only via PyIceberg
        has_deletes = self._iceberg.has_deletes_between(
            src["database"], src["table_name"], from_snap_int, to_snap_int)

        if not has_deletes:
            log.info("Pipeline %s: append-only sync (no deletes detected via PyIceberg)",
                     pid[:8])
            return self._submit_append_only_sync(
                pipeline, from_snapshot, to_snapshot, src, tgt,
                pk_col, source_ref, target_ref, table_bucket_arn)
        else:
            log.info("Pipeline %s: mixed sync (deletes detected)", pid[:8])
            return self._submit_full_partition_sync(
                pipeline, from_snapshot, to_snapshot, src, tgt,
                pk_col, source_ref, target_ref, table_bucket_arn)

    def _submit_append_only_sync(self, pipeline: dict,
                                  from_snapshot: str, to_snapshot: str,
                                  src: dict, tgt: dict, pk_col: str,
                                  source_ref: str, target_ref: str,
                                  table_bucket_arn: str) -> dict:
        """Append-only sync: INSERT only new rows added between snapshots.
        
        Uses PyIceberg to get new file paths, then INSERT via $path filter.
        """
        pid = pipeline["id"]

        try:
            from_snap_int = int(from_snapshot)
            to_snap_int = int(to_snapshot)
        except (ValueError, TypeError):
            return self._repo.update_status(
                pid, "failed",
                error_message=f"Invalid snapshot IDs: from={from_snapshot}, to={to_snapshot}")

        # Get new data file paths via PyIceberg (no Athena metadata table needed)
        new_files = self._iceberg.get_new_data_files(
            src["database"], src["table_name"], from_snap_int, to_snap_int)

        if not new_files:
            # No new files — already in sync
            self._snapshots.update_cursor(pid, to_snapshot, phase="in_sync")
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: no new files between snapshots, in_sync", pid[:8])
            return self._repo.get(pid)

        log.info("Pipeline %s: %d new files to sync (append-only)", pid[:8], len(new_files))

        # Build INSERT using $path filter with the new file paths
        # For large file lists, batch them to avoid SQL length limits
        if len(new_files) <= 100:
            paths_list = ", ".join(f"'{f}'" for f in new_files)
            insert_sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                where=f"\"$path\" IN ({paths_list})",
                target_type="s3_tables", table_bucket_arn=table_bucket_arn)
        else:
            # Too many files — fall back to partition-based INSERT
            # Get affected partitions and INSERT those
            affected = self._iceberg.get_affected_partitions(
                src["database"], src["table_name"],
                from_snap_int, to_snap_int, pk_col)
            if affected:
                pk_filters = [self._build_partition_filter(
                    p["source_column"], p["value"], p.get("transform", "identity"))
                    for p in affected]
                where = " OR ".join(f"({f})" for f in pk_filters if f)
                insert_sql = athena_insert_into(
                    target_db=tgt["database"], target_table=tgt["table_name"],
                    source_db=src["database"], source_table=src["table_name"],
                    where=where, target_type="s3_tables", table_bucket_arn=table_bucket_arn)
            else:
                insert_sql = athena_insert_into(
                    target_db=tgt["database"], target_table=tgt["table_name"],
                    source_db=src["database"], source_table=src["table_name"],
                    target_type="s3_tables", table_bucket_arn=table_bucket_arn)
                where = ""

        # Route to EMR if engine is not Athena
        engine = self._engine(pipeline)
        if engine != "athena":
            _spec = pipeline.get("spec_json")
            if isinstance(_spec, str):
                _spec = json.loads(_spec)
            _where = where if 'where' in locals() and where else ""
            return self._submit_sync_emr(
                pipeline, "append_only", _where,
                to_snapshot, _spec, file_paths=new_files if len(new_files) <= 100 else None)

        try:
            qid = self._athena.submit_query(
                insert_sql, database=src["database"],
                description=f"IMA-iceberg-inc-append-sync-{pid[:8]}")
            self._repo.update_emr_job_run_id(pid, f"athena:{qid}")
            self._snapshots.update_cursor(pid, to_snapshot, phase="converging")

            # Record sync as a batch for tracking
            existing_batches = self._batches.list_by_pipeline(pid)
            next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
            sync_label = f"__sync__append__{to_snapshot[:12]}__"
            self._batches.create(pid, batch_number=next_batch_num,
                                 partitions=[sync_label])
            self._batches.mark_running(next_batch_num, f"athena:{qid}", pipeline_id=pid)

            log.info("Pipeline %s: submitted append-only INSERT (qid=%s, batch=%d)",
                     pid[:8], qid, next_batch_num)
            return self._repo.get(pid)
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"Append sync failed: {e}")

    def _submit_full_partition_sync(self, pipeline: dict,
                                     from_snapshot: str, to_snapshot: str,
                                     src: dict, tgt: dict, pk_col: str,
                                     source_ref: str, target_ref: str,
                                     table_bucket_arn: str) -> dict:
        """Full partition rewrite sync: identify affected partitions via PyIceberg,
        then DELETE + INSERT for those partitions.
        
        Uses PyIceberg for change detection (no Athena metadata tables needed).
        """
        pid = pipeline["id"]

        try:
            from_snap_int = int(from_snapshot)
            to_snap_int = int(to_snapshot)
        except (ValueError, TypeError):
            return self._repo.update_status(
                pid, "failed",
                error_message=f"Invalid snapshot IDs: from={from_snapshot}, to={to_snapshot}")

        # Get affected partitions via PyIceberg
        affected = self._iceberg.get_affected_partitions(
            src["database"], src["table_name"],
            from_snap_int, to_snap_int, pk_col)

        if not affected:
            # PyIceberg couldn't identify specific partitions — but we know the
            # snapshot advanced (otherwise we wouldn't be here). Fall back to
            # full table rewrite to ensure correctness.
            log.warning("Pipeline %s: snapshot advanced but no affected partitions "
                        "detected (partition extraction may have failed). "
                        "Falling back to full table rewrite.", pid[:8])

            engine = self._engine(pipeline)
            if engine != "athena":
                _spec = pipeline.get("spec_json")
                if isinstance(_spec, str):
                    _spec = json.loads(_spec)
                return self._submit_sync_emr(
                    pipeline, "full_sync", "", to_snapshot, _spec)

            delete_sql = athena_delete(
                tgt["database"], tgt["table_name"],
                where="1=1", target_type="s3_tables", table_bucket_arn=table_bucket_arn)
            insert_sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                target_type="s3_tables", table_bucket_arn=table_bucket_arn)
            try:
                del_qid = self._athena.submit_query(
                    delete_sql, database=src["database"],
                    description=f"IMA-iceberg-inc-sync-delete-full-{pid[:8]}")
                s = poll_until_complete(
                    check=lambda: self._athena.get_query_status(del_qid),
                    is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"),
                    interval=1.0, max_attempts=120)
                if s["state"] in ("FAILED", "CANCELLED"):
                    reason = s.get("state_reason", "DELETE failed")
                    return self._repo.update_status(
                        pid, "failed",
                        error_message=f"Full sync DELETE failed: {reason}")
                qid = self._athena.submit_query(
                    insert_sql, database=src["database"],
                    description=f"IMA-iceberg-inc-sync-insert-full-{pid[:8]}")
                self._repo.update_emr_job_run_id(pid, f"athena:{qid}")
                self._snapshots.update_cursor(pid, to_snapshot, phase="converging")

                # Record sync as a batch
                existing_batches = self._batches.list_by_pipeline(pid)
                next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
                self._batches.create(pid, batch_number=next_batch_num,
                                     partitions=[f"__sync__full_fallback__{to_snapshot[:12]}__"])
                self._batches.mark_running(next_batch_num, f"athena:{qid}", pipeline_id=pid)

                return self._repo.get(pid)
            except Exception as e:
                return self._repo.update_status(
                    pid, "failed", error_message=f"Full sync failed: {e}")

        log.info("Pipeline %s: %d affected partition(s) to sync: %s",
                 pid[:8], len(affected), affected[:5])

        # Build WHERE clause for affected partitions
        # affected is now list of dicts: {"value": "54", "transform": "year", "source_column": "event_date"}
        pk_filters = []
        for part in affected:
            v = part["value"]
            transform = part.get("transform", "identity")
            col = part.get("source_column", pk_col)
            pk_filters.append(self._build_partition_filter(col, v, transform))

        where = " OR ".join(f"({f})" for f in pk_filters if f)
        if not where:
            # Fallback: full table rewrite
            log.warning("Pipeline %s: could not build partition filters, "
                        "falling back to full rewrite", pid[:8])
            where = "1=1"

        # DELETE affected partitions from target, then INSERT from source
        delete_sql = athena_delete(
            tgt["database"], tgt["table_name"],
            where=where, target_type="s3_tables", table_bucket_arn=table_bucket_arn)
        insert_sql = athena_insert_into(
            target_db=tgt["database"], target_table=tgt["table_name"],
            source_db=src["database"], source_table=src["table_name"],
            where=where, target_type="s3_tables", table_bucket_arn=table_bucket_arn)
        log.info("Pipeline %s: sync SQL WHERE: %s", pid[:8], where)

        # Route to EMR if engine is not Athena
        engine = self._engine(pipeline)
        if engine != "athena":
            _spec = pipeline.get("spec_json")
            if isinstance(_spec, str):
                _spec = json.loads(_spec)
            return self._submit_sync_emr(
                pipeline, "partition_sync", where, to_snapshot,
                _spec, num_partitions=len(affected))

        # Athena path: Submit DELETE and wait
        try:
            del_qid = self._athena.submit_query(
                delete_sql, database=src["database"],
                description=f"IMA-iceberg-inc-sync-delete-{pid[:8]}")
            s = poll_until_complete(
                check=lambda: self._athena.get_query_status(del_qid),
                is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"),
                interval=1.0, max_attempts=120)
            if s["state"] in ("FAILED", "CANCELLED"):
                reason = s.get("state_reason", "DELETE failed")
                return self._repo.update_status(
                    pid, "failed", error_message=f"Sync DELETE failed: {reason}")
            elif s["state"] != "SUCCEEDED":
                return self._repo.update_status(
                    pid, "failed", error_message="Sync DELETE timed out")
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"Sync DELETE error: {e}")

        # Submit INSERT
        try:
            ins_qid = self._athena.submit_query(
                insert_sql, database=src["database"],
                description=f"IMA-iceberg-inc-sync-insert-{pid[:8]}")
            self._repo.update_emr_job_run_id(pid, f"athena:{ins_qid}")
            self._snapshots.update_cursor(pid, to_snapshot, phase="converging")

            # Record sync as a batch for tracking
            existing_batches = self._batches.list_by_pipeline(pid)
            next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
            sync_label = f"__sync__rewrite__{len(affected)}_partitions__"
            self._batches.create(pid, batch_number=next_batch_num,
                                 partitions=[sync_label])
            self._batches.mark_running(next_batch_num, f"athena:{ins_qid}", pipeline_id=pid)

            log.info("Pipeline %s: submitted sync INSERT for %d partitions (qid=%s, batch=%d)",
                     pid[:8], len(affected), ins_qid, next_batch_num)
            return self._repo.get(pid)
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"Sync INSERT error: {e}")

    # ── Partition filter helpers ────────────────────────────────────

    @staticmethod
    def _build_partition_filter(column: str, value: str, transform: str) -> str:
        """Build a SQL WHERE clause for a partition value with its transform.
        
        Converts Iceberg partition transform values back to source column filters:
        - identity: direct equality (event_date = DATE '2024-03-15')
        - year: range filter for the year (event_date >= '2024-01-01' AND event_date < '2025-01-01')
        - month: range filter for the month
        - day: equality on the date
        - hour: range filter for the hour
        - bucket/truncate: use the transform function in the filter
        """
        from datetime import date, timedelta

        if transform == "identity":
            # Direct value — detect type
            if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                return f"{column} = {value}"
            elif len(value) == 10 and value[4:5] == '-' and value[7:8] == '-':
                return f"{column} = DATE '{value}'"
            else:
                return f"{column} = '{value}'"

        elif transform == "year":
            # Value is years since epoch (1970). E.g., 54 = 2024
            try:
                year = 1970 + int(value)
                return (f"{column} >= DATE '{year}-01-01' AND "
                        f"{column} < DATE '{year + 1}-01-01'")
            except (ValueError, TypeError):
                return f"year({column}) = {value}"

        elif transform == "month":
            # Value is months since epoch. E.g., 648 = 2024-01
            try:
                total_months = int(value)
                year = 1970 + total_months // 12
                month = total_months % 12 + 1
                start = date(year, month, 1)
                if month == 12:
                    end = date(year + 1, 1, 1)
                else:
                    end = date(year, month + 1, 1)
                return (f"{column} >= DATE '{start.isoformat()}' AND "
                        f"{column} < DATE '{end.isoformat()}'")
            except (ValueError, TypeError):
                return f"month({column}) = {value}"

        elif transform == "day":
            # Value is days since epoch. E.g., 19750 = 2024-01-15
            try:
                d = date(1970, 1, 1) + timedelta(days=int(value))
                return f"{column} = DATE '{d.isoformat()}'"
            except (ValueError, TypeError):
                return f"date({column}) = {value}"

        elif transform == "hour":
            # Value is hours since epoch
            try:
                from datetime import datetime, timezone
                hours = int(value)
                dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(hours=hours)
                next_hour = dt + timedelta(hours=1)
                return (f"{column} >= TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}' AND "
                        f"{column} < TIMESTAMP '{next_hour.strftime('%Y-%m-%d %H:%M:%S')}'")
            except (ValueError, TypeError):
                return f"hour({column}) = {value}"

        else:
            # bucket, truncate, or unknown — use transform function
            # This works in Athena for Iceberg tables
            return f"{transform}({column}) = {value}"

    # ── Batch submission ─────────────────────────────────────────

    def _submit_next_batch(self, pipeline: dict,
                           pending_batches: list[dict]) -> dict:
        """Submit the next pending batch via Athena or EMR."""
        pid = pipeline["id"]
        batch = pending_batches[0]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        partitions = batch.get("partitions", "[]")
        if isinstance(partitions, str):
            partitions = json.loads(partitions)

        src = spec_json.get("source", {})
        tgt = spec_json.get("target", {})
        table_bucket_arn = pipeline.get("target_bucket_arn", "")


        bn = batch["batch_number"]
        engine = self._engine(pipeline)

        if partitions == ["__create_table__"]:
            # Already submitted during submit() — skip
            return pipeline

        elif partitions[0].startswith("__discover_date_range__"):
            # Discovery always via Athena (lightweight, needs result reading)
            marker = partitions[0].replace("__discover_date_range__", "", 1).strip("_")
            marker_parts = marker.split("__")
            mode = marker_parts[0]
            cols_str = marker_parts[1] if len(marker_parts) > 1 else ""
            col_names = cols_str.split(",")
            date_col = col_names[0]

            if mode == "composite_ymd":
                year_col, month_col = col_names[0], col_names[1]
                day_col = col_names[2] if len(col_names) >= 3 else None
                if day_col:
                    date_expr = f"DATE(CAST({year_col} AS VARCHAR) || '-' || LPAD(CAST({month_col} AS VARCHAR), 2, '0') || '-' || LPAD(CAST({day_col} AS VARCHAR), 2, '0'))"
                else:
                    date_expr = f"DATE(CAST({year_col} AS VARCHAR) || '-' || LPAD(CAST({month_col} AS VARCHAR), 2, '0') || '-01')"
                sql = athena_select(
                    [f"CAST(MIN({date_expr}) AS VARCHAR) as min_val",
                     f"CAST(MAX({date_expr}) AS VARCHAR) as max_val"],
                    src["database"], src["table_name"])
            else:
                sql = athena_select(
                    [f"CAST(MIN({date_col}) AS VARCHAR) as min_val",
                     f"CAST(MAX({date_col}) AS VARCHAR) as max_val"],
                    src["database"], src["table_name"])

            try:
                qid = self._athena.submit_query(
                    sql, database=src["database"],
                    description=f"IMA-iceberg-inc-batch-{bn}-{pid[:8]}")
            except Exception as e:
                self._batches.mark_failed(batch["id"], error_message=str(e))
                return self._repo.update_status(
                    pid, "failed", error_message=f"Batch {bn} failed: {e}")

            self._batches.mark_running(batch["id"], f"athena:{qid}", pipeline_id=pid)
            self._repo.update_emr_job_run_id(pid, f"athena:{qid}")
            return self._repo.update_status(pid, "running")

        # Data movement batches — route to EMR or Athena
        if engine != "athena":
            return self._submit_next_batch_emr(pipeline, batch, spec_json, partitions)

        # Athena path
        if partitions == ["__full_insert__"]:
            sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                target_type="s3_tables", table_bucket_arn=table_bucket_arn)
        elif partitions[0].startswith("__date_range__"):
            range_str = partitions[0].replace("__date_range__", "", 1).strip("_")
            range_parts = range_str.split("__")
            mode = range_parts[0]
            cols_str = range_parts[1]
            start_val = range_parts[2]
            end_val = range_parts[3]
            col_names = cols_str.split(",")

            partition_keys = src.get("partition_keys", [])
            columns = [pk for pk in partition_keys if pk["name"] in col_names]
            if not columns:
                columns = [{"name": n, "type": "string"} for n in col_names]

            where_clause = build_date_filter_sql(
                col_names[0], start_val, end_val, mode, columns)
            sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                where=where_clause, target_type="s3_tables", table_bucket_arn=table_bucket_arn)
        else:
            sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                target_type="s3_tables", table_bucket_arn=table_bucket_arn)

        try:
            qid = self._athena.submit_query(
                sql, database=src["database"],
                description=f"IMA-iceberg-inc-batch-{bn}-{pid[:8]}")
        except Exception as e:
            self._batches.mark_failed(batch["id"], error_message=str(e))
            return self._repo.update_status(
                pid, "failed", error_message=f"Batch {bn} failed: {e}")

        self._batches.mark_running(batch["id"], f"athena:{qid}", pipeline_id=pid)
        self._repo.update_emr_job_run_id(pid, f"athena:{qid}")
        return self._repo.update_status(pid, "running")

    def _submit_next_batch_emr(self, pipeline: dict, batch: dict,
                                spec_json: dict, partitions: list[str]) -> dict:
        """Submit a data movement batch via EMR Serverless."""
        pid = pipeline["id"]
        bn = batch["batch_number"]
        src = spec_json.get("source", {})

        from ..engines.emr import resolve_script_path
        script_path = resolve_script_path("iceberg_sync.py")

        # Determine operation and WHERE clause
        if partitions == ["__full_insert__"]:
            operation = "full_insert"
            where = ""
        elif partitions[0].startswith("__date_range__"):
            range_str = partitions[0].replace("__date_range__", "", 1).strip("_")
            range_parts = range_str.split("__")
            mode = range_parts[0]
            cols_str = range_parts[1]
            start_val = range_parts[2]
            end_val = range_parts[3]
            col_names = cols_str.split(",")

            partition_keys = src.get("partition_keys", [])
            columns = [pk for pk in partition_keys if pk["name"] in col_names]
            if not columns:
                columns = [{"name": n, "type": "string"} for n in col_names]

            where = build_date_filter_sql(col_names[0], start_val, end_val, mode, columns)
            operation = "insert_where"
        else:
            operation = "full_insert"
            where = ""

        spark_props = self._build_spark_props(pipeline)
        job_name = f"IMA-iceberg-inc-batch-{bn}-{pid[:8]}"

        script_args = [
            "--spec", json.dumps(spec_json),
            "--operation", operation,
            "--where", where,
        ]

        try:
            job_run_id = self._emr.submit_job(
                name=job_name, script_path=script_path,
                script_args=script_args, spark_properties=spark_props)
        except Exception as e:
            self._batches.mark_failed(batch["id"], error_message=str(e))
            return self._repo.update_status(
                pid, "failed", error_message=f"EMR batch {bn} failed: {e}")

        log.info("Pipeline %s: submitted EMR batch %d (op=%s, job=%s)",
                 pid[:8], bn, operation, job_run_id)
        self._batches.mark_running(batch["id"], job_run_id, pipeline_id=pid)
        self._repo.update_emr_job_run_id(pid, job_run_id)
        return self._repo.update_status(pid, "running")

    def _submit_emr_setup(self, pipeline: dict, spec_json: dict,
                           snapshot_before: str) -> dict:
        """Submit the CREATE TABLE job via EMR. Discovery is done by the orchestrator."""
        pid = pipeline["id"]
        src = spec_json.get("source", {})
        from ..engines.emr import resolve_script_path
        script_path = resolve_script_path("create_table.py")

        # Ensure partition spec is in the spec for the Spark script
        ip = spec_json.setdefault("iceberg_properties", {})
        if not ip.get("partition_spec"):
            partition_spec = self._iceberg.get_partition_spec(src["database"], src["table_name"])
            if partition_spec:
                ip["partition_spec"] = partition_spec

        spark_props = self._build_spark_props(pipeline)
        job_name = f"IMA-iceberg-setup-{src['database']}-{src['table_name']}"

        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=["--spec", json.dumps(spec_json)],
                spark_properties=spark_props)
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"EMR setup submit failed: {e}")

        log.info("Pipeline %s: submitted EMR create-table job %s", pid[:8], job_run_id)
        self._batches.create(pid, batch_number=0, partitions=["__create_table__"])
        self._batches.mark_running(0, job_run_id, pipeline_id=pid)
        self._snapshots.update_phase(pid, "initial_load")
        self._repo.update_emr_job_run_id(pid, job_run_id)
        return self._repo.update_status(pid, "running")

    def _build_spark_props(self, pipeline: dict) -> dict:
        """Build Spark properties for EMR Serverless jobs."""
        props = {
            "spark.sql.catalog.glue_catalog":
                "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.glue_catalog.catalog-impl":
                "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.glue_catalog.warehouse":
                pipeline.get("source_location", ""),
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        }
        if pipeline.get("target_type") == "s3_tables":
            # S3 Tables via Iceberg GlueCatalog with glue.id + warehouse (EMR >= 7.5).
            props.update(s3tables_spark_props(pipeline))
        return props

    # ── EMR sync helper ─────────────────────────────────────────

    def _submit_sync_emr(self, pipeline: dict, operation: str,
                          where: str, to_snapshot: str,
                          spec_json: dict, num_partitions: int = 0,
                          file_paths: list[str] = None) -> dict:
        """Submit a sync operation via EMR Serverless."""
        pid = pipeline["id"]
        from ..engines.emr import resolve_script_path
        script_path = resolve_script_path("iceberg_sync.py")

        spark_props = self._build_spark_props(pipeline)
        job_name = f"IMA-iceberg-sync-{operation}-{pid[:8]}"

        script_args = [
            "--spec", json.dumps(spec_json),
            "--operation", operation,
            "--where", where or "",
        ]
        if file_paths:
            script_args.extend(["--file-paths", json.dumps(file_paths)])

        try:
            job_run_id = self._emr.submit_job(
                name=job_name, script_path=script_path,
                script_args=script_args, spark_properties=spark_props)
        except Exception as e:
            return self._repo.update_status(
                pid, "failed", error_message=f"EMR sync failed: {e}")

        self._repo.update_emr_job_run_id(pid, job_run_id)
        self._snapshots.update_cursor(pid, to_snapshot, phase="converging")

        # Record sync as a batch
        existing_batches = self._batches.list_by_pipeline(pid)
        next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
        sync_label = f"__sync__rewrite__{num_partitions}_partitions__" if num_partitions else f"__sync__{operation}__"
        self._batches.create(pid, batch_number=next_batch_num,
                             partitions=[sync_label])
        self._batches.mark_running(next_batch_num, job_run_id, pipeline_id=pid)

        log.info("Pipeline %s: submitted EMR sync (op=%s, job=%s, batch=%d)",
                 pid[:8], operation, job_run_id, next_batch_num)
        return self._repo.get(pid)

    # ── Cancel ───────────────────────────────────────────────────

    def cancel(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        job_ref = pipeline.get("emr_job_run_id", "")
        if job_ref.startswith("athena:"):
            try:
                self._athena.cancel_query(job_ref[7:])
            except Exception as e:
                log.warning("Failed to cancel query: %s", e)
        elif job_ref:
            try:
                self._emr.cancel_job(job_ref)
            except Exception as e:
                log.warning("Failed to cancel EMR job: %s", e)
        self._snapshots.delete(pid)
        return self._repo.update_status(pid, "cancelled")
