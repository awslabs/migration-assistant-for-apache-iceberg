# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Morph strategy — file tracking + in-process manifest generation.

No data movement. Scans source Parquet files, reads footers for
metadata, and generates Iceberg manifests pointing to the original
files. Ongoing changes are tracked via S3 object events in the
object_tracking table and committed incrementally.

Only supported for columnar formats: Parquet, Hudi, Delta.
Only supported for s3_iceberg target (not S3 Tables, which requires
data to be in the S3 Tables bucket).

Lifecycle:
  1. submit:     Create S3 event tracker (Lambda + notification),
                 scan source S3 prefix, read Parquet footers,
                 create target Iceberg table, add files with full stats.
                 Tracker is created BEFORE scan to avoid missing events
                 during the scan window.
  2. reconcile:  Periodically process pending S3 events from
                 object_tracking, commit incremental manifest updates.
  3. cancel:     Mark pipeline as cancelled, remove tracker.

All work runs in-process — no EMR jobs needed.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3

from .base import MigrationStrategy
from ..db import PipelineRepo, ObjectTrackingRepo, ProgressRepo, BatchRepo
from ..morph.scanner import (
    list_data_files, parse_partition_values, FileMetadata,
)
from ..morph.manifest import (
    ManifestBuilder, schema_from_glue, build_partition_spec,
)

log = logging.getLogger(__name__)

MORPH_SUPPORTED_FORMATS = ("parquet", "hudi", "delta")
_SYNC_INTERVAL = int(os.getenv(
    "ENGINE_MORPH_INTERVAL_SECONDS", "3600"))


def _fetch_glue_schema(database: str, table_name: str,
                       region: str = "us-east-1") -> dict:
    """Fetch table schema from Glue catalog.

    Returns dict with 'columns' and 'partition_keys' lists,
    each containing {'name': str, 'type': str} dicts.
    """
    glue = boto3.client("glue", region_name=region)
    resp = glue.get_table(DatabaseName=database, Name=table_name)
    t = resp["Table"]
    sd = t.get("StorageDescriptor", {})
    return {
        "columns": [
            {"name": c["Name"], "type": c["Type"]}
            for c in sd.get("Columns", [])
        ],
        "partition_keys": [
            {"name": p["Name"], "type": p["Type"]}
            for p in t.get("PartitionKeys", [])
        ],
    }


class MorphStrategy(MigrationStrategy):

    # Track which pipelines have an active background thread
    _active_threads: set = set()

    # Global cap on concurrent morph initial-load background threads (0.1).
    # Morph does its heavy work (Parquet footer reads + manifest build) IN-PROCESS
    # on the engine box, unlike rewrite/incremental which offload to EMR/Athena.
    # Each background load itself fans out to ENGINE_MORPH_WORKERS footer-read
    # threads, so without a global cap N concurrent morph pipelines would spawn
    # N × MORPH_WORKERS threads and exhaust memory/FDs on a single task. This
    # semaphore bounds concurrent loads; pipelines that can't get a slot stay in
    # their pending/initial_load state and are retried on the next reconcile sweep.
    _MORPH_MAX_CONCURRENT = int(os.getenv("ENGINE_MORPH_MAX_CONCURRENT", "6"))
    _load_semaphore = None  # lazily created (see _get_load_semaphore)

    @classmethod
    def _get_load_semaphore(cls):
        import threading
        if cls._load_semaphore is None:
            cls._load_semaphore = threading.BoundedSemaphore(cls._MORPH_MAX_CONCURRENT)
        return cls._load_semaphore

    def __init__(self):
        self._repo = PipelineRepo()
        self._tracking = ObjectTrackingRepo()
        self._progress = ProgressRepo()
        self._batches = BatchRepo()
        from ..tracking import TrackingManager
        self._tracker = TrackingManager()

    def validate(self, pipeline: dict) -> tuple[bool, str]:
        if not pipeline.get("source_location"):
            return False, "Source table has no S3 location"
        fmt = pipeline.get("source_format", "")
        if fmt not in MORPH_SUPPORTED_FORMATS:
            return (False,
                    f"Morph only supports columnar formats "
                    f"({', '.join(MORPH_SUPPORTED_FORMATS)}). "
                    f"Got '{fmt}'.")
        if pipeline.get("target_type") == "s3_tables":
            return (False,
                    "Morph is not supported with S3 Tables target "
                    "(requires data rewrite)")
        return True, "OK"

    def _get_spec(self, pipeline: dict) -> dict:
        spec = pipeline.get("spec_json")
        if isinstance(spec, str):
            spec = json.loads(spec)
        return spec or {}

    def _get_builder(self, pipeline: dict) -> ManifestBuilder:
        """Create a ManifestBuilder from pipeline config."""
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        catalog = tgt.get("catalog", "glue")
        region = src.get("region", "us-east-1")
        # For the warehouse location, prefer target.location, then
        # fall back to the table_bucket_arn-based path, then source location.
        location = tgt.get("location", "")
        if not location:
            # Derive from table_bucket_arn if available
            bucket_arn = tgt.get("table_bucket_arn", "")
            if bucket_arn:
                # arn:aws:s3:::bucket-name → s3://bucket-name
                bucket_name = bucket_arn.rsplit(":", 1)[-1] if bucket_arn.startswith("arn:") else bucket_arn
                location = f"s3://{bucket_name}"
        return ManifestBuilder(
            catalog_name=catalog,
            catalog_type=catalog,
            catalog_props={"region_name": region},
            warehouse=location)

    def submit(self, pipeline: dict) -> dict:
        """Kick off morph pipeline — returns immediately.

        Sets status to running/initial_load and creates batch records.
        The actual work (scan, table creation, manifest build) is driven
        by the reconcile loop, consistent with the incremental strategy.

        Batch lifecycle:
          Batch 0: Setup — create tracker, fetch schema, create target table
          Batch 1: Initial load — scan files, build manifests
        """
        pid = pipeline["id"]
        ok, reason = self.validate(pipeline)
        if not ok:
            return self._repo.update_status(pid, "failed",
                                            error_message=reason)

        self._repo.update_status(pid, "running")
        self._repo.update_sync_status(pid, "initial_load")

        # Determine if this is an XTable conversion (Hudi/Delta)
        fmt = pipeline.get("source_format", "").lower()
        if fmt in ("hudi", "delta"):
            # XTable-based morph: single batch for Lambda invocation
            self._batches.create(pid, batch_number=0,
                                 partitions=["__xtable_convert__"])
            self._batches.mark_running(0, "xtable-lambda", pipeline_id=pid)
            log.info("Pipeline %s: XTable morph submitted (async) for %s source",
                     pid[:8], fmt)
        else:
            # Parquet-native morph: setup + scan
            self._batches.create(pid, batch_number=0,
                                 partitions=["__morph_setup__"])
            self._batches.create(pid, batch_number=1,
                                 partitions=["__morph_initial_load__"])
            self._batches.mark_running(0, "in-process", pipeline_id=pid)
            log.info("Pipeline %s: morph submitted (async), batches created",
                     pid[:8])

        return self._repo.get(pid)

    def _execute_setup(self, pipeline: dict) -> bool:
        """Batch 0: Create tracker + fetch schema + create target table.

        Returns True on success, False on failure (pipeline marked failed).
        """
        pid = pipeline["id"]
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        s3_prefix = src.get("location", "")
        region = src.get("region", "us-east-1")
        source_database = src.get("database", "")
        source_table = src.get("table_name", "")

        # Phase 0: Create tracker BEFORE scanning
        source_location = pipeline.get("source_location", "") or s3_prefix
        if source_location:
            try:
                tracker_result = self._tracker.create_tracker(
                    pid, source_location,
                    source_account=pipeline.get("source_account", ""))
                log.info("Pipeline %s: tracker active — %s",
                         pid[:8], tracker_result.get("status"))
            except Exception as e:
                log.error("Pipeline %s: tracker creation failed: %s",
                          pid[:8], e)
                self._batches.mark_failed(0, error_message=str(e),
                                          pipeline_id=pid)
                self._repo.update_status(
                    pid, "failed",
                    error_message=f"Tracker creation failed: {e}")
                return False

        # Phase 1: Fetch schema from Glue catalog
        log.info("Pipeline %s: fetching schema from Glue: %s.%s",
                 pid[:8], source_database, source_table)
        try:
            glue_schema = _fetch_glue_schema(
                source_database, source_table, region)
        except Exception as e:
            log.error("Pipeline %s: Glue schema fetch failed: %s",
                      pid[:8], e)
            self._batches.mark_failed(0, error_message=str(e),
                                      pipeline_id=pid)
            self._repo.update_status(
                pid, "failed",
                error_message=f"Glue schema fetch failed: {e}")
            return False

        columns = glue_schema["columns"]
        partition_keys = glue_schema["partition_keys"]
        partition_columns = [pk["name"] for pk in
                             src.get("partition_keys", [])]
        if not partition_columns and partition_keys:
            partition_columns = [pk["name"] for pk in partition_keys]

        schema = schema_from_glue(columns, partition_keys)
        p_spec = build_partition_spec(partition_columns, schema)

        # Phase 2: Create target Iceberg table
        try:
            builder = self._get_builder(pipeline)
            builder.ensure_table(
                tgt.get("database", ""),
                tgt.get("table_name", ""),
                schema, p_spec,
                location=tgt.get("location", ""))
        except Exception as e:
            log.error("Pipeline %s: target table creation failed: %s",
                      pid[:8], e)
            self._batches.mark_failed(0, error_message=str(e),
                                      pipeline_id=pid)
            self._repo.update_status(
                pid, "failed",
                error_message=f"Target table creation failed: {e}")
            return False

        # Setup complete
        self._batches.mark_completed(0, pipeline_id=pid)
        log.info("Pipeline %s: setup complete (tracker + schema + table)",
                 pid[:8])
        return True

    def _execute_initial_load(self, pipeline: dict) -> bool:
        """Batch 1: Scan files and build initial manifests.

        Returns True on success, False on failure.
        """
        pid = pipeline["id"]
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        s3_prefix = src.get("location", "")
        region = src.get("region", "us-east-1")
        partition_columns = [pk["name"] for pk in
                             src.get("partition_keys", [])]

        self._batches.mark_running(1, "in-process", pipeline_id=pid)

        try:
            # List all files
            log.info("Pipeline %s: listing files under %s",
                     pid[:8], s3_prefix)
            file_list = list_data_files(s3_prefix, region)

            if not file_list:
                log.warning("Pipeline %s: no files found", pid[:8])
                self._batches.mark_completed(1, pipeline_id=pid)
                self._repo.update_sync_status(pid, "in_sync")
                return True

            file_paths = [f["full_path"] for f in file_list]
            file_sizes = {f["full_path"]: f["size"] for f in file_list}
            total_bytes = sum(f["size"] for f in file_list)
            total_file_count = len(file_list)

            # Load target table
            builder = self._get_builder(pipeline)
            table = builder._catalog.load_table(
                f"{tgt.get('database', '')}.{tgt.get('table_name', '')}")

            # Restartability: skip already-committed files
            current_snapshot = table.current_snapshot()
            if current_snapshot is not None:
                log.info("Pipeline %s: checking for already-committed files",
                         pid[:8])
                existing_paths = set()
                try:
                    for task in table.scan().plan_files():
                        existing_paths.add(task.file.file_path)
                except Exception as e:
                    log.warning("Pipeline %s: scan existing failed, "
                                "starting fresh: %s", pid[:8], e)
                    existing_paths = set()

                if existing_paths:
                    before = len(file_paths)
                    file_paths = [fp for fp in file_paths
                                  if fp not in existing_paths]
                    file_sizes = {fp: sz for fp, sz in file_sizes.items()
                                  if fp not in existing_paths}
                    log.info("Pipeline %s: resuming — %d already done, "
                             "%d remaining",
                             pid[:8], before - len(file_paths),
                             len(file_paths))

            already_done = total_file_count - len(file_paths)

            if not file_paths:
                log.info("Pipeline %s: all files already committed",
                         pid[:8])
                self._repo.update_progress(
                    pid, bytes_processed=total_bytes,
                    partitions_completed=total_file_count,
                    partitions_total=total_file_count)
            else:
                self._repo.update_progress(
                    pid,
                    bytes_processed=int(
                        total_bytes * already_done / total_file_count),
                    partitions_completed=already_done,
                    partitions_total=total_file_count)

                def on_progress(files_done: int, files_total: int):
                    total_done = already_done + files_done
                    self._repo.update_progress(
                        pid,
                        bytes_processed=int(
                            total_bytes * total_done / total_file_count),
                        partitions_completed=total_done,
                        partitions_total=total_file_count)

                builder.add_files_to_table(
                    table, file_paths,
                    file_sizes=file_sizes,
                    on_progress=on_progress)

            # Final progress
            self._repo.update_progress(
                pid, bytes_processed=total_bytes,
                partitions_completed=total_file_count,
                partitions_total=total_file_count)

            # Drain events that arrived during scan
            initial_paths = set(f["full_path"] for f in file_list)
            self._drain_gap_events(pid, pipeline, builder, table,
                                   initial_paths, s3_prefix,
                                   partition_columns)

            self._batches.mark_completed(1, pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: initial morph load complete, "
                     "%d files committed", pid[:8], total_file_count)
            return True

        except Exception as e:
            log.exception("Pipeline %s: morph initial load failed",
                          pid[:8])
            self._batches.mark_failed(1, error_message=str(e)[:500],
                                      pipeline_id=pid)
            self._repo.update_status(
                pid, "failed", error_message=str(e)[:500])
            return False

    def _drain_gap_events(self, pid: str, pipeline: dict,
                          builder, table,
                          initial_paths: set, s3_prefix: str,
                          partition_columns: list):
        """Drain events that arrived during the scan window."""
        gap_events = self._tracking.get_pending(pid)
        if not gap_events:
            return

        new_in_gap: list[str] = []
        deleted_in_gap: list[FileMetadata] = []

        for event in gap_events:
            full_path = f"s3://{event['bucket']}/{event['key']}"
            if event["event_type"] in ("PUT", "ObjectCreated", "created"):
                if full_path in initial_paths:
                    self._tracking.mark_processed(event["id"],
                                                  pipeline_id=pid)
                else:
                    new_in_gap.append(full_path)
                    self._tracking.mark_processed(event["id"],
                                                  pipeline_id=pid)
            elif event["event_type"] in ("DELETE", "ObjectRemoved",
                                         "deleted"):
                if full_path in initial_paths:
                    pv = event.get("partition_values")
                    if isinstance(pv, str):
                        try:
                            pv = json.loads(pv)
                        except (json.JSONDecodeError, TypeError):
                            pv = None
                    if not pv:
                        pv = parse_partition_values(
                            full_path, s3_prefix, partition_columns)
                    deleted_in_gap.append(FileMetadata(
                        file_path=full_path,
                        partition_values=pv or {},
                        record_count=0,
                        file_size_bytes=event.get("size_bytes", 0),
                        status="DELETED"))
                self._tracking.mark_processed(event["id"],
                                              pipeline_id=pid)
            else:
                self._tracking.mark_processed(event["id"],
                                              pipeline_id=pid)

        if new_in_gap:
            builder.add_files_incremental(table, new_in_gap)
            log.info("Pipeline %s: added %d gap file(s)",
                     pid[:8], len(new_in_gap))
        if deleted_in_gap:
            builder.commit_incremental(table, [], deleted_in_gap)
            log.info("Pipeline %s: removed %d gap file(s)",
                     pid[:8], len(deleted_in_gap))

    # ═══════════════════════════════════════════════════════════════
    # STATE MACHINE HOOKS
    # ═══════════════════════════════════════════════════════════════

    def submit_batch(self, pipeline: dict, batch: dict) -> dict:
        """Mark a pending batch as running so execute_batch picks it up next cycle."""
        pid = pipeline["id"]
        self._batches.mark_running(batch["batch_number"], "in-process", pipeline_id=pid)
        return self._repo.get(pid)

    def execute_batch(self, pipeline: dict, batch: dict) -> dict:
        """Execute an in-process batch — spawns background thread for heavy work.
        
        Routes to XTable or Parquet based on batch partition markers.
        Returns immediately — progress tracked via DynamoDB batch/pipeline updates.
        """
        pid = pipeline["id"]
        batch_job_ref = batch.get("emr_job_run_id", "")

        # If already dispatched to a background thread, don't re-dispatch
        if batch_job_ref == "thread:running":
            return pipeline
        if pid in MorphStrategy._active_threads:
            log.debug("Pipeline %s: background thread already active, skipping", pid[:8])
            return pipeline

        partitions = batch.get("partitions", "")
        if isinstance(partitions, str):
            try:
                partitions = json.loads(partitions)
            except (json.JSONDecodeError, TypeError):
                partitions = [partitions]

        if "__xtable_convert__" in partitions:
            # XTable Lambda is fast enough to run inline (synchronous call)
            return self._execute_xtable_batch(pipeline)
        elif "__morph_setup__" in partitions:
            # Setup is quick (create tracker + schema + table) — run inline
            if not self._execute_setup(pipeline):
                return self._repo.get(pid)
            batch1 = self._batches.get_by_number(pid, 1)
            if batch1 and batch1.get("status") == "pending":
                self._batches.mark_running(1, "in-process", pipeline_id=pid)
            return self._repo.get(pid)
        elif "__morph_initial_load__" in partitions:
            # Heavy, in-process operation — spawn in a background thread, but only
            # if a global morph-load slot is available (0.1). If not, leave the
            # batch pending and let the next reconcile sweep retry — this bounds
            # total concurrent morph loads (and their footer-read thread pools)
            # so many morph pipelines can't exhaust the engine host.
            sem = self._get_load_semaphore()
            if not sem.acquire(blocking=False):
                log.info("Pipeline %s: morph load deferred — %d concurrent slots in use",
                         pid[:8], self._MORPH_MAX_CONCURRENT)
                return pipeline
            MorphStrategy._active_threads.add(pid)
            self._batches.mark_running(batch["batch_number"], "thread:running", pipeline_id=pid)
            import threading
            t = threading.Thread(
                target=self._run_initial_load_background,
                args=(pipeline,),
                daemon=True,
                name=f"morph-load-{pid[:8]}")
            t.start()
            log.info("Pipeline %s: morph initial load dispatched to background thread", pid[:8])
            return self._repo.get(pid)
        else:
            log.warning("Pipeline %s: unknown batch type: %s", pid[:8], partitions)
            return self._repo.get(pid)

    def _run_initial_load_background(self, pipeline: dict):
        """Background thread: runs the heavy morph file scan + manifest build.
        Updates the pipeline store directly with progress and completion status.
        """
        pid = pipeline["id"]
        try:
            self._execute_initial_load(pipeline)
        except Exception as e:
            log.exception("Pipeline %s: background morph load failed", pid[:8])
            self._batches.mark_failed(1, error_message=str(e)[:500], pipeline_id=pid)
            self._repo.update_status(pid, "failed", error_message=str(e)[:500])
        finally:
            MorphStrategy._active_threads.discard(pid)
            # Release the global morph-load slot acquired in execute_batch (0.1).
            self._get_load_semaphore().release()

    def detect_changes(self, pipeline: dict) -> bool:
        """Check if source has new changes to sync."""
        source_format = pipeline.get("source_format", "").lower()
        if source_format in ("hudi", "delta"):
            return self._detect_xtable_changes(pipeline)
        else:
            return self._detect_parquet_changes(pipeline)

    def execute_sync(self, pipeline: dict, batch_number: int) -> dict:
        """Execute an ongoing sync operation."""
        source_format = pipeline.get("source_format", "").lower()
        if source_format in ("hudi", "delta"):
            return self._execute_xtable_sync(pipeline, batch_number)
        else:
            return self._execute_parquet_sync(pipeline, batch_number)

    def on_initial_load_complete(self, pipeline: dict) -> dict:
        """All batches done — transition to in_sync."""
        self._repo.update_sync_status(pipeline["id"], "in_sync")
        log.info("Pipeline %s: initial load complete, in_sync", pipeline["id"][:8])
        return self._repo.get(pipeline["id"])

    def reconcile(self, pipeline: dict) -> dict:
        """Drive morph pipeline through its lifecycle.

        During initial_load: executes setup (batch 0) then initial scan (batch 1).
        After initial load:
          - Parquet: processes pending S3 events for incremental sync
          - Hudi/Delta: checks commit state and invokes XTable incremental sync
        """
        pid = pipeline["id"]
        sync_status = pipeline.get("sync_status", "pending")
        source_format = pipeline.get("source_format", "").lower()

        # ── Initial Load Phase ──
        if sync_status in ("initial_load", "pending"):
            return self._reconcile_initial_load(pipeline)

        # ── Determine sync path based on source format ──
        is_xtable_source = source_format in ("hudi", "delta")

        # ── Ongoing Sync Phase ──
        # Handle failed sync — retry after backoff
        if sync_status == "failed":
            if not self._sync_interval_elapsed(pipeline):
                return pipeline
            if is_xtable_source:
                self._repo.update_sync_status(pid, "out_of_sync")
                log.info("Pipeline %s: resetting failed XTable sync for retry", pid[:8])
            else:
                self._tracking.reset_failed(pid)
                self._repo.update_sync_status(pid, "out_of_sync")
                log.info("Pipeline %s: resetting failed sync for retry", pid[:8])
            return self._repo.get(pid)

        # XTable sources: check commit state for out_of_sync detection
        if is_xtable_source:
            return self._reconcile_xtable_sync(pipeline)

        # Parquet sources: check S3 object events
        if sync_status == "in_sync":
            # Check if there are pending events — if so, mark out_of_sync
            # immediately for UI visibility, but only process on interval
            pending = self._tracking.get_pending(pid, limit=1)
            if pending:
                self._repo.update_sync_status(pid, "out_of_sync")
                if not self._sync_interval_elapsed(pipeline):
                    return self._repo.get(pid)
            else:
                return pipeline

        # Get pending events
        events = self._tracking.get_pending(pid)
        if not events:
            log.debug("Pipeline %s: no pending events", pid[:8])
            if sync_status != "in_sync":
                self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        log.info("Pipeline %s: processing %d morph event(s)",
                 pid[:8], len(events))

        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        s3_prefix = src.get("location", "")
        partition_columns = [pk["name"] for pk in
                             src.get("partition_keys", [])]

        self._repo.update_sync_status(pid, "syncing")

        # Classify events into adds and deletes
        new_file_paths: list[str] = []
        deleted_files: list[FileMetadata] = []

        for event in events:
            full_path = f"s3://{event['bucket']}/{event['key']}"
            pv = event.get("partition_values")
            if isinstance(pv, str):
                try:
                    pv = json.loads(pv)
                except (json.JSONDecodeError, TypeError):
                    pv = None
            if not pv:
                pv = parse_partition_values(
                    full_path, s3_prefix, partition_columns)

            if event["event_type"] in ("PUT", "ObjectCreated", "created"):
                new_file_paths.append(full_path)
            elif event["event_type"] in ("DELETE", "ObjectRemoved",
                                         "deleted"):
                deleted_files.append(FileMetadata(
                    file_path=full_path,
                    partition_values=pv or {},
                    record_count=0,
                    file_size_bytes=event.get("size_bytes", 0),
                    status="DELETED"))

        if not new_file_paths and not deleted_files:
            for event in events:
                self._tracking.mark_processed(event["id"],
                                              pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        try:
            builder = self._get_builder(pipeline)
            try:
                table = builder._catalog.load_table(
                    f"{tgt.get('database', '')}.{tgt.get('table_name', '')}")
            except Exception as e:
                log.warning("Pipeline %s: target table not found (%s), "
                            "marking events processed and setting out_of_sync",
                            pid[:8], e)
                for event in events:
                    self._tracking.mark_processed(event["id"],
                                                  pipeline_id=pid)
                self._repo.update_sync_status(pid, "out_of_sync")
                return self._repo.get(pid)

            # Add new files via add_files (extracts full column stats)
            added = 0
            if new_file_paths:
                added = builder.add_files_incremental(
                    table, new_file_paths)

            # Remove deleted files
            deleted = 0
            if deleted_files:
                result = builder.commit_incremental(
                    table, [], deleted_files)
                deleted = result["deleted"]

            # Mark all events as processed
            for event in events:
                self._tracking.mark_processed(event["id"],
                                              pipeline_id=pid)

            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: incremental commit — "
                     "%d added, %d deleted",
                     pid[:8], added, deleted)

        except Exception:
            log.exception("Pipeline %s: morph reconcile failed", pid[:8])
            for event in events:
                self._tracking.mark_failed(event["id"],
                                           pipeline_id=pid)
            self._repo.update_sync_status(pid, "failed")

        return self._repo.get(pid)

    def _reconcile_initial_load(self, pipeline: dict) -> dict:
        """Drive the initial load phase.

        For Parquet: batch 0 (setup) → batch 1 (scan).
        For Hudi/Delta: batch 0 (XTable Lambda conversion).
        """
        pid = pipeline["id"]

        # Check batch 0 status
        batch0 = self._batches.get_by_number(pid, 0)
        if not batch0:
            return self._repo.get(pid)

        # Determine if this is XTable or native morph
        partitions = batch0.get("partitions", "")
        if isinstance(partitions, str):
            try:
                partitions = json.loads(partitions)
            except (json.JSONDecodeError, TypeError):
                partitions = [partitions]

        is_xtable = "__xtable_convert__" in partitions

        if is_xtable:
            return self._reconcile_xtable(pipeline, batch0)

        # Native Parquet morph path
        if batch0.get("status") == "running":
            if not self._execute_setup(pipeline):
                return self._repo.get(pid)
            batch1 = self._batches.get_by_number(pid, 1)
            if batch1 and batch1.get("status") == "pending":
                self._batches.mark_running(1, "in-process",
                                           pipeline_id=pid)
            return self._repo.get(pid)

        batch1 = self._batches.get_by_number(pid, 1)
        if batch1 and batch1.get("status") == "running":
            self._execute_initial_load(pipeline)
            return self._repo.get(pid)

        if batch1 and batch1.get("status") == "pending":
            self._batches.mark_running(1, "in-process", pipeline_id=pid)
            return self._repo.get(pid)

        if batch1 and batch1.get("status") == "completed":
            self._repo.update_sync_status(pid, "in_sync")

        return self._repo.get(pid)

    def _reconcile_xtable(self, pipeline: dict, batch0: dict) -> dict:
        """Execute XTable Lambda conversion for Hudi/Delta sources."""
        pid = pipeline["id"]

        if batch0.get("status") != "running":
            # Already completed or failed
            if batch0.get("status") == "completed":
                self._repo.update_sync_status(pid, "in_sync")
            elif batch0.get("status") == "failed":
                # Propagate batch failure to pipeline level
                error_msg = batch0.get("error_message", "XTable conversion failed")
                self._repo.update_status(pid, "failed", error_message=error_msg)
            return self._repo.get(pid)

        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        source_format = src.get("format", "").upper()
        source_location = src.get("location", "")
        target_table = tgt.get("table_name", "")
        target_database = tgt.get("database", "")

        # Normalize location: strip symlink manifest path suffix for Delta tables
        # Glue registers Delta tables with /_symlink_format_manifest/ as location
        # but XTable needs the actual table root (where _delta_log/ lives)
        if "_symlink_format_manifest" in source_location:
            source_location = source_location.split("_symlink_format_manifest")[0].rstrip("/") + "/"

        try:
            from ..xtable.client import XTableClient
            client = XTableClient()

            if not client.is_configured():
                raise ValueError(
                    "XTable Lambda ARN not configured. "
                    "Set ENGINE_XTABLE_LAMBDA_ARN in Settings.")

            # Clean up any previous broken metadata to prevent schema evolution errors
            self._cleanup_stale_metadata(source_location, pipeline)

            result = client.convert(
                source_format=source_format,
                table_base_path=source_location,
                table_name=target_table,
                database_name=target_database,
                sync_mode="FULL")

            log.info("Pipeline %s: XTable conversion complete — %s",
                     pid[:8], result.get("body", {}).get("message", ""))

            # Register the Iceberg table in Glue catalog via pyiceberg
            self._register_iceberg_table(
                pipeline, source_location, target_database, target_table)

            self._batches.mark_completed(0, pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")

            # Store the current commit as last_synced for ongoing sync
            from ..xtable.commit_tracker import has_new_commits
            _, latest = has_new_commits(
                source_format.lower(), source_location, None,
                spec.get("source", {}).get("region", "us-east-1"))
            if latest:
                self._repo.update_metadata(pid, "last_synced_commit", latest)

            log.info("Pipeline %s: XTable morph complete, table in_sync",
                     pid[:8])

        except Exception as e:
            log.error("Pipeline %s: XTable conversion failed: %s",
                      pid[:8], e)
            self._batches.mark_failed(0, error_message=str(e)[:500],
                                      pipeline_id=pid)
            self._repo.update_status(
                pid, "failed", error_message=str(e)[:500])

        return self._repo.get(pid)

    def _reconcile_xtable_sync(self, pipeline: dict) -> dict:
        """Ongoing sync for Hudi/Delta sources via XTable incremental mode.

        Checks if new commits exist in the source table. If so, invokes
        XTable Lambda with INCREMENTAL sync mode to translate new commits.
        """
        pid = pipeline["id"]
        sync_status = pipeline.get("sync_status", "pending")
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        source_format = src.get("format", "").lower()
        source_location = src.get("location", "")
        region = src.get("region", "us-east-1")

        # Normalize location for Delta symlink manifest tables
        if "_symlink_format_manifest" in source_location:
            source_location = source_location.split("_symlink_format_manifest")[0].rstrip("/") + "/"

        # Get last synced commit from pipeline metadata
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            import json as _json
            try:
                spec_json = _json.loads(spec_json)
            except (ValueError, TypeError):
                spec_json = {}
        metadata = (spec_json or {}).get("_metadata", {})
        last_synced = metadata.get("last_synced_commit", "")

        if sync_status == "in_sync":
            # Check for new commits
            from ..xtable.commit_tracker import has_new_commits
            try:
                has_new, latest = has_new_commits(
                    source_format, source_location, last_synced or None, region)
            except Exception as e:
                log.warning("Pipeline %s: commit check failed: %s",
                            pid[:8], e)
                return pipeline

            if not has_new:
                return pipeline

            # New commits detected — mark out_of_sync
            self._repo.update_sync_status(pid, "out_of_sync")
            log.info("Pipeline %s: new %s commits detected (latest=%s, last_synced=%s)",
                     pid[:8], source_format, latest, last_synced)

            if not self._sync_interval_elapsed(pipeline):
                return self._repo.get(pid)

        elif sync_status == "out_of_sync":
            if not self._sync_interval_elapsed(pipeline):
                return self._repo.get(pid)

        # Execute incremental XTable sync
        self._repo.update_sync_status(pid, "syncing")
        try:
            from ..xtable.client import XTableClient
            client = XTableClient()

            if not client.is_configured():
                log.error("Pipeline %s: XTable Lambda ARN not configured",
                          pid[:8])
                self._repo.update_sync_status(pid, "failed")
                return self._repo.get(pid)

            result = client.convert(
                source_format=source_format.upper(),
                table_base_path=source_location,
                table_name=tgt.get("table_name", ""),
                database_name=tgt.get("database", ""),
                sync_mode="INCREMENTAL")

            log.info("Pipeline %s: XTable incremental sync complete — %s",
                     pid[:8], result.get("body", {}).get("message", ""))

            # Update last synced commit
            from ..xtable.commit_tracker import has_new_commits
            _, latest = has_new_commits(
                source_format, source_location, None, region)
            if latest:
                self._repo.update_metadata(pid, "last_synced_commit", latest)

            # Update Glue metadata_location pointer to latest metadata file
            self._update_iceberg_metadata_pointer(
                pipeline, source_location,
                tgt.get("database", ""), tgt.get("table_name", ""))

            self._repo.update_sync_status(pid, "in_sync")

        except Exception as e:
            log.error("Pipeline %s: XTable incremental sync failed: %s",
                      pid[:8], e)
            self._repo.update_sync_status(pid, "failed")

        return self._repo.get(pid)

    # ═══════════════════════════════════════════════════════════════
    # STATE MACHINE SYNC HOOKS (XTable + Parquet)
    # ═══════════════════════════════════════════════════════════════

    def _execute_xtable_batch(self, pipeline: dict) -> dict:
        """Execute XTable Lambda FULL conversion (batch 0)."""
        pid = pipeline["id"]
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        source_format = src.get("format", "").upper()
        source_location = src.get("location", "")
        target_table = tgt.get("table_name", "")
        target_database = tgt.get("database", "")

        if "_symlink_format_manifest" in source_location:
            source_location = source_location.split("_symlink_format_manifest")[0].rstrip("/") + "/"

        try:
            from ..xtable.client import XTableClient
            client = XTableClient()
            if not client.is_configured():
                raise ValueError("XTable Lambda ARN not configured. Set ENGINE_XTABLE_LAMBDA_ARN in Settings.")

            self._cleanup_stale_metadata(source_location, pipeline)
            result = client.convert(
                source_format=source_format, table_base_path=source_location,
                table_name=target_table, database_name=target_database, sync_mode="FULL")
            log.info("Pipeline %s: XTable conversion complete — %s",
                     pid[:8], result.get("body", {}).get("message", ""))

            self._register_iceberg_table(pipeline, source_location, target_database, target_table)
            self._batches.mark_completed(0, pipeline_id=pid)

            from ..xtable.commit_tracker import has_new_commits
            _, latest = has_new_commits(source_format.lower(), source_location, None, src.get("region", "us-east-1"))
            if latest:
                self._repo.update_metadata(pid, "last_synced_commit", latest)
            log.info("Pipeline %s: XTable morph complete", pid[:8])
        except Exception as e:
            log.error("Pipeline %s: XTable conversion failed: %s", pid[:8], e)
            self._batches.mark_failed(0, error_message=str(e)[:500], pipeline_id=pid)
            self._repo.update_status(pid, "failed", error_message=str(e)[:500])
        return self._repo.get(pid)

    def _detect_xtable_changes(self, pipeline: dict) -> bool:
        """Check if Hudi/Delta source has new commits."""
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        source_format = src.get("format", "").lower()
        source_location = src.get("location", "")
        region = src.get("region", "us-east-1")

        if "_symlink_format_manifest" in source_location:
            source_location = source_location.split("_symlink_format_manifest")[0].rstrip("/") + "/"

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            try:
                spec_json = json.loads(spec_json)
            except (ValueError, TypeError):
                spec_json = {}
        metadata = (spec_json or {}).get("_metadata", {})
        last_synced = metadata.get("last_synced_commit", "")

        from ..xtable.commit_tracker import has_new_commits
        has_new, latest = has_new_commits(source_format, source_location, last_synced or None, region)
        if has_new:
            log.info("Pipeline %s: new %s commits (latest=%s, last_synced=%s)",
                     pipeline["id"][:8], source_format, latest, last_synced)
        return has_new

    def _detect_parquet_changes(self, pipeline: dict) -> bool:
        """Check if Parquet source has pending S3 events."""
        pending = self._tracking.get_pending(pipeline["id"], limit=1)
        return len(pending) > 0

    def _execute_xtable_sync(self, pipeline: dict, batch_number: int) -> dict:
        """Execute XTable incremental sync + create batch record."""
        pid = pipeline["id"]
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        source_format = src.get("format", "").lower()
        source_location = src.get("location", "")
        region = src.get("region", "us-east-1")

        if "_symlink_format_manifest" in source_location:
            source_location = source_location.split("_symlink_format_manifest")[0].rstrip("/") + "/"

        # Create sync batch
        self._batches.create(pid, batch_number=batch_number,
                             partitions=[f"__sync__xtable__{source_format}__"])
        self._batches.mark_running(batch_number, "xtable-incremental", pipeline_id=pid)

        try:
            from ..xtable.client import XTableClient
            client = XTableClient()
            if not client.is_configured():
                raise ValueError("XTable Lambda ARN not configured")

            result = client.convert(
                source_format=source_format.upper(), table_base_path=source_location,
                table_name=tgt.get("table_name", ""), database_name=tgt.get("database", ""),
                sync_mode="INCREMENTAL")
            log.info("Pipeline %s: XTable incremental sync complete — %s",
                     pid[:8], result.get("body", {}).get("message", ""))

            from ..xtable.commit_tracker import has_new_commits
            _, latest = has_new_commits(source_format, source_location, None, region)
            if latest:
                self._repo.update_metadata(pid, "last_synced_commit", latest)

            self._update_iceberg_metadata_pointer(
                pipeline, source_location, tgt.get("database", ""), tgt.get("table_name", ""))

            self._batches.mark_completed(batch_number, pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")
        except Exception as e:
            log.error("Pipeline %s: XTable incremental sync failed: %s", pid[:8], e)
            self._batches.mark_failed(batch_number, error_message=str(e)[:500], pipeline_id=pid)
            self._repo.update_sync_status(pid, "failed")

        return self._repo.get(pid)

    def _execute_parquet_sync(self, pipeline: dict, batch_number: int) -> dict:
        """Execute Parquet morph sync (process S3 events) + create batch record."""
        pid = pipeline["id"]
        spec = self._get_spec(pipeline)
        src = spec.get("source", {})
        tgt = spec.get("target", {})
        s3_prefix = src.get("location", "")
        partition_columns = [pk["name"] for pk in src.get("partition_keys", [])]

        events = self._tracking.get_pending(pid)
        if not events:
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        # Create sync batch
        self._batches.create(pid, batch_number=batch_number,
                             partitions=[f"__sync__morph__{len(events)}_events__"])
        self._batches.mark_running(batch_number, "in-process", pipeline_id=pid)

        # Classify events
        new_file_paths = []
        deleted_files = []
        for event in events:
            full_path = f"s3://{event['bucket']}/{event['key']}"
            pv = event.get("partition_values")
            if isinstance(pv, str):
                try:
                    pv = json.loads(pv)
                except (json.JSONDecodeError, TypeError):
                    pv = None
            if not pv:
                pv = parse_partition_values(full_path, s3_prefix, partition_columns)
            if event["event_type"] in ("PUT", "ObjectCreated", "created"):
                new_file_paths.append(full_path)
            elif event["event_type"] in ("DELETE", "ObjectRemoved", "deleted"):
                deleted_files.append(FileMetadata(
                    file_path=full_path, partition_values=pv or {},
                    record_count=0, file_size_bytes=event.get("size_bytes", 0), status="DELETED"))

        if not new_file_paths and not deleted_files:
            for event in events:
                self._tracking.mark_processed(event["id"], pipeline_id=pid)
            self._batches.mark_completed(batch_number, pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        try:
            builder = self._get_builder(pipeline)
            table = builder._catalog.load_table(
                f"{tgt.get('database', '')}.{tgt.get('table_name', '')}")

            added = 0
            if new_file_paths:
                added = builder.add_files_incremental(table, new_file_paths)
            deleted = 0
            if deleted_files:
                result = builder.commit_incremental(table, [], deleted_files)
                deleted = result["deleted"]

            for event in events:
                self._tracking.mark_processed(event["id"], pipeline_id=pid)
            self._batches.mark_completed(batch_number, pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: morph sync — %d added, %d deleted", pid[:8], added, deleted)
        except Exception as e:
            log.exception("Pipeline %s: morph sync failed", pid[:8])
            for event in events:
                self._tracking.mark_failed(event["id"], pipeline_id=pid)
            self._batches.mark_failed(batch_number, error_message=str(e)[:500], pipeline_id=pid)
            self._repo.update_sync_status(pid, "failed")

        return self._repo.get(pid)

    def _register_iceberg_table(self, pipeline: dict, source_location: str,
                                database: str, table_name: str):
        """Register the XTable-generated Iceberg metadata in Glue catalog.
        
        XTable generates UUID-named metadata files which Athena doesn't accept.
        We copy the latest metadata to v1.metadata.json (Iceberg standard naming)
        and register with that path. Also reads schema from metadata to populate
        Glue columns (required by Athena).
        """
        pid = pipeline["id"]
        try:
            metadata_location = self._find_latest_metadata(source_location, pipeline)
            if not metadata_location:
                log.warning("Pipeline %s: no Iceberg metadata found at %s",
                            pid[:8], source_location)
                return

            # If metadata file is UUID-named, copy it to vN.metadata.json for Athena compat
            metadata_location = self._ensure_athena_compatible_metadata(
                metadata_location, source_location, pipeline)

            # Read schema from metadata file for Glue column registration
            columns = self._read_columns_from_metadata(metadata_location, pipeline)

            import boto3
            region = pipeline.get("source_region", "us-east-1")
            glue = boto3.client("glue", region_name=region)

            table_input = {
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "table_type": "ICEBERG",
                    "metadata_location": metadata_location,
                },
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": source_location,
                    "InputFormat": "org.apache.hadoop.mapred.FileInputFormat",
                    "OutputFormat": "org.apache.hadoop.mapred.FileOutputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                },
            }

            # Check if table already exists
            try:
                glue.get_table(DatabaseName=database, Name=table_name)
                glue.update_table(DatabaseName=database, TableInput=table_input)
                log.info("Pipeline %s: updated Iceberg table %s.%s",
                         pid[:8], database, table_name)
            except glue.exceptions.EntityNotFoundException:
                glue.create_table(DatabaseName=database, TableInput=table_input)
                log.info("Pipeline %s: registered Iceberg table %s.%s in Glue",
                         pid[:8], database, table_name)

        except Exception as e:
            log.error("Pipeline %s: failed to register Iceberg table: %s", pid[:8], e)
            # Non-fatal — table can be registered manually

    def _update_iceberg_metadata_pointer(self, pipeline: dict, source_location: str,
                                         database: str, table_name: str):
        """Update the Glue table's metadata_location to the latest metadata file."""
        pid = pipeline["id"]
        try:
            metadata_location = self._find_latest_metadata(source_location, pipeline)
            if not metadata_location:
                return

            # Ensure Athena compatibility
            metadata_location = self._ensure_athena_compatible_metadata(
                metadata_location, source_location, pipeline)

            # Read updated schema
            columns = self._read_columns_from_metadata(metadata_location, pipeline)

            import boto3
            region = pipeline.get("source_region", "us-east-1")
            glue = boto3.client("glue", region_name=region)

            glue.update_table(
                DatabaseName=database,
                TableInput={
                    "Name": table_name,
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {
                        "table_type": "ICEBERG",
                        "metadata_location": metadata_location,
                    },
                    "StorageDescriptor": {
                        "Columns": columns,
                        "Location": source_location,
                        "InputFormat": "org.apache.hadoop.mapred.FileInputFormat",
                        "OutputFormat": "org.apache.hadoop.mapred.FileOutputFormat",
                        "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                    },
                })
            log.info("Pipeline %s: updated metadata_location for %s.%s",
                     pid[:8], database, table_name)
        except Exception as e:
            log.warning("Pipeline %s: failed to update metadata pointer: %s", pid[:8], e)

    def _read_columns_from_metadata(self, metadata_location: str, pipeline: dict) -> list[dict]:
        """Read column definitions from an Iceberg metadata.json file on S3."""
        import boto3

        _ICEBERG_TYPE_MAP = {
            "string": "string", "long": "bigint", "int": "int", "integer": "int",
            "boolean": "boolean", "double": "double", "float": "float",
            "date": "date", "timestamp": "timestamp", "timestamptz": "timestamp",
            "binary": "binary", "fixed": "binary", "uuid": "string",
        }

        try:
            path = metadata_location
            if not path.startswith("s3://"):
                return []

            without_scheme = path[5:]
            bucket, _, key = without_scheme.partition("/")

            region = pipeline.get("source_region", "us-east-1")
            s3 = boto3.client("s3", region_name=region)
            resp = s3.get_object(Bucket=bucket, Key=key)
            metadata = json.loads(resp["Body"].read().decode())

            # Find current schema
            current_schema_id = metadata.get("current-schema-id", 0)
            schema = None
            for s in metadata.get("schemas", []):
                if s.get("schema-id") == current_schema_id:
                    schema = s
                    break

            if not schema or not schema.get("fields"):
                # Fallback to last schema with fields
                for s in reversed(metadata.get("schemas", [])):
                    if s.get("fields"):
                        schema = s
                        break

            if not schema or not schema.get("fields"):
                return []

            columns = []
            for field in schema["fields"]:
                iceberg_type = field.get("type", "string")
                if isinstance(iceberg_type, dict):
                    # Complex type (struct, list, map)
                    glue_type = "string"
                elif iceberg_type.startswith("decimal"):
                    glue_type = iceberg_type  # e.g., "decimal(38,10)"
                else:
                    glue_type = _ICEBERG_TYPE_MAP.get(iceberg_type.lower(), "string")
                columns.append({"Name": field["name"], "Type": glue_type})

            return columns

        except Exception as e:
            log.warning("Pipeline %s: failed to read metadata schema: %s",
                        pipeline.get("id", "?")[:8], e)
            return []

    def _ensure_athena_compatible_metadata(self, metadata_location: str,
                                           source_location: str,
                                           pipeline: dict) -> str:
        """If metadata file is UUID-named, copy it to vN.metadata.json for Athena.
        
        Athena rejects UUID-named metadata files. We copy to a versioned name
        and return the new path.
        """
        import boto3

        basename = metadata_location.rsplit("/", 1)[-1]
        # Already versioned (v1.metadata.json, v2.metadata.json)
        if basename.startswith("v") and basename[1:].split(".")[0].isdigit():
            return metadata_location

        # Need to find the next version number
        path = source_location.rstrip("/")
        without_scheme = path[5:]
        bucket, _, prefix = without_scheme.partition("/")
        metadata_prefix = f"{prefix}/metadata/" if prefix else "metadata/"

        region = pipeline.get("source_region", "us-east-1")
        s3 = boto3.client("s3", region_name=region)

        # Find existing vN files to determine next version
        paginator = s3.get_paginator("list_objects_v2")
        max_version = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=metadata_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                fname = key.rsplit("/", 1)[-1]
                if fname.startswith("v") and fname.endswith(".metadata.json"):
                    try:
                        v = int(fname[1:].split(".")[0])
                        max_version = max(max_version, v)
                    except ValueError:
                        pass

        next_version = max_version + 1
        new_key = f"{metadata_prefix}v{next_version}.metadata.json"

        # Copy the UUID-named file to the versioned name
        source_key = metadata_location[5:].split("/", 1)[1]  # strip s3://bucket/
        try:
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": source_key},
                Key=new_key)
            new_location = f"s3://{bucket}/{new_key}"
            log.info("Pipeline %s: renamed metadata %s → %s",
                     pipeline["id"][:8], basename, f"v{next_version}.metadata.json")
            return new_location
        except Exception as e:
            log.warning("Pipeline %s: failed to copy metadata to versioned name: %s",
                        pipeline["id"][:8], e)
            # Fall back to original (may not work with Athena)
            return metadata_location

    def _find_latest_metadata(self, source_location: str, pipeline: dict) -> str | None:
        """Find the latest Iceberg metadata.json file in the table's metadata/ dir."""
        import boto3

        path = source_location.rstrip("/")
        if not path.startswith("s3://"):
            return None

        without_scheme = path[5:]
        bucket, _, prefix = without_scheme.partition("/")
        metadata_prefix = f"{prefix}/metadata/" if prefix else "metadata/"

        region = pipeline.get("source_region", "us-east-1")
        s3 = boto3.client("s3", region_name=region)
        paginator = s3.get_paginator("list_objects_v2")

        metadata_files = []
        for page in paginator.paginate(Bucket=bucket, Prefix=metadata_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                basename = key.rsplit("/", 1)[-1]
                # Match any metadata.json file (v1.metadata.json, <uuid>.metadata.json, etc.)
                if basename.endswith(".metadata.json"):
                    metadata_files.append((key, obj.get("LastModified")))

        if not metadata_files:
            return None

        # Prefer vN.metadata.json files (Iceberg standard), fall back to UUID-named
        v_files = [(k, t) for k, t in metadata_files if k.rsplit("/", 1)[-1][0] == "v"]
        if v_files:
            # Sort by version number (highest = latest)
            def _extract_version(item):
                fname = item[0].rsplit("/", 1)[-1]
                try:
                    return int(fname[1:].split(".")[0])
                except ValueError:
                    return 0
            v_files.sort(key=_extract_version, reverse=True)
            latest_key = v_files[0][0]
        else:
            # UUID-named: sort by last modified (most recent)
            metadata_files.sort(key=lambda x: x[1] or "", reverse=True)
            latest_key = metadata_files[0][0]

        return f"s3://{bucket}/{latest_key}"

    def _cleanup_stale_metadata(self, source_location: str, pipeline: dict):
        """Remove any existing Iceberg metadata dir before a FULL XTable sync.
        
        XTable has a bug where it creates an empty v1.metadata.json first,
        then fails to add required columns on schema evolution. Starting fresh avoids this.
        """
        import boto3

        path = source_location.rstrip("/")
        if not path.startswith("s3://"):
            return

        without_scheme = path[5:]
        bucket, _, prefix = without_scheme.partition("/")
        metadata_prefix = f"{prefix}/metadata/" if prefix else "metadata/"

        region = pipeline.get("source_region", "us-east-1")
        s3 = boto3.client("s3", region_name=region)
        paginator = s3.get_paginator("list_objects_v2")

        objects_to_delete = []
        for page in paginator.paginate(Bucket=bucket, Prefix=metadata_prefix):
            for obj in page.get("Contents", []):
                objects_to_delete.append({"Key": obj["Key"]})

        if objects_to_delete:
            # Delete in batches of 1000
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            log.info("Pipeline %s: cleaned up %d stale metadata files before XTable FULL sync",
                     pipeline["id"][:8], len(objects_to_delete))

    def _sync_interval_elapsed(self, pipeline: dict) -> bool:
        updated_at = pipeline.get("updated_at", "")
        if not updated_at:
            return True
        try:
            last = datetime.fromisoformat(
                updated_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return (now - last).total_seconds() >= _SYNC_INTERVAL
        except (ValueError, TypeError):
            return True

    def cancel(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        # Clean up the tracker if it exists
        source_location = pipeline.get("source_location", "")
        if source_location:
            try:
                self._tracker.destroy_tracker(pid, source_location)
                log.info("Pipeline %s: tracker removed", pid[:8])
            except Exception as e:
                log.warning("Pipeline %s: failed to remove tracker: %s",
                            pid[:8], e)
        return self._repo.update_status(pid, "cancelled")
