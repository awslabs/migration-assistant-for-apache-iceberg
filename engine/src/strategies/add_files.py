# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Add Files strategy — register Hive-Parquet files into Iceberg via add_files procedure.

Uses Iceberg's native `add_files` procedure (via Spark on EMR Serverless) to register
existing Hive-Parquet data files into an Iceberg table without any data movement.

Work is chunked by partition for tracking and restartability:
  Batch 0:   Create target Iceberg table (EMR Serverless / Spark DDL)
  Batch 1-N: Run add_files for a chunk of partitions each

Ongoing sync uses S3 object tracking to detect changed partitions and
re-runs add_files with overwrite semantics for affected partitions.

Constraints:
  - Source format MUST be Hive-Parquet (partitioned)
  - Execution engine MUST be EMR Serverless (not Athena)
  - Target type MUST be s3_iceberg (files stay in place)

Lifecycle:
  1. submit:     Create tracker, discover partitions, create batched work plan
  2. reconcile:  State machine drives batches (EMR jobs)
  3. sync:       Object tracking detects changes, submits EMR job for affected partitions
  4. cancel:     Cancel running job, destroy tracker
"""

import json
import logging
import os

import boto3

from .base import MigrationStrategy
from .common import s3tables_spark_props
from ..db import PipelineRepo, ObjectTrackingRepo, BatchRepo
from ..engines.emr import EmrServerlessClient, _config as emr_config
from ..tracking import TrackingManager

log = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 10
_SYNC_INTERVAL = int(os.getenv("ENGINE_ADD_FILES_INTERVAL_SECONDS", "3600"))


def _get_script_path(script_name: str) -> str:
    """Resolve script S3 path from scripts URI or log URI."""
    scripts_uri = os.getenv("ENGINE_EMR_SCRIPTS_URI", "").rstrip("/")
    if not scripts_uri:
        scripts_uri = (os.getenv("ENGINE_EMR_LOG_URI", "") or
                       emr_config.get("log_uri", "")).rstrip("/")
    # Avoid double /scripts/ if the URI already ends with /scripts
    if scripts_uri.endswith("/scripts"):
        return f"{scripts_uri}/{script_name}"
    return f"{scripts_uri}/scripts/{script_name}"


def _build_spark_props(pipeline: dict) -> dict[str, str]:
    """Build Spark properties for Iceberg catalog access."""
    target_type = pipeline.get("target_type", "s3_iceberg")
    props = {
        # Configure default catalog to use Hive with Glue metastore
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.hive.metastore.client.factory.class":
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        # Iceberg catalog for target tables
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse": pipeline.get("target_location", ""),
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    }
    if target_type == "s3_tables":
        # S3 Tables via Iceberg GlueCatalog with glue.id + warehouse (EMR >= 7.5).
        props.update(s3tables_spark_props(pipeline))
    return props


def _discover_partitions(pipeline: dict) -> list[list[str]]:
    """Discover partition values from Glue catalog.

    Returns a list of partition value lists, e.g.:
      [["2024-01-01"], ["2024-01-02"], ...]
    for a single partition key, or:
      [["us-east-1", "2024-01-01"], ["eu-west-1", "2024-01-02"], ...]
    for composite partition keys.
    """
    spec_json = pipeline.get("spec_json")
    if isinstance(spec_json, str):
        spec_json = json.loads(spec_json)

    src = (spec_json or {}).get("source", {})
    database = src.get("database", pipeline.get("source_database", ""))
    table_name = src.get("table_name", pipeline.get("source_table", ""))
    region = src.get("region", "us-east-1")

    glue = boto3.client("glue", region_name=region)
    partitions = []
    next_token = None

    while True:
        kwargs = {
            "DatabaseName": database,
            "TableName": table_name,
        }
        if next_token:
            kwargs["NextToken"] = next_token
        resp = glue.get_partitions(**kwargs)
        for p in resp.get("Partitions", []):
            partitions.append(p["Values"])
        next_token = resp.get("NextToken")
        if not next_token:
            break

    return partitions


def _chunk_partitions(partitions: list[list[str]],
                      batch_size: int) -> list[list[list[str]]]:
    """Split partition list into chunks of batch_size."""
    return [partitions[i:i + batch_size]
            for i in range(0, len(partitions), batch_size)]


class AddFilesStrategy(MigrationStrategy):
    """Register Hive-Parquet files into Iceberg via add_files procedure on EMR Serverless."""

    def __init__(self):
        self._emr = EmrServerlessClient()
        self._repo = PipelineRepo()
        self._tracking = ObjectTrackingRepo()
        self._batches = BatchRepo()
        self._tracker = TrackingManager()

    # ═══════════════════════════════════════════════════════════════
    # REQUIRED: validate / submit / cancel
    # ═══════════════════════════════════════════════════════════════

    def validate(self, pipeline: dict) -> tuple[bool, str]:
        """Validate pipeline is eligible for add_files strategy."""
        fmt = pipeline.get("source_format", "").lower()
        if fmt != "parquet":
            return False, (
                "Add Files strategy only supports Hive-Parquet sources "
                f"(got '{fmt}')")

        # Must be partitioned
        is_partitioned = pipeline.get("source_partitioned")
        if not is_partitioned:
            spec_json = pipeline.get("spec_json")
            if isinstance(spec_json, str):
                try:
                    spec_json = json.loads(spec_json)
                except (ValueError, TypeError):
                    spec_json = None
            pk = (spec_json or {}).get("source", {}).get("partition_keys", [])
            pk_direct = pipeline.get("source_partition_keys")
            if isinstance(pk_direct, str):
                try:
                    pk_direct = json.loads(pk_direct)
                except (ValueError, TypeError):
                    pk_direct = []
            if not pk and not pk_direct:
                return False, (
                    "Add Files strategy requires partitioned tables")

        # Must use EMR Serverless
        engine = pipeline.get("execution_engine", "emr")
        if engine == "athena":
            return False, (
                "Add Files strategy requires EMR Serverless "
                "(Athena is not supported)")

        # Must target s3_iceberg
        target_type = pipeline.get("target_type", "s3_iceberg")
        if target_type not in ("s3_iceberg",):
            return False, (
                f"Add Files strategy only supports s3_iceberg target "
                f"(got '{target_type}')")

        if not pipeline.get("source_location"):
            return False, "Source table has no S3 location"

        return True, ""

    def submit(self, pipeline: dict) -> dict:
        """Submit add_files pipeline: create tracker, discover partitions, create batches."""
        pid = pipeline["id"]
        # Use the pipeline's EMR config (application_id/region the app injected
        # from Settings) rather than the engine env defaults, so StartJobRun
        # targets the right regional application.
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        ok, reason = self.validate(pipeline)
        if not ok:
            return self._repo.update_status(pid, "failed",
                                            error_message=reason)

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        src = (spec_json or {}).get("source", {})
        batch_size = (spec_json or {}).get(
            "strategy", {}).get("batch_size", _DEFAULT_BATCH_SIZE)

        # Mark running immediately
        self._repo.update_status(pid, "running")
        self._repo.update_sync_status(pid, "initial_load")

        # Clear any stale batches from a previous failed run (retry scenario)
        self._batches.delete_from_number(pid, 0)
        # Also clear the emr_job_run_id from previous run so state machine
        # doesn't try to poll a stale job
        self._repo.update_emr_job_run_id(pid, "")

        # Step 1: Create S3 event tracker BEFORE partition discovery
        source_location = pipeline.get("source_location", "") or src.get("location", "")
        if source_location:
            try:
                self._tracker.create_tracker(
                    pid, source_location,
                    source_account=pipeline.get("source_account", ""))
                log.info("Pipeline %s: S3 event tracker created", pid[:8])
            except Exception as e:
                log.error("Pipeline %s: tracker creation failed: %s", pid[:8], e)
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"S3 event tracker creation failed: {e}")

        # Step 2: Discover partitions from Glue
        try:
            partitions = _discover_partitions(pipeline)
        except Exception as e:
            log.error("Pipeline %s: partition discovery failed: %s", pid[:8], e)
            return self._repo.update_status(
                pid, "failed",
                error_message=f"Partition discovery failed: {e}")

        if not partitions:
            return self._repo.update_status(
                pid, "failed",
                error_message="No partitions found in source table")

        log.info("Pipeline %s: discovered %d partitions, batch_size=%d",
                 pid[:8], len(partitions), batch_size)

        # Step 3: Create batches
        # Batch 0: create target Iceberg table
        self._batches.create(pid, batch_number=0,
                             partitions=["__create_table__"])

        # Batch 1..N: partition chunks for add_files
        chunks = _chunk_partitions(partitions, batch_size)
        for i, chunk in enumerate(chunks):
            label = f"__add_files_batch_{i+1}__{len(chunk)}_partitions__"
            self._batches.create(
                pid, batch_number=i + 1,
                partitions=[label, json.dumps(chunk)])

        log.info("Pipeline %s: created %d batches (1 setup + %d add_files)",
                 pid[:8], len(chunks) + 1, len(chunks))

        return self._repo.get(pid)

    def cancel(self, pipeline: dict) -> dict:
        """Cancel in-flight work and destroy tracker."""
        pid = pipeline["id"]
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        job_run_id = pipeline.get("emr_job_run_id", "")

        # Cancel running EMR job
        if job_run_id and not job_run_id.startswith(("athena:", "in-process")):
            try:
                self._emr._client.cancel_job_run(
                    applicationId=self._emr.application_id,
                    jobRunId=job_run_id)
            except Exception as e:
                log.warning("Pipeline %s: failed to cancel EMR job: %s",
                            pid[:8], e)

        # Destroy tracker
        source_location = pipeline.get("source_location", "")
        if source_location:
            try:
                self._tracker.destroy_tracker(pid, source_location)
            except Exception as e:
                log.warning("Pipeline %s: failed to destroy tracker: %s",
                            pid[:8], e)

        return self._repo.update_status(pid, "cancelled")

    # ═══════════════════════════════════════════════════════════════
    # STATE MACHINE HOOKS: Batch Execution
    # ═══════════════════════════════════════════════════════════════

    def submit_batch(self, pipeline: dict, batch: dict) -> dict:
        """Submit a pending batch as an EMR Serverless job.

        Batch 0: Spark DDL to create target Iceberg table
        Batch 1+: Spark add_files for a chunk of partitions
        """
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        batch_number = batch["batch_number"]
        partitions_raw = batch.get("partitions", "[]")

        if isinstance(partitions_raw, str):
            try:
                partitions_raw = json.loads(partitions_raw)
            except (ValueError, TypeError):
                partitions_raw = [partitions_raw]

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        spark_props = _build_spark_props(pipeline)

        if batch_number == 0:
            # Create table batch
            return self._submit_create_table(
                pipeline, spec_json, spark_props, batch)
        else:
            # add_files batch — partitions_raw is [label, json_chunk]
            # Extract the actual partition data from the second element
            partition_values = []
            for item in partitions_raw:
                if isinstance(item, str) and item.startswith("__add_files_batch_"):
                    continue  # Skip the label
                if isinstance(item, str):
                    try:
                        partition_values = json.loads(item)
                    except (ValueError, TypeError):
                        pass
                elif isinstance(item, list):
                    partition_values = item
            return self._submit_add_files_batch(
                pipeline, spec_json, spark_props, batch, partition_values)

    def _submit_create_table(self, pipeline: dict, spec_json: dict,
                             spark_props: dict, batch: dict) -> dict:
        """Submit EMR job to create empty Iceberg target table.

        Uses add_files.py with 'create_table' operation — reads schema directly
        from source Parquet files to avoid catalog resolution issues.
        """
        pid = pipeline["id"]
        script_path = _get_script_path("add_files.py")
        job_name = (
            f"IMA-add-files-create-table-"
            f"{pipeline['source_database']}-"
            f"{pipeline['source_table']}")

        script_args = [
            "--spec", json.dumps(spec_json),
            "--operation", "create_table",
            "--partitions", "[]",
        ]

        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=script_args,
                spark_properties=spark_props)
        except Exception as e:
            log.error("Pipeline %s: create table job submit failed: %s",
                      pid[:8], e)
            self._batches.mark_failed(batch["id"], error_message=str(e))
            return self._repo.update_status(
                pid, "failed",
                error_message=f"Create table job failed: {e}")

        log.info("Pipeline %s: create table job submitted: %s",
                 pid[:8], job_run_id)
        self._batches.mark_running(batch["id"], job_run_id, pipeline_id=pid)
        self._repo.update_emr_job_run_id(pid, job_run_id)
        return self._repo.get(pid)

    def _submit_add_files_batch(self, pipeline: dict, spec_json: dict,
                                spark_props: dict, batch: dict,
                                partition_values: list) -> dict:
        """Submit EMR job to run add_files for a chunk of partitions."""
        pid = pipeline["id"]
        batch_number = batch["batch_number"]
        script_path = _get_script_path("add_files.py")

        job_name = (
            f"IMA-add-files-batch-{batch_number}-"
            f"{pipeline['source_database']}-"
            f"{pipeline['source_table']}")

        script_args = [
            "--spec", json.dumps(spec_json),
            "--operation", "add_files",
            "--partitions", json.dumps(partition_values),
        ]

        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=script_args,
                spark_properties=spark_props)
        except Exception as e:
            log.error("Pipeline %s: add_files batch %d submit failed: %s",
                      pid[:8], batch_number, e)
            self._batches.mark_failed(batch["id"], error_message=str(e))
            return self._repo.update_status(
                pid, "failed",
                error_message=f"add_files batch {batch_number} failed: {e}")

        log.info("Pipeline %s: add_files batch %d submitted: %s (%d partitions)",
                 pid[:8], batch_number, job_run_id,
                 len(partition_values) if isinstance(partition_values, list) else 0)
        self._batches.mark_running(batch["id"], job_run_id, pipeline_id=pid)
        self._repo.update_emr_job_run_id(pid, job_run_id)
        return self._repo.get(pid)

    # ═══════════════════════════════════════════════════════════════
    # STATE MACHINE HOOKS: Lifecycle Transitions
    # ═══════════════════════════════════════════════════════════════

    def on_initial_load_complete(self, pipeline: dict) -> dict:
        """All initial load batches done - transition to in_sync."""
        pid = pipeline["id"]
        self._repo.update_sync_status(pid, "in_sync")
        log.info("Pipeline %s: add_files initial load complete, in_sync",
                 pid[:8])
        return self._repo.get(pid)

    # ═══════════════════════════════════════════════════════════════
    # STATE MACHINE HOOKS: Ongoing Sync
    # ═══════════════════════════════════════════════════════════════

    def detect_changes(self, pipeline: dict) -> bool:
        """Check if source has pending S3 events."""
        pending = self._tracking.get_pending(pipeline["id"], limit=1)
        return len(pending) > 0

    def execute_sync(self, pipeline: dict, batch_number: int) -> dict:
        """Execute ongoing sync: add_files for partitions with changed objects.

        Groups pending S3 events by partition, submits a single EMR job
        that re-runs add_files with overwrite for all affected partitions.
        """
        pid = pipeline["id"]
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        src = (spec_json or {}).get("source", {})
        partition_keys = src.get("partition_keys", [])
        source_location = src.get("location", pipeline.get("source_location", ""))

        # Fetch pending events
        events = self._tracking.get_pending(pid)
        if not events:
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        # Extract affected partitions from event S3 keys
        affected_partitions = self._extract_affected_partitions(
            events, source_location, partition_keys)

        if not affected_partitions:
            # Events don't map to recognizable partitions — mark processed
            for event in events:
                self._tracking.mark_processed(event["id"], pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        log.info("Pipeline %s: sync — %d events affecting %d partitions",
                 pid[:8], len(events), len(affected_partitions))

        # Create sync batch — include event IDs so they can be marked
        # processed only after the job succeeds (not optimistically).
        event_ids = [event["id"] for event in events]
        sync_label = f"__sync__add_files__{len(affected_partitions)}_partitions__"
        existing_batches = self._batches.list_by_pipeline(pid)
        next_batch_num = max(
            (b["batch_number"] for b in existing_batches), default=-1) + 1

        self._batches.create(pid, batch_number=next_batch_num,
                             partitions=[sync_label,
                                         f"__event_ids__{json.dumps(event_ids)}__"])

        # Submit EMR job
        spark_props = _build_spark_props(pipeline)
        script_path = _get_script_path("add_files.py")
        job_name = (
            f"IMA-add-files-sync-"
            f"{pipeline['source_database']}-"
            f"{pipeline['source_table']}")

        script_args = [
            "--spec", json.dumps(spec_json),
            "--operation", "sync_partitions",
            "--partitions", json.dumps(affected_partitions),
        ]

        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=script_args,
                spark_properties=spark_props)
        except Exception as e:
            log.error("Pipeline %s: sync job submit failed: %s", pid[:8], e)
            self._batches.mark_failed(next_batch_num, error_message=str(e),
                                      pipeline_id=pid)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.get(pid)

        self._batches.mark_running(next_batch_num, job_run_id, pipeline_id=pid)
        self._repo.update_emr_job_run_id(pid, job_run_id)

        log.info("Pipeline %s: sync job submitted: %s", pid[:8], job_run_id)
        return self._repo.get(pid)

    # ═══════════════════════════════════════════════════════════════
    # LIFECYCLE HOOKS (called by state machine)
    # ═══════════════════════════════════════════════════════════════

    def on_job_success(self, pipeline: dict, batch: dict | None) -> dict | None:
        """Mark tracking events processed after sync batch completes.

        Event IDs are stored in the batch partitions metadata during
        execute_sync, and only marked processed here — ensuring failed
        sync jobs retain their events for retry.
        """
        if not batch:
            return None

        pid = pipeline["id"]
        partitions = batch.get("partitions", "[]")
        if isinstance(partitions, str):
            try:
                partitions = json.loads(partitions)
            except (ValueError, TypeError):
                partitions = [partitions]

        # Only process sync batches that carry event IDs
        is_sync = any(isinstance(p, str) and p.startswith("__sync__add_files__")
                      for p in partitions)
        if not is_sync:
            return None

        # Extract event IDs from the batch metadata
        for p in partitions:
            if isinstance(p, str) and p.startswith("__event_ids__"):
                ids_json = p.replace("__event_ids__", "", 1).rstrip("_")
                try:
                    event_ids = json.loads(ids_json)
                    for eid in event_ids:
                        self._tracking.mark_processed(eid, pipeline_id=pid)
                    log.info("Pipeline %s: marked %d tracking events processed",
                             pid[:8], len(event_ids))
                except (ValueError, TypeError) as e:
                    log.warning("Pipeline %s: failed to parse event IDs: %s",
                                pid[:8], e)
                break

        return None  # Let state machine handle default behavior

    # ═══════════════════════════════════════════════════════════════
    # PRIVATE HELPERS
    # ═══════════════════════════════════════════════════════════════

    def _extract_affected_partitions(
            self, events: list[dict], source_location: str,
            partition_keys: list[dict]) -> list[list[str]]:
        """Extract unique partition values from S3 event keys.

        Parses Hive-style partition paths (e.g., year=2024/month=01/day=15)
        from the S3 key and returns unique partition value tuples.
        """
        from urllib.parse import urlparse

        if not partition_keys:
            return []

        # Normalize source prefix for stripping
        parsed = urlparse(source_location)
        prefix = parsed.path.lstrip("/")
        key_names = [pk["name"] for pk in partition_keys]

        seen = set()
        result = []

        for event in events:
            key = event.get("key", "")
            # Strip the table prefix to get the partition path
            relative = key
            if prefix and key.startswith(prefix):
                relative = key[len(prefix):]
            relative = relative.lstrip("/")

            # Parse Hive partition values from path segments
            parts = relative.split("/")
            values = {}
            for part in parts:
                if "=" in part:
                    col, val = part.split("=", 1)
                    values[col] = val

            # Build ordered value list matching partition_keys order
            partition_values = []
            for name in key_names:
                if name in values:
                    partition_values.append(values[name])
                else:
                    break  # Incomplete partition path

            if len(partition_values) == len(key_names):
                key_tuple = tuple(partition_values)
                if key_tuple not in seen:
                    seen.add(key_tuple)
                    result.append(partition_values)

        return result
