# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Rewrite strategy — full table CTAS via EMR Serverless or Athena.

Flow:
  1. validate: check source location and format compatibility
  2. submit:   build CTAS, submit to EMR Serverless or Athena
  3. reconcile: poll job/query status until complete
  4. cancel:   cancel the in-flight job/query
"""

import json
import logging

from .base import MigrationStrategy
from .common import NamespaceError, ensure_s3tables_namespace, s3tables_spark_props
from ..engines.emr import EmrServerlessClient
from ..engines.athena import AthenaClient
from ..db import PipelineRepo

log = logging.getLogger(__name__)


def _derive_target_location(pipeline: dict) -> str:
    """Derive target_location if not set."""
    loc = pipeline.get("target_location", "")
    if not loc:
        src = pipeline.get("source_location", "").rstrip("/")
        base = src.rsplit("/", 1)[0] if "/" in src else src
        loc = f"{base}/{pipeline.get('target_table', 'iceberg')}/"
    return loc


class RewriteStrategy(MigrationStrategy):

    _one_shot = True  # No ongoing sync — single CTAS job

    def __init__(self):
        self._emr = EmrServerlessClient()
        self._athena = AthenaClient()
        self._repo = PipelineRepo()

    def _engine(self, pipeline: dict) -> str:
        return pipeline.get("execution_engine", "athena")

    @staticmethod
    def _packages_for_source(pipeline: dict) -> list[str]:
        """Maven packages needed for the source format's native reader.

        Returns empty — EMR Serverless ships Hudi and Delta pre-installed.
        """
        return []

    def validate(self, pipeline: dict) -> tuple[bool, str]:
        if not pipeline.get("source_location"):
            return False, "Source table has no S3 location"
        fmt = pipeline.get("source_format", "")
        if fmt == "iceberg" and pipeline.get("target_type") != "s3_tables":
            return False, (
                "Iceberg-to-Iceberg rewrite only supported "
                "for S3 Tables target")
        self._engine(pipeline)
        return True, "OK"

    def submit(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        # Use the pipeline's Athena/EMR config (region/workgroup/output location
        # and EMR application_id/region the app injected from Settings) rather
        # than the engine env defaults.
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        ok, reason = self.validate(pipeline)
        if not ok:
            return self._repo.update_status(pid, "failed",
                                            error_message=reason)

        engine = self._engine(pipeline)
        if engine == "athena":
            return self._submit_athena(pipeline)
        return self._submit_emr(pipeline)

    def _submit_athena(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        target_type = pipeline.get("target_type", "")
        table_bucket_arn = pipeline.get("target_bucket_arn", "")
        log.info("Pipeline %s: target_type=%s, table_bucket_arn=%s",
                 pid[:8], target_type, table_bucket_arn)
        target_location = ""
        # Only set location for s3_iceberg (external) targets, not s3_tables (managed)
        if target_type != "s3_tables":
            target_location = _derive_target_location(pipeline)
        ip = spec_json.get("iceberg_properties", {}) if spec_json else {}

        # For S3 Tables: the namespace must exist before the CTAS.
        if target_type == "s3_tables" and table_bucket_arn:
            try:
                ensure_s3tables_namespace(
                    table_bucket_arn, pipeline.get("target_database", ""))
            except NamespaceError as e:
                log.error("Pipeline %s: %s", pid[:8], e)
                return self._repo.update_status(
                    pid, "failed", error_message=str(e)[:500])
        sql = self._athena.build_ctas_sql(
            source_db=pipeline["source_database"],
            source_table=pipeline["source_table"],
            target_db=pipeline.get("target_database", ""),
            target_table=pipeline.get("target_table", ""),
            target_location=target_location,
            partition_spec=ip.get("partition_spec"),
            sort_order=ip.get("sort_order"),
            table_properties=ip.get("table_properties"),
            target_type=target_type,
            table_bucket_arn=table_bucket_arn,
        )

        try:
            qid = self._athena.submit_query(
                sql, database=pipeline["source_database"],
                description=f"IMA-rewrite-{pipeline['source_table']}")
        except Exception as e:
            log.error("Athena submit failed for pipeline %s: %s", pid, e)
            return self._repo.update_status(
                pid, "failed", error_message=f"Athena submit failed: {e}")

        log.info("Submitted Athena query %s for pipeline %s", qid, pid)
        self._repo.update_emr_job_run_id(pid, f"athena:{qid}")

        # Record as a batch for tracking
        from ..db import BatchRepo
        batches = BatchRepo()
        batches.create(pid, batch_number=0, partitions=["__rewrite_ctas__"])
        batches.mark_running(0, f"athena:{qid}", pipeline_id=pid)

        return self._repo.update_status(pid, "running")

    def _submit_emr(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        target_location = _derive_target_location(pipeline)

        spark_props = {
            # Configure default catalog to use Hive with Glue metastore
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.hive.metastore.client.factory.class":
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            # Disable Spark's Hive-to-Parquet conversion optimization to avoid
            # type mismatch errors when reading Delta/Hudi tables via Glue.
            "spark.sql.hive.convertMetastoreParquet": "false",
            # Configure glue_catalog for Iceberg target tables
            "spark.sql.catalog.glue_catalog":
                "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.glue_catalog.catalog-impl":
                "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.glue_catalog.warehouse":
                target_location,
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions",
        }
        if pipeline.get("target_type") == "s3_tables":
            # S3 Tables via Iceberg GlueCatalog with glue.id + warehouse (EMR >= 7.5).
            spark_props.update(s3tables_spark_props(pipeline))

        # Add format-specific jars and config for Hudi/Delta sources.
        source_fmt = pipeline.get("source_format", "").lower()
        if source_fmt == "hudi":
            spark_props["spark.jars"] = (
                "/usr/lib/hudi/hudi-spark-bundle.jar,"
                "/usr/lib/hudi/hudi-utilities-bundle.jar,"
                "/usr/lib/hudi/hudi-aws-bundle.jar")
            spark_props["spark.serializer"] = (
                "org.apache.spark.serializer.KryoSerializer")
        elif source_fmt == "delta":
            spark_props["spark.jars"] = (
                "/usr/share/aws/delta/lib/delta-spark.jar,"
                "/usr/share/aws/delta/lib/delta-storage.jar")
            spark_props["spark.sql.extensions"] = (
                "io.delta.sql.DeltaSparkSessionExtension,"
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            spark_props["spark.sql.catalog.spark_catalog"] = (
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        from ..engines.emr import resolve_script_path
        script_path = resolve_script_path("rewrite_ctas.py")
        job_name = (f"IMA-rewrite-"
                    f"{pipeline['source_database']}-"
                    f"{pipeline['source_table']}")

        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=["--spec", json.dumps(spec_json)],
                spark_properties=spark_props,
                packages=self._packages_for_source(pipeline))
        except Exception as e:
            log.error("EMR submit failed for pipeline %s: %s", pid, e)
            return self._repo.update_status(
                pid, "failed", error_message=f"EMR submit failed: {e}")

        log.info("Submitted EMR job %s for pipeline %s", job_run_id, pid)
        self._repo.update_emr_job_run_id(pid, job_run_id)

        # Record as a batch for tracking
        from ..db import BatchRepo
        batches = BatchRepo()
        batches.create(pid, batch_number=0, partitions=["__rewrite_ctas__"])
        batches.mark_running(0, job_run_id, pipeline_id=pid)

        return self._repo.update_status(pid, "running")

    def reconcile(self, pipeline: dict) -> dict:
        """Legacy reconcile — delegated to state machine via engine.
        Kept for backward compat during migration."""
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        job_ref = pipeline.get("emr_job_run_id", "")
        if not job_ref:
            return pipeline

        if job_ref.startswith("athena:"):
            return self._reconcile_athena(pipeline, job_ref[7:])
        return self._reconcile_emr(pipeline, job_ref)

    def on_job_success(self, pipeline: dict, batch=None) -> dict:
        """Apply sort order after successful CTAS, then mark completed."""
        pid = pipeline["id"]

        # Apply sort order via ALTER TABLE if specified in the spec
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)
        if spec_json:
            ip = spec_json.get("iceberg_properties", {})
            sort_order = ip.get("sort_order")
            if sort_order:
                target_type = pipeline.get("target_type", "")
                table_bucket_arn = pipeline.get("target_bucket_arn", "")
                alter_sql = self._athena.build_alter_write_order_sql(
                    pipeline.get("target_database", ""),
                    pipeline.get("target_table", ""),
                    sort_order,
                    target_type=target_type,
                    table_bucket_arn=table_bucket_arn)
                if alter_sql:
                    try:
                        self._athena.submit_query(
                            alter_sql,
                            database=pipeline.get("target_database", ""),
                            description=f"IMA-sort-order-{pipeline.get('target_table', '')}")
                        log.info("Pipeline %s: submitted ALTER TABLE for sort order", pid[:8])
                    except Exception as e:
                        log.warning("Pipeline %s: ALTER TABLE for sort order failed (non-fatal): %s",
                                    pid[:8], e)

        from ..core.state_machine import PipelineStateMachine
        sm = PipelineStateMachine()
        sm._update_batch_progress(pid)
        return self._repo.update_status(pid, "completed")

    def on_initial_load_complete(self, pipeline: dict) -> dict:
        """Rewrite is one-shot — mark completed when batch finishes."""
        return self._repo.update_status(pipeline["id"], "completed")

    def _reconcile_athena(self, pipeline: dict, qid: str) -> dict:
        pid = pipeline["id"]
        try:
            status = self._athena.get_query_status(qid)
        except Exception as e:
            log.error("Failed to poll Athena query %s: %s", qid, e)
            return pipeline

        state = status["state"]
        log.info("Pipeline %s Athena query %s: %s", pid, qid, state)

        if state == "SUCCEEDED":
            from ..db import BatchRepo, CostRepo
            batches = BatchRepo()
            running = batches.get_running(pid)
            if running:
                batches.mark_completed(running["batch_number"], pipeline_id=pid)

            # Record Athena cost
            bytes_scanned = status.get("data_scanned_bytes", 0)
            if bytes_scanned > 0:
                try:
                    costs = CostRepo()
                    costs.record_athena_query(pid, bytes_scanned)
                    log.info("Pipeline %s: recorded Athena cost (%d bytes scanned)",
                             pid[:8], bytes_scanned)
                except Exception as e:
                    log.warning("Pipeline %s: failed to record cost: %s", pid[:8], e)

            # Apply sort order via ALTER TABLE if specified in the spec
            spec_json = pipeline.get("spec_json")
            if isinstance(spec_json, str):
                spec_json = json.loads(spec_json)
            if spec_json:
                ip = spec_json.get("iceberg_properties", {})
                sort_order = ip.get("sort_order")
                if sort_order:
                    target_type = pipeline.get("target_type", "")
                    table_bucket_arn = pipeline.get("target_bucket_arn", "")
                    alter_sql = self._athena.build_alter_write_order_sql(
                        pipeline.get("target_database", ""),
                        pipeline.get("target_table", ""),
                        sort_order,
                        target_type=target_type,
                        table_bucket_arn=table_bucket_arn)
                    if alter_sql:
                        try:
                            self._athena.submit_query(
                                alter_sql,
                                database=pipeline.get("target_database", ""),
                                description=f"IMA-sort-order-{pipeline.get('target_table', '')}")
                            log.info("Pipeline %s: submitted ALTER TABLE for sort order", pid[:8])
                        except Exception as e:
                            log.warning("Pipeline %s: ALTER TABLE for sort order failed (non-fatal): %s",
                                        pid[:8], e)

            return self._repo.update_status(pid, "completed")
        elif state == "FAILED":
            reason = status.get("state_reason", "Athena query failed")
            from ..db import BatchRepo
            batches = BatchRepo()
            running = batches.get_running(pid)
            if running:
                batches.mark_failed(running["batch_number"], error_message=reason, pipeline_id=pid)
            return self._repo.update_status(pid, "failed",
                                            error_message=reason)
        elif state == "CANCELLED":
            reason = status.get("state_reason", "Query cancelled (workgroup limit or system)")
            from ..db import BatchRepo
            batches = BatchRepo()
            running = batches.get_running(pid)
            if running:
                batches.mark_failed(running["batch_number"], error_message=reason, pipeline_id=pid)
            return self._repo.update_status(pid, "failed",
                                            error_message=reason)
        return pipeline

    def _reconcile_emr(self, pipeline: dict, job_run_id: str) -> dict:
        pid = pipeline["id"]
        try:
            status = self._emr.get_job_status(job_run_id)
        except Exception as e:
            log.error("Failed to poll EMR job %s: %s", job_run_id, e)
            return pipeline

        state = status["state"]
        log.info("Pipeline %s EMR job %s: %s", pid, job_run_id, state)

        if state == "SUCCESS":
            from ..db import BatchRepo, CostRepo
            batches = BatchRepo()
            running = batches.get_running(pid)
            if running:
                batches.mark_completed(running["batch_number"], pipeline_id=pid)

            # Record EMR cost
            resource_util = status.get("total_resource_utilization", {})
            vcpu_hours = resource_util.get("vCPUHour", 0)
            memory_gb_hours = resource_util.get("memoryGBHour", 0)
            storage_gb_hours = resource_util.get("storageGBHour", 0)
            if vcpu_hours > 0 or memory_gb_hours > 0:
                try:
                    costs = CostRepo()
                    costs.record_emr_job(pid, vcpu_hours, memory_gb_hours, storage_gb_hours)
                    log.info("Pipeline %s: recorded EMR cost (%.2f vCPU-hrs, %.2f mem-GB-hrs)",
                             pid[:8], vcpu_hours, memory_gb_hours)
                except Exception as e:
                    log.warning("Pipeline %s: failed to record cost: %s", pid[:8], e)

            return self._repo.update_status(pid, "completed")
        elif state == "FAILED":
            details = status.get("state_details", "EMR job failed")
            from ..db import BatchRepo
            batches = BatchRepo()
            running = batches.get_running(pid)
            if running:
                batches.mark_failed(running["batch_number"], error_message=details, pipeline_id=pid)
            return self._repo.update_status(pid, "failed",
                                            error_message=details)
        elif state == "CANCELLED":
            details = status.get("state_details", "Job cancelled (system)")
            from ..db import BatchRepo
            batches = BatchRepo()
            running = batches.get_running(pid)
            if running:
                batches.mark_failed(running["batch_number"], error_message=details, pipeline_id=pid)
            return self._repo.update_status(pid, "failed",
                                            error_message=details)
        return pipeline

    def cancel(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        job_ref = pipeline.get("emr_job_run_id", "")

        if job_ref.startswith("athena:"):
            try:
                self._athena.cancel_query(job_ref[7:])
            except Exception as e:
                log.warning("Failed to cancel Athena query: %s", e)
        elif job_ref:
            try:
                self._emr.cancel_job(job_ref)
            except Exception as e:
                log.warning("Failed to cancel EMR job: %s", e)

        return self._repo.update_status(pid, "cancelled")
