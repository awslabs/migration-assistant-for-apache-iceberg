# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Incremental strategy — batched initial load + periodic partition sync.

Initial load:
  1. submit:  List all source partitions, split into batches of batch_size,
              create batch records, submit the first batch as an EMR job.
  2. reconcile (initial): When a batch job completes, submit the next pending
              batch. After all batches complete, transition to ongoing sync.

Ongoing sync:
  3. reconcile (sync): Periodically check for pending S3 object events,
              group by affected partition, submit a rewrite job for dirty
              partitions.

The batch_size is specified in strategy.batch_size (default 10).
The sync interval defaults to 3600s, configurable via
ENGINE_INCREMENTAL_INTERVAL_SECONDS.
"""

import json
import logging
import os
from datetime import datetime, timezone

from .base import MigrationStrategy
from .common import NamespaceError, ensure_s3tables_namespace, s3tables_spark_props
from ..engines.emr import EmrServerlessClient
from ..engines.athena import AthenaClient
from ..db import PipelineRepo, ObjectTrackingRepo, ProgressRepo, BatchRepo
from ..utils.sql_safe import validate_identifier
from ..utils.sql_builder import (
    athena_table_ref, athena_drop_table, athena_insert_into,
    athena_delete, athena_select,
)
from ..utils.polling import poll_until_complete

log = logging.getLogger(__name__)

_SYNC_INTERVAL = int(os.getenv("ENGINE_INCREMENTAL_INTERVAL_SECONDS", "3600"))
_DEFAULT_BATCH_SIZE = 5

# Date-related type patterns for partition column detection
_DATE_TYPES = {"date", "timestamp", "datetime"}
_DATE_NAME_PATTERNS = {"date", "dt", "event_date", "partition_date",
                       "created_at", "updated_at", "ts",
                       "event_time", "load_date", "ingestion_date"}
class _DatePartitionInfo:
    """Describes how a table is date-partitioned."""

    def __init__(self, mode: str, columns: list[dict]):
        # mode: "single_date", "single_timestamp", "single_string", "composite_ymd"
        self.mode = mode
        self.columns = columns  # list of {"name": ..., "type": ...}

    @property
    def primary_column(self) -> str:
        return self.columns[0]["name"]

    @property
    def column_names(self) -> list[str]:
        return [c["name"] for c in self.columns]


def _detect_date_partitioning(partition_keys: list[dict]) -> _DatePartitionInfo | None:
    """Detect date-based partitioning scheme.

    Returns _DatePartitionInfo or None if no date partitioning found.
    Supports:
    - Single date/timestamp typed column
    - Single string column with date-like name
    - Composite year/month/day integer columns
    - Iceberg transform types (month, day, year, hour) indicating date-based source column
    """
    if not partition_keys:
        return None

    # Check for composite year/month/day pattern
    names_lower = {pk.get("name", "").lower(): pk for pk in partition_keys}
    year_col = names_lower.get("year") or names_lower.get("yr")
    month_col = names_lower.get("month") or names_lower.get("mon")
    day_col = names_lower.get("day") or names_lower.get("dy")

    if year_col and month_col:
        cols = [year_col, month_col]
        if day_col:
            cols.append(day_col)
        return _DatePartitionInfo("composite_ymd", cols)

    # Check for Iceberg transform types (month, day, year, hour → source column is date/timestamp)
    _ICEBERG_DATE_TRANSFORMS = {"month", "day", "year", "hour"}
    for pk in partition_keys:
        col_type = pk.get("type", "").lower()
        if col_type in _ICEBERG_DATE_TRANSFORMS:
            return _DatePartitionInfo("single_date", [pk])

    # Check for single date/timestamp typed column
    for pk in partition_keys:
        col_type = pk.get("type", "").lower()
        if "timestamp" in col_type:
            return _DatePartitionInfo("single_timestamp", [pk])
        if "date" in col_type:
            return _DatePartitionInfo("single_date", [pk])

    # Check for single string column with date-like name
    for pk in partition_keys:
        col_name = pk.get("name", "").lower()
        if col_name in _DATE_NAME_PATTERNS:
            return _DatePartitionInfo("single_string", [pk])
        if any(p in col_name for p in ("date", "_dt")):
            return _DatePartitionInfo("single_string", [pk])

    return None


def _generate_date_range_batches(min_val: str, max_val: str,
                                  date_col: str,
                                  batch_count: int,
                                  mode: str = "single_date",
                                  columns: list[dict] = None) -> list[str]:
    """Generate N date-range batch partition markers from oldest to newest.

    Returns list of partition strings like:
    __date_range__{mode}__{col1,col2,...}__{start}__{end}__
    """
    from datetime import date, timedelta

    try:
        min_date = date.fromisoformat(min_val[:10])
        max_date = date.fromisoformat(max_val[:10])
    except (ValueError, TypeError):
        return []

    total_days = (max_date - min_date).days + 1
    if total_days <= 0:
        return []

    # Ensure we don't create more batches than days
    actual_batches = min(batch_count, total_days)
    days_per_batch = total_days / actual_batches

    cols_str = ",".join(c["name"] for c in columns) if columns else date_col

    batches = []
    for i in range(actual_batches):
        start = min_date + timedelta(days=int(i * days_per_batch))
        if i < actual_batches - 1:
            end = min_date + timedelta(days=int((i + 1) * days_per_batch))
        else:
            end = max_date + timedelta(days=1)  # inclusive of max
        batches.append(f"__date_range__{mode}__{cols_str}__{start.isoformat()}__{end.isoformat()}__")

    return batches


def _build_date_filter_sql(date_col: str, start_val: str, end_val: str,
                           mode: str, columns: list[dict] = None) -> str:
    """Build a WHERE clause for date-range filtering based on partition mode.

    Uses CAST for safe type coercion — works with date, timestamp, and
    timestamp with time zone columns.
    """
    from datetime import date as _date

    if mode == "single_date":
        return (f"CAST({date_col} AS DATE) >= DATE '{start_val}' "
                f"AND CAST({date_col} AS DATE) < DATE '{end_val}'")
    elif mode == "single_timestamp":
        return (f"{date_col} >= TIMESTAMP '{start_val} 00:00:00' "
                f"AND {date_col} < TIMESTAMP '{end_val} 00:00:00'")
    elif mode == "single_string":
        # If values look like ISO dates, use DATE literals for type safety
        import re as _re
        if _re.match(r'^\d{4}-\d{2}-\d{2}$', start_val):
            return (f"CAST({date_col} AS DATE) >= DATE '{start_val}' "
                    f"AND CAST({date_col} AS DATE) < DATE '{end_val}'")
        return f"{date_col} >= '{start_val}' AND {date_col} < '{end_val}'"
    elif mode == "composite_ymd" and columns:
        # Parse start/end dates into year/month/day components
        try:
            start_d = _date.fromisoformat(start_val[:10])
            end_d = _date.fromisoformat(end_val[:10])
        except (ValueError, TypeError):
            return f"{date_col} >= '{start_val}' AND {date_col} < '{end_val}'"

        col_names = [c["name"] for c in columns]
        year_col = col_names[0]
        month_col = col_names[1]
        has_day = len(col_names) >= 3
        day_col = col_names[2] if has_day else None

        # Build composite range filter
        # Start: (year > sy) OR (year = sy AND month > sm) OR (year = sy AND month = sm AND day >= sd)
        # End: (year < ey) OR (year = ey AND month < em) OR (year = ey AND month = em AND day < ed)
        sy, sm, sd = start_d.year, start_d.month, start_d.day
        ey, em, ed = end_d.year, end_d.month, end_d.day

        if has_day:
            start_filter = (
                f"({year_col} > {sy} OR "
                f"({year_col} = {sy} AND {month_col} > {sm}) OR "
                f"({year_col} = {sy} AND {month_col} = {sm} AND {day_col} >= {sd}))")
            end_filter = (
                f"({year_col} < {ey} OR "
                f"({year_col} = {ey} AND {month_col} < {em}) OR "
                f"({year_col} = {ey} AND {month_col} = {em} AND {day_col} < {ed}))")
        else:
            start_filter = (
                f"({year_col} > {sy} OR "
                f"({year_col} = {sy} AND {month_col} >= {sm}))")
            end_filter = (
                f"({year_col} < {ey} OR "
                f"({year_col} = {ey} AND {month_col} < {em}))")

        return f"{start_filter} AND {end_filter}"
    else:
        return f"{date_col} >= '{start_val}' AND {date_col} < '{end_val}'"


def _extract_partition_from_key(key: str) -> dict | None:
    """Extract partition values from Hive-style S3 key."""
    values = {}
    for part in key.split("/"):
        if "=" in part:
            k, v = part.split("=", 1)
            values[k] = v
    return values or None


def _build_iceberg_spark_props(pipeline: dict) -> dict:
    props = {
        # Configure default catalog to use Hive with Glue metastore
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.hive.metastore.client.factory.class":
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        # Disable Spark's Hive-to-Parquet conversion optimization. When enabled
        # (the default), Spark applies the Hive-declared schema to the underlying
        # Parquet files via its native reader — which throws if the declared type
        # disagrees with the physical type (e.g. Delta/Hudi tables registered in
        # Glue with a possibly-stale schema). Disabling it makes Spark read
        # through the format's own reader (Delta/Hudi) or the generic Hive path.
        "spark.sql.hive.convertMetastoreParquet": "false",
        # Configure glue_catalog for Iceberg target tables
        "spark.sql.catalog.glue_catalog":
            "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl":
            "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse":
            pipeline.get("target_location", ""),
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
    }
    if pipeline.get("target_type") == "s3_tables":
        # S3 Tables via Iceberg GlueCatalog with glue.id + warehouse (EMR >= 7.5).
        props.update(s3tables_spark_props(pipeline))
    # Add format-specific jars and config for Hudi/Delta sources.
    # EMR Serverless ships these pre-installed but they must be explicitly
    # added to spark.jars and their extensions/serializer configured.
    source_fmt = pipeline.get("source_format", "").lower()
    if source_fmt == "hudi":
        props["spark.jars"] = (
            "/usr/lib/hudi/hudi-spark-bundle.jar,"
            "/usr/lib/hudi/hudi-utilities-bundle.jar,"
            "/usr/lib/hudi/hudi-aws-bundle.jar")
        props["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
    elif source_fmt == "delta":
        # EMR 7.0+ renamed delta-core.jar to delta-spark.jar
        props["spark.jars"] = (
            "/usr/share/aws/delta/lib/delta-spark.jar,"
            "/usr/share/aws/delta/lib/delta-storage.jar")
        props["spark.sql.extensions"] = (
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        props["spark.sql.catalog.spark_catalog"] = (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return props


# Maven coordinates for format-specific runtime dependencies.
# Note: EMR Serverless ships Hudi and Delta jars pre-installed, so no
# --packages download is needed. The native readers
# (spark.read.format("hudi"/"delta").load(...)) use classes already on
# the EMR classpath. The --packages approach fails because EMR's network
# environment cannot reach Maven Central.


def _packages_for_source(pipeline: dict) -> list[str]:
    """Return Maven packages needed for the source format.

    Currently returns empty — EMR Serverless ships Hudi and Delta
    pre-installed. This function exists as a hook for environments
    that don't bundle these (e.g. custom Spark clusters).
    """
    return []


def _get_script_path(script_name: str) -> str:
    """Resolve script S3 path (delegates to the shared EMR resolver)."""
    from ..engines.emr import resolve_script_path
    return resolve_script_path(script_name)


class IncrementalStrategy(MigrationStrategy):

    def __init__(self):
        self._emr = EmrServerlessClient()
        self._athena = AthenaClient()
        self._repo = PipelineRepo()
        self._tracking = ObjectTrackingRepo()
        self._progress = ProgressRepo()
        self._batches = BatchRepo()
        from ..tracking import TrackingManager
        self._tracker = TrackingManager()

    def _engine(self, pipeline: dict) -> str:
        return pipeline.get("execution_engine", "athena")

    def validate(self, pipeline: dict) -> tuple[bool, str]:
        if not pipeline.get("source_location"):
            return False, "Source table has no S3 location"
        supported = ("parquet", "hudi", "delta", "orc", "avro",
                     "csv", "json")
        fmt = (pipeline.get("source_format", "") or "").lower()
        if fmt not in supported:
            return False, (
                f"Format '{fmt}' not supported for incremental")
        # Hudi/Delta incremental works on BOTH engines:
        #   - EMR: native Spark readers honor the commit timeline.
        #   - Athena: reads the Glue-registered source directly. Hudi CoW is
        #     registered with HoodieParquetInputFormat (Athena v3 reads the
        #     current snapshot natively). Delta is read via its symlink
        #     manifest, which _sync_commit_tracked refreshes before the SELECT
        #     so Athena sees the latest committed data.
        # Check partitioned flag OR presence of partition_keys in spec
        is_partitioned = pipeline.get("source_partitioned")
        if not is_partitioned:
            # Also check spec_json for partition keys
            spec_json = pipeline.get("spec_json")
            if isinstance(spec_json, str):
                import json as _json
                try:
                    spec_json = _json.loads(spec_json)
                except (ValueError, TypeError):
                    spec_json = None
            if spec_json:
                pk = spec_json.get("source", {}).get("partition_keys", [])
                if pk:
                    is_partitioned = True
            # Also check source_partition_keys directly on pipeline
            pk_direct = pipeline.get("source_partition_keys")
            if pk_direct:
                if isinstance(pk_direct, str):
                    import json as _json
                    try:
                        pk_direct = _json.loads(pk_direct)
                    except (ValueError, TypeError):
                        pk_direct = []
                if pk_direct:
                    is_partitioned = True
        if not is_partitioned:
            return False, (
                "Incremental strategy requires a partitioned table. "
                "Use rewrite for unpartitioned tables.")
        return True, "OK"

    # ── Submit: partition discovery + batch creation ──────────────

    def submit(self, pipeline: dict) -> dict:
        """Discover partitions, split into batches, submit first batch."""
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

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        # Get batch_size from spec strategy
        batch_size = _DEFAULT_BATCH_SIZE
        if spec_json:
            batch_size = spec_json.get("strategy", {}).get(
                "batch_size", _DEFAULT_BATCH_SIZE)

        # Discover partitions from source partition keys
        partition_keys = []
        if spec_json:
            partition_keys = spec_json.get(
                "source", {}).get("partition_keys", [])

        if not partition_keys:
            # Unpartitioned table — single batch, full CTAS
            return self._submit_full_ctas(pipeline, spec_json)

        # Partitioned table: the initial CTAS is tracked as batch 0 and the
        # Spark job discovers partitions and batches them internally.
        log.info("Pipeline %s: creating batched initial load "
                 "(batch_size=%d)", pid, batch_size)

        engine = self._engine(pipeline)
        if engine != "athena":
            return self._submit_emr_setup(pipeline, spec_json)

        # Athena path: Create batch 0 — initial table creation
        self._batches.create(pid, batch_number=0,
                             partitions=["__initial_ctas__"])

        # Submit the initial CTAS
        return self._submit_batch_job(
            pipeline, spec_json, batch_number=0,
            partitions=["__initial_ctas__"],
            script="rewrite_ctas.py",
            job_suffix="init")

    def _submit_full_ctas(self, pipeline: dict,
                          spec_json: dict) -> dict:
        """Unpartitioned table — single CTAS, no batching."""
        pid = pipeline["id"]
        engine = self._engine(pipeline)
        if engine != "athena":
            return self._submit_emr_setup(pipeline, spec_json)
        self._batches.create(pid, batch_number=0,
                             partitions=["__full_table__"])
        return self._submit_batch_job(
            pipeline, spec_json, batch_number=0,
            partitions=["__full_table__"],
            script="rewrite_ctas.py",
            job_suffix="init")

    def _submit_emr_setup(self, pipeline: dict, spec_json: dict) -> dict:
        """Create the target Iceberg table via pyiceberg + Glue APIs.

        No EMR job needed — uses Glue GetTable for schema and pyiceberg
        to create the empty target table directly. Handles custom partition
        transforms, sort order, and table properties from the spec.
        """
        pid = pipeline["id"]
        src = spec_json.get("source", {})
        tgt = spec_json.get("target", {})
        ip = spec_json.get("iceberg_properties", {})

        source_database = src.get("database", "")
        source_table = src.get("table_name", "")
        region = self._source_region(pipeline)
        partition_columns = [pk["name"] for pk in src.get("partition_keys", [])]

        # Retry-from-scratch: clear any leftover data batches (>=1) from a
        # prior run so the state machine doesn't hard-fail on a stale failed
        # batch or double-process a stale pending one. Batch 0 is recreated
        # immediately below and batch 1 is re-armed after create-table.
        if hasattr(self._batches, "delete_from_number"):
            removed = self._batches.delete_from_number(pid, 1)
            if removed:
                log.info("Pipeline %s: cleared %d leftover data batch(es) "
                         "before retry", pid[:8], removed)

        self._batches.create(pid, batch_number=0, partitions=["__create_table__"])
        self._batches.mark_running(0, "in-process", pipeline_id=pid)
        self._repo.update_status(pid, "running")

        try:
            # Fetch schema from Glue catalog
            from ..strategies.morph import _fetch_glue_schema
            from ..morph.manifest import (
                ManifestBuilder, schema_from_glue, build_partition_spec,
                build_partition_spec_from_transforms, build_sort_order_from_spec)

            log.info("Pipeline %s: fetching Glue schema for %s.%s",
                     pid[:8], source_database, source_table)
            glue_schema = _fetch_glue_schema(source_database, source_table, region)
            columns = glue_schema["columns"]
            partition_keys = glue_schema["partition_keys"]

            if not partition_columns and partition_keys:
                partition_columns = [pk["name"] for pk in partition_keys]

            schema = schema_from_glue(columns, partition_keys)

            # Reconcile the catalog schema against the ACTUAL Parquet physical
            # types. Glue metadata sometimes disagrees with the data (e.g. a
            # column declared 'int' that is physically stored as INT64/long),
            # which otherwise makes the Spark load abort with
            # SchemaColumnConvertNotSupportedException. We widen safe numeric
            # mismatches (int->long, float->double) and fail loudly on any
            # incompatible disagreement rather than silently coercing.
            source_format = (src.get("format", "")
                             or pipeline.get("source_format", "")).lower()
            source_location = src.get("location", "")
            # Only Hive/plain-Parquet sources are safe to sample directly. Table
            # formats like Hudi/Delta/Iceberg keep non-Parquet metadata (and, for
            # Hudi, base files interleaved with logs) under the same prefix, so a
            # naive "first file" read is unreliable — skip reconciliation there
            # and trust the catalog schema.
            if source_location and source_format in ("", "parquet"):
                from ..morph.manifest import (
                    reconcile_schema_with_parquet)
                from ..morph.scanner import (
                    data_file_candidates, read_parquet_schema)
                parquet_types = None
                try:
                    candidates = data_file_candidates(source_location, region, limit=5)
                except Exception as e:
                    candidates = []
                    log.warning("Pipeline %s: could not list source files (%s)",
                                pid[:8], e)
                # Try candidates until one reads as Parquet — tolerates a stray
                # non-Parquet object slipping past the listing filters.
                for cand in candidates:
                    try:
                        parquet_types = read_parquet_schema(cand)
                        break
                    except Exception as e:
                        log.warning("Pipeline %s: sample file not readable as "
                                    "Parquet (%s): %s", pid[:8], cand, e)
                if not candidates:
                    log.warning("Pipeline %s: no source data files under %s; "
                                "skipping schema reconciliation",
                                pid[:8], source_location)
                elif parquet_types is None:
                    log.warning("Pipeline %s: no readable Parquet sample found; "
                                "skipping schema reconciliation", pid[:8])
                if parquet_types:
                    # A genuine incompatible type conflict IS fatal.
                    schema = reconcile_schema_with_parquet(schema, parquet_types)

            # Build partition spec — prefer custom transforms from iceberg_properties
            custom_transforms = ip.get("partition_spec", [])
            if custom_transforms:
                p_spec = build_partition_spec_from_transforms(custom_transforms, schema)
            else:
                p_spec = build_partition_spec(partition_columns, schema)

            # Build sort order from spec
            sort_fields = ip.get("sort_order", [])
            sort_order = build_sort_order_from_spec(sort_fields, schema) if sort_fields else None

            # Table properties
            table_props = ip.get("table_properties", {
                "format-version": "2",
                "write.parquet.compression-codec": "zstd",
            })

            # Determine target location
            target_location = tgt.get("location", "")
            if not target_location and tgt.get("type") == "s3_iceberg":
                src_loc = src.get("location", "").rstrip("/")
                base = src_loc.rsplit("/", 1)[0] if "/" in src_loc else src_loc
                target_location = f"{base}/{tgt.get('table_name', '')}/"

            # Create the Iceberg table via pyiceberg. For S3 Tables targets the
            # builder uses the Glue Iceberg REST catalog (SigV4); for
            # Iceberg-on-S3 it uses the Glue catalog as before.
            catalog_type = tgt.get("catalog", "glue")
            target_type = pipeline.get("target_type", "") or tgt.get("type", "")
            table_bucket_arn = (pipeline.get("target_bucket_arn", "")
                                or tgt.get("table_bucket_arn", ""))
            account_id = tgt.get("aws_account_id", "")
            builder = ManifestBuilder(
                catalog_name=catalog_type,
                catalog_type=catalog_type,
                catalog_props={"region_name": region},
                warehouse=target_location,
                target_type=target_type,
                table_bucket_arn=table_bucket_arn,
                region=region,
                account_id=account_id)

            builder.ensure_table(
                tgt.get("database", ""),
                tgt.get("table_name", ""),
                schema, p_spec,
                location=target_location,
                sort_order=sort_order,
                properties=table_props)

            log.info("Pipeline %s: target table created via pyiceberg (%s.%s)",
                     pid[:8], tgt.get("database", ""), tgt.get("table_name", ""))

            # Create the data-load batch(es) BEFORE marking batch 0 completed.
            # Batch 0 completes in-process here, but the reconcile loop runs
            # concurrently on its own thread and can reconcile this pipeline the
            # instant batch 0 flips to "completed". If batch 1 does not yet
            # exist at that moment, _advance_initial_load sees the only batch
            # (0) already completed and jumps the pipeline straight to in_sync,
            # stranding the unstarted initial-load batches (observed as a
            # pipeline that "completed batch 0 and immediately went in_sync"
            # with no data loaded). Creating batch 1+ first closes that window:
            # by the time batch 0 is completed, pending data batches always
            # exist. For date-partitioned sources this resolves the range from
            # Glue and creates concrete date-range batches (see
            # _ensure_first_data_batch); it reads only source metadata and does
            # not depend on the freshly-created target table.
            self._ensure_first_data_batch(pipeline, spec_json)

            self._batches.mark_completed(0, pipeline_id=pid)

        except Exception as e:
            log.error("Pipeline %s: table creation failed: %s", pid[:8], e)
            self._batches.mark_failed(0, error_message=str(e)[:500],
                                      pipeline_id=pid)
            return self._repo.update_status(
                pid, "failed", error_message=f"Table creation failed: {e}")

        # Mark the pipeline as in initial-load so the reconcile loop routes to
        # batch advancement (Phase 2), not the in_sync/change-detection phase.
        self._repo.update_sync_status(pid, "initial_load")

        # Batch 0 (create-table) completes in-process on the EMR path, so it
        # never flows through on_job_success(batch 0). Run the same post-batch-0
        # setup here — apply sort order and, critically, create the S3 event
        # tracker (Lambda). _post_batch0_setup raises on tracker failure, which
        # we surface as a pipeline failure with an accurate message.
        try:
            self._post_batch0_setup(pipeline)
        except Exception as e:
            return self._repo.update_status(
                pid, "failed",
                error_message=f"S3 event tracker setup failed: {e}")

        return self._repo.update_status(pid, "running")

    def _ensure_first_data_batch(self, pipeline: dict, spec_json: dict) -> None:
        """Create batch 1 (the first data-load batch).

        Shared by the Athena and EMR paths so their batch progression can't
        drift. Decides between date-range batches and a single full INSERT
        based on the source partition keys.

        Discovery differs by engine:
          - Athena: create a __discover_date_range__ marker batch. The
            orchestrator later runs a lightweight MIN/MAX query and expands
            it into date-range batches.
          - EMR: resolve the min/max range NOW from Glue partition metadata
            (in-process, no Spark job) and create the concrete date-range
            batches directly. This avoids submitting a Spark "discovery" job
            whose printed result can't be read back through the Athena API.

        Uses create() (a put that resets the record to pending), so on a
        retry-from-scratch batch 1 is re-armed rather than left in a stale
        completed/failed state.
        """
        pid = pipeline["id"]
        src = spec_json.get("source", {})
        partition_keys = src.get("partition_keys", [])
        date_info = _detect_date_partitioning(partition_keys)

        if not date_info:
            self._batches.create(pid, batch_number=1,
                                 partitions=["__full_insert__"])
            return

        engine = self._engine(pipeline)
        if engine != "athena":
            # EMR: discover the range from Glue partitions in-process and
            # create concrete date-range batches (batch 1..N).
            batch_count = (spec_json or {}).get("strategy", {}).get(
                "batch_size", _DEFAULT_BATCH_SIZE)
            min_val, max_val = self._discover_date_range(pipeline, date_info, src)
            if min_val and max_val:
                date_batches = _generate_date_range_batches(
                    min_val, max_val, date_info.primary_column,
                    batch_count, mode=date_info.mode, columns=date_info.columns)
                if date_batches:
                    for i, batch_marker in enumerate(date_batches):
                        self._batches.create(pid, batch_number=1 + i,
                                             partitions=[batch_marker])
                    log.info("Pipeline %s: created %d date-range batches (%s to %s)",
                             pid[:8], len(date_batches), min_val, max_val)
                    return
            # Glue discovery yielded nothing usable — fall back to full insert.
            log.warning("Pipeline %s: date-range discovery from Glue found no "
                        "partitions; falling back to full insert", pid[:8])
            self._batches.create(pid, batch_number=1,
                                 partitions=["__full_insert__"])
            return

        # Athena: keep the discovery-marker batch (query-based discovery).
        cols_str = ",".join(c["name"] for c in date_info.columns)
        self._batches.create(
            pid, batch_number=1,
            partitions=[f"__discover_date_range__{date_info.mode}__{cols_str}__"])

    def _submit_batch_job(self, pipeline: dict, spec_json: dict,
                          batch_number: int, partitions: list[str],
                          script: str,
                          job_suffix: str = "") -> dict:
        """Submit a batch job via EMR Serverless or Athena."""
        pid = pipeline["id"]
        engine = self._engine(pipeline)

        if engine == "athena":
            return self._submit_batch_athena(
                pipeline, spec_json, batch_number, partitions)

        # EMR Serverless path
        spark_props = _build_iceberg_spark_props(pipeline)
        script_path = _get_script_path(script)

        suffix = job_suffix or f"batch-{batch_number}"
        job_name = (
            f"IMA-incremental-{suffix}-"
            f"{pipeline['source_database']}-"
            f"{pipeline['source_table']}")

        if script == "rewrite_ctas.py":
            script_args = ["--spec", json.dumps(spec_json)]
        else:
            script_args = [
                "--spec", json.dumps(spec_json),
                "--partitions", json.dumps(partitions),
            ]

        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=script_args,
                spark_properties=spark_props,
                packages=_packages_for_source(pipeline),
            )
        except Exception as e:
            log.error("Failed to submit batch %d for pipeline %s: %s",
                      batch_number, pid, e)
            batch = self._batches.get_by_number(pid, batch_number)
            if batch:
                self._batches.mark_failed(batch["id"],
                                          error_message=str(e))
            return self._repo.update_status(
                pid, "failed",
                error_message=f"EMR submit failed (batch {batch_number}): {e}")

        log.info("Submitted batch %d job %s for pipeline %s "
                 "(%d partitions)",
                 batch_number, job_run_id, pid, len(partitions))

        batch = self._batches.get_by_number(pid, batch_number)
        if batch:
            self._batches.mark_running(batch["id"], job_run_id, pipeline_id=pid)

        self._repo.update_emr_job_run_id(pid, job_run_id)
        return self._repo.update_status(pid, "running")

    def _submit_batch_athena(self, pipeline: dict, spec_json: dict,
                             batch_number: int,
                             partitions: list[str]) -> dict:
        """Submit a batch via Athena: CREATE TABLE (batch 0) or INSERT INTO (subsequent)."""
        pid = pipeline["id"]
        src = spec_json.get("source", {})
        tgt = spec_json.get("target", {})
        ip = spec_json.get("iceberg_properties", {})

        target_loc = ""
        if pipeline.get("target_type") != "s3_tables":
            target_loc = tgt.get("location", "")
            if not target_loc:
                sl = src.get("location", "").rstrip("/")
                base = sl.rsplit("/", 1)[0] if "/" in sl else sl
                target_loc = f"{base}/{tgt.get('table_name', 'iceberg')}/"

        if batch_number == 0 and partitions in (["__initial_ctas__"], ["__full_table__"]):
            # Step 1: Drop target table if it exists (handles retries cleanly)
            target_type = pipeline.get("target_type", "")
            table_bucket_arn = pipeline.get("target_bucket_arn", "")
            drop_sql = athena_drop_table(
                tgt["database"], tgt["table_name"], target_type, table_bucket_arn)
            try:
                drop_qid = self._athena.submit_query(
                    drop_sql, database=src["database"],
                    description=f"IMA-incremental-drop-existing-{tgt.get('table_name','')}")
                # Wait for drop to complete before creating
                poll_until_complete(
                    check=lambda: self._athena.get_query_status(drop_qid),
                    is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED"),
                    interval=1.0, max_attempts=30)
            except Exception as e:
                log.warning("Drop existing table failed (continuing): %s", e)

            # Step 2: Create namespace if needed (S3 Tables)
            if target_type == "s3_tables" and table_bucket_arn:
                try:
                    ensure_s3tables_namespace(table_bucket_arn, tgt["database"])
                except NamespaceError as e:
                    log.error("Pipeline %s: %s", pid[:8], e)
                    return self._repo.update_status(
                        pid, "failed", error_message=str(e)[:500])
            # Step 3: Create empty Iceberg table with source schema
            create_sql = self._athena.build_create_table_sql(
                source_db=src["database"],
                source_table=src["table_name"],
                target_db=tgt["database"],
                target_table=tgt["table_name"],
                target_location=target_loc,
                partition_spec=ip.get("partition_spec"),
                sort_order=ip.get("sort_order"),
                table_properties=ip.get("table_properties"),
                target_type=target_type,
                table_bucket_arn=table_bucket_arn)

            try:
                qid = self._athena.submit_query(
                    create_sql, database=src["database"],
                    description=f"IMA-incremental-create-table-{tgt.get('table_name','')}")
            except Exception as e:
                log.error("Athena CREATE TABLE failed for pipeline %s: %s", pid, e)
                batch = self._batches.get_by_number(pid, batch_number)
                if batch:
                    self._batches.mark_failed(batch["id"], error_message=str(e))
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"Athena CREATE TABLE failed: {e}")

            batch_rec = self._batches.get_by_number(pid, batch_number)
            if batch_rec:
                self._batches.mark_running(batch_rec["id"], f"athena:{qid}", pipeline_id=pid)

            # Determine batching strategy and create the first data-load batch.
            # Shared with the EMR path so the two engines can't drift.
            if partitions == ["__full_table__"]:
                # Unpartitioned full-table load — force a single full INSERT
                self._batches.create(pid, batch_number=1,
                                     partitions=["__full_insert__"])
            else:
                self._ensure_first_data_batch(pipeline, spec_json)

            # Track the create table query — reconcile will submit next batch after CREATE completes
            self._repo.update_emr_job_run_id(pid, f"athena:{qid}")
            return self._repo.update_status(pid, "running")

        elif partitions and len(partitions) == 1 and partitions[0].startswith("__discover_date_range__"):
            # Discovery batch: query min/max of the date partition column
            # Format: __discover_date_range__{mode}__{col1,col2,...}__
            marker = partitions[0].replace("__discover_date_range__", "", 1).strip("_")
            marker_parts = marker.split("__")
            mode = marker_parts[0]
            cols_str = marker_parts[1] if len(marker_parts) > 1 else ""
            col_names = cols_str.split(",")

            validate_identifier(src["database"])
            validate_identifier(src["table_name"])

            # Build discovery SQL based on mode
            if mode == "composite_ymd":
                # For year/month/day: construct a synthetic date from components
                year_col = col_names[0]
                month_col = col_names[1]
                day_col = col_names[2] if len(col_names) >= 3 else None
                if day_col:
                    date_expr = f"DATE(CAST({year_col} AS VARCHAR) || '-' || LPAD(CAST({month_col} AS VARCHAR), 2, '0') || '-' || LPAD(CAST({day_col} AS VARCHAR), 2, '0'))"
                else:
                    date_expr = f"DATE(CAST({year_col} AS VARCHAR) || '-' || LPAD(CAST({month_col} AS VARCHAR), 2, '0') || '-01')"
                discover_sql = athena_select(
                    [f"CAST(MIN({date_expr}) AS VARCHAR) as min_val",
                     f"CAST(MAX({date_expr}) AS VARCHAR) as max_val"],
                    src["database"], src["table_name"])
            else:
                # Single column: just MIN/MAX cast to string
                date_col = col_names[0]
                discover_sql = athena_select(
                    [f"CAST(MIN({date_col}) AS VARCHAR) as min_val",
                     f"CAST(MAX({date_col}) AS VARCHAR) as max_val"],
                    src["database"], src["table_name"])

            try:
                qid = self._athena.submit_query(
                    discover_sql, database=src["database"],
                    description=f"IMA-incremental-discover-range-{cols_str}")
            except Exception as e:
                log.error("Athena date range discovery failed for pipeline %s: %s", pid, e)
                batch = self._batches.get_by_number(pid, batch_number)
                if batch:
                    self._batches.mark_failed(batch["id"], error_message=str(e))
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"Date range discovery failed: {e}")

            job_ref = f"athena:{qid}"
            batch_rec = self._batches.get_by_number(pid, batch_number)
            if batch_rec:
                self._batches.mark_running(batch_rec["id"], job_ref, pipeline_id=pid)
            self._repo.update_emr_job_run_id(pid, job_ref)
            return self._repo.update_status(pid, "running")

        elif partitions and len(partitions) == 1 and partitions[0] == "__full_insert__":
            # Non-partitioned: single full INSERT
            sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                target_type=pipeline.get("target_type", ""),
                table_bucket_arn=pipeline.get("target_bucket_arn", ""))

            try:
                qid = self._athena.submit_query(
                    sql, database=src["database"],
                    description=f"IMA-incremental-load-{tgt.get('table_name', '')}")
            except Exception as e:
                log.error("Athena INSERT failed for pipeline %s: %s", pid, e)
                batch = self._batches.get_by_number(pid, batch_number)
                if batch:
                    self._batches.mark_failed(batch["id"], error_message=str(e))
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"Athena INSERT failed: {e}")

            job_ref = f"athena:{qid}"
            log.info("Submitted Athena full INSERT for pipeline %s (query=%s)", pid, qid)
            batch = self._batches.get_by_number(pid, batch_number)
            if batch:
                self._batches.mark_running(batch["id"], job_ref, pipeline_id=pid)
            self._repo.update_emr_job_run_id(pid, job_ref)
            return self._repo.update_status(pid, "running")

        elif partitions and len(partitions) == 1 and partitions[0].startswith("__date_range__"):
            # Date-range batch INSERT: filter by date range (oldest to newest)
            # Format: __date_range__{mode}__{col1,col2,...}__{start}__{end}__
            range_str = partitions[0].replace("__date_range__", "", 1).strip("_")
            range_parts = range_str.split("__")
            mode = range_parts[0]
            cols_str = range_parts[1]
            start_val = range_parts[2]
            end_val = range_parts[3]
            col_names = cols_str.split(",")


            # Build columns info for composite mode
            partition_keys = src.get("partition_keys", [])
            columns = [pk for pk in partition_keys if pk["name"] in col_names]
            if not columns:
                columns = [{"name": n, "type": "string"} for n in col_names]

            where_clause = _build_date_filter_sql(
                col_names[0], start_val, end_val, mode, columns)

            sql = athena_insert_into(
                target_db=tgt["database"], target_table=tgt["table_name"],
                source_db=src["database"], source_table=src["table_name"],
                where=where_clause,
                target_type=pipeline.get("target_type", ""),
                table_bucket_arn=pipeline.get("target_bucket_arn", ""))

            try:
                qid = self._athena.submit_query(
                    sql, database=src["database"],
                    description=f"IMA-incremental-daterange-{col_names[0]}-{start_val}-to-{end_val}")
            except Exception as e:
                log.error("Athena date-range INSERT failed for pipeline %s: %s", pid, e)
                batch = self._batches.get_by_number(pid, batch_number)
                if batch:
                    self._batches.mark_failed(batch["id"], error_message=str(e))
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"Athena date-range INSERT failed: {e}")

            job_ref = f"athena:{qid}"
            log.info("Submitted Athena date-range INSERT batch %d for pipeline %s "
                     "(%s: %s to %s)", batch_number, pid, col_names[0], start_val, end_val)
            batch = self._batches.get_by_number(pid, batch_number)
            if batch:
                self._batches.mark_running(batch["id"], job_ref, pipeline_id=pid)
            self._repo.update_emr_job_run_id(pid, job_ref)
            return self._repo.update_status(pid, "running")

        else:
            # Partition-based INSERT for ongoing sync
            filters = []
            for pk_str in partitions:
                if pk_str in ("__unpartitioned__", "__initial_ctas__", "__full_table__"):
                    continue
                parts = []
                for kv in pk_str.split("&"):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        try:
                            int(v)
                            parts.append(f"{k} = {v}")
                        except ValueError:
                            parts.append(f"{k} = '{v}'")
                if parts:
                    filters.append(f"({' AND '.join(parts)})")
            where = " OR ".join(filters) if filters else ""
            sql = self._athena.build_insert_into_sql(
                source_db=src["database"],
                source_table=src["table_name"],
                target_db=tgt["database"],
                target_table=tgt["table_name"],
                partition_filter=where)

            try:
                qid = self._athena.submit_query(
                    sql, database=src["database"],
                    description=f"IMA-incremental-batch-{batch_number}")
            except Exception as e:
                log.error("Athena submit failed batch %d pipeline %s: %s",
                          batch_number, pid, e)
                batch = self._batches.get_by_number(pid, batch_number)
                if batch:
                    self._batches.mark_failed(batch["id"],
                                              error_message=str(e))
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"Athena failed (batch {batch_number}): {e}")

            job_ref = f"athena:{qid}"
            log.info("Submitted Athena batch %d query %s for pipeline %s",
                     batch_number, qid, pid)
            batch = self._batches.get_by_number(pid, batch_number)
            if batch:
                self._batches.mark_running(batch["id"], job_ref, pipeline_id=pid)
            self._repo.update_emr_job_run_id(pid, job_ref)
            return self._repo.update_status(pid, "running")

    # ═══════════════════════════════════════════════════════════════
    # STATE MACHINE HOOKS
    # ═══════════════════════════════════════════════════════════════

    def submit_batch(self, pipeline: dict, batch: dict) -> dict:
        """Submit the next pending batch as an Athena/EMR job."""
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        return self._submit_next_batch(pipeline, [batch])

    def on_job_success(self, pipeline: dict, batch=None) -> dict:
        """Handle a successful job completion (state-machine hook).

        Performs incremental-specific post-processing after the state machine
        has marked the batch completed and recorded cost:
          - sync batches      -> mark tracked S3 events processed
          - discovery batches -> read MIN/MAX results and expand into
                                 concrete date-range batches (Athena path)
          - batch 0           -> post-create-table setup (sort order + tracker)
        """
        # The state machine already marked the batch completed and recorded cost.
        # We need the incremental-specific post-processing.
        pid = pipeline["id"]

        if not batch:
            # Sync job completed (no running batch)
            return self._handle_sync_job_success(pipeline)

        # Check if this is a sync batch (not initial load)
        batch_partitions = batch.get("partitions", "[]")
        if isinstance(batch_partitions, str):
            batch_partitions = json.loads(batch_partitions)
        is_sync_batch = any(isinstance(p, str) and p.startswith("__sync__")
                           for p in batch_partitions)
        if is_sync_batch:
            return self._handle_sync_job_success(pipeline)

        # Update progress
        from ..core.state_machine import PipelineStateMachine
        PipelineStateMachine()._update_batch_progress(pid)

        if (batch_partitions and len(batch_partitions) == 1
                and batch_partitions[0].startswith("__discover_date_range__")):
            # Parse and create date-range batches
            marker = batch_partitions[0].replace("__discover_date_range__", "", 1).strip("_")
            marker_parts = marker.split("__")
            mode = marker_parts[0]
            cols_str = marker_parts[1] if len(marker_parts) > 1 else ""
            col_names = cols_str.split(",")
            job_ref = pipeline.get("emr_job_run_id", "")
            # Discovery-marker batches are only produced on the Athena path
            # (the EMR path resolves the range from Glue up front and creates
            # concrete date-range batches). If we somehow get here without an
            # Athena query ref, fail loudly rather than silently loading the
            # whole table as one batch.
            if not job_ref.startswith("athena:"):
                msg = (f"Discovery batch completed but job ref '{job_ref}' is "
                       f"not an Athena query; cannot read MIN/MAX results")
                log.error("Pipeline %s: %s", pid[:8], msg)
                self._batches.mark_failed(batch["batch_number"],
                                          error_message=msg, pipeline_id=pid)
                return self._repo.update_status(pid, "failed", error_message=msg)
            try:
                qid = job_ref.replace("athena:", "")
                results = self._athena.get_query_results(qid)
                min_val = results[0].get("min_val", "") if results else ""
                max_val = results[0].get("max_val", "") if results else ""
                if not (min_val and max_val):
                    # Athena query returned NULLs (common for Hudi/Delta tables
                    # that Athena can't read properly). Fall back to Glue
                    # partition metadata — same mechanism the EMR path uses.
                    log.warning("Pipeline %s: Athena discovery returned no "
                                "MIN/MAX for %s; falling back to Glue partitions",
                                pid[:8], cols_str)
                    spec_json = pipeline.get("spec_json")
                    if isinstance(spec_json, str):
                        spec_json = json.loads(spec_json)
                    src = (spec_json or {}).get("source", {})
                    partition_keys = src.get("partition_keys", [])
                    date_info = _detect_date_partitioning(partition_keys)
                    if date_info:
                        min_val, max_val = self._discover_date_range(
                            pipeline, date_info, src)
                    if not (min_val and max_val):
                        raise RuntimeError(
                            f"discovery returned no MIN/MAX for {cols_str} "
                            f"(both Athena query and Glue partition fallback)")
                spec_json = pipeline.get("spec_json")
                if isinstance(spec_json, str):
                    spec_json = json.loads(spec_json)
                batch_count = (spec_json or {}).get("strategy", {}).get("batch_size", 10)
                partition_keys = (spec_json or {}).get("source", {}).get("partition_keys", [])
                columns = [pk for pk in partition_keys if pk["name"] in col_names]
                if not columns:
                    columns = [{"name": n, "type": "string"} for n in col_names]
                date_batches = _generate_date_range_batches(
                    min_val, max_val, col_names[0], batch_count, mode=mode, columns=columns)
                if not date_batches:
                    raise RuntimeError(
                        f"no date-range batches generated for {min_val}..{max_val}")
                for i, batch_marker in enumerate(date_batches):
                    self._batches.create(pid, batch_number=batch["batch_number"] + 1 + i,
                                         partitions=[batch_marker])
                log.info("Pipeline %s: created %d date-range batches (%s to %s)",
                         pid[:8], len(date_batches), min_val, max_val)
            except Exception as e:
                msg = f"Date range discovery failed: {e}"
                log.error("Pipeline %s: %s", pid[:8], msg)
                self._batches.mark_failed(batch["batch_number"],
                                          error_message=msg, pipeline_id=pid)
                return self._repo.update_status(pid, "failed", error_message=msg)

        # Batch 0 completion: apply sort order + setup tracker
        if batch["batch_number"] == 0:
            self._post_batch0_setup(pipeline)

        self._repo.update_emr_job_run_id(pid, "")
        return self._repo.get(pid)

    def on_initial_load_complete(self, pipeline: dict) -> dict:
        """All initial load batches done — transition to in_sync."""
        pid = pipeline["id"]
        self._repo.update_sync_status(pid, "in_sync")
        log.info("Pipeline %s: initial load complete, in_sync", pid[:8])
        return self._repo.get(pid)

    @staticmethod
    def _is_commit_tracked(pipeline: dict) -> bool:
        """True for sources tracked by commit timeline (hudi/delta) rather than
        S3 object events. These sync via full-partition-rewrite of the
        partitions changed since the last synced commit."""
        return (pipeline.get("source_format") or "").lower() in ("hudi", "delta")

    @staticmethod
    def _source_region(pipeline: dict) -> str:
        spec = pipeline.get("spec_json")
        if isinstance(spec, str):
            try:
                spec = json.loads(spec)
            except (ValueError, TypeError):
                spec = {}
        src = (spec or {}).get("source", {}) if isinstance(spec, dict) else {}
        # The source Glue catalog lives in the account/region the engine runs
        # in. When the spec doesn't carry an explicit source.region, fall back
        # to the engine's configured region (ENGINE_ATHENA_REGION /
        # AWS_DEFAULT_REGION) — NOT a hardcoded us-east-1, which caused Glue
        # GetTable EntityNotFoundException when the engine ran in another region.
        return (src.get("region")
                or pipeline.get("source_region")
                or os.getenv("ENGINE_ATHENA_REGION")
                or os.getenv("AWS_DEFAULT_REGION")
                or "us-east-1")

    @staticmethod
    def _last_synced_commit(pipeline: dict) -> str | None:
        spec = pipeline.get("spec_json")
        if isinstance(spec, str):
            try:
                spec = json.loads(spec)
            except (ValueError, TypeError):
                spec = {}
        meta = (spec or {}).get("_metadata", {}) if isinstance(spec, dict) else {}
        return meta.get("last_synced_commit")

    def detect_changes(self, pipeline: dict) -> bool:
        """Check if the source has changes to sync.

        - hudi/delta: compare the source's latest commit against the stored
          last_synced_commit (commit-timeline tracking).
        - everything else: check for pending S3 object events.
        """
        if self._is_commit_tracked(pipeline):
            from ..xtable.commit_tracker import has_new_commits
            fmt = (pipeline.get("source_format") or "").lower()
            source_location = pipeline.get("source_location", "")
            if not source_location:
                return False
            has_new, _latest = has_new_commits(
                fmt, source_location, self._last_synced_commit(pipeline),
                self._source_region(pipeline))
            return has_new
        pending = self._tracking.get_pending(pipeline["id"], limit=1)
        return len(pending) > 0

    def execute_sync(self, pipeline: dict, batch_number: int) -> dict:
        """Execute ongoing sync.

        - hudi/delta: full-partition-rewrite of the partitions changed since the
          last synced commit (works for both s3_iceberg and S3 Tables targets
          because data is copied into the target).
        - everything else: process pending S3 object events.

        Both paths create their own __sync__ batch and job. The commit cursor is
        advanced only after the sync job SUCCEEDS (see _handle_sync_job_success).
        """
        self._athena = AthenaClient.from_pipeline(pipeline)
        self._emr = EmrServerlessClient.from_pipeline(pipeline)
        if self._is_commit_tracked(pipeline):
            return self._sync_commit_tracked(pipeline)
        return self._process_events(pipeline)

    def _handle_sync_job_success(self, pipeline: dict) -> dict:
        """Handle sync job completion.

        - hudi/delta: advance the last_synced_commit cursor to the value pinned
          when this sync was submitted (stored on the pending sync batch).
        - everything else: mark the processed S3 events.
        """
        pid = pipeline["id"]

        if self._is_commit_tracked(pipeline):
            # Advance the cursor to the commit we pinned at sync submission time.
            # It was stashed on the running sync batch's partitions marker so a
            # concurrent source write between submit and success cannot advance
            # the cursor past what this job actually copied.
            pinned = self._pinned_sync_commit(pid)
            if pinned:
                self._repo.update_metadata(pid, "last_synced_commit", pinned)
                log.info("Pipeline %s: commit sync complete, cursor -> %s",
                         pid[:8], pinned)
            self._repo.update_emr_job_run_id(pid, "")
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        sync_started_at = pipeline.get("updated_at", "")
        pending_events = self._tracking.get_pending(pid)
        marked = 0
        for event in pending_events:
            event_received = event.get("received_at", "")
            if event_received and sync_started_at and event_received <= sync_started_at:
                self._tracking.mark_processed(event["id"], pipeline_id=pid)
                marked += 1
            elif not sync_started_at:
                self._tracking.mark_processed(event["id"], pipeline_id=pid)
                marked += 1
        if marked:
            log.info("Pipeline %s: sync complete, marked %d/%d events",
                     pid[:8], marked, len(pending_events))
        self._repo.update_emr_job_run_id(pid, "")
        self._repo.update_sync_status(pid, "in_sync")
        return self._repo.get(pid)

    def _pinned_sync_commit(self, pid: str) -> str | None:
        """Read the commit pinned on the most recent running/last commit-sync
        batch (encoded in its partitions marker as __sync__commit__<commit>__)."""
        batches = self._batches.list_by_pipeline(pid) or []
        # Prefer a running batch, else the highest-numbered commit-sync batch.
        candidates = sorted(batches, key=lambda b: b.get("batch_number", 0),
                            reverse=True)
        for b in candidates:
            parts = b.get("partitions")
            if isinstance(parts, str):
                try:
                    parts = json.loads(parts)
                except (ValueError, TypeError):
                    parts = []
            for marker in (parts or []):
                if isinstance(marker, str) and marker.startswith("__sync__commit__"):
                    # __sync__commit__<commit>__<n>_partitions__
                    body = marker[len("__sync__commit__"):]
                    commit = body.split("__", 1)[0]
                    return commit or None
        return None

    def _post_batch0_setup(self, pipeline: dict):
        """Post batch 0 (create-table) completion: apply sort order + set up
        the S3 event tracker.

        Called by both engines after the target table exists:
          - Athena: from on_job_success once the CREATE TABLE query succeeds.
          - EMR: inline from _submit_emr_setup (batch 0 runs in-process).
        """
        pid = pipeline["id"]
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)
        if not spec_json:
            return

        # Apply sort order if specified.
        # On EMR the table is created via pyiceberg with the sort order already
        # applied (ensure_table(..., sort_order=...)), so an Athena ALTER here
        # would be redundant and would make a pure-EMR pipeline depend on
        # Athena. Only the Athena path needs the post-create ALTER TABLE.
        ip = spec_json.get("iceberg_properties", {})
        sort_order = ip.get("sort_order")
        if sort_order and self._engine(pipeline) == "athena":
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

        # Set up ongoing change tracking.
        src = spec_json.get("source", {})
        source_location = src.get("location", "")

        # hudi/delta: commit-timeline tracking — NO S3 event tracker (and thus
        # no ENGINE_TRACKING_LAMBDA_ROLE_ARN dependency). Seed the initial
        # commit cursor to the source's current latest commit so the first sync
        # only picks up commits landed AFTER the initial load.
        if self._is_commit_tracked(pipeline):
            if source_location:
                from ..xtable.commit_tracker import has_new_commits
                fmt = (pipeline.get("source_format") or "").lower()
                try:
                    _has_new, latest = has_new_commits(
                        fmt, source_location, None, self._source_region(pipeline))
                    if latest:
                        self._repo.update_metadata(pid, "last_synced_commit", latest)
                        log.info("Pipeline %s: commit cursor seeded (%s) — %s "
                                 "commit tracking, no S3 event tracker", pid[:8],
                                 latest, fmt)
                    else:
                        log.warning("Pipeline %s: could not read initial %s commit; "
                                    "cursor unset (first sync will do a full rewrite)",
                                    pid[:8], fmt)
                except Exception as e:
                    # Non-fatal: an unset cursor just means the first sync
                    # rewrites everything (still correct).
                    log.warning("Pipeline %s: commit cursor seed failed: %s",
                                pid[:8], e)
            return

        # Everything else: provision the S3 event tracker.
        if source_location:
            try:
                from ..tracking import TrackingManager
                tracker = TrackingManager()
                tracker.create_tracker(
                    pid, source_location,
                    source_account=pipeline.get("source_account", ""))
                log.info("Pipeline %s: S3 event tracker created", pid[:8])
            except Exception as e:
                log.error("Pipeline %s: tracker creation failed: %s", pid[:8], e)
                self._repo.update_status(
                    pid, "failed",
                    error_message=f"S3 event tracker creation failed: {e}")
                raise

    def _submit_next_batch(self, pipeline: dict,
                           pending_batches: list[dict]) -> dict:
        """Submit the next pending batch."""
        batch = pending_batches[0]

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        partitions = batch.get("partitions", "[]")
        if isinstance(partitions, str):
            partitions = json.loads(partitions)

        # Determine script: initial CTAS for batch 0, incremental
        # rewrite for subsequent batches
        bn = batch["batch_number"]
        if bn == 0:
            script = "rewrite_ctas.py"
        else:
            script = "incremental_rewrite.py"

        return self._submit_batch_job(
            pipeline, spec_json, batch_number=bn,
            partitions=partitions, script=script,
            job_suffix=f"batch-{bn}")

    def _discover_date_range(self, pipeline: dict, date_info,
                             src: dict) -> tuple[str, str]:
        """Discover min/max date range using Glue partition metadata.

        Glue's partition `Values` tuples are POSITIONAL and follow the
        table's declared PartitionKeys order — which is not always
        [date, ...]. A table partitioned by (region, event_date) has
        event_date at index 1, not 0. This looks up each date column's
        actual index within a freshly-fetched, authoritative partition key
        list (rather than assuming position 0/1/2), so date discovery is
        correct regardless of how many other partition keys precede it.

        Returns (min_val, max_val) as ISO date strings, or (None, None) on failure.
        """
        import boto3
        database = src.get("database", pipeline.get("source_database", ""))
        table_name = src.get("table_name", pipeline.get("source_table", ""))
        region = self._source_region(pipeline)
        pid = pipeline["id"]

        try:
            # Fetch the table's PartitionKeys fresh from Glue — this is the
            # authoritative order that Values tuples are positional against,
            # rather than trusting the spec's source.partition_keys ordering.
            from ..strategies.morph import _fetch_glue_schema
            glue_schema = _fetch_glue_schema(database, table_name, region)
            full_partition_keys = glue_schema["partition_keys"]
            key_index = {pk["name"]: i for i, pk in enumerate(full_partition_keys)}

            glue = boto3.client("glue", region_name=region)
            paginator = glue.get_paginator("get_partitions")
            all_values = []
            for page in paginator.paginate(DatabaseName=database, TableName=table_name):
                for p in page.get("Partitions", []):
                    vals = p.get("Values", [])
                    if vals:
                        all_values.append(vals)

            if not all_values:
                log.warning("Pipeline %s: no Glue partitions found for %s.%s",
                            pid[:8], database, table_name)
                return None, None

            # Extract date values based on mode, using the REAL index of
            # each relevant column within the full partition key list.
            if date_info.mode == "composite_ymd":
                year_idx = key_index.get(date_info.columns[0]["name"])
                month_idx = key_index.get(date_info.columns[1]["name"])
                day_idx = (key_index.get(date_info.columns[2]["name"])
                          if len(date_info.columns) >= 3 else None)
                if year_idx is None or month_idx is None:
                    log.warning(
                        "Pipeline %s: could not locate year/month partition "
                        "columns in %s.%s's partition keys", pid[:8],
                        database, table_name)
                    return None, None
                dates = []
                for vals in all_values:
                    try:
                        y, m = vals[year_idx], vals[month_idx]
                        if day_idx is not None and day_idx < len(vals):
                            d = f"{y}-{str(m).zfill(2)}-{str(vals[day_idx]).zfill(2)}"
                        else:
                            d = f"{y}-{str(m).zfill(2)}-01"
                        dates.append(d)
                    except (IndexError, ValueError):
                        continue
                if dates:
                    return min(dates), max(dates)
            else:
                date_col = date_info.primary_column
                date_idx = key_index.get(date_col)
                if date_idx is None:
                    log.warning(
                        "Pipeline %s: could not locate partition column "
                        "'%s' in %s.%s's partition keys", pid[:8], date_col,
                        database, table_name)
                    return None, None
                vals = sorted(v[date_idx] for v in all_values
                             if date_idx < len(v) and v[date_idx])
                if vals:
                    return vals[0], vals[-1]

        except Exception as e:
            log.warning("Pipeline %s: Glue partition discovery failed (%s), "
                        "falling back to full insert", pid[:8], e)

        return None, None

    def _sync_interval_elapsed(self, pipeline: dict) -> bool:
        """Check if enough time has passed since last sync.
        
        Uses sync_interval from the pipeline's spec (default 60s).
        Falls back to ENGINE_INCREMENTAL_INTERVAL_SECONDS env var.
        """
        # Get per-pipeline sync_interval from spec
        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            try:
                spec_json = json.loads(spec_json)
            except (json.JSONDecodeError, TypeError):
                spec_json = None
        sync_interval = _SYNC_INTERVAL  # global default
        if spec_json:
            sync_interval = spec_json.get("strategy", {}).get("sync_interval", _SYNC_INTERVAL)

        updated_at = pipeline.get("updated_at", "")
        if not updated_at:
            return True
        try:
            last = datetime.fromisoformat(
                updated_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return (now - last).total_seconds() >= sync_interval
        except (ValueError, TypeError):
            return True

    @staticmethod
    def _refresh_delta_symlink_manifest(pipeline: dict, region: str) -> None:
        """Regenerate a Delta table's symlink manifest so Athena's
        Glue-registered (SymlinkTextInputFormat) source reflects the latest
        commit before we SELECT from it.

        Delta on Athena is read via the symlink manifest, which is a
        point-in-time snapshot — stale until regenerated after each commit.
        delta-rs (the `deltalake` package) can regenerate it without Spark. If
        the library isn't available we log a clear warning: the sync proceeds
        but Athena may read the last-generated manifest (older data).
        """
        location = (pipeline.get("source_location") or "").rstrip("/")
        # Athena registers Delta against .../_symlink_format_manifest/; the
        # Delta table root is that path's parent.
        if location.endswith("/_symlink_format_manifest"):
            location = location.rsplit("/_symlink_format_manifest", 1)[0]
        if not location:
            return
        try:
            from deltalake import DeltaTable
            storage_options = {"region": region}
            dt = DeltaTable(location, storage_options=storage_options)
            # delta-rs exposes symlink manifest generation via the same verb as
            # Spark's Delta ("symlink_format_manifest").
            gen = getattr(dt, "generate", None)
            if callable(gen):
                gen("symlink_format_manifest")
                log.info("Pipeline %s: refreshed Delta symlink manifest at %s",
                         pipeline["id"][:8], location)
            else:
                log.warning(
                    "Pipeline %s: installed deltalake has no symlink-manifest "
                    "generator; Athena may read a stale Delta manifest. Consider "
                    "the EMR engine for Delta incremental.", pipeline["id"][:8])
        except ImportError:
            log.warning(
                "Pipeline %s: deltalake package not available to refresh the "
                "Delta symlink manifest; Athena may read stale data. Install "
                "deltalake in the engine, or use the EMR engine for Delta "
                "incremental.", pipeline["id"][:8])
        except Exception as e:
            log.warning("Pipeline %s: Delta symlink manifest refresh failed "
                        "(%s); Athena may read stale data.", pipeline["id"][:8], e)

    def _sync_commit_tracked(self, pipeline: dict) -> dict:
        """Commit-based sync for hudi/delta: full-partition-rewrite of the
        partitions changed since last_synced_commit.

        Determines the affected partitions (or the whole table when the precise
        set can't be scoped), then DELETE+INSERTs those partitions into the
        target — reading the source's CURRENT state via the target engine
        (Athena INSERT...SELECT from the Glue-registered source, or EMR
        incremental_rewrite.py). The commit being synced-to is pinned onto the
        sync batch marker; the cursor advances only on job success.
        """
        from ..xtable.commit_tracker import (
            get_affected_partitions_since, ALL_PARTITIONS)

        pid = pipeline["id"]
        fmt = (pipeline.get("source_format") or "").lower()
        source_location = pipeline.get("source_location", "")
        region = self._source_region(pipeline)
        last_synced = self._last_synced_commit(pipeline)

        affected, latest = get_affected_partitions_since(
            fmt, source_location, last_synced, region)

        if latest is None or affected == []:
            # Nothing new to sync (or no commits at all).
            log.info("Pipeline %s: commit sync — no new commits", pid[:8])
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        # Build the partition_list in the same "k=v&k2=v2" form the rest of the
        # sync machinery uses. ALL_PARTITIONS → whole-table rewrite.
        whole_table = (affected == ALL_PARTITIONS)
        if whole_table:
            partition_list: list[str] = []
            log.info("Pipeline %s: commit sync — rewriting the WHOLE table "
                     "(affected set unavailable) up to commit %s", pid[:8], latest)
        else:
            partition_list = [
                "&".join(f"{k}={v}" for k, v in sorted(pm.items()))
                for pm in affected
            ]
            log.info("Pipeline %s: commit sync — %d affected partition(s) "
                     "up to commit %s", pid[:8], len(partition_list), latest)

        engine = self._engine(pipeline)

        # Pin the target commit + partition count into the sync batch marker so
        # _handle_sync_job_success advances the cursor to exactly this commit.
        n_parts = len(partition_list) if partition_list else 0
        sync_label = f"__sync__commit__{latest}__{n_parts}_partitions__"
        existing_batches = self._batches.list_by_pipeline(pid)
        next_batch_num = max((b["batch_number"] for b in existing_batches),
                             default=-1) + 1

        if engine == "athena":
            # Delta on Athena is read through its symlink manifest — refresh it
            # to the current commit BEFORE the SELECT so Athena sees the latest
            # committed data (Hudi CoW is read natively and needs no refresh).
            if fmt == "delta":
                self._refresh_delta_symlink_manifest(pipeline, region)
            # Reuse the DELETE+INSERT partition-rewrite path. Not append-only:
            # we overwrite each affected partition with its current source state.
            src = spec_json.get("source", {}) if spec_json else {}
            tgt = spec_json.get("target", {}) if spec_json else {}
            source_ref = athena_table_ref(
                src.get("database", pipeline["source_database"]),
                src.get("table_name", pipeline["source_table"])).sql(dialect="trino")
            target_ref = self._athena.format_table_ref(
                tgt.get("database", pipeline.get("target_database", "")),
                tgt.get("table_name", pipeline.get("target_table", "")),
                pipeline.get("target_type", ""), pipeline.get("target_bucket_arn", ""))
            # Create the batch up front so the pinned commit is recorded even
            # though _sync_athena tracks the Athena query id itself.
            self._batches.create(pid, batch_number=next_batch_num,
                                 partitions=[sync_label])
            result = self._sync_athena(
                pipeline, partition_list, source_ref, target_ref,
                is_append_only=False,
                sync_start_time=datetime.now(timezone.utc).isoformat(),
                new_file_paths=None)
            # Bind the just-created batch to the Athena query the sync started.
            job_ref = self._repo.get(pid).get("emr_job_run_id", "")
            if job_ref:
                self._batches.mark_running(next_batch_num, job_ref, pipeline_id=pid)
            return result

        # EMR Serverless path — same script as the parquet sync, scoped to the
        # affected partitions. For a whole-table rewrite send the explicit
        # __full_rewrite__ marker (DELETE all + INSERT all); an empty list would
        # be a no-op on EMR (run() iterates the partition list).
        emr_partitions = ["__full_rewrite__"] if whole_table else partition_list
        spark_props = _build_iceberg_spark_props(pipeline)
        script_path = _get_script_path("incremental_rewrite.py")
        job_name = (f"IMA-incremental-commitsync-"
                    f"{pipeline['source_database']}-{pipeline['source_table']}")
        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=[
                    "--spec", json.dumps(spec_json),
                    "--partitions", json.dumps(emr_partitions),
                ],
                spark_properties=spark_props,
                packages=_packages_for_source(pipeline),
            )
        except Exception as e:
            log.error("Failed to submit commit-sync job for pipeline %s: %s", pid, e)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.update_status(
                pid, "failed", error_message=f"Commit sync submit failed: {e}")

        log.info("Submitted commit-sync job %s for pipeline %s (%d partitions)",
                 job_run_id, pid, n_parts)
        self._repo.update_emr_job_run_id(pid, job_run_id)
        self._repo.update_sync_status(pid, "syncing")
        self._batches.create(pid, batch_number=next_batch_num,
                             partitions=[sync_label])
        self._batches.mark_running(next_batch_num, job_run_id, pipeline_id=pid)
        return self._repo.get(pid)

    def _process_events(self, pipeline: dict) -> dict:
        """Ongoing sync: read pending S3 events, group by partition,
        submit appropriate sync job.
        
        Append-only optimization: if all events for a partition are 'created'
        (no deletes), use INSERT INTO with file path filter instead of
        DELETE + INSERT (avoids full partition rewrite).
        """
        pid = pipeline["id"]
        events = self._tracking.get_pending(pid)

        if not events:
            log.debug("Pipeline %s: no pending events", pid)
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        # Filter out zero-byte marker files (_SUCCESS, _committed_, etc.)
        # But keep delete events (they always have size_bytes=0 since S3 doesn't report deleted file size)
        marker_events = [e for e in events
                         if e.get("size_bytes", 0) == 0
                         and e.get("event_type") not in ("deleted", "ObjectRemoved")]
        events = [e for e in events
                  if e.get("size_bytes", 0) > 0
                  or e.get("event_type") in ("deleted", "ObjectRemoved")]

        # Mark marker files as processed immediately (they're not data)
        if marker_events:
            for e in marker_events:
                self._tracking.mark_processed(e["id"], pipeline_id=pid)
            log.info("Pipeline %s: skipped %d zero-byte marker file(s)",
                     pid[:8], len(marker_events))

        if not events:
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        # Record sync start time for race condition handling (Recommendation 4)
        sync_start_time = datetime.now(timezone.utc).isoformat()

        log.info("Pipeline %s: processing %d pending event(s)",
                 pid, len(events))

        spec_json = pipeline.get("spec_json")
        if isinstance(spec_json, str):
            spec_json = json.loads(spec_json)

        # Group events by partition
        dirty: dict[str, list[dict]] = {}
        unpartitioned: list[dict] = []

        for event in events:
            pv = event.get("partition_values")
            if isinstance(pv, str):
                try:
                    pv = json.loads(pv)
                except (json.JSONDecodeError, TypeError):
                    pv = None
            if not pv:
                pv = _extract_partition_from_key(event["key"])
            if pv:
                pk_str = "&".join(
                    f"{k}={v}" for k, v in sorted(pv.items()))
                dirty.setdefault(pk_str, []).append(event)
            else:
                unpartitioned.append(event)

        if not dirty and unpartitioned:
            dirty["__unpartitioned__"] = unpartitioned

        if not dirty:
            for event in events:
                self._tracking.mark_processed(event["id"], pipeline_id=pid)
            self._repo.update_sync_status(pid, "in_sync")
            return self._repo.get(pid)

        log.info("Pipeline %s: %d dirty partition(s)", pid[:8], len(dirty))

        # Determine if this is append-only (all events are 'created', no deletes)
        is_append_only = all(
            e.get("event_type") == "created"
            for events_list in dirty.values()
            for e in events_list
        )
        if is_append_only:
            log.info("Pipeline %s: append-only detected, using INSERT optimization", pid[:8])

        # Create progress records
        for pk_str, pk_events in dirty.items():
            total_bytes = sum(e.get("size_bytes", 0) for e in pk_events)
            parts = pk_str.split("&")
            pk_name = (parts[0].split("=")[0]
                       if "=" in parts[0] else "partition")
            rec = self._progress.upsert(
                pid, pk_name, pk_str,
                objects_total=len(pk_events),
                bytes_total=total_bytes)
            if rec:
                self._progress.mark_running(rec.get("sk") or rec.get("id"), pipeline_id=pid)

        # Submit the sync job
        partition_list = list(dirty.keys())
        engine = self._engine(pipeline)
        src = spec_json.get("source", {}) if spec_json else {}
        tgt = spec_json.get("target", {}) if spec_json else {}
        source_ref = athena_table_ref(
            src.get("database", pipeline["source_database"]),
            src.get("table_name", pipeline["source_table"])).sql(dialect="trino")
        target_ref = self._athena.format_table_ref(
            tgt.get("database", pipeline.get("target_database", "")),
            tgt.get("table_name", pipeline.get("target_table", "")),
            pipeline.get("target_type", ""), pipeline.get("target_bucket_arn", ""))

        # For append-only, collect the S3 paths of new files
        new_file_paths = []
        if is_append_only:
            for events_list in dirty.values():
                for e in events_list:
                    bucket = e.get("bucket", "")
                    key = e.get("key", "")
                    if bucket and key:
                        new_file_paths.append(f"s3://{bucket}/{key}")

        if engine == "athena":
            return self._sync_athena(pipeline, partition_list, source_ref, target_ref,
                                     is_append_only=is_append_only,
                                     sync_start_time=sync_start_time,
                                     new_file_paths=new_file_paths)

        # EMR Serverless path
        spark_props = _build_iceberg_spark_props(pipeline)
        script_path = _get_script_path("incremental_rewrite.py")
        job_name = (
            f"IMA-incremental-sync-"
            f"{pipeline['source_database']}-"
            f"{pipeline['source_table']}")

        try:
            job_run_id = self._emr.submit_job(
                name=job_name,
                script_path=script_path,
                script_args=[
                    "--spec", json.dumps(spec_json),
                    "--partitions", json.dumps(partition_list),
                ],
                spark_properties=spark_props,
                packages=_packages_for_source(pipeline),
            )
        except Exception as e:
            log.error("Failed to submit sync job for pipeline %s: %s",
                      pid, e)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.get(pid)

        log.info("Submitted sync job %s for pipeline %s "
                 "(%d partitions)", job_run_id, pid, len(partition_list))
        self._repo.update_emr_job_run_id(pid, job_run_id)
        self._repo.update_sync_status(pid, "syncing")

        # Record sync as a batch for tracking
        existing_batches = self._batches.list_by_pipeline(pid)
        next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
        sync_label = f"__sync__rewrite__{len(partition_list)}_partitions__"
        self._batches.create(pid, batch_number=next_batch_num,
                             partitions=[sync_label])
        self._batches.mark_running(next_batch_num, job_run_id, pipeline_id=pid)

        return self._repo.get(pid)

    def _sync_athena(self, pipeline: dict, partition_list: list[str],
                     source_ref: str, target_ref: str,
                     is_append_only: bool = False,
                     sync_start_time: str = "",
                     new_file_paths: list[str] = None) -> dict:
        """Sync dirty partitions via Athena.

        Append-only: INSERT INTO target with partition filter (no DELETE needed).
        Mixed (creates + deletes): DELETE partition then INSERT partition.

        Waits for DELETE to complete before submitting INSERT.
        Events are NOT marked processed here — only after the INSERT succeeds
        (handled by the reconcile SUCCESS path).
        Stores sync_start_time on the pipeline for race condition handling.
        """
        pid = pipeline["id"]

        # Store sync start time for race condition handling (Recommendation 4)
        # The SUCCESS handler will only mark events received BEFORE this time as processed
        if sync_start_time:
            self._repo.update_sync_status(pid, "syncing")

        # Build partition filter
        filters = []
        for pk_str in partition_list:
            if pk_str == "__unpartitioned__":
                continue
            parts = []
            for kv in pk_str.split("&"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    try:
                        int(v)
                        parts.append(f"{k} = {v}")
                    except ValueError:
                        if len(v) >= 10 and v[4:5] == '-' and v[7:8] == '-':
                            # Date or timestamp — extract just the date part
                            date_val = v[:10]
                            parts.append(f"{k} = DATE '{date_val}'")
                        else:
                            parts.append(f"{k} = '{v}'")
            if parts:
                filters.append(f"({' AND '.join(parts)})")

        where = " OR ".join(filters) if filters else ""

        if is_append_only and where and new_file_paths:
            # Append-only optimization: INSERT only the new files using $path filter
            # This avoids re-inserting existing data in the partition
            path_list = ", ".join(f"'{p}'" for p in new_file_paths)
            _src_db = pipeline["source_database"]
            _src_tbl = pipeline["source_table"]
            _tgt_db = pipeline.get("target_database", "")
            _tgt_tbl = pipeline.get("target_table", "")
            _tgt_type = pipeline.get("target_type", "")
            _tgt_arn = pipeline.get("target_bucket_arn", "")
            insert_sql = athena_insert_into(
                target_db=_tgt_db, target_table=_tgt_tbl,
                source_db=_src_db, source_table=_src_tbl,
                where=f"\"$path\" IN ({path_list})",
                target_type=_tgt_type, table_bucket_arn=_tgt_arn)
            log.info("Pipeline %s: append-only sync — INSERT %d new file(s) only",
                     pid[:8], len(new_file_paths))

            try:
                insert_qid = self._athena.submit_query(
                    insert_sql, database=pipeline["source_database"],
                    description=f"IMA-sync-append-{pid[:8]}")
                log.info("Submitted append INSERT for pipeline %s (qid=%s)",
                         pid[:8], insert_qid)
            except Exception as e:
                log.error("Failed to submit append INSERT for pipeline %s: %s", pid[:8], e)
                self._repo.update_sync_status(pid, "failed")
                return self._repo.update_status(
                    pid, "failed",
                    error_message=f"Sync append INSERT failed: {e}")

            self._repo.update_emr_job_run_id(pid, f"athena:{insert_qid}")
            self._repo.update_sync_status(pid, "syncing")

            # Record sync as a batch for tracking
            existing_batches = self._batches.list_by_pipeline(pid)
            next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
            sync_label = f"__sync__append__{len(new_file_paths)}_files__"
            self._batches.create(pid, batch_number=next_batch_num,
                                 partitions=[sync_label])
            self._batches.mark_running(next_batch_num, f"athena:{insert_qid}", pipeline_id=pid)

            return self._repo.get(pid)

        # Full partition rewrite: DELETE + INSERT
        _src_db = pipeline["source_database"]
        _src_tbl = pipeline["source_table"]
        _tgt_db = pipeline.get("target_database", "")
        _tgt_tbl = pipeline.get("target_table", "")
        _tgt_type = pipeline.get("target_type", "")
        _tgt_arn = pipeline.get("target_bucket_arn", "")
        if not where:
            delete_sql = athena_delete(
                _tgt_db, _tgt_tbl, where="1=1",
                target_type=_tgt_type, table_bucket_arn=_tgt_arn)
            insert_sql = athena_insert_into(
                target_db=_tgt_db, target_table=_tgt_tbl,
                source_db=_src_db, source_table=_src_tbl,
                target_type=_tgt_type, table_bucket_arn=_tgt_arn)
        else:
            delete_sql = athena_delete(
                _tgt_db, _tgt_tbl, where=where,
                target_type=_tgt_type, table_bucket_arn=_tgt_arn)
            insert_sql = athena_insert_into(
                target_db=_tgt_db, target_table=_tgt_tbl,
                source_db=_src_db, source_table=_src_tbl,
                where=where, target_type=_tgt_type, table_bucket_arn=_tgt_arn)

        # Step 1: Submit and WAIT for DELETE to complete
        try:
            delete_qid = self._athena.submit_query(
                delete_sql, database=pipeline["source_database"],
                description=f"IMA-sync-delete-{pid[:8]}")
            log.info("Submitted partition DELETE for pipeline %s (qid=%s)",
                     pid[:8], delete_qid)
        except Exception as e:
            log.error("Failed to submit DELETE for pipeline %s: %s", pid[:8], e)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.update_status(
                pid, "failed",
                error_message=f"Sync DELETE submit failed: {e}")

        # Wait for DELETE to complete
        def _check_delete():
            try:
                return self._athena.get_query_status(delete_qid)
            except Exception:
                return {"state": "PENDING"}

        status = poll_until_complete(
            check=_check_delete,
            is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"),
            interval=1.0, max_attempts=120)
        if status["state"] == "SUCCEEDED":
            log.info("DELETE completed for pipeline %s", pid[:8])
        elif status["state"] in ("FAILED", "CANCELLED"):
            reason = status.get("state_reason", "DELETE query failed")
            log.error("DELETE failed for pipeline %s: %s", pid[:8], reason)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.update_status(
                pid, "failed",
                error_message=f"Sync DELETE failed: {reason}")
        else:
            self._repo.update_sync_status(pid, "failed")
            return self._repo.update_status(
                pid, "failed",
                error_message="Sync DELETE timed out after 120 seconds")

        # Step 2: Submit INSERT (only after DELETE succeeded)
        try:
            insert_qid = self._athena.submit_query(
                insert_sql, database=pipeline["source_database"],
                description=f"IMA-sync-insert-{pid[:8]}")
            log.info("Submitted partition INSERT for pipeline %s (qid=%s)",
                     pid[:8], insert_qid)
        except Exception as e:
            log.error("Failed to submit INSERT for pipeline %s: %s", pid[:8], e)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.update_status(
                pid, "failed",
                error_message=f"Sync INSERT submit failed: {e}")

        # Track the INSERT query — reconcile will poll it and mark events processed on success
        self._repo.update_emr_job_run_id(pid, f"athena:{insert_qid}")
        self._repo.update_sync_status(pid, "syncing")

        # Record sync as a batch for tracking
        existing_batches = self._batches.list_by_pipeline(pid)
        next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1
        num_partitions = len([f for f in filters if f])
        sync_label = f"__sync__rewrite__{num_partitions}_partitions__"
        self._batches.create(pid, batch_number=next_batch_num,
                             partitions=[sync_label])
        self._batches.mark_running(next_batch_num, f"athena:{insert_qid}", pipeline_id=pid)

        return self._repo.get(pid)

    def cancel(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        self._athena = AthenaClient.from_pipeline(pipeline)
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

        # Destroy the S3 event tracker
        source_location = pipeline.get("source_location", "")
        if source_location:
            try:
                self._tracker.destroy_tracker(pid, source_location)
            except Exception as e:
                log.warning("Failed to destroy tracker for pipeline %s: %s", pid[:8], e)

        return self._repo.update_status(pid, "cancelled")
