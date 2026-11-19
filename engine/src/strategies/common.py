# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared utilities for migration strategies.

Extracted from incremental.py for reuse across strategies.
"""

import logging
from datetime import date, timedelta
from typing import Optional

log = logging.getLogger(__name__)


def s3tables_warehouse(table_bucket_arn: str, account_id: str = "") -> str:
    """Build the Glue Iceberg REST ``warehouse`` (catalog id) for an S3 Tables bucket.

    Format: ``<account_id>:s3tablescatalog/<bucket_name>``. Parsed from the S3
    Tables bucket ARN (arn:aws:s3tables:<region>:<account>:bucket/<name>) when
    present, falling back to the supplied account_id. Matches the identifier the
    engine's pyiceberg path uses, so EMR and the engine address the same catalog.
    """
    acct = account_id
    bucket_name = table_bucket_arn
    if table_bucket_arn.startswith("arn:"):
        parts = table_bucket_arn.split(":")
        if len(parts) >= 6:
            acct = parts[4] or account_id
            bucket_name = parts[5].split("/", 1)[-1]
    return f"{acct}:s3tablescatalog/{bucket_name}"


def s3tables_spark_props(pipeline: dict) -> dict:
    """Spark catalog props for S3 Tables on EMR, per AWS's documented config.

    AWS's EMR S3 Tables setup uses Iceberg's **GlueCatalog** with the S3 Tables
    catalog id supplied via ``glue.id`` — NOT a REST catalog. The catalog id is
    ``<account>:s3tablescatalog/<bucket>``. This is the config from the AWS docs:

        spark.sql.catalog.<name>              = org.apache.iceberg.spark.SparkCatalog
        spark.sql.catalog.<name>.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
        spark.sql.catalog.<name>.client.region= <region>
        spark.sql.catalog.<name>.glue.id      = <account>:s3tablescatalog/<bucket>
        spark.sql.defaultCatalog              = <name>

    The earlier REST-endpoint attempt (type=rest / RESTCatalog / sigv4 / uri)
    was wrong for EMR Spark: the runtime kept building a plain GlueCatalog and
    failed with "Cannot derive default warehouse location" because GlueCatalog
    had no catalog id to target. The missing piece was ``glue.id`` (the S3
    Tables catalog id), not the REST wiring. GlueCatalog + glue.id routes to the
    S3 Tables catalog directly.

    The catalog is named 'icebergrest' (matching spark_table_ref). The name is
    arbitrary; it just has to match between the config and the table refs.
    """
    region = pipeline.get("target_region") or "us-east-1"
    # The S3 Tables catalog id: <account>:s3tablescatalog/<bucket>
    catalog_id = s3tables_warehouse(
        pipeline.get("target_bucket_arn", ""),
        pipeline.get("target_account", ""))
    # Per the AWS EMR "use an Iceberg cluster with Spark" doc, the GlueCatalog
    # config REQUIRES a `warehouse` even when a catalog id (glue.id) is set:
    # GlueCatalog.defaultWarehouseLocation reads `warehouse` to place a NEW
    # table. Omitting it is what caused "Cannot derive default warehouse
    # location, warehouse path must not be null or empty" on CREATE. For S3
    # Tables the warehouse is the S3 Tables catalog id (same value as glue.id).
    return {
        "spark.sql.catalog.icebergrest":
            "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.icebergrest.catalog-impl":
            "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.icebergrest.warehouse": catalog_id,
        "spark.sql.catalog.icebergrest.glue.id": catalog_id,
        "spark.sql.catalog.icebergrest.client.region": region,
        # Make icebergrest the default catalog (AWS example sets this too).
        "spark.sql.defaultCatalog": "icebergrest",
    }


# Date-related type patterns for partition column detection
_DATE_TYPES = {"date", "timestamp", "datetime"}
_DATE_NAME_PATTERNS = {"date", "dt", "event_date", "partition_date",
                       "created_at", "updated_at", "ts",
                       "event_time", "load_date", "ingestion_date"}


class DatePartitionInfo:
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


def detect_date_partitioning(partition_keys: list[dict]) -> Optional[DatePartitionInfo]:
    """Detect date-based partitioning scheme.

    Returns DatePartitionInfo or None if no date partitioning found.
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
        return DatePartitionInfo("composite_ymd", cols)

    # Check for Iceberg transform types (month, day, year, hour → source column is date/timestamp)
    _ICEBERG_DATE_TRANSFORMS = {"month", "day", "year", "hour"}
    for pk in partition_keys:
        col_type = pk.get("type", "").lower()
        if col_type in _ICEBERG_DATE_TRANSFORMS:
            # The source column is date or timestamp — use CAST to DATE for safe comparison
            return DatePartitionInfo("single_date", [pk])

    # Check for single date/timestamp typed column
    for pk in partition_keys:
        col_type = pk.get("type", "").lower()
        if "timestamp" in col_type:
            return DatePartitionInfo("single_timestamp", [pk])
        if "date" in col_type:
            return DatePartitionInfo("single_date", [pk])

    # Check for single string column with date-like name
    for pk in partition_keys:
        col_name = pk.get("name", "").lower()
        if col_name in _DATE_NAME_PATTERNS:
            return DatePartitionInfo("single_string", [pk])
        if any(p in col_name for p in ("date", "_dt")):
            return DatePartitionInfo("single_string", [pk])

    return None


def generate_date_range_batches(min_val: str, max_val: str,
                                date_col: str,
                                batch_count: int,
                                mode: str = "single_date",
                                columns: list[dict] = None) -> list[str]:
    """Generate N date-range batch partition markers from oldest to newest.

    Returns list of partition strings like:
    __date_range__{mode}__{col1,col2,...}__{start}__{end}__
    """
    try:
        min_date = date.fromisoformat(min_val[:10])
        max_date = date.fromisoformat(max_val[:10])
    except (ValueError, TypeError):
        return []

    total_days = (max_date - min_date).days + 1
    if total_days <= 0:
        return []

    actual_batches = min(batch_count, total_days)
    days_per_batch = total_days / actual_batches

    cols_str = ",".join(c["name"] for c in columns) if columns else date_col

    batches = []
    for i in range(actual_batches):
        start = min_date + timedelta(days=int(i * days_per_batch))
        if i < actual_batches - 1:
            end = min_date + timedelta(days=int((i + 1) * days_per_batch))
        else:
            end = max_date + timedelta(days=1)
        batches.append(f"__date_range__{mode}__{cols_str}__{start.isoformat()}__{end.isoformat()}__")

    return batches


def build_date_filter_sql(date_col: str, start_val: str, end_val: str,
                          mode: str, columns: list[dict] = None) -> str:
    """Build a WHERE clause for date-range filtering based on partition mode.

    Uses CAST for safe type coercion — works with date, timestamp, and
    timestamp with time zone columns.
    """
    if mode == "single_date":
        # CAST to TIMESTAMP handles both date and timestamp(tz) columns
        return (f"CAST({date_col} AS DATE) >= DATE '{start_val}' "
                f"AND CAST({date_col} AS DATE) < DATE '{end_val}'")
    elif mode == "single_timestamp":
        return (f"{date_col} >= TIMESTAMP '{start_val} 00:00:00' "
                f"AND {date_col} < TIMESTAMP '{end_val} 00:00:00'")
    elif mode == "single_string":
        # If values look like ISO dates, use DATE literals for type safety
        # (handles cases where column is actually date/timestamp but mode was mis-detected)
        import re as _re
        if _re.match(r'^\d{4}-\d{2}-\d{2}$', start_val):
            return (f"CAST({date_col} AS DATE) >= DATE '{start_val}' "
                    f"AND CAST({date_col} AS DATE) < DATE '{end_val}'")
        return f"{date_col} >= '{start_val}' AND {date_col} < '{end_val}'"
    elif mode == "composite_ymd" and columns:
        try:
            start_d = date.fromisoformat(start_val[:10])
            end_d = date.fromisoformat(end_val[:10])
        except (ValueError, TypeError):
            return f"{date_col} >= '{start_val}' AND {date_col} < '{end_val}'"

        col_names = [c["name"] for c in columns]
        year_col = col_names[0]
        month_col = col_names[1]
        has_day = len(col_names) >= 3
        day_col = col_names[2] if has_day else None

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


def build_partition_where(partition_list: list[str]) -> str:
    """Build a WHERE clause from a list of partition key=value strings.
    
    Handles date values (YYYY-MM-DD format) with DATE cast,
    integers without quotes, and strings with quotes.
    """
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
                    if len(v) == 10 and v[4] == '-' and v[7] == '-':
                        parts.append(f"{k} = DATE '{v}'")
                    else:
                        parts.append(f"{k} = '{v}'")
        if parts:
            filters.append(f"({' AND '.join(parts)})")
    return " OR ".join(filters)


class NamespaceError(Exception):
    """The S3 Tables namespace could not be created or confirmed."""


def ensure_s3tables_namespace(table_bucket_arn: str, namespace: str) -> bool:
    """Create the target namespace in the table bucket if it is missing.

    Uses the S3 Tables API (GetNamespace, then CreateNamespace), which is
    deterministic and reports permission problems. The Athena path used to run
    CREATE SCHEMA and ignored a FAILED result, so a namespace that was never
    created only showed up later as "Schema ... not found" on the CTAS.

    Returns True if the namespace was created, False if it already existed.
    Raises NamespaceError with the reason on any failure. Needs
    s3tables:GetNamespace and s3tables:CreateNamespace on the table bucket.
    """
    from ..engines.s3tables_migration import S3TablesMigrationClient, region_from_arn
    try:
        client = S3TablesMigrationClient(region_from_arn(table_bucket_arn))
        created = client.ensure_namespace(table_bucket_arn, namespace)
    except Exception as e:  # botocore ClientError and anything else
        raise NamespaceError(
            f"Could not create namespace '{namespace}' in {table_bucket_arn}: {e}") from e
    if created:
        log.info("Created S3 Tables namespace '%s' in %s", namespace, table_bucket_arn)
    return created
