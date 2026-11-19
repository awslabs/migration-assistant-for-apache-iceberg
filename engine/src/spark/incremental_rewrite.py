# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PySpark script for incremental partition rewrite to Iceberg.

Submitted to EMR Serverless by the IncrementalStrategy and IcebergIncrementalStrategy.

Supports multiple batch types:
  - Partition key=value pairs: ["year=2024&month=06", "year=2024&month=07"]
  - Date-range markers: ["__date_range__single_date__event_date__2024-01-01__2024-04-01__"]
  - Full insert: ["__full_insert__"]
  - Discovery: ["__discover_date_range__single_date__event_date__"] (returns min/max as output)
  - Unpartitioned: ["__unpartitioned__"]

Source read strategy:
  For plain Hive/Parquet sources, reads via spark.read.parquet(source_location)
  which uses the PHYSICAL Parquet types — avoids SchemaColumnConvertNotSupportedException
  when the Glue catalog declares a narrower type than the physical data. For Hudi/Delta/
  Iceberg, reads via catalog table reference (essential for those formats' metadata layers).

Usage:
  spark-submit incremental_rewrite.py \\
    --spec '{"source": {...}, "target": {...}, ...}' \\
    --partitions '["year=2024&month=06", "year=2024&month=07"]'
"""

import argparse
import json
import re as _re
from datetime import date

from pyspark.sql import SparkSession

from spark_sql_builder import (
    spark_table_ref, spark_source_ref, delete_from,
)


# ── Value validation (kept — validates values, not identifiers) ──────────
def _validate_val(val: str) -> str:
    """Validate a SQL literal value. Raises ValueError on injection patterns."""
    if val and _re.search(r'[;\x00]|--|/\*', val):
        raise ValueError(f"Invalid SQL value: {val!r}")
    return val


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True,
                        help="Spec as JSON string")
    parser.add_argument("--partitions", required=True,
                        help="JSON array of partition strings to process")
    return parser.parse_args()


def build_source_table_name(spec: dict) -> str:
    """Source catalog table reference."""
    src = spec["source"]
    return spark_source_ref(src['database'], src['table_name'])


def build_target_table_name(spec: dict) -> str:
    tgt = spec["target"]
    return spark_table_ref(tgt['database'], tgt['table_name'], tgt.get('type', ''))


def _hive_type_to_spark_sql(hive_type: str) -> str:
    """Map a Glue/Hive partition column type string to a Spark SQL type
    name usable in a CAST expression.

    Only used for PARTITION columns (their values come from directory
    names, not file data, so there's no physical-vs-declared mismatch to
    worry about) — this makes the cast deterministic rather than depending
    on Spark's own partition-value type inference.
    """
    t = (hive_type or "").lower().strip()
    if t in ("string", "varchar", "char"):
        return "string"
    if t in ("int", "integer", "smallint", "tinyint"):
        return "int"
    if t == "bigint":
        return "bigint"
    if t == "float":
        return "float"
    if t == "double":
        return "double"
    if t == "boolean":
        return "boolean"
    if t == "date":
        return "date"
    if t.startswith("timestamp"):
        return "timestamp"
    if t.startswith("decimal"):
        return t  # e.g. "decimal(10,2)" is already valid Spark SQL syntax
    return "string"  # safe fallback


def _declared_column_order(spark, spec: dict) -> list[str]:
    """Column names in the source catalog's DECLARED order.

    Metadata-only lookup (resolves the Hive/Glue metastore's declared
    schema) — does NOT scan the physical data, so it cannot trip the
    physical-vs-declared type mismatch read_source() exists to avoid.
    Used to make the final column selection/order deterministic instead of
    depending on incidental Parquet-footer / partition-append ordering.
    """
    try:
        ref = build_source_table_name(spec)
        return [f.name for f in spark.table(ref).schema.fields]
    except Exception as e:
        print(f"Warning: could not fetch declared column order ({e}); "
              f"falling back to the physical read's natural column order")
        return []


def _detect_format_from_catalog(spark, spec: dict) -> str:
    """Detect the actual table format from the Hive/Glue catalog registration.

    Uses DESCRIBE FORMATTED (metadata-only, no data scan) to read the
    InputFormat / SerDe / table parameters and classify the format exactly
    like the app's inventory scanner does. Returns a lowercase format
    string ('parquet', 'delta', 'hudi', 'orc', 'iceberg', 'csv', 'json',
    'avro') or '' if detection fails (e.g. catalog access error).
    """
    try:
        ref = build_source_table_name(spec)
        rows = spark.sql(f"DESCRIBE FORMATTED {ref}").collect()
        text = " ".join(r[1] or "" for r in rows).lower()
        params_text = " ".join((r[2] or "") for r in rows).lower()
        combined = text + " " + params_text

        if "iceberg" in combined:
            return "iceberg"
        if "delta" in combined or "symlinktextinputformat" in combined:
            return "delta"
        if "hudi" in combined:
            return "hudi"
        if "orc" in combined:
            return "orc"
        if "avro" in combined:
            return "avro"
        if "json" in combined:
            return "json"
        if "csv" in combined or "opencsv" in combined:
            return "csv"
        if "parquet" in combined:
            return "parquet"
    except Exception as e:
        print(f"Warning: catalog format detection failed ({e}); "
              f"trusting spec's declared format")
    return ""


def read_source(spark, spec: dict):
    """Build the source DataFrame.

    For plain Parquet sources, reads directly by S3 path so Spark infers
    column types from the PHYSICAL Parquet footers rather than the
    Hive/Glue catalog's declared schema — this is what avoids
    SchemaColumnConvertNotSupportedException when the catalog's declared
    type disagrees with the physical data (e.g. a column registered as
    'int' in Glue that was actually written as INT64/long). Partition
    columns (not physically present in the row files) are reattached via
    Spark's built-in Hive-style partition discovery, then explicitly cast
    to the types declared in the spec (deterministic, rather than relying
    on Spark's own inference), and all columns are reordered to match the
    source catalog's declared order so a positional
    `INSERT INTO ... SELECT *` lands correctly on the target table.

    For Hudi/Delta/Iceberg sources, reads via the catalog reference —
    essential for those formats, since their native readers apply
    commit/transaction-log filtering that a raw path-based Parquet read
    would bypass (returning stale or duplicate files).
    """
    src = spec["source"]
    declared_fmt = src.get("format", "").lower()

    # The spec's declared format can be stale or wrong (e.g. the app
    # defaults an unclassified source to 'parquet'). Verify against the
    # catalog's ACTUAL registration before committing to a physical
    # path-based read — a wrong physical read on a Hudi/Delta/Iceberg
    # table would target the wrong S3 location entirely (e.g. a Delta
    # table's declared Location is its _symlink_format_manifest directory,
    # not its real data files).
    detected_fmt = _detect_format_from_catalog(spark, spec)
    if detected_fmt and detected_fmt != declared_fmt:
        print(f"Warning: spec declared source format '{declared_fmt}' but "
              f"the catalog registration indicates '{detected_fmt}'; "
              f"using '{detected_fmt}'")
    fmt = detected_fmt or declared_fmt

    if fmt == "delta":
        # Delta tables registered in Glue point their Location at the symlink
        # manifest (for Athena compatibility). Read directly from the spec's
        # source.location using Spark's native Delta reader.
        location = src.get("location", "").rstrip("/")
        if not location:
            ref = build_source_table_name(spec)
            print(f"No source location in spec; reading Delta via catalog: {ref}")
            return spark.table(ref)
        if location.endswith("/_symlink_format_manifest"):
            location = location.rsplit("/_symlink_format_manifest", 1)[0]
        print(f"Reading Delta source by path: {location}")
        return spark.read.format("delta").load(location)
    elif fmt == "hudi":
        # Read Hudi via path using the native Hudi reader, which honors the
        # commit timeline (only reads files from the latest valid snapshot,
        # not superseded/compacted files). Requires Hudi jars on the EMR
        # Serverless application classpath (configured via --packages or
        # --jars in the application's spark-defaults).
        location = src.get("location", "").rstrip("/")
        if not location:
            ref = build_source_table_name(spec)
            print(f"No source location in spec; reading Hudi via catalog: {ref}")
            return spark.table(ref)
        print(f"Reading Hudi source by path (native reader): {location}")
        return spark.read.format("hudi").load(location)
    elif fmt != "parquet":
        # Other formats (Iceberg, CSV, ORC, etc.) — use catalog reference
        ref = build_source_table_name(spec)
        print(f"Reading source via catalog: {ref}")
        return spark.table(ref)

    # Parquet path — read by S3 location with physical types
    location = src.get("location", "").rstrip("/")
    if not location:
        ref = build_source_table_name(spec)
        print(f"No source location in spec; reading via catalog: {ref}")
        return spark.table(ref)

    print(f"Reading source by path (physical types): {location}")
    df = spark.read.parquet(location)

    from pyspark.sql import functions as F
    partition_keys = src.get("partition_keys", [])
    for pk in partition_keys:
        name = pk.get("name", "")
        if name and name in df.columns:
            spark_type = _hive_type_to_spark_sql(pk.get("type", "string"))
            df = df.withColumn(name, F.col(name).cast(spark_type))

    declared_order = _declared_column_order(spark, spec)
    if declared_order:
        missing = [c for c in declared_order if c not in df.columns]
        if missing:
            raise RuntimeError(
                f"Column(s) {missing} are declared in the source catalog "
                f"but not found in the physical Parquet data at {location}")
        df = df.select(*declared_order)

    return df


def parse_partition_string(pk_str: str) -> dict[str, str]:
    """Parse 'year=2024&month=06' into {'year': '2024', 'month': '06'}."""
    result = {}
    for part in pk_str.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            result[k] = v
    return result


def build_partition_filter(partition_values: dict[str, str]) -> str:
    """Build a SQL WHERE clause from partition values (applied via
    spark.sql against the source temp view)."""
    conditions = []
    for k, v in partition_values.items():
        try:
            int(v)
            conditions.append(f"{k} = {v}")
        except ValueError:
            if len(v) == 10 and v[4] == '-' and v[7] == '-':
                conditions.append(f"{k} = DATE '{v}'")
            else:
                conditions.append(f"{k} = '{v}'")
    return " AND ".join(conditions)


def build_date_filter_sql(date_col: str, start_val: str, end_val: str,
                          mode: str, col_names: list[str]) -> str:
    """Build a WHERE clause for date-range filtering (Spark SQL syntax).

    Used for writing the INSERT INTO target (via SQL against the Iceberg
    catalog) with a filter applied to the source DataFrame registered as
    a temp view.
    """
    if mode == "single_date":
        return (f"CAST({date_col} AS DATE) >= DATE '{start_val}' "
                f"AND CAST({date_col} AS DATE) < DATE '{end_val}'")
    elif mode == "single_timestamp":
        return (f"{date_col} >= TIMESTAMP '{start_val} 00:00:00' "
                f"AND {date_col} < TIMESTAMP '{end_val} 00:00:00'")
    elif mode == "single_string":
        import re as _re
        if _re.match(r'^\d{4}-\d{2}-\d{2}$', start_val):
            return (f"CAST({date_col} AS DATE) >= DATE '{start_val}' "
                    f"AND CAST({date_col} AS DATE) < DATE '{end_val}'")
        return f"{date_col} >= '{start_val}' AND {date_col} < '{end_val}'"
    elif mode == "composite_ymd":
        try:
            start_d = date.fromisoformat(start_val[:10])
            end_d = date.fromisoformat(end_val[:10])
        except (ValueError, TypeError):
            return f"{date_col} >= '{start_val}' AND {date_col} < '{end_val}'"

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


# Name for the temp view registered from the source DataFrame
_SOURCE_VIEW = "__ima_source__"


def run(spec: dict, partitions: list[str]):
    spark = SparkSession.builder \
        .appName(f"IMA-incremental-{spec['source']['table_name']}") \
        .getOrCreate()

    try:
        source_df = read_source(spark, spec)
        # Register as a temp view so SQL INSERT INTO target SELECT ... works
        source_df.createOrReplaceTempView(_SOURCE_VIEW)

        target_table = build_target_table_name(spec)
        tgt = spec["target"]

        total = len(partitions)
        completed = 0
        failed = 0

        for pk_str in partitions:
            try:
                if pk_str == "__unpartitioned__" or pk_str == "__full_insert__":
                    # Full table INSERT (source is temp view, not catalog table)
                    print("Executing full table INSERT")
                    sql = (f"INSERT INTO {target_table} " # nosec B608
                           f"SELECT * FROM {_SOURCE_VIEW}")
                    print(f"SQL: {sql}")
                    spark.sql(sql)
                    completed += 1

                elif pk_str == "__full_rewrite__":
                    # Whole-table REWRITE: DELETE all rows then INSERT the
                    # source's current state. Used by the commit-tracked
                    # (hudi/delta) sync when the precise set of changed
                    # partitions can't be scoped, so we replace the entire
                    # table (DELETE+INSERT, NOT append) to stay correct.
                    delete_sql = delete_from(
                        tgt['database'], tgt['table_name'],
                        target_type=tgt.get('type', ''), where="1=1")
                    insert_sql = (f"INSERT INTO {target_table} " # nosec B608
                                  f"SELECT * FROM {_SOURCE_VIEW}")
                    print(f"Full rewrite DELETE: {delete_sql}")
                    spark.sql(delete_sql)
                    print(f"Full rewrite INSERT: {insert_sql}")
                    spark.sql(insert_sql)
                    completed += 1

                elif pk_str.startswith("__discover_date_range__"):
                    # Discovery: find min/max of date partition column
                    marker = pk_str.replace("__discover_date_range__", "").strip("_")
                    marker_parts = marker.split("__")
                    mode = marker_parts[0]
                    cols_str = marker_parts[1] if len(marker_parts) > 1 else ""
                    col_names = cols_str.split(",")
                    date_col = col_names[0]

                    if mode == "composite_ymd":
                        year_col, month_col = col_names[0], col_names[1]
                        day_col = col_names[2] if len(col_names) >= 3 else None
                        if day_col:
                            date_expr = f"DATE(CONCAT(CAST({year_col} AS STRING), '-', LPAD(CAST({month_col} AS STRING), 2, '0'), '-', LPAD(CAST({day_col} AS STRING), 2, '0')))"
                        else:
                            date_expr = f"DATE(CONCAT(CAST({year_col} AS STRING), '-', LPAD(CAST({month_col} AS STRING), 2, '0'), '-01'))"
                        discover_sql = f"SELECT CAST(MIN({date_expr}) AS STRING) as min_val, CAST(MAX({date_expr}) AS STRING) as max_val FROM {_SOURCE_VIEW}" # nosec B608
                    else:
                        discover_sql = f"SELECT CAST(MIN({date_col}) AS STRING) as min_val, CAST(MAX({date_col}) AS STRING) as max_val FROM {_SOURCE_VIEW}" # nosec B608

                    print(f"Discovery SQL: {discover_sql}")
                    result = spark.sql(discover_sql).collect()
                    if result:
                        min_val = result[0]["min_val"]
                        max_val = result[0]["max_val"]
                        print(f"DISCOVERY_RESULT:{json.dumps({'min_val': min_val, 'max_val': max_val})}")
                    completed += 1

                elif pk_str.startswith("__date_range__"):
                    # Date-range INSERT batch
                    range_str = pk_str.replace("__date_range__", "").strip("_")
                    range_parts = range_str.split("__")
                    mode = range_parts[0]
                    cols_str = range_parts[1]
                    start_val = range_parts[2]
                    end_val = range_parts[3]
                    col_names = cols_str.split(",")

                    where_clause = build_date_filter_sql(
                        col_names[0], start_val, end_val, mode, col_names)

                    sql = (f"INSERT INTO {target_table} " # nosec B608
                           f"SELECT * FROM {_SOURCE_VIEW} "
                           f"WHERE {where_clause}")
                    print(f"Date-range INSERT: {col_names[0]} [{start_val} -> {end_val}]")
                    print(f"SQL: {sql}")
                    spark.sql(sql)
                    completed += 1

                elif pk_str.startswith("__sync__"):
                    # Sync markers are tracking-only, skip
                    completed += 1

                else:
                    # Standard partition key=value format
                    pv = parse_partition_string(pk_str)
                    if not pv:
                        print(f"Skipping invalid partition string: {pk_str}")
                        failed += 1
                        continue

                    where = build_partition_filter(pv)
                    print(f"Rewriting partition: {pk_str}")

                    # DELETE from target + INSERT from source for partition rewrite
                    delete_sql = delete_from(
                        tgt['database'], tgt['table_name'],
                        target_type=tgt.get('type', ''), where=where)
                    insert_sql = (f"INSERT INTO {target_table} " # nosec B608
                                  f"SELECT * FROM {_SOURCE_VIEW} WHERE {where}")

                    print(f"DELETE: {delete_sql}")
                    spark.sql(delete_sql)
                    print(f"INSERT: {insert_sql}")
                    spark.sql(insert_sql)
                    completed += 1
                    print(f"  Partition {pk_str} rewritten successfully")

            except Exception as e:
                failed += 1
                print(f"  Batch item '{pk_str[:60]}' failed: {e}")

        print(f"Incremental job complete: "
              f"{completed}/{total} succeeded, {failed} failed")

        if failed > 0:
            raise RuntimeError(
                f"{failed}/{total} batch item(s) failed")

    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    spec = json.loads(args.spec)
    partitions = json.loads(args.partitions)
    run(spec, partitions)
