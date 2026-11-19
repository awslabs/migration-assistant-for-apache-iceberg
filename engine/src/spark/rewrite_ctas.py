# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PySpark script for full-table CTAS rewrite to Iceberg.

Submitted to EMR Serverless by the RewriteStrategy.

Usage:
  spark-submit rewrite_ctas.py --spec '{"source": {...}, "target": {...}, ...}'
"""

import argparse
import json
import re as _re

from pyspark.sql import SparkSession

from spark_sql_builder import (
    spark_table_ref, spark_source_ref, alter_write_order, alter_rename,
)


def _validate_val(val: str) -> str:
    """Validate a SQL literal value. Raises ValueError on injection patterns."""
    if val and _re.search(r'[;\x00]|--|/\*', val):
        raise ValueError(f"Invalid SQL value: {val!r}")
    return val


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True,
                        help="Spec as JSON string")
    return parser.parse_args()


def build_source_table_name(spec: dict) -> str:
    """Source catalog table reference.

    For non-Parquet sources (Hudi/Delta/Iceberg) this is used to actually
    read data, and for DDL statements (DROP/RENAME) during table swap. For
    Parquet sources it is also used for a metadata-only schema lookup
    (declared column order) — never for reading data — see read_source().
    """
    src = spec["source"]
    return spark_source_ref(src['database'], src['table_name'])


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

        # Detection order mirrors source_service.py's heuristics.
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
    source catalog's declared order so the CTAS's `SELECT *` lands
    correctly on the target table.

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

    if fmt == "parquet":
        location = src.get("location", "").rstrip("/")
        if not location:
            ref = build_source_table_name(spec)
            print(f"No source location in spec; reading via catalog: {ref}")
            return spark.table(ref)
        print(f"Reading source by path (physical types): {location}")
        df = spark.read.parquet(location)
    elif fmt == "delta":
        # Delta tables registered in Glue point their Location at the symlink
        # manifest (for Athena compatibility), not the actual data root. Read
        # directly from the spec's source.location (the real Delta table root)
        # using Spark's native Delta reader, which honors the _delta_log.
        location = src.get("location", "").rstrip("/")
        if not location:
            ref = build_source_table_name(spec)
            print(f"No source location in spec; reading Delta via catalog: {ref}")
            return spark.table(ref)
        # Strip symlink manifest suffix if accidentally included in the spec location
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
    else:
        # Other formats (Iceberg, CSV, ORC, etc.) — use catalog reference
        ref = build_source_table_name(spec)
        print(f"Reading source via catalog: {ref}")
        return spark.table(ref)

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


# Name for the temp view registered from the source DataFrame
_SOURCE_VIEW = "__ima_source__"


def build_target_table_name(spec: dict) -> str:
    """Build the fully qualified target table reference."""
    tgt = spec["target"]
    return spark_table_ref(tgt['database'], tgt['table_name'], tgt.get('type', ''))


def build_table_properties(spec: dict) -> str:
    """Build TBLPROPERTIES clause from iceberg_properties."""
    ip = spec.get("iceberg_properties", {})
    props = ip.get("table_properties", {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
    })
    if not props:
        return ""
    pairs = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    return f"TBLPROPERTIES ({pairs})"


def build_partition_clause(spec: dict) -> str:
    """Build PARTITIONED BY clause from iceberg partition spec."""
    ip = spec.get("iceberg_properties", {})
    transforms = ip.get("partition_spec", [])
    if not transforms:
        return ""
    parts = []
    for t in transforms:
        col = t["source_column"]
        transform = t["transform"]
        if transform == "identity":
            parts.append(col)
        elif transform in ("year", "month", "day", "hour"):
            parts.append(f"{transform}({col})")
        elif transform == "bucket":
            parts.append(f"bucket({t.get('num_buckets', 16)}, {col})")
        elif transform == "truncate":
            parts.append(f"truncate({t.get('width', 10)}, {col})")
        else:
            parts.append(col)
    return f"PARTITIONED BY ({', '.join(parts)})"


def run(spec: dict):
    spark = SparkSession.builder \
        .appName(f"IMA-rewrite-{spec['source']['table_name']}") \
        .getOrCreate()

    try:
        source_table = build_source_table_name(spec)
        target_table = build_target_table_name(spec)
        tbl_props = build_table_properties(spec)
        partition_clause = build_partition_clause(spec)
        tgt = spec["target"]

        # Read source (physical types for Parquet, catalog reference for
        # Hudi/Delta/Iceberg — see read_source()) and register as a temp
        # view so the CTAS below scans it instead of the catalog table
        # directly.
        source_df = read_source(spark, spec)
        source_df.createOrReplaceTempView(_SOURCE_VIEW)
        source_count = spark.table(_SOURCE_VIEW).count()
        print(f"Source table {source_table} has {source_count} rows")

        tgt_ref = spark_table_ref(tgt['database'], tgt['table_name'], tgt.get('type', ''))

        if tgt.get('type', '') == "s3_tables":
            # S3 Tables target: split the CTAS into a standalone CREATE TABLE
            # (pure DDL) followed by INSERT INTO. A single CREATE TABLE ... AS
            # SELECT routes the create through Spark's session catalog planning
            # and lands on GlueCatalog (empty warehouse -> "Cannot derive
            # default warehouse location") instead of our REST 'icebergrest'
            # catalog. A standalone CREATE binds to the catalog named in the
            # identifier — the same path the incremental strategy uses
            # successfully. Non-s3_tables targets keep the original CTAS below.
            #
            # Column list comes from the source temp view's resolved schema, so
            # the empty CREATE matches exactly what the INSERT ... SELECT *
            # produces (same view, same column order).
            col_defs = ", ".join(
                f"{f.name} {f.dataType.simpleString()}"
                for f in spark.table(_SOURCE_VIEW).schema.fields
            )
            create_parts = [
                f"CREATE TABLE {tgt_ref} ({col_defs})",
                "USING iceberg",
            ]
            if partition_clause:
                create_parts.append(partition_clause)
            if tbl_props:
                create_parts.append(tbl_props)
            create_sql = "\n".join(create_parts)
            print(f"Executing CREATE TABLE:\n{create_sql}")
            spark.sql(create_sql)

            # Load data. If the INSERT fails, roll back by dropping the table we
            # just created so a failed run doesn't leave an empty orphan table
            # (which would also break a retry's CREATE with "table exists").
            insert_sql = f"INSERT INTO {tgt_ref} SELECT * FROM {_SOURCE_VIEW}"  # nosec B608
            print(f"Executing INSERT:\n{insert_sql}")
            try:
                spark.sql(insert_sql)
            except Exception as insert_err:
                print(f"INSERT failed ({insert_err}); rolling back — "
                      f"dropping {tgt_ref}")
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {tgt_ref}")
                    print(f"Rollback complete: dropped {tgt_ref}")
                except Exception as drop_err:
                    print(f"WARNING: rollback DROP TABLE failed for {tgt_ref}: "
                          f"{drop_err}. Manual cleanup may be required before retry.")
                raise
        else:
            # Non-S3 Tables (Iceberg on S3): single CTAS — unchanged.
            ctas_parts = [
                f"CREATE TABLE {tgt_ref}",
                "USING iceberg",
            ]
            if partition_clause:
                ctas_parts.append(partition_clause)
            if tbl_props:
                ctas_parts.append(tbl_props)
            ctas_parts.append(f"AS SELECT * FROM {_SOURCE_VIEW}") # nosec B608

            ctas_sql = "\n".join(ctas_parts)
            print(f"Executing CTAS:\n{ctas_sql}")
            spark.sql(ctas_sql)

        # Apply sort order if specified
        ip = spec.get("iceberg_properties", {})
        sort_fields = ip.get("sort_order", [])
        if sort_fields:
            order_parts = []
            for sf in sort_fields:
                col = sf["column"]
                direction = sf.get("direction", "asc").upper()
                null_order = sf.get("null_order", "nulls_last") \
                    .replace("_", " ").upper()
                order_parts.append(f"{col} {direction} {null_order}")
            alter_sql = alter_write_order(
                tgt['database'], tgt['table_name'], order_parts,
                target_type=tgt.get('type', ''))
            print(f"Setting sort order:\n{alter_sql}")
            spark.sql(alter_sql)

        # Verify row count
        target_count = spark.table(target_table).count()
        print(f"Target table {target_table} has {target_count} rows")

        if target_count != source_count:
            raise RuntimeError(
                f"Row count mismatch: source={source_count}, "
                f"target={target_count}")

        # Table swap: drop original, rename target to original name (if enabled)
        src = spec["source"]
        swap_enabled = spec.get("strategy", {}).get("swap_tables", False)
        if swap_enabled and tgt.get("table_name") != src.get("table_name"):
            print(f"Performing table swap: drop {source_table}, "
                  f"rename {target_table} → {src['table_name']}")
            spark.sql(f"DROP TABLE IF EXISTS {source_table}")
            spark.sql(alter_rename(
                tgt['database'], tgt['table_name'], src['table_name'],
                target_type=tgt.get('type', '')))
            print(f"Table swap complete: {tgt['database']}.{src['table_name']} "
                  "is now an Iceberg table")
        elif not swap_enabled:
            print("Table swap disabled — both tables coexist")

        print("Conversion completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    spec = json.loads(args.spec)
    run(spec)
