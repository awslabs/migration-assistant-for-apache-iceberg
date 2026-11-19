# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PySpark script for Iceberg add_files procedure.

Submitted to EMR Serverless by the AddFilesStrategy.

Registers existing Hive-Parquet data files into an Iceberg table without
any data movement using Iceberg's system.add_files procedure.

Operations:
  - add_files:        Register files for a batch of partitions (initial load)
  - sync_partitions:  Delete + re-add files for changed partitions (ongoing sync)

Usage:
  spark-submit add_files.py \
    --spec '{"source": {...}, "target": {...}, ...}' \
    --operation add_files \
    --partitions '[["2024-01-01"], ["2024-01-02"]]'

Partition format:
  Each element in --partitions is a list of values matching the partition
  key order defined in spec.source.partition_keys. For example, with
  partition_keys=[{name: "region"}, {name: "event_date"}]:
    [["us-east-1", "2024-01-01"], ["eu-west-1", "2024-01-02"]]
"""

import argparse
import json
import re as _re
import sys

from pyspark.sql import SparkSession

from spark_sql_builder import (
    spark_table_ref, spark_source_ref, drop_table, delete_from,
    call_add_files,
)


def _validate_val(val: str) -> str:
    """Validate a SQL literal value. Raises ValueError on injection patterns."""
    if val and _re.search(r'[;\x00]|--|/\*', val):
        raise ValueError(f"Invalid SQL value: {val!r}")
    return val


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True,
                        help="Pipeline spec as JSON string")
    parser.add_argument("--operation", required=True,
                        choices=["create_table", "add_files", "sync_partitions"],
                        help="Operation to perform")
    parser.add_argument("--partitions", required=True,
                        help="JSON array of partition value lists")
    return parser.parse_args()


def build_target_table_name(spec: dict) -> str:
    """Build fully qualified Iceberg target table name."""
    tgt = spec["target"]
    return spark_table_ref(tgt['database'], tgt['table_name'], tgt.get('type', ''))


def build_source_location(spec: dict) -> str:
    """Get the S3 base location of the source table."""
    return spec["source"]["location"].rstrip("/")


def build_partition_filter_map(partition_keys: list[dict],
                               values: list[str]) -> str:
    """Build a Spark map() expression for partition_filter argument.

    Returns a SQL string like: map('year', '2024', 'month', '01')
    """
    pairs = []
    for pk, v in zip(partition_keys, values):
        _validate_val(pk['name'])
        _validate_val(str(v))
        pairs.append(f"'{pk['name']}'")
        pairs.append(f"'{v}'")
    return f"map({', '.join(pairs)})"


def build_where_clause(partition_keys: list[dict],
                       partition_values_list: list[list[str]]) -> str:
    """Build a WHERE clause to delete rows matching any of the given partitions.

    Example output:
      (region = 'us-east-1' AND event_date = '2024-01-01') OR
      (region = 'eu-west-1' AND event_date = '2024-01-02')
    """
    or_clauses = []
    for values in partition_values_list:
        conditions = []
        for pk, v in zip(partition_keys, values):
            # Quote the value as a string literal
            conditions.append(f"{pk['name']} = '{v}'")
        or_clauses.append(f"({' AND '.join(conditions)})")
    return " OR ".join(or_clauses)


def run_add_files(spark: SparkSession, spec: dict,
                  partition_values_list: list[list[str]]):
    """Register files for a batch of partitions using add_files procedure.

    Calls add_files once per partition with partition_filter.
    Each call creates a separate snapshot.
    """
    source_location = build_source_location(spec)
    partition_keys = spec["source"].get("partition_keys", [])
    tgt = spec["target"]

    total = len(partition_values_list)
    success = 0
    errors = []

    for i, values in enumerate(partition_values_list):
        partition_filter = build_partition_filter_map(partition_keys, values)

        sql = call_add_files(
            target_db=tgt['database'],
            target_table=tgt['table_name'],
            source_location=source_location,
            partition_filter=partition_filter,
        )

        partition_desc = "/".join(
            f"{pk['name']}={v}" for pk, v in zip(partition_keys, values))
        print(f"[{i+1}/{total}] add_files: {partition_desc}")

        try:
            spark.sql(sql)
            success += 1
        except Exception as e:
            err_msg = str(e)
            if "No files found" in err_msg or "Cannot find" in err_msg:
                print(f"  SKIP (no files): {partition_desc}")
                success += 1
            else:
                print(f"  ERROR: {err_msg[:200]}")
                errors.append(f"{partition_desc}: {err_msg[:200]}")

    print(f"\nadd_files complete: {success}/{total} partitions succeeded")
    if errors:
        print(f"Errors ({len(errors)}):")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)


def run_sync_partitions(spark: SparkSession, spec: dict,
                        partition_values_list: list[list[str]]):
    """Sync changed partitions: delete existing rows then re-add files.

    This provides overwrite semantics for partitions that have new/changed
    files in the source. Two steps:
      1. DELETE FROM target WHERE partition IN (affected)
      2. add_files for each affected partition
    """
    partition_keys = spec["source"].get("partition_keys", [])
    tgt = spec["target"]

    if not partition_values_list:
        print("sync_partitions: no partitions to sync")
        return

    # Step 1: Delete existing data for affected partitions
    where_clause = build_where_clause(partition_keys, partition_values_list)
    delete_sql = delete_from(
        tgt['database'], tgt['table_name'],
        target_type=tgt.get('type', ''), where=where_clause)

    print(f"Deleting existing rows for {len(partition_values_list)} partitions...")
    try:
        spark.sql(delete_sql)
        print("  DELETE complete")
    except Exception as e:
        # If table is empty or partitions don't exist yet, DELETE may error
        err_msg = str(e)
        if "AnalysisException" in err_msg and "cannot be found" in err_msg.lower():
            print(f"  DELETE skipped (no matching data): {err_msg[:100]}")
        else:
            print(f"  DELETE failed: {err_msg[:200]}")
            # Don't fail entirely — proceed with add_files to register new data

    # Step 2: Re-add files for affected partitions
    print(f"Re-adding files for {len(partition_values_list)} partitions...")
    run_add_files(spark, spec, partition_values_list)


def run_create_table(spark: SparkSession, spec: dict):
    """Create an empty Iceberg target table.

    Uses spark.table() to read source schema (resolves via Hive/Glue metastore)
    then writeTo() API to create the Iceberg target — avoids cross-catalog CTAS
    which doesn't resolve the source correctly.
    """
    target_table = build_target_table_name(spec)
    src = spec["source"]
    source_table = spark_source_ref(src['database'], src['table_name'])
    ip = spec.get("iceberg_properties", {})

    # Drop target if exists (handles retries)
    tgt = spec["target"]
    print(f"Dropping target table if exists: {target_table}")
    try:
        spark.sql(drop_table(tgt['database'], tgt['table_name'], tgt.get('type', '')))
    except Exception as e:
        print(f"  DROP skipped (may not exist): {e}")

    # Read source schema via DataFrame API — spark.table() uses the default
    # Hive session catalog which resolves through Glue metastore factory
    print(f"Reading source table schema: {source_table}")
    source_df = spark.table(source_table)
    print(f"  Schema ({len(source_df.columns)} cols): {', '.join(source_df.columns)}")

    # Create empty Iceberg table via writeTo API (targets glue_catalog)
    writer = source_df.limit(0).writeTo(target_table).using("iceberg")

    # Set explicit location to avoid glue_catalog appending db/table to warehouse path
    target_location = tgt.get("location", "").rstrip("/")
    if target_location:
        writer = writer.tableProperty("location", target_location)

    # Table properties
    table_props = ip.get("table_properties", {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
    })
    for k, v in table_props.items():
        writer = writer.tableProperty(k, v)

    # Partition transforms — add_files requires IDENTITY transforms only
    # (source Hive partitions are directory-based identity partitions)
    partition_spec = ip.get("partition_spec", [])
    if partition_spec:
        from pyspark.sql.functions import col
        transforms = [col(t["source_column"]) for t in partition_spec]
        writer = writer.partitionedBy(*transforms)

    writer.create()
    print(f"Target table created successfully: {target_table}")


def main():
    args = parse_args()
    spec = json.loads(args.spec)
    partition_values_list = json.loads(args.partitions)
    operation = args.operation

    src = spec["source"]
    tgt = spec["target"]
    print(f"Operation: {operation}")
    print(f"Source: {src['database']}.{src['table_name']}")
    print(f"Target: {tgt['database']}.{tgt['table_name']}")
    print(f"Partitions to process: {len(partition_values_list)}")

    spark = (SparkSession.builder
             .appName(f"IMA-add-files-{src['table_name']}")
             .getOrCreate())

    try:
        if operation == "create_table":
            run_create_table(spark, spec)
        elif operation == "add_files":
            run_add_files(spark, spec, partition_values_list)
        elif operation == "sync_partitions":
            run_sync_partitions(spark, spec, partition_values_list)
        else:
            print(f"Unknown operation: {operation}")
            sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
