# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PySpark script for Iceberg incremental sync operations.

Submitted to EMR Serverless by the IcebergIncrementalStrategy.

Supports:
  - Full table sync (DELETE all + INSERT all)
  - Partition-filtered sync (DELETE WHERE + INSERT WHERE)
  - Append-only sync (INSERT only, no DELETE)
  - Date-range INSERT (for initial load batches)

Usage:
  spark-submit iceberg_sync.py \
    --spec '{"source": {...}, "target": {...}, ...}' \
    --operation sync_partitions \
    --where "event_date >= DATE '2024-01-01' AND event_date < DATE '2025-01-01'" \
    --append-only false
"""

import argparse
import json
import re as _re

from pyspark.sql import SparkSession

from spark_sql_builder import (
    spark_table_ref, spark_source_ref, insert_into, delete_from,
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
    parser.add_argument("--operation", required=True,
                        choices=["full_sync", "partition_sync", "append_only",
                                 "insert_where", "full_insert"],
                        help="Type of sync operation")
    parser.add_argument("--where", default="",
                        help="WHERE clause for partition filtering")
    parser.add_argument("--file-paths", default="",
                        help="JSON array of S3 file paths for append-only sync")
    return parser.parse_args()


def build_source_table_name(spec: dict) -> str:
    """Source tables are Hive/Parquet — use default catalog (no prefix), Glue resolves via metastore config."""
    src = spec["source"]
    return spark_source_ref(src['database'], src['table_name'])


def build_target_table_name(spec: dict) -> str:
    tgt = spec["target"]
    return spark_table_ref(tgt['database'], tgt['table_name'], tgt.get('type', ''))


def run(spec: dict, operation: str, where: str, file_paths: list[str]):
    spark = SparkSession.builder \
        .appName(f"IMA-iceberg-sync-{spec['source']['table_name']}") \
        .getOrCreate()

    try:
        source_table = build_source_table_name(spec)
        target_table = build_target_table_name(spec)
        src = spec["source"]
        tgt = spec["target"]

        if operation == "full_sync":
            # Full table DELETE + INSERT
            print(f"Full table sync: {source_table} → {target_table}")
            delete_sql = delete_from(
                tgt['database'], tgt['table_name'],
                target_type=tgt.get('type', ''), where="1=1")
            insert_sql = insert_into(
                tgt['database'], tgt['table_name'],
                src['database'], src['table_name'],
                target_type=tgt.get('type', ''))
            print(f"DELETE: {delete_sql}")
            spark.sql(delete_sql)
            print(f"INSERT: {insert_sql}")
            spark.sql(insert_sql)
            print("Full sync completed")

        elif operation == "partition_sync":
            # Partition-filtered DELETE + INSERT
            if not where:
                raise ValueError("--where is required for partition_sync")
            print(f"Partition sync: WHERE {where}")
            delete_sql = delete_from(
                tgt['database'], tgt['table_name'],
                target_type=tgt.get('type', ''), where=where)
            insert_sql = insert_into(
                tgt['database'], tgt['table_name'],
                src['database'], src['table_name'],
                target_type=tgt.get('type', ''), where=where)
            print(f"DELETE: {delete_sql}")
            spark.sql(delete_sql)
            print(f"INSERT: {insert_sql}")
            spark.sql(insert_sql)
            print("Partition sync completed")

        elif operation == "append_only":
            # Append-only INSERT (no DELETE)
            if file_paths:
                # INSERT only rows from specific files
                paths_str = ", ".join(f"'{p}'" for p in file_paths)
                file_where = f"input_file_name() IN ({paths_str})"
                insert_sql = insert_into(
                    tgt['database'], tgt['table_name'],
                    src['database'], src['table_name'],
                    target_type=tgt.get('type', ''), where=file_where)
                print(f"Append-only INSERT ({len(file_paths)} files)")
            elif where:
                # INSERT with partition filter
                insert_sql = insert_into(
                    tgt['database'], tgt['table_name'],
                    src['database'], src['table_name'],
                    target_type=tgt.get('type', ''), where=where)
                print(f"Append-only INSERT: WHERE {where}")
            else:
                insert_sql = insert_into(
                    tgt['database'], tgt['table_name'],
                    src['database'], src['table_name'],
                    target_type=tgt.get('type', ''))
                print("Append-only INSERT: full table")
            print(f"SQL: {insert_sql}")
            spark.sql(insert_sql)
            print("Append-only sync completed")

        elif operation == "insert_where":
            # Simple INSERT with WHERE clause (for initial load batches)
            if not where:
                raise ValueError("--where is required for insert_where")
            insert_sql = insert_into(
                tgt['database'], tgt['table_name'],
                src['database'], src['table_name'],
                target_type=tgt.get('type', ''), where=where)
            print(f"INSERT WHERE: {where}")
            print(f"SQL: {insert_sql}")
            spark.sql(insert_sql)
            print("Insert completed")

        elif operation == "full_insert":
            # Full table INSERT (no filter)
            insert_sql = insert_into(
                tgt['database'], tgt['table_name'],
                src['database'], src['table_name'],
                target_type=tgt.get('type', ''))
            print(f"Full INSERT: {source_table} → {target_table}")
            spark.sql(insert_sql)
            print("Full insert completed")

        else:
            raise ValueError(f"Unknown operation: {operation}")

    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    spec = json.loads(args.spec)
    file_paths = json.loads(args.file_paths) if args.file_paths else []
    run(spec, args.operation, args.where, file_paths)
