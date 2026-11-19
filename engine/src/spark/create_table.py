# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PySpark script to create an empty Iceberg target table.

Submitted to EMR Serverless as the first step of incremental migrations.
Creates the target table with the same schema and partition spec as the source.

Usage:
  spark-submit create_table.py --spec '{"source": {...}, "target": {...}, ...}'
"""

import argparse
import json

from pyspark.sql import SparkSession

from spark_sql_builder import (
    spark_table_ref, spark_source_ref, drop_table, create_table_ctas,
    create_namespace,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True, help="Spec as JSON string")
    return parser.parse_args()


def build_source_table_name(spec: dict) -> str:
    """Build the source table reference.

    Source tables are regular Hive/Parquet tables in Glue. Reference them
    without a catalog prefix — EMR Serverless uses Glue as the default
    Hive metastore when configured with hive.metastore.client.factory.class.
    """
    src = spec["source"]
    return spark_source_ref(src['database'], src['table_name'])


def build_target_table_name(spec: dict) -> str:
    tgt = spec["target"]
    return spark_table_ref(tgt['database'], tgt['table_name'], tgt.get('type', ''))


def build_partition_clause(spec: dict) -> str:
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


def build_table_properties(spec: dict) -> str:
    ip = spec.get("iceberg_properties", {})
    props = ip.get("table_properties", {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
    })
    if not props:
        return ""
    pairs = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    return f"TBLPROPERTIES ({pairs})"


def run(spec: dict):
    spark = SparkSession.builder \
        .appName(f"IMA-create-table-{spec['source']['table_name']}") \
        .getOrCreate()

    try:
        target_table = build_target_table_name(spec)
        tgt = spec["target"]
        src = spec["source"]

        # Drop target if exists (handles retries)
        print(f"Dropping target table if exists: {target_table}")
        spark.sql(drop_table(tgt['database'], tgt['table_name'], tgt.get('type', '')))

        # Create namespace for S3 Tables
        if tgt.get("type") == "s3_tables":
            try:
                spark.sql(create_namespace(tgt['database']))
                print(f"Namespace created: {tgt['database']}")
            except Exception as e:
                print(f"Namespace creation (may exist): {e}")

        # Create empty target table with source schema
        partition_clause = build_partition_clause(spec)
        tbl_props = build_table_properties(spec)

        ctas_sql = create_table_ctas(
            target_db=tgt['database'],
            target_table=tgt['table_name'],
            source_db=src['database'],
            source_table=src['table_name'],
            target_type=tgt.get('type', ''),
            partition_clause=partition_clause,
            tbl_properties=tbl_props,
        )
        print(f"Creating target table:\n{ctas_sql}")
        spark.sql(ctas_sql)
        print("Target table created successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    args = parse_args()
    spec = json.loads(args.spec)
    run(spec)
