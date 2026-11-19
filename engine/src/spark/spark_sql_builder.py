# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Standalone SQL builder for Spark EMR scripts.

This module provides safe SQL construction for PySpark scripts running on
EMR Serverless. It uses identifier validation to prevent SQL injection
without requiring external dependencies (sqlglot is not available on EMR).

All SQL is constructed with proper identifier quoting appropriate for
Spark SQL (backtick-quoting for special characters, no quoting for safe names).

This file is uploaded to S3 alongside the Spark scripts and included
via --py-files in the spark-submit parameters.
"""

import re

# ═══════════════════════════════════════════════════════════════════════════
# IDENTIFIER VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

_SAFE_ID_RE = re.compile(r'^[a-zA-Z0-9_.\-]+$')


def _validate_id(name: str) -> str:
    """Validate and return a SQL identifier. Raises ValueError on invalid input."""
    if not name or not _SAFE_ID_RE.match(name) or len(name) > 255:
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


# ═══════════════════════════════════════════════════════════════════════════
# TABLE REFERENCES
# ═══════════════════════════════════════════════════════════════════════════


def spark_table_ref(database: str, table_name: str, target_type: str = "") -> str:
    """Build a fully qualified Spark table reference with catalog prefix.

    For s3_tables: icebergrest.database.table
    For regular:   glue_catalog.database.table

    The S3 Tables catalog is named ``icebergrest`` and is configured (in
    strategies/common.s3tables_spark_props) as an Iceberg GlueCatalog with the
    S3 Tables catalog id set via ``glue.id`` and ``warehouse``, plus
    ``spark.sql.defaultCatalog=icebergrest``. (The name ``icebergrest`` is
    historical — the catalog is GlueCatalog-backed, not REST.)
    """
    catalog = "icebergrest" if target_type == "s3_tables" else "glue_catalog"
    return f"{catalog}.{_validate_id(database)}.{_validate_id(table_name)}"


def spark_source_ref(database: str, table_name: str) -> str:
    """Build a source table reference, pinned to the Spark session catalog.

    Source tables live in the Glue-backed session catalog (spark_catalog, since
    spark.sql.catalogImplementation=hive). We qualify with ``spark_catalog.``
    explicitly so the source always resolves there — otherwise, when an S3
    Tables job sets spark.sql.defaultCatalog=icebergrest, an unqualified
    db.table would resolve against the icebergrest (S3 Tables) catalog and fail
    with TABLE_OR_VIEW_NOT_FOUND (source data is not in the target catalog).
    """
    return f"spark_catalog.{_validate_id(database)}.{_validate_id(table_name)}"


# ═══════════════════════════════════════════════════════════════════════════
# DDL BUILDERS
# ═══════════════════════════════════════════════════════════════════════════


def drop_table(database: str, table_name: str, target_type: str = "") -> str:
    """Build DROP TABLE IF EXISTS."""
    ref = spark_table_ref(database, table_name, target_type)
    return f"DROP TABLE IF EXISTS {ref}"


def create_table_ctas(
    target_db: str,
    target_table: str,
    source_db: str,
    source_table: str,
    target_type: str = "",
    partition_clause: str = "",
    tbl_properties: str = "",
) -> str:
    """Build CREATE TABLE ... USING iceberg ... AS SELECT * FROM source WHERE 1=0."""
    tgt = spark_table_ref(target_db, target_table, target_type)
    src = spark_source_ref(source_db, source_table)

    parts = [f"CREATE TABLE {tgt}", "USING iceberg"]
    if partition_clause:
        parts.append(partition_clause)
    if tbl_properties:
        parts.append(tbl_properties)
    parts.append(f"AS SELECT * FROM {src} WHERE 1=0") # nosec B608
    return "\n".join(parts)


def create_namespace(database: str, catalog: str = "icebergrest") -> str:
    """Build CREATE NAMESPACE IF NOT EXISTS.

    Default catalog is ``icebergrest`` (our Glue Iceberg REST catalog for S3
    Tables) — see spark_table_ref for why we avoid the EMR-reserved
    ``s3tablescatalog`` name.
    """
    return f"CREATE NAMESPACE IF NOT EXISTS {_validate_id(catalog)}.{_validate_id(database)}"


# ═══════════════════════════════════════════════════════════════════════════
# DML BUILDERS
# ═══════════════════════════════════════════════════════════════════════════


def insert_into(
    target_db: str,
    target_table: str,
    source_db: str,
    source_table: str,
    target_type: str = "",
    where: str = "",
    select_cols: str = "*",
) -> str:
    """Build INSERT INTO target SELECT ... FROM source [WHERE ...]."""
    tgt = spark_table_ref(target_db, target_table, target_type)
    src = spark_source_ref(source_db, source_table)

    sql = f"INSERT INTO {tgt}\nSELECT {select_cols} FROM {src}" # nosec B608
    if where:
        sql += f"\nWHERE {where}"
    return sql


def delete_from(
    database: str,
    table_name: str,
    target_type: str = "",
    where: str = "",
) -> str:
    """Build DELETE FROM table [WHERE ...]."""
    ref = spark_table_ref(database, table_name, target_type)
    sql = f"DELETE FROM {ref}" # nosec B608
    if where:
        sql += f"\nWHERE {where}"
    return sql


# ═══════════════════════════════════════════════════════════════════════════
# ALTER TABLE
# ═══════════════════════════════════════════════════════════════════════════


def alter_write_order(
    database: str,
    table_name: str,
    order_parts: list,
    target_type: str = "",
) -> str:
    """Build ALTER TABLE ... WRITE ORDERED BY ..."""
    ref = spark_table_ref(database, table_name, target_type)
    order_str = ", ".join(order_parts)
    return f"ALTER TABLE {ref}\nWRITE ORDERED BY {order_str}"


def alter_rename(
    database: str,
    old_table: str,
    new_table: str,
    target_type: str = "",
) -> str:
    """Build ALTER TABLE ... RENAME TO ..."""
    old_ref = spark_table_ref(database, old_table, target_type)
    return f"ALTER TABLE {old_ref} RENAME TO {_validate_id(database)}.{_validate_id(new_table)}"


# ═══════════════════════════════════════════════════════════════════════════
# ICEBERG PROCEDURES
# ═══════════════════════════════════════════════════════════════════════════


def call_add_files(
    target_db: str,
    target_table: str,
    source_location: str,
    partition_filter: str = "",
) -> str:
    """Build CALL glue_catalog.system.add_files(...)."""
    table_short = f"{_validate_id(target_db)}.{_validate_id(target_table)}"
    parts = [
        "CALL glue_catalog.system.add_files(",
        f"table => '{table_short}', ",
        f"source_table => '`parquet`.`{source_location}`'",
    ]
    if partition_filter:
        parts[-1] += ", "
        parts.append(f"partition_filter => {partition_filter})")
    else:
        parts.append(")")
    return "".join(parts)
