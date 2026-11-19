# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQL builder utility using sqlglot.

Provides safe, injection-proof SQL construction for Athena (Trino) and Spark
dialects. Replaces f-string SQL construction throughout the codebase.

All identifiers are properly quoted by the dialect generator, eliminating
the need for manual validation and string interpolation.
"""

from __future__ import annotations

from typing import Optional

import sqlglot
from sqlglot import exp, select as sg_select


# ═══════════════════════════════════════════════════════════════════════════
# TABLE REFERENCES
# ═══════════════════════════════════════════════════════════════════════════


def table_ref(
    database: str,
    table_name: str,
    catalog: str = "",
    quoted: bool = True,
) -> exp.Table:
    """Build a fully qualified table reference.

    Args:
        database: Database/schema name
        table_name: Table name
        catalog: Optional catalog (e.g., "s3tablescatalog/bucket-name")
        quoted: Whether to quote identifiers (default True for Athena/Trino)

    Returns:
        sqlglot Table expression
    """
    kwargs: dict = {
        "this": exp.to_identifier(table_name, quoted=quoted),
        "db": exp.to_identifier(database, quoted=quoted),
    }
    if catalog:
        kwargs["catalog"] = exp.to_identifier(catalog, quoted=quoted)
    return exp.Table(**kwargs)


def athena_table_ref(
    database: str,
    table_name: str,
    target_type: str = "",
    table_bucket_arn: str = "",
) -> exp.Table:
    """Build an Athena table reference, with S3 Tables catalog if applicable.

    For s3_tables targets: "s3tablescatalog/<bucket>"."<database>"."<table>"
    For regular targets:   "<database>"."<table>"
    """
    catalog = ""
    if target_type == "s3_tables" and table_bucket_arn:
        bucket_name = (
            table_bucket_arn.rsplit("/", 1)[-1]
            if "/" in table_bucket_arn
            else table_bucket_arn
        )
        catalog = f"s3tablescatalog/{bucket_name}"
    return table_ref(database, table_name, catalog=catalog, quoted=True)


def spark_table_ref(
    database: str,
    table_name: str,
    target_type: str = "",
) -> exp.Table:
    """Build a Spark table reference with catalog prefix.

    For s3_tables: icebergrest.database.table (unquoted)
    For regular:   glue_catalog.database.table (unquoted)

    The S3 Tables Spark catalog is named ``icebergrest`` to avoid EMR
    Serverless's reserved default ``s3tablescatalog`` (GlueCatalog-backed),
    which would otherwise shadow our Glue Iceberg REST config. See
    spark/spark_sql_builder.spark_table_ref for the full rationale.
    """
    catalog = "icebergrest" if target_type == "s3_tables" else "glue_catalog"
    return table_ref(database, table_name, catalog=catalog, quoted=False)


# ═══════════════════════════════════════════════════════════════════════════
# ATHENA SQL BUILDERS
# ═══════════════════════════════════════════════════════════════════════════


def athena_drop_table(
    database: str,
    table_name: str,
    target_type: str = "",
    table_bucket_arn: str = "",
) -> str:
    """Build DROP TABLE IF EXISTS for Athena."""
    tbl = athena_table_ref(database, table_name, target_type, table_bucket_arn)
    stmt = exp.Drop(this=tbl, exists=True, kind="TABLE")
    return stmt.sql(dialect="trino")


def athena_insert_into(
    target_db: str,
    target_table: str,
    source_db: str,
    source_table: str,
    where: str = "",
    target_type: str = "",
    table_bucket_arn: str = "",
) -> str:
    """Build INSERT INTO target SELECT * FROM source [WHERE ...] for Athena."""
    tgt = athena_table_ref(target_db, target_table, target_type, table_bucket_arn)
    src = table_ref(source_db, source_table, quoted=True)

    query = sg_select("*").from_(src)
    if where:
        query = query.where(where, dialect="trino")

    stmt = exp.Insert(this=tgt, expression=query)
    return stmt.sql(dialect="trino")


def athena_delete(
    database: str,
    table_name: str,
    where: str = "",
    target_type: str = "",
    table_bucket_arn: str = "",
) -> str:
    """Build DELETE FROM table [WHERE ...] for Athena."""
    tbl = athena_table_ref(database, table_name, target_type, table_bucket_arn)
    stmt = exp.Delete(this=tbl)
    if where:
        stmt = stmt.where(where, dialect="trino")
    return stmt.sql(dialect="trino")


def athena_select(
    columns: str | list[str],
    database: str,
    table_name: str,
    where: str = "",
) -> str:
    """Build SELECT ... FROM table [WHERE ...] for Athena."""
    src = table_ref(database, table_name, quoted=True)
    cols = columns if isinstance(columns, list) else [columns]
    query = sg_select(*cols, dialect="trino").from_(src)
    if where:
        query = query.where(where, dialect="trino")
    return query.sql(dialect="trino")


def athena_ctas(
    target_db: str,
    target_table: str,
    source_db: str,
    source_table: str,
    select_cols: str | list[str] = "*",
    where: str = "1=0",
    properties: Optional[dict[str, str]] = None,
    target_type: str = "",
    table_bucket_arn: str = "",
) -> str:
    """Build CREATE TABLE ... WITH (...) AS SELECT ... for Athena.

    Args:
        select_cols: Column expression(s) — either "*" or a list of column exprs
        properties: Dict of WITH clause properties, e.g.:
            {"table_type": "ICEBERG", "format": "PARQUET", "location": "s3://..."}
    """
    tgt = athena_table_ref(target_db, target_table, target_type, table_bucket_arn)
    src = table_ref(source_db, source_table, quoted=True)

    cols = select_cols if isinstance(select_cols, list) else [select_cols]
    query = sg_select(*cols, dialect="trino").from_(src)
    if where:
        query = query.where(where, dialect="trino")

    # Build WITH properties as raw SQL fragments since Athena uses
    # non-standard property syntax (ARRAY[], is_external without quotes, etc.)
    if properties:
        with_parts = []
        for k, v in properties.items():
            if k == "is_external":
                with_parts.append(f"{k} = {v}")
            elif v.startswith("ARRAY["):
                with_parts.append(f"{k} = {v}")
            elif "'" in v:
                with_parts.append(f"{k} = {v}")
            else:
                with_parts.append(f"{k} = '{v}'")
        with_clause = ", ".join(with_parts)
        tgt_sql = tgt.sql(dialect="trino")
        query_sql = query.sql(dialect="trino")
        return f"CREATE TABLE {tgt_sql}\nWITH ({with_clause})\nAS {query_sql}"

    create = exp.Create(this=tgt, kind="TABLE", expression=query)
    return create.sql(dialect="trino")


def athena_create_schema(
    database: str,
    catalog: str = "",
) -> str:
    """Build CREATE SCHEMA IF NOT EXISTS for Athena."""
    schema = exp.Schema(this=exp.to_identifier(database, quoted=True))
    stmt = exp.Create(this=schema, kind="SCHEMA", exists=True)
    return stmt.sql(dialect="trino")


def athena_alter_rename(
    database: str,
    old_table: str,
    new_table: str,
) -> str:
    """Build ALTER TABLE ... RENAME TO ... for Athena."""
    sql = f'ALTER TABLE "{database}"."{old_table}" RENAME TO "{database}"."{new_table}"'
    # Parse and regenerate to ensure proper quoting
    return sqlglot.parse_one(sql, dialect="trino").sql(dialect="trino")
