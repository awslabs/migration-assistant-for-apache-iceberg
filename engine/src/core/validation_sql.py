# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQL builders for validation checks (Athena / Trino dialect).

Injection-proof SQL construction via sqlglot expression trees — the same
approach used across the engine (see utils/sql_builder.py). Identifiers
(tables, columns, aliases) are built as sqlglot nodes and quoted by the Trino
generator, so there is no raw f-string interpolation of user-controlled names
and no need for manual quoting or Bandit suppressions.

Source tables are always plain Glue tables (no catalog prefix). Target tables
use athena_table_ref, which applies the s3tablescatalog/<bucket> prefix when
target_type == "s3_tables".
"""

from __future__ import annotations

from sqlglot import exp, select as sg_select

from ..utils.sql_builder import table_ref, athena_table_ref


# ── table references (sqlglot Table nodes) ──

def _src_ref(database: str, table_name: str) -> exp.Table:
    return table_ref(database, table_name, quoted=True)


def _ref(database: str, table_name: str,
         target_type: str = "", table_bucket_arn: str = "") -> exp.Table:
    """Target ref (s3tables-aware) when target_type/bucket given, else plain."""
    if target_type or table_bucket_arn:
        return athena_table_ref(database, table_name, target_type, table_bucket_arn)
    return _src_ref(database, table_name)


# ── expression helpers ──

def _col(name: str) -> exp.Column:
    """A quoted column reference."""
    return exp.column(name, quoted=True)


def _alias(node: exp.Expression, name: str) -> exp.Alias:
    """Alias an expression with a quoted output name."""
    return exp.alias_(node, name, quoted=True)


def _count_star(alias: str = "row_count") -> exp.Alias:
    return _alias(exp.Count(this=exp.Star()), alias)


def _trino(query: exp.Expression) -> str:
    return query.sql(dialect="trino")


# ── V1: row count ──

def build_count_sql(database: str, table_name: str,
                    target_type: str = "", table_bucket_arn: str = "") -> str:
    ref = _ref(database, table_name, target_type, table_bucket_arn)
    return _trino(sg_select(_count_star()).from_(ref))


# ── V3 / V8: partition count (V8 = grouped per-partition) ──

def build_partition_count_sql(database: str, table_name: str,
                              partition_cols: list[str],
                              target_type: str = "", table_bucket_arn: str = "",
                              group: bool = False) -> str:
    ref = _ref(database, table_name, target_type, table_bucket_arn)
    if not partition_cols:
        # No partition columns — degenerate to a single-group count.
        return _trino(sg_select(_count_star()).from_(ref))

    cols = [_col(c) for c in partition_cols]
    if group:
        # Per-partition row counts (V8). Ordered for deterministic comparison.
        query = (sg_select(*cols, _count_star())
                 .from_(ref)
                 .group_by(*cols)
                 .order_by(*cols))
        return _trino(query)

    # Distinct partition count (V3): COUNT(1) over SELECT DISTINCT <cols>.
    inner = sg_select(*cols).distinct().from_(ref)
    query = sg_select(_count_star("partition_count")).from_(inner.subquery())
    return _trino(query)


# ── V4: null counts on key columns ──

def build_null_count_sql(database: str, table_name: str,
                         columns: list[str],
                         target_type: str = "", table_bucket_arn: str = "") -> str:
    ref = _ref(database, table_name, target_type, table_bucket_arn)
    if not columns:
        return _trino(sg_select(_count_star()).from_(ref))

    # SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END) AS "null_<col>" per column.
    projections = []
    for c in columns:
        case_expr = (
            exp.Case()
            .when(exp.Is(this=_col(c), expression=exp.Null()), exp.Literal.number(1))
            .else_(exp.Literal.number(0))
        )
        projections.append(_alias(exp.Sum(this=case_expr), "null_" + c))
    return _trino(sg_select(*projections).from_(ref))


# ── V5: non-emptiness + bounds ──

def build_bounds_sql(database: str, table_name: str, bound_col: str = "",
                     target_type: str = "", table_bucket_arn: str = "") -> str:
    ref = _ref(database, table_name, target_type, table_bucket_arn)
    if not bound_col:
        return _trino(sg_select(_count_star()).from_(ref))
    projections = [
        _count_star(),
        _alias(exp.Min(this=_col(bound_col)), "min_bound"),
        _alias(exp.Max(this=_col(bound_col)), "max_bound"),
    ]
    return _trino(sg_select(*projections).from_(ref))


# ── V7: per-column aggregates ──
# NOTE: the concrete column list is resolved at run time from the resolved
# schema; this builder produces a COUNT baseline and per-numeric-column
# sum/min/max/ndv aggregates when columns are known.

def build_aggregate_sql(database: str, table_name: str,
                        target_type: str = "", table_bucket_arn: str = "",
                        numeric_columns: list[str] | None = None) -> str:
    ref = _ref(database, table_name, target_type, table_bucket_arn)
    projections: list[exp.Expression] = [_count_star()]
    for c in (numeric_columns or []):
        col = _col(c)
        projections.append(_alias(exp.Sum(this=col), "sum_" + c))
        projections.append(_alias(exp.Min(this=col), "min_" + c))
        projections.append(_alias(exp.Max(this=col), "max_" + c))
        projections.append(
            _alias(exp.Count(this=col, distinct=True), "ndv_" + c))
    return _trino(sg_select(*projections).from_(ref))

