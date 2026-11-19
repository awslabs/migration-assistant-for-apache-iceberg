# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validation check definitions — pure, engine-agnostic builders (no AWS).

Given a pipeline record and a tier ("basic" | "advanced"), produces an ordered
list of *check plans*. Each check plan is a small dict describing:
  - what the check is (id, name, tier)
  - how to obtain the source value and the target value
  - how to compare them into a pass/fail

For the Athena path, checks carry ready-to-run SQL (built via the existing
sql_builder helpers) whose result the validator reads and compares in Python.
For the EMR path, the same check catalog is serialized into the job spec and
the Spark script (spark/validate.py) performs the reads + comparison itself in
one session. Either way the recorded result shape is identical.

This module has NO side effects and imports nothing from AWS clients — it only
builds strings/dicts, so it is unit-testable in isolation.

Check catalog (V10 type-promotion intentionally deferred):
  Basic:    V1 row-count, V2 schema, V3 partition-count, V4 null-count,
            V5 non-empty+bounds, V6 morph-manifest (morph only)
  Advanced: V7 per-column aggregates, V8 per-partition
"""

from __future__ import annotations

from .validation_sql import (
    build_count_sql,
    build_null_count_sql,
    build_bounds_sql,
    build_aggregate_sql,
    build_partition_count_sql,
)


# ── Catalog metadata (surfaced to the UI so it can list what each tier runs) ──

BASIC_CHECKS = [
    {"id": "V1", "name": "Row count parity",
     "desc": "Source and target have the same total row count."},
    {"id": "V2", "name": "Schema conformance",
     "desc": "Target column set and types match the migration spec."},
    {"id": "V3", "name": "Partition count",
     "desc": "Target partition count matches the source (partitioned tables)."},
    {"id": "V4", "name": "Null-count parity (key columns)",
     "desc": "Null counts on identifier/sort columns match between source and target."},
    {"id": "V5", "name": "Non-emptiness and bounds",
     "desc": "Target is non-empty when the source is; partition/date bounds fall within the source range."},
    {"id": "V6", "name": "Morph manifest integrity",
     "desc": "Iceberg manifest references existing files and reconciles with a direct read (morph only)."},
]

ADVANCED_CHECKS = [
    {"id": "V7", "name": "Per-column aggregates",
     "desc": "sum / min / max / count(distinct) per column match between source and target."},
    {"id": "V8", "name": "Per-partition reconciliation",
     "desc": "Per-partition row counts match, localizing any mismatch (partitioned tables)."},
]


# ── Column helpers ──

def _key_columns(pipeline: dict) -> list[str]:
    """Identifier/sort columns to null-check (V4). Falls back to partition keys."""
    cols: list[str] = []
    sort_order = pipeline.get("sort_order") or []
    if isinstance(sort_order, list):
        for s in sort_order:
            if isinstance(s, dict) and s.get("column"):
                cols.append(s["column"])
    if not cols:
        pks = pipeline.get("source_partition_keys") or []
        if isinstance(pks, list):
            for pk in pks:
                if isinstance(pk, dict) and pk.get("name"):
                    cols.append(pk["name"])
    # De-dup, preserve order.
    seen = set()
    return [c for c in cols if not (c in seen or seen.add(c))]


def _partition_columns(pipeline: dict) -> list[str]:
    pks = pipeline.get("source_partition_keys") or []
    out = []
    if isinstance(pks, list):
        for pk in pks:
            if isinstance(pk, dict) and pk.get("name"):
                out.append(pk["name"])
    return out


# ── Check-plan builder ──

def build_check_plan(pipeline: dict, tier: str) -> list[dict]:
    """Build the ordered list of check plans for a pipeline and tier.

    Each returned dict:
      {
        "id": "V1", "name": "...", "tier": "basic",
        "source": {"kind": "sql"|"schema"|"manifest", "sql": "...", ...},
        "target": {...},
        "compare": "equal"|"target_le_source"|"target_nonempty_if_source"|...
      }
    The validator interprets `source`/`target`/`compare`. Purely declarative.
    """
    migration_type = (pipeline.get("migration_type") or "").lower()
    is_partitioned = bool(pipeline.get("source_partitioned"))
    key_cols = _key_columns(pipeline)
    part_cols = _partition_columns(pipeline)

    src_db = pipeline.get("source_database", "")
    src_tbl = pipeline.get("source_table", "")
    tgt_db = pipeline.get("target_database", "")
    tgt_tbl = pipeline.get("target_table", "")
    target_type = pipeline.get("target_type", "")
    bucket_arn = pipeline.get("target_bucket_arn", "")

    plan: list[dict] = []

    # V1 — row count parity (always).
    plan.append({
        "id": "V1", "name": "Row count parity", "tier": "basic",
        "source": {"kind": "sql", "sql": build_count_sql(src_db, src_tbl)},
        "target": {"kind": "sql", "sql": build_count_sql(
            tgt_db, tgt_tbl, target_type, bucket_arn)},
        "compare": "equal",
    })

    # V2 — schema conformance (metadata; validator/Spark resolves and compares).
    plan.append({
        "id": "V2", "name": "Schema conformance", "tier": "basic",
        "source": {"kind": "schema", "database": src_db, "table": src_tbl},
        "target": {"kind": "schema", "database": tgt_db, "table": tgt_tbl,
                   "target_type": target_type, "table_bucket_arn": bucket_arn},
        "compare": "schema_conformant",
    })

    # V3 — partition count (partitioned only).
    if is_partitioned:
        plan.append({
            "id": "V3", "name": "Partition count", "tier": "basic",
            "source": {"kind": "sql", "sql": build_partition_count_sql(
                src_db, src_tbl, part_cols)},
            "target": {"kind": "sql", "sql": build_partition_count_sql(
                tgt_db, tgt_tbl, part_cols, target_type, bucket_arn)},
            "compare": "equal",
        })

    # V4 — null-count parity on key columns.
    if key_cols:
        plan.append({
            "id": "V4", "name": "Null-count parity (key columns)", "tier": "basic",
            "source": {"kind": "sql", "sql": build_null_count_sql(
                src_db, src_tbl, key_cols)},
            "target": {"kind": "sql", "sql": build_null_count_sql(
                tgt_db, tgt_tbl, key_cols, target_type, bucket_arn)},
            "compare": "equal_map",
        })

    # V5 — non-emptiness + bounds (bounds only when there is a partition/key col).
    bound_col = part_cols[0] if part_cols else (key_cols[0] if key_cols else "")
    plan.append({
        "id": "V5", "name": "Non-emptiness and bounds", "tier": "basic",
        "source": {"kind": "sql", "sql": build_bounds_sql(
            src_db, src_tbl, bound_col)},
        "target": {"kind": "sql", "sql": build_bounds_sql(
            tgt_db, tgt_tbl, bound_col, target_type, bucket_arn)},
        "compare": "nonempty_and_bounds",
    })

    # V6 — morph manifest integrity (morph only; primary correctness proof).
    if migration_type == "morph":
        plan.append({
            "id": "V6", "name": "Morph manifest integrity", "tier": "basic",
            "source": {"kind": "manifest",
                       "location": pipeline.get("source_location", "")},
            "target": {"kind": "manifest", "database": tgt_db, "table": tgt_tbl,
                       "target_type": target_type, "table_bucket_arn": bucket_arn},
            "compare": "manifest_integrity",
        })

    if tier != "advanced":
        return plan

    # ── Advanced ──

    # V7 — per-column aggregates. The concrete (numeric) column list
    # isn't known to this AWS-free builder, so we emit a COUNT fallback and mark
    # the check "needs_numeric_columns": the validator resolves the numeric
    # columns from Glue and rebuilds both sides before running. Carrying
    # database/table on each side lets it do so.
    plan.append({
        "id": "V7", "name": "Per-column aggregates", "tier": "advanced",
        "needs_numeric_columns": True,
        "source": {"kind": "sql", "database": src_db, "table": src_tbl,
                   "sql": build_aggregate_sql(src_db, src_tbl)},
        "target": {"kind": "sql", "database": tgt_db, "table": tgt_tbl,
                   "target_type": target_type, "table_bucket_arn": bucket_arn,
                   "sql": build_aggregate_sql(tgt_db, tgt_tbl, target_type, bucket_arn)},
        "compare": "equal_map",
    })

    # V8 — per-partition reconciliation (partitioned only).
    if is_partitioned and part_cols:
        plan.append({
            "id": "V8", "name": "Per-partition reconciliation", "tier": "advanced",
            "source": {"kind": "sql", "sql": build_partition_count_sql(
                src_db, src_tbl, part_cols, group=True)},
            "target": {"kind": "sql", "sql": build_partition_count_sql(
                tgt_db, tgt_tbl, part_cols, target_type, bucket_arn, group=True)},
            "compare": "per_partition_equal",
        })

    return plan
