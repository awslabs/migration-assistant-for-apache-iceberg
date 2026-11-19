# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PySpark script for on-demand migration validation on EMR Serverless.

Submitted as a SINGLE job for a whole validation run by core/validator.py
(_run_emr_batch). It loops the run's pipelines sequentially, computes the
tier's checks by comparing each pipeline's source and target inside one Spark
session, and writes each pipeline's result row directly to the DynamoDB
validation-results table as it completes (progressive persistence, so a crash
midway still leaves earlier pipelines' results recorded).

Because it runs in Spark, this path can perform the schema and manifest checks
that the Athena path skips.

Usage:
  spark-submit validate.py --spec '{
    "tier": "basic|advanced",
    "validation_table": "ima-validation-results",
    "region": "us-east-1",
    "pipelines": [
      {"run_id": "...", "pipeline": {
        "id","migration_type","source_database","source_table",
        "source_location","source_format","source_partitioned",
        "source_partition_keys","sort_order","target_database",
        "target_table","target_type","target_bucket_arn","estimated_size_gb"}}
    ]
  }'

Self-contained: like the other spark/*.py scripts it imports only argparse,
json, boto3 (present on EMR), pyspark, and spark_sql_builder (auto-included as
a --py-files dependency). It deliberately does NOT import the engine's db /
core packages — those are not on the EMR classpath.
"""

import argparse
import json
import math
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_sql_builder import spark_source_ref, spark_table_ref

# Relative tolerance for float/double sums (see _check_aggregate). Same default
# as the Athena path (ENGINE_VALIDATION_FLOAT_REL_TOL).
_FLOAT_SUM_REL_TOL = 1e-9


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True, help="Run spec as JSON string")
    return parser.parse_args()


# ── DynamoDB persistence (mirrors ValidationRepo's dynamo write path) ──

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_run(table, run_id: str, pipeline_id: str,
                overall_status: str, results: list) -> None:
    """Fill in results + final status for a run row created by the engine.

    Raises on persistent failure. A silently-swallowed write failure here is
    exactly what produces the confusing "job ended SUCCESS without writing a
    result" state: the Spark job succeeds, but the engine's reconcile still
    sees the row as 'running' and marks it error. By raising, the failure
    surfaces in the job logs (and, via run(), fails the job) with the real
    DynamoDB error (wrong region/table, missing IAM permission, etc.) instead
    of being hidden.
    """
    last_err = None
    for attempt in range(3):
        try:
            table.update_item(
                Key={"pipeline_id": pipeline_id, "run_id": run_id},
                UpdateExpression=(
                    "SET overall_status = :st, results_json = :rj, "
                    "completed_at = :ca"),
                ExpressionAttributeValues={
                    ":st": overall_status,
                    ":rj": json.dumps(results),
                    ":ca": _now(),
                })
            return
        except Exception as e:
            last_err = e
            print(f"WARNING: write attempt {attempt + 1} failed for "
                  f"{pipeline_id}/{run_id}: {e}")
    raise RuntimeError(
        f"failed to write validation result for {pipeline_id}/{run_id} to "
        f"DynamoDB after 3 attempts: {last_err}")


# ── source / target dataframes ──

def _source_df(spark, p: dict):
    """Read the source table. Prefer physical parquet read for parquet sources
    (matches rewrite_ctas semantics); otherwise resolve via the session catalog.
    """
    fmt = (p.get("source_format") or "").lower()
    location = (p.get("source_location") or "").rstrip("/")
    if fmt == "parquet" and location:
        try:
            return spark.read.parquet(location)
        except Exception as e:
            print(f"Parquet path read failed ({e}); falling back to catalog")
    if fmt == "delta" and location:
        loc = location
        if loc.endswith("/_symlink_format_manifest"):
            loc = loc.rsplit("/_symlink_format_manifest", 1)[0]
        return spark.read.format("delta").load(loc)
    if fmt == "hudi" and location:
        return spark.read.format("hudi").load(location)
    ref = spark_source_ref(p["source_database"], p["source_table"])
    return spark.table(ref)


def _target_df(spark, p: dict):
    ref = spark_table_ref(p["target_database"], p["target_table"],
                          p.get("target_type", ""))
    return spark.table(ref)


# ── check helpers ──

def _as_list(v):
    """Coerce a field to a list, tolerating JSON-string form.

    The engine stores source_partition_keys / sort_order as JSON STRINGS in
    DynamoDB (e.g. '[{"name":"d_date","type":"date"}]') and the pipeline repo
    returns them un-deserialized. Treating a JSON string as "not a list" is
    what silently dropped V3/V4/V5/V7/V8: the column lists came back empty even
    for partitioned/sorted tables, so those checks were skipped. Parse strings
    here so the column-dependent checks actually run.
    """
    if isinstance(v, list):
        return v
    if isinstance(v, str) and v.strip():
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, TypeError):
            return []
    return []


def _partition_columns(p: dict) -> list[str]:
    out = []
    for pk in _as_list(p.get("source_partition_keys")):
        if isinstance(pk, dict) and pk.get("name"):
            out.append(pk["name"])
        elif isinstance(pk, str):
            out.append(pk)
    return out


def _key_columns(p: dict) -> list[str]:
    cols = []
    for s in _as_list(p.get("sort_order")):
        if isinstance(s, dict) and s.get("column"):
            cols.append(s["column"])
    if not cols:
        cols = _partition_columns(p)
    return cols


def _result(check_id, name, status, sv=None, tv=None, detail=""):
    return {"check_id": check_id, "name": name, "status": status,
            "source_value": sv, "target_value": tv, "detail": detail}


# ── individual checks ──

def _check_row_count(sdf, tdf) -> dict:
    sc, tc = sdf.count(), tdf.count()
    status = "passed" if sc == tc else "failed"
    return _result("V1", "Row count", status, str(sc), str(tc),
                   "" if status == "passed" else f"source={sc} target={tc}")


def _check_schema(sdf, tdf) -> dict:
    """V2 — target schema conforms to source (same columns, compatible types)."""
    s_fields = {f.name.lower(): f.dataType.simpleString() for f in sdf.schema.fields}
    t_fields = {f.name.lower(): f.dataType.simpleString() for f in tdf.schema.fields}
    missing = [c for c in s_fields if c not in t_fields]
    if missing:
        return _result("V2", "Schema conformance", "failed",
                       str(len(s_fields)), str(len(t_fields)),
                       f"target missing column(s): {', '.join(missing[:20])}")
    type_diffs = [f"{c}: source={s_fields[c]} target={t_fields[c]}"
                  for c in s_fields if s_fields[c] != t_fields[c]]
    if type_diffs:
        return _result("V2", "Schema conformance", "passed",
                       str(len(s_fields)), str(len(t_fields)),
                       "type differences (informational): " +
                       "; ".join(type_diffs[:10]))
    return _result("V2", "Schema conformance", "passed",
                   str(len(s_fields)), str(len(t_fields)), "schema matches")


def _check_partition_count(sdf, tdf, part_cols) -> dict:
    """V3 — distinct partition count matches."""
    sc = sdf.select(*part_cols).distinct().count()
    tc = tdf.select(*part_cols).distinct().count()
    status = "passed" if sc == tc else "failed"
    return _result("V3", "Partition count", status, str(sc), str(tc),
                   "" if status == "passed"
                   else f"distinct partitions source={sc} target={tc}")


def _check_null_counts(sdf, tdf, key_cols) -> dict:
    """V4 — null counts on key/sort columns match."""
    mismatches = []
    sv, tv = {}, {}
    for c in key_cols:
        s_nulls = sdf.filter(F.col(c).isNull()).count()
        t_nulls = tdf.filter(F.col(c).isNull()).count()
        sv[c], tv[c] = s_nulls, t_nulls
        if s_nulls != t_nulls:
            mismatches.append(f"{c}: source={s_nulls} target={t_nulls}")
    status = "passed" if not mismatches else "failed"
    return _result("V4", "Null counts (key columns)", status,
                   json.dumps(sv), json.dumps(tv),
                   "" if status == "passed" else "; ".join(mismatches[:10]))


def _check_bounds(sdf, tdf, key_cols) -> dict:
    """V5 — min/max bounds on key columns fall within source range."""
    detail_parts = []
    ok = True
    for c in key_cols:
        s_row = sdf.select(F.min(c).alias("mn"), F.max(c).alias("mx")).collect()[0]
        t_row = tdf.select(F.min(c).alias("mn"), F.max(c).alias("mx")).collect()[0]
        detail_parts.append(
            f"{c}: source[{s_row['mn']},{s_row['mx']}] "
            f"target[{t_row['mn']},{t_row['mx']}]")
        if str(s_row["mn"]) != str(t_row["mn"]) or \
                str(s_row["mx"]) != str(t_row["mx"]):
            ok = False
    status = "passed" if ok else "failed"
    return _result("V5", "Value bounds (key columns)", status, None, None,
                   "; ".join(detail_parts[:10]))


def _check_manifest(spark, tdf, p) -> dict:
    """V6 — morph manifest integrity: target Iceberg table has files/rows and
    is readable (no orphaned/missing data files)."""
    try:
        cnt = tdf.count()
        if cnt <= 0:
            return _result("V6", "Morph manifest integrity", "failed",
                           None, str(cnt),
                           "target table is readable but empty")
        return _result("V6", "Morph manifest integrity", "passed",
                       None, str(cnt),
                       f"target readable with {cnt} rows (manifests resolve)")
    except Exception as e:
        return _result("V6", "Morph manifest integrity", "error",
                       None, None, str(e)[:500])


def _check_aggregate(sdf, tdf, key_cols) -> dict:
    """V7 — numeric aggregate (sum) parity on numeric key columns."""
    from pyspark.sql.types import NumericType
    numeric = [c for c in key_cols
               if isinstance(sdf.schema[c].dataType, NumericType)]
    if not numeric:
        return _result("V7", "Aggregate parity", "skipped", None, None,
                       "no numeric key columns to aggregate")
    from pyspark.sql.types import FloatType, DoubleType
    float_types = (FloatType, DoubleType)
    mismatches, within_tol, sv, tv = [], [], {}, {}
    for c in numeric:
        s_sum = sdf.select(F.sum(c)).collect()[0][0]
        t_sum = tdf.select(F.sum(c)).collect()[0][0]
        sv[c], tv[c] = str(s_sum), str(t_sum)
        if str(s_sum) == str(t_sum):
            continue
        # Float/double sums depend on the order values are added, so identical
        # data can differ in the last digits. Compare those with a relative
        # tolerance; integer and decimal sums stay exact.
        is_float = (isinstance(sdf.schema[c].dataType, float_types)
                    or isinstance(tdf.schema[c].dataType, float_types))
        if (is_float and s_sum is not None and t_sum is not None
                and math.isclose(float(s_sum), float(t_sum),
                                 rel_tol=_FLOAT_SUM_REL_TOL, abs_tol=_FLOAT_SUM_REL_TOL)):
            within_tol.append(f"{c}: source={s_sum} target={t_sum}")
            continue
        mismatches.append(f"{c}: source={s_sum} target={t_sum}")
    status = "passed" if not mismatches else "failed"
    if mismatches:
        detail = "; ".join(mismatches[:10])
    elif within_tol:
        detail = ("within float tolerance (%g relative): " % _FLOAT_SUM_REL_TOL
                  + "; ".join(within_tol[:10]))
    else:
        detail = ""
    return _result("V7", "Aggregate parity", status,
                   json.dumps(sv), json.dumps(tv), detail)



def _check_per_partition(sdf, tdf, part_cols) -> dict:
    """V8 — per-partition row counts match."""
    s_rows = {tuple(str(r[c]) for c in part_cols): r["cnt"]
              for r in sdf.groupBy(*part_cols)
              .agg(F.count(F.lit(1)).alias("cnt")).collect()}
    t_rows = {tuple(str(r[c]) for c in part_cols): r["cnt"]
              for r in tdf.groupBy(*part_cols)
              .agg(F.count(F.lit(1)).alias("cnt")).collect()}
    mismatches = []
    for k in set(s_rows) | set(t_rows):
        if s_rows.get(k) != t_rows.get(k):
            mismatches.append(f"{k}: source={s_rows.get(k)} target={t_rows.get(k)}")
    status = "passed" if not mismatches else "failed"
    return _result("V8", "Per-partition row counts", status,
                   str(len(s_rows)), str(len(t_rows)),
                   f"{len(s_rows)} partitions matched" if status == "passed"
                   else f"{len(mismatches)} partition(s) differ: " +
                        "; ".join(mismatches[:10]))


# ── per-pipeline driver ──

def _safe_check(check_id, name, fn) -> dict:
    """Run one check function, capturing any failure as an ``error`` result for
    THAT check only. Mirrors the Athena path so one bad check never aborts the
    pipeline's run or contaminates the other checks."""
    try:
        return fn()
    except Exception as e:  # pragma: no cover - defensive, needs a live Spark
        print(f"Check {check_id} errored: {e}")
        return _result(check_id, name, "error", detail=str(e)[:500])


def _run_pipeline_checks(spark, p: dict, tier: str) -> list:
    """Run the tier's checks for one pipeline; return list of result dicts.

    Each check is isolated: a failure in one is recorded as an ``error`` result
    for that check and the rest still run.
    """
    migration_type = (p.get("migration_type") or "").lower()
    is_partitioned = bool(p.get("source_partitioned"))
    part_cols = _partition_columns(p)
    key_cols = _key_columns(p)

    sdf = _source_df(spark, p)
    tdf = _target_df(spark, p)

    results = [
        _safe_check("V1", "Row count", lambda: _check_row_count(sdf, tdf)),
        _safe_check("V2", "Schema conformance", lambda: _check_schema(sdf, tdf)),
    ]
    if is_partitioned and part_cols:
        results.append(_safe_check("V3", "Partition count",
                                   lambda: _check_partition_count(sdf, tdf, part_cols)))
    if key_cols:
        results.append(_safe_check("V4", "Null counts (key columns)",
                                   lambda: _check_null_counts(sdf, tdf, key_cols)))
        results.append(_safe_check("V5", "Value bounds (key columns)",
                                   lambda: _check_bounds(sdf, tdf, key_cols)))
    if migration_type == "morph":
        results.append(_safe_check("V6", "Morph manifest integrity",
                                   lambda: _check_manifest(spark, tdf, p)))

    if tier == "advanced":
        if key_cols:
            results.append(_safe_check("V7", "Aggregate parity",
                                       lambda: _check_aggregate(sdf, tdf, key_cols)))
        if is_partitioned and part_cols:
            results.append(_safe_check("V8", "Per-partition row counts",
                                       lambda: _check_per_partition(sdf, tdf, part_cols)))

    return results


def _overall(results: list) -> str:
    statuses = {r.get("status") for r in results}
    if "error" in statuses:
        return "error"
    if "failed" in statuses:
        return "failed"
    return "passed"


def run(spec: dict):
    import boto3

    tier = spec.get("tier", "basic")
    table_name = spec.get("validation_table", "ima-validation-results")
    region = spec.get("region", "us-east-1")
    pipelines = spec.get("pipelines", [])

    table = boto3.resource("dynamodb", region_name=region).Table(table_name)

    spark = SparkSession.builder \
        .appName(f"IMA-validate-{tier}-{len(pipelines)}pl") \
        .getOrCreate()

    write_failures = []
    try:
        for unit in pipelines:
            run_id = unit["run_id"]
            p = unit["pipeline"]
            pid = p["id"]
            print(f"=== Validating pipeline {pid} (run {run_id}) ===")
            try:
                results = _run_pipeline_checks(spark, p, tier)
                overall = _overall(results)
            except Exception as e:
                print(f"Pipeline {pid} validation errored: {e}")
                results = [_result("_run", "Validation run", "error",
                                   detail=str(e)[:500])]
                overall = "error"
            # Progressive write: persist this pipeline's result before moving on.
            # A write failure must NOT be swallowed — otherwise the job reports
            # SUCCESS while the engine still sees the run as 'running' and marks
            # it error ("job ended SUCCESS without writing a result"). Record
            # the failure, keep persisting the other pipelines, then fail the
            # job at the end so the real DynamoDB error is visible.
            try:
                _update_run(table, run_id, pid, overall, results)
                print(f"Pipeline {pid}: {overall}")
            except Exception as e:
                print(f"Pipeline {pid}: RESULT WRITE FAILED: {e}")
                write_failures.append(f"{pid}/{run_id}: {e}")
    finally:
        spark.stop()

    if write_failures:
        raise RuntimeError(
            "validation ran but result persistence failed for "
            f"{len(write_failures)} pipeline(s): " + "; ".join(write_failures))


if __name__ == "__main__":
    args = parse_args()
    run(json.loads(args.spec))
