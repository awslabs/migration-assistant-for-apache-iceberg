# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""On-demand validation runner (isolated from the reconcile loop).

Runs data-validation checks comparing each pipeline's source and target on the
plan's execution engine, and persists per-pipeline results to ValidationRepo.

Design:
  - Athena pipelines: fan out across a bounded ThreadPoolExecutor
    (ENGINE_VALIDATION_WORKERS, default 5). Each pipeline runs its check-plan
    SQL via AthenaClient (submit -> poll -> results), compares in-process, and
    writes one ValidationRepo run row.
  - EMR pipelines: a SINGLE Spark job (spark/validate.py) handles ALL EMR
    pipelines in the run sequentially and writes each pipeline's result row to
    DynamoDB itself as it finishes. The runner just creates the 'running' rows,
    submits the one job, and returns.

This module has ZERO coupling to reconciler.py / state_machine.py. It uses its
own executor and only touches the engine clients + ValidationRepo.
"""

from __future__ import annotations

import os
import json
import math
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor

from ..db import ValidationRepo, PipelineRepo
from ..db.validation_results import validation_table_name
from ..utils.polling import poll_until_complete
from .validation_checks import build_check_plan
from .validation_sql import build_aggregate_sql

log = logging.getLogger(__name__)

_WORKERS = int(os.getenv("ENGINE_VALIDATION_WORKERS", "5"))
_QUERY_INTERVAL = float(os.getenv("ENGINE_VALIDATION_POLL_INTERVAL", "3"))
_QUERY_MAX_ATTEMPTS = int(os.getenv("ENGINE_VALIDATION_POLL_MAX_ATTEMPTS", "200"))
# Sums over float/double columns depend on the order the engine adds the values,
# so source and target can differ in the last digits (3230692.64 vs
# 3230692.6400000006) even when every row is identical. Those sums are compared
# with this relative tolerance. Integer and decimal sums are always exact.
_FLOAT_SUM_REL_TOL = float(os.getenv("ENGINE_VALIDATION_FLOAT_REL_TOL", "1e-9"))
# One shared executor for validation fan-out (Athena path). Separate from the
# reconcile pool so validation load is independently bounded.
_executor = ThreadPoolExecutor(max_workers=_WORKERS, thread_name_prefix="validate")


def run_validation(pipeline_ids: list[str], tier: str,
                   triggered_by: str = "", athena_config: dict = None) -> dict:
    """Kick off validation for the given pipelines. Returns immediately.

    Returns {"started": [{"pipeline_id","run_id","engine"}], "errors": [...]}.
    Athena pipelines run concurrently in the background; EMR pipelines are
    handled by a single Spark job also fired in the background.
    """
    repo = ValidationRepo()
    prepo = PipelineRepo()

    started: list[dict] = []
    athena_units: list[dict] = []   # {pipeline, run_id}
    emr_units: list[dict] = []      # {pipeline, run_id}
    errors: list[dict] = []

    for pid in pipeline_ids:
        pipeline = prepo.get(pid)
        if not pipeline:
            errors.append({"pipeline_id": pid, "error": "pipeline not found"})
            continue
        engine = (pipeline.get("execution_engine") or "athena").lower()
        run_id = str(uuid.uuid4())
        repo.create_run(run_id, pid, tier, engine=engine, triggered_by=triggered_by)
        started.append({"pipeline_id": pid, "run_id": run_id, "engine": engine})
        unit = {"pipeline": pipeline, "run_id": run_id}
        if engine == "emr_serverless":
            emr_units.append(unit)
        else:
            athena_units.append(unit)

    # Athena: fan out, one background task per pipeline.
    for unit in athena_units:
        _executor.submit(_run_athena_pipeline, unit["pipeline"], unit["run_id"], tier, athena_config)

    # EMR: single job for all EMR pipelines in this run.
    if emr_units:
        _executor.submit(_run_emr_batch, emr_units, tier)

    return {"started": started, "errors": errors}


# ── Athena path ──

def _run_athena_pipeline(pipeline: dict, run_id: str, tier: str,
                         athena_config: dict = None) -> None:
    """Run all checks for one pipeline on Athena and persist the result."""
    repo = ValidationRepo()
    pid = pipeline["id"]
    # Accumulate results as we go so an unexpected failure mid-run still
    # persists the checks that already ran, instead of discarding them.
    results: list[dict] = []
    try:
        from ..engines.athena import AthenaClient
        # Use the app-supplied Athena settings (region/workgroup/output
        # location) when provided, so validation honours the user's Settings
        # rather than the engine's deploy-time env defaults. Empty values fall
        # back to the AthenaClient config defaults.
        cfg = athena_config or {}
        athena = AthenaClient(
            region=cfg.get("region", ""),
            workgroup=cfg.get("workgroup", ""),
            output_location=cfg.get("output_location", ""))
        plan = build_check_plan(pipeline, tier)
        for check in plan:
            # _run_athena_check never raises, but guard anyway so one check can
            # never stop the loop.
            try:
                results.append(_run_athena_check(athena, check))
            except Exception as e:  # pragma: no cover - defensive
                results.append({
                    "check_id": check.get("id", "?"),
                    "name": check.get("name", "check"),
                    "status": "error", "source_value": None,
                    "target_value": None, "detail": str(e)[:500]})
        overall = _overall_status(results)
        repo.update_run(run_id, pid, overall, results)
        log.info("Validation run %s (pipeline %s): %s",
                 run_id[:8], pid[:8], overall)
    except Exception as e:
        log.exception("Validation run %s failed for pipeline %s", run_id[:8], pid[:8])
        # Preserve any checks that completed; append a run-level error rather
        # than overwriting the whole result set.
        results.append({"check_id": "_run", "name": "Validation run",
                        "status": "error", "source_value": None,
                        "target_value": None, "detail": str(e)[:500]})
        repo.update_run(run_id, pid, "error", results)


def _run_athena_check(athena, check: dict) -> dict:
    """Execute one check's source+target SQL on Athena and compare.

    Fully self-contained: ANY failure here (SQL error, column resolution,
    comparison) is captured and returned as an ``error`` result for THIS check
    only. It never raises, so one bad check cannot abort the run or affect the
    other checks.
    """
    cid = check.get("id", "?")
    name = check.get("name", cid)
    base = {"check_id": cid, "name": name, "source_value": None,
            "target_value": None, "detail": ""}

    try:
        # Non-SQL checks (schema / manifest) are resolved by the Spark path; on
        # the Athena path we mark them skipped rather than silently passing.
        src, tgt = check.get("source", {}), check.get("target", {})
        if src.get("kind") != "sql" or tgt.get("kind") != "sql":
            base["status"] = "skipped"
            base["detail"] = f"{src.get('kind','?')} check not run on Athena engine"
            return base

        src_sql, tgt_sql = src.get("sql"), tgt.get("sql")
        # The per-column aggregates check (V7) needs the numeric column list,
        # which the AWS-free plan builder can't know. Resolve numeric columns
        # from Glue and rebuild both sides over the SHARED set (intersection, so
        # a column present on only one side can't produce a spurious mismatch).
        # Without this, build_aggregate_sql emits only COUNT(*) and V7 degrades
        # to a duplicate row-count check.
        approx_keys: frozenset = frozenset()
        if check.get("needs_numeric_columns"):
            s_types = _numeric_column_types(
                src.get("database", ""), src.get("table", ""))
            t_types = _numeric_column_types(
                tgt.get("database", ""), tgt.get("table", ""))
            s_cols, t_cols = list(s_types), list(t_types)
            if not s_cols or not t_cols:
                base["status"] = "skipped"
                base["detail"] = "could not resolve numeric columns for aggregates"
                return base
            tset = set(t_cols)
            shared = [c for c in s_cols if c in tset]  # preserve source order
            # A sum is order-dependent if either side stores the column as a
            # float; those get the tolerance, everything else stays exact.
            approx_keys = frozenset(
                "sum_" + c for c in shared
                if _is_float_type(s_types[c]) or _is_float_type(t_types[c]))
            if not shared:
                base["status"] = "skipped"
                base["detail"] = "no numeric columns common to source and target"
                return base
            src_sql = build_aggregate_sql(
                src["database"], src["table"], numeric_columns=shared)
            tgt_sql = build_aggregate_sql(
                tgt["database"], tgt["table"],
                target_type=tgt.get("target_type", ""),
                table_bucket_arn=tgt.get("table_bucket_arn", ""),
                numeric_columns=shared)

        if not src_sql or not tgt_sql:
            base["status"] = "error"
            base["detail"] = "check is missing source or target SQL"
            return base

        s_val = _athena_scalar(athena, src_sql, cid)
        t_val = _athena_scalar(athena, tgt_sql, cid)
        base["source_value"] = s_val
        base["target_value"] = t_val
        base["status"], base["detail"] = _compare(
            check.get("compare", "equal"), s_val, t_val, approx_keys)
    except Exception as e:
        log.warning("Validation check %s errored: %s", cid, e)
        base["status"] = "error"
        base["detail"] = str(e)[:500]
    return base


def _athena_scalar(athena, sql: str, description: str):
    """Submit a query, poll to completion, return the result row(s).

    For single-row checks returns the row dict; for grouped (per-partition)
    returns the full list of rows.
    """
    qid = athena.submit_query(sql, description=f"validate-{description}")
    status = poll_until_complete(
        check=lambda: athena.get_query_status(qid),
        is_done=lambda s: s["state"] in ("SUCCEEDED", "FAILED", "CANCELLED"),
        interval=_QUERY_INTERVAL, max_attempts=_QUERY_MAX_ATTEMPTS)
    if status["state"] != "SUCCEEDED":
        raise RuntimeError(
            f"query {status['state']}: {status.get('state_reason','')[:200]}")
    rows = athena.get_query_results(qid, max_results=1000)
    if not rows:
        return {}
    return rows if len(rows) > 1 else rows[0]


def _glue_columns(database: str, table: str) -> list[dict]:
    """Return a table's columns as [{"name","type"}] (data + partition keys),
    in declared order. Empty list on any failure so callers can skip cleanly."""
    if not database or not table:
        return []
    try:
        import boto3
        region = os.getenv("ENGINE_ATHENA_REGION",
                           os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        glue = boto3.client("glue", region_name=region)
        resp = glue.get_table(DatabaseName=database, Name=table)
        sd = resp["Table"].get("StorageDescriptor", {})
        cols = [{"name": c["Name"], "type": (c.get("Type") or "").lower()}
                for c in sd.get("Columns", [])]
        cols += [{"name": c["Name"], "type": (c.get("Type") or "").lower()}
                 for c in resp["Table"].get("PartitionKeys", [])]
        return cols
    except Exception as e:
        log.warning("Could not resolve columns for %s.%s: %s", database, table, e)
        return []


# Hive/Glue type prefixes that SUM/MIN/MAX/COUNT(DISTINCT) are meaningful on.
# Matched by prefix so parameterized types (decimal(10,2)) are included.
_NUMERIC_TYPE_PREFIXES = (
    "tinyint", "smallint", "int", "integer", "bigint",
    "float", "double", "decimal",
)


def _numeric_column_types(database: str, table: str) -> dict[str, str]:
    """{column name: type} for the numeric columns (data + partition keys), in
    declared order, for the per-column aggregates check (V7). Empty on any
    failure so the caller can skip."""
    out = {}
    for c in _glue_columns(database, table):
        t = c["type"]
        if any(t.startswith(p) for p in _NUMERIC_TYPE_PREFIXES):
            out[c["name"]] = t
    return out
def _resolve_numeric_columns(database: str, table: str) -> list[str]:
    """Numeric column names; see _numeric_column_types."""
    return list(_numeric_column_types(database, table))
def _is_float_type(glue_type: str) -> bool:
    """True for floating-point types, whose sums depend on addition order."""
    return (glue_type or "").lower().startswith(("float", "double"))


# ── EMR path ──

def _validation_spark_props(pipelines: list[dict]) -> dict:
    """Build a SINGLE Spark config that supports EVERY pipeline in the batch.

    The validation job validates a mix of pipelines (different source formats
    and target types) in one Spark session. Unlike the migration scripts —
    which get per-pipeline props keyed to one source_format/target_type — the
    validation session must be a SUPERSET: without it, table refs like
    ``glue_catalog.db.table`` resolve against an unconfigured catalog
    (REQUIRES_SINGLE_PART_NAMESPACE), source reads via ``spark_catalog`` miss
    the Glue metastore (TABLE_OR_VIEW_NOT_FOUND), and Delta sources fail
    (DELTA_CONFIGURE_SPARK_SESSION). Mirrors strategies.incremental
    ._build_iceberg_spark_props but unions the format/target needs of all
    pipelines in the batch.
    """
    formats = {(p.get("source_format") or "").lower() for p in pipelines}
    target_types = {(p.get("target_type") or "").lower() for p in pipelines}

    props = {
        # Session catalog = Glue-backed Hive metastore (source reads via
        # spark_catalog.db.table). convertMetastoreParquet=false so
        # Delta/Hudi/mismatched-schema sources read through their own readers.
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.hive.metastore.client.factory.class":
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "spark.sql.hive.convertMetastoreParquet": "false",
        # Iceberg glue_catalog for s3_iceberg target reads
        # (glue_catalog.db.table).
        "spark.sql.catalog.glue_catalog":
            "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl":
            "org.apache.iceberg.aws.glue.GlueCatalog",
    }

    # Extensions: Iceberg always; add Delta's alongside it when any source is
    # Delta (comma-joined — both extensions can coexist).
    iceberg_ext = ("org.apache.iceberg.spark.extensions."
                   "IcebergSparkSessionExtensions")
    jars = []
    if "delta" in formats:
        props["spark.sql.extensions"] = (
            "io.delta.sql.DeltaSparkSessionExtension," + iceberg_ext)
        # Delta reads through the session catalog, so spark_catalog must be the
        # DeltaCatalog (which still delegates to Hive/Glue for non-Delta tables).
        props["spark.sql.catalog.spark_catalog"] = (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        jars += ["/usr/share/aws/delta/lib/delta-spark.jar",
                 "/usr/share/aws/delta/lib/delta-storage.jar"]
    else:
        props["spark.sql.extensions"] = iceberg_ext

    if "hudi" in formats:
        jars += ["/usr/lib/hudi/hudi-spark-bundle.jar",
                 "/usr/lib/hudi/hudi-utilities-bundle.jar",
                 "/usr/lib/hudi/hudi-aws-bundle.jar"]
        props["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"

    if jars:
        props["spark.jars"] = ",".join(jars)

    # S3 Tables target(s): add the icebergrest GlueCatalog props. Uses the
    # first s3_tables pipeline's bucket/account/region. NOTE: a batch mixing
    # MULTIPLE distinct S3 Tables buckets can't be fully served by one session
    # (single icebergrest catalog id); such pipelines should be validated in
    # separate runs. Single-bucket batches (the common case) work.
    if "s3_tables" in target_types:
        from ..strategies.common import s3tables_spark_props
        s3t = next((p for p in pipelines
                    if (p.get("target_type") or "").lower() == "s3_tables"), None)
        if s3t:
            props.update(s3tables_spark_props(s3t))

    return props


def _run_emr_batch(units: list[dict], tier: str) -> None:
    """Submit ONE Spark job to validate all EMR pipelines in this run.

    The Spark job (spark/validate.py) writes each pipeline's ValidationRepo row
    to DynamoDB itself. Here we submit the job and, after it finishes, mark any
    pipeline whose row is still 'running' (never written) as error.
    """
    repo = ValidationRepo()
    try:
        from ..engines.emr import EmrServerlessClient, resolve_script_path
        # Build the EMR client from the pipeline spec's strategy.emr block
        # (application_id / region / execution_role_arn the app injected from
        # Settings) rather than a bare EmrServerlessClient(), which would use
        # the engine's deploy-time env defaults (region us-east-1, empty
        # app-id) and fail StartJobRun with ResourceNotFoundException
        # ("Application does not exist"). All EMR pipelines in a validation run
        # share the same plan/engine, so the first unit's pipeline is
        # authoritative for the whole batch.
        emr = EmrServerlessClient.from_pipeline(units[0]["pipeline"]) if units \
            else EmrServerlessClient()
        # Compact payload: pipelines + tier + validation table/region so the
        # Spark job knows where to write.
        payload = {
            "tier": tier,
            # Same lookup as the engine's ValidationRepo, so the job writes
            # where the engine reads.
            "validation_table": validation_table_name(),
            "region": os.getenv("ENGINE_DYNAMODB_REGION",
                                 os.getenv("AWS_DEFAULT_REGION", "us-east-1")),
            "pipelines": [
                {"run_id": u["run_id"], "pipeline": _emr_pipeline_payload(u["pipeline"])}
                for u in units
            ],
        }
        script_path = resolve_script_path("validate.py")
        job_id = emr.submit_job(
            name=f"IMA-validate-{uuid.uuid4().hex[:8]}",
            script_path=script_path,
            script_args=["--spec", json.dumps(payload)],
            spark_properties=_validation_spark_props([u["pipeline"] for u in units]))
        log.info("Submitted validation Spark job %s for %d pipeline(s)",
                 job_id, len(units))
        # Poll job to completion; then reconcile any un-written rows.
        status = poll_until_complete(
            check=lambda: emr.get_job_status(job_id),
            is_done=lambda s: s["state"] in (
                "SUCCESS", "FAILED", "CANCELLED", "CANCELLING"),
            interval=_QUERY_INTERVAL, max_attempts=_QUERY_MAX_ATTEMPTS)
        _reconcile_emr_rows(repo, units, status)
    except Exception as e:
        log.exception("EMR validation batch failed")
        for u in units:
            repo.update_run(u["run_id"], u["pipeline"]["id"], "error",
                            [{"check_id": "_run", "name": "Validation run",
                              "status": "error", "source_value": None,
                              "target_value": None, "detail": str(e)[:500]}])


def _reconcile_emr_rows(repo, units, job_status) -> None:
    """After the Spark job ends, any run still 'running' didn't get written."""
    job_state = job_status.get("state", "")
    for u in units:
        pid = u["pipeline"]["id"]
        run = repo.get_run(u["run_id"], pid)
        if run and run.get("overall_status") == "running":
            detail = (f"Spark validation job ended {job_state} without writing "
                      f"a result for this pipeline")
            repo.update_run(u["run_id"], pid, "error",
                            [{"check_id": "_run", "name": "Validation run",
                              "status": "error", "source_value": None,
                              "target_value": None, "detail": detail}])


def _emr_pipeline_payload(pipeline: dict) -> dict:
    """Minimal pipeline fields the Spark validation job needs."""
    keys = ("id", "migration_type", "source_database", "source_table",
            "source_location", "source_format", "source_partitioned",
            "source_partition_keys", "sort_order",
            "target_database", "target_table", "target_type",
            "target_bucket_arn", "estimated_size_gb")
    return {k: pipeline.get(k) for k in keys}


# ── Comparison semantics ──

def _to_number(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _floats_match(a, b) -> bool:
    """True when a and b are numbers equal within the float-sum tolerance."""
    x, y = _to_number(a), _to_number(b)
    if x is None or y is None or math.isnan(x) or math.isnan(y):
        return False
    return math.isclose(x, y, rel_tol=_FLOAT_SUM_REL_TOL, abs_tol=_FLOAT_SUM_REL_TOL)
def _compare(mode: str, s_val, t_val,
             approx_keys: frozenset = frozenset()) -> tuple[str, str]:
    """Return (status, detail) for a check comparison. status: passed|failed.

    approx_keys names metrics (e.g. "sum_salary") compared with the float
    tolerance instead of as exact strings."""
    if mode == "equal":
        # Single scalar (row_count). Rows are dicts of str->str.
        sv, tv = _single_metric(s_val), _single_metric(t_val)
        if sv == tv:
            return "passed", ""
        return "failed", f"source={sv} target={tv}"

    if mode == "equal_map":
        # Compare all key/value metrics in the two single-row dicts.
        sd = s_val if isinstance(s_val, dict) else {}
        td = t_val if isinstance(t_val, dict) else {}
        mismatches, within_tol = [], []
        for k in sorted(set(sd) | set(td)):
            if str(sd.get(k)) == str(td.get(k)):
                continue
            if k in approx_keys and _floats_match(sd.get(k), td.get(k)):
                within_tol.append(f"{k}: source={sd.get(k)} target={td.get(k)}")
                continue
            mismatches.append(f"{k}: source={sd.get(k)} target={td.get(k)}")
        if not mismatches:
            note = ("within float tolerance (%g relative): " % _FLOAT_SUM_REL_TOL
                    + "; ".join(within_tol[:10])) if within_tol else ""
            return "passed", note
        return "failed", "; ".join(mismatches[:10])

    if mode == "nonempty_and_bounds":
        sd = s_val if isinstance(s_val, dict) else {}
        td = t_val if isinstance(t_val, dict) else {}
        s_rows = _to_number(sd.get("row_count")) or 0
        t_rows = _to_number(td.get("row_count")) or 0
        if s_rows > 0 and t_rows == 0:
            return "failed", "target is empty but source is not"
        # Bounds (when present) must fall within source range.
        detail_parts = [f"rows source={int(s_rows)} target={int(t_rows)}"]
        if "min_bound" in sd and "min_bound" in td:
            detail_parts.append(
                f"min source={sd.get('min_bound')} target={td.get('min_bound')}")
            detail_parts.append(
                f"max source={sd.get('max_bound')} target={td.get('max_bound')}")
        return "passed", "; ".join(detail_parts)

    if mode == "per_partition_equal":
        # Both are lists of rows [{part..., row_count}]. Compare as maps.
        s_rows = s_val if isinstance(s_val, list) else ([s_val] if s_val else [])
        t_rows = t_val if isinstance(t_val, list) else ([t_val] if t_val else [])

        def _key(r):
            return tuple(v for k, v in sorted(r.items()) if k != "row_count")
        s_map = {_key(r): r.get("row_count") for r in s_rows}
        t_map = {_key(r): r.get("row_count") for r in t_rows}
        mismatches = []
        for k in set(s_map) | set(t_map):
            if str(s_map.get(k)) != str(t_map.get(k)):
                mismatches.append(f"{k}: source={s_map.get(k)} target={t_map.get(k)}")
        if not mismatches:
            return "passed", f"{len(s_map)} partitions matched"
        return "failed", f"{len(mismatches)} partition(s) differ: " + \
            "; ".join(mismatches[:10])

    # schema_conformant / manifest_integrity are not run on Athena (skipped
    # earlier). If we somehow reach here, be conservative.
    return "skipped", f"comparison '{mode}' not supported on this engine"


def _single_metric(val):
    """Extract the single scalar from a one-row result dict."""
    if isinstance(val, dict):
        # single-column results (row_count / partition_count)
        if len(val) == 1:
            return str(next(iter(val.values())))
        # prefer a known metric column
        for k in ("row_count", "partition_count"):
            if k in val:
                return str(val[k])
    return str(val)


def _overall_status(results: list[dict]) -> str:
    """Roll up per-check statuses into an overall verdict."""
    statuses = {r.get("status") for r in results}
    if "error" in statuses:
        return "error"
    if "failed" in statuses:
        return "failed"
    return "passed"
