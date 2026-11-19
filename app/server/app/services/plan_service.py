# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Migration plan service."""
import json
import logging
import re
import uuid
from datetime import datetime
from typing import Optional

import httpx
from app import engine_client

from app.database import get_conn, _fetchall, _fetchone, _run_sql, _commit
from app.models import CreatePlanRequest, MigrationPlan, PlanTable

log = logging.getLogger(__name__)

def _get_live_statuses_batch(tables: list[dict]) -> dict[str, str]:
    """Fetch live status for multiple pipelines from the engine.
    Returns {pipeline_id: mapped_status} for all reachable pipelines.
    """
    from app.services.pipeline_sync import _map_status
    engine_url = _get_engine_url()
    results = {}
    for t in tables:
        pid = t.get("pipeline_id")
        if not pid:
            continue
        try:
            resp = engine_client.get(f"{engine_url}/api/pipeline/{pid}", timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                status = _map_status(
                    data.get("status", ""),
                    t.get("migration_type", "rewrite"),
                    "",
                    data.get("sync_status", ""))
                results[pid] = status
        except httpx.RequestError as e:
            log.debug("Engine unreachable for pipeline %s: %s", pid, e)
            continue
    return results


def _get_live_status_details_batch(tables: list[dict]) -> dict[str, dict]:
    """Like _get_live_statuses_batch, but also returns the engine's live
    error_message so callers can surface WHY a pipeline is failed.

    Returns {pipeline_id: {"status": <mapped>, "error_message": <str>}}.
    """
    from app.services.pipeline_sync import _map_status
    engine_url = _get_engine_url()
    results: dict[str, dict] = {}
    for t in tables:
        pid = t.get("pipeline_id")
        if not pid:
            continue
        try:
            resp = engine_client.get(f"{engine_url}/api/pipeline/{pid}", timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                status = _map_status(
                    data.get("status", ""),
                    t.get("migration_type", "rewrite"),
                    "",
                    data.get("sync_status", ""))
                results[pid] = {
                    "status": status,
                    "error_message": data.get("error_message") or "",
                }
        except httpx.RequestError as e:
            log.debug("Engine unreachable for pipeline %s: %s", pid, e)
            continue
    return results

def _get_engine_url():
    from app.services.settings_service import get_setting
    return get_setting("ENGINE_URL") or "http://localhost:8001"


# ── Confirmed engine actions ────────────────────────────────────────────────
# Pause/resume/cancel/delete used to fire the engine call and update the app
# regardless of the answer. These helpers report the status the engine actually
# ends up in, so callers only record what the engine confirmed.

class EngineActionError(Exception):
    """An engine action could not be confirmed. The message is shown to users."""


# Final engine statuses that count as success for each action. "deleted" means
# the engine no longer has the pipeline.
_PAUSE_OK = lambda s: s not in (None, "running")                       # noqa: E731
_RESUME_OK = lambda s: s not in (None, "paused")                       # noqa: E731
_CANCEL_OK = lambda s: s in ("cancelled", "completed", "failed", "deleted")  # noqa: E731
_DELETE_OK = lambda s: s in ("deleted", "completed")                   # noqa: E731


def _engine_status(engine_url: str, pid: str) -> Optional[str]:
    """The pipeline's status per the engine, "deleted" if gone, None if unknown."""
    try:
        resp = engine_client.get(f"{engine_url}/api/pipeline/{pid}", timeout=10.0)
    except httpx.RequestError:
        return None
    if resp.status_code == 404:
        return "deleted"
    if resp.status_code == 200:
        try:
            return (resp.json() or {}).get("status")
        except ValueError:
            return None
    return None


def _engine_action(engine_url: str, pid: str, action: str) -> tuple[Optional[str], str]:
    """Run pause/resume/cancel/delete on a pipeline.

    Returns (status the engine reports afterwards, error text). When the engine
    refuses (for example pausing something already paused) the current status
    is read back, so callers can tell "already in that state" from a failure.
    """
    url = f"{engine_url}/api/pipeline/{pid}"
    try:
        if action == "delete":
            resp = engine_client.delete(url, timeout=10.0)
        else:
            resp = engine_client.post(f"{url}/{action}", timeout=10.0)
    except httpx.RequestError as e:
        return None, f"engine unreachable ({e})"
    if resp.status_code in (200, 201):
        if action == "delete":
            return "deleted", ""
        try:
            status = (resp.json() or {}).get("status")
        except ValueError:
            status = None
        if status:
            return status, ""
    elif resp.status_code == 404:
        return "deleted", ""
    err = "" if resp.status_code in (200, 201) else (resp.text or "")[:200]
    return _engine_status(engine_url, pid), err


def _failure_text(label: str, status: Optional[str], err: str) -> str:
    if status is None:
        return f"{label}: {err or 'engine status unknown'}"
    return f"{label}: engine reports '{status}'" + (f" ({err})" if err else "")


def _raise_if_failed(action: str, done: int, total: int, failures: list[str]) -> None:
    if not failures:
        return
    raise EngineActionError(
        f"{action} not confirmed for {len(failures)} of {total} table(s)"
        + (f" ({done} done)" if done else "") + ": " + "; ".join(failures[:5])
        + (f"; and {len(failures) - 5} more" if len(failures) > 5 else ""))

def _load_plan(conn, p) -> MigrationPlan:
    tables = _fetchall(conn,
        "SELECT source_database, source_table, target_database, target_table, "
        "migration_type, status, "
        "source_format, source_location, source_account, "
        "source_partitioned, source_partition_keys, source_compression, "
        "target_type, target_location, target_bucket_arn, "
        "partition_spec, sort_order, table_properties, "
        "pipeline_id, "
        "recommended_type, recommended_batch_size, "
        "estimated_size_gb, estimated_cost_usd, "
        "estimated_time_min, analysis_summary, analysis_status, "
        "error_message, schema_compatible, schema_issues "
        "FROM plan_tables WHERE plan_id=? "
        "ORDER BY source_database, source_table", (p["id"],))
    mt = p["migration_type"] or '["full_table"]'
    migration_types = json.loads(mt) if mt.startswith("[") else [mt]
    parsed_tables = []
    for t in tables:
        td = dict(t)
        # Deserialize JSON fields
        td["source_partitioned"] = bool(td.get("source_partitioned", 0))
        for jf in ("source_partition_keys", "partition_spec", "sort_order"):
            val = td.get(jf)
            if isinstance(val, str):
                try:
                    td[jf] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    td[jf] = []
            elif val is None:
                td[jf] = None
        # Schema evolution fields
        sc = td.get("schema_compatible")
        td["schema_compatible"] = bool(sc) if sc is not None else None
        si = td.get("schema_issues")
        if isinstance(si, str):
            try:
                td["schema_issues"] = json.loads(si)
            except (json.JSONDecodeError, TypeError):
                td["schema_issues"] = []
        elif si is None:
            td["schema_issues"] = None
        # Deserialize table_properties as dict
        tp_val = td.get("table_properties")
        if isinstance(tp_val, str):
            try:
                td["table_properties"] = json.loads(tp_val)
            except (json.JSONDecodeError, TypeError):
                td["table_properties"] = None
        elif tp_val is None:
            td["table_properties"] = None

        parsed_tables.append(td)

    # Batch-fetch live statuses from the engine for all tables with pipeline_ids.
    # Also apply the live error_message so a failed status shows WHY it failed
    # (the stale plan_tables.error_message may be empty/outdated).
    tables_with_pipelines = [t for t in parsed_tables if t.get("pipeline_id")]
    if tables_with_pipelines:
        live = _get_live_status_details_batch(tables_with_pipelines)
        for t in parsed_tables:
            pid = t.get("pipeline_id")
            if pid and pid in live:
                t["status"] = live[pid]["status"]
                if live[pid]["status"] == "failed":
                    t["error_message"] = live[pid]["error_message"]

    # Convert to PlanTable objects
    plan_tables = [PlanTable(**t) for t in parsed_tables]
    return MigrationPlan(id=p["id"], name=p["name"], description=p["description"],
        tables=plan_tables, status=p["status"],
        target=p.get("target", "s3_tables"),
        migration_types=migration_types,
        execution_engine=p.get("execution_engine", "athena"),
        swap_tables=bool(p.get("swap_tables", 0)),
        max_table_size_gb=p["max_table_size_gb"],
        table_bucket_arn=p.get("table_bucket_arn", ""),
        owners=json.loads(p["owners"] or "[]"),
        created_at=p["created_at"], created_by=p["created_by"])

def list_plans() -> list[MigrationPlan]:
    conn = get_conn()
    plans = _fetchall(conn, "SELECT * FROM migration_plans WHERE archived=0 ORDER BY created_at DESC")
    result = [_load_plan(conn, p) for p in plans]
    conn.close()
    return result

def get_plan(plan_id: str) -> Optional[MigrationPlan]:
    conn = get_conn()
    p = _fetchone(conn, "SELECT * FROM migration_plans WHERE id=?", (plan_id,))
    if not p: conn.close(); return None
    plan = _load_plan(conn, p); conn.close(); return plan

def create_plan(request: CreatePlanRequest, username: str = "admin") -> MigrationPlan:
    # Validate: add_files requires EMR Serverless, not Athena
    if "add_files" in request.migration_types and request.execution_engine == "athena":
        raise ValueError(
            "Add Files strategy requires EMR Serverless as the execution engine. "
            "Athena is not supported for add_files.")
    plan_id = str(uuid.uuid4()); now = datetime.now().isoformat()
    conn = get_conn()
    _run_sql(conn, "INSERT INTO migration_plans (id,name,description,status,target,migration_type,execution_engine,swap_tables,max_table_size_gb,table_bucket_arn,owners,created_at,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (plan_id, request.name, request.description, "draft", request.target, json.dumps(request.migration_types), request.execution_engine, 1 if request.swap_tables else 0, request.max_table_size_gb, request.table_bucket_arn, json.dumps(request.owners), now, username))
    _commit(conn); conn.close(); return get_plan(plan_id)

def update_plan(plan_id: str, request: CreatePlanRequest) -> Optional[MigrationPlan]:
    # Validate: add_files requires EMR Serverless, not Athena
    if "add_files" in request.migration_types and request.execution_engine == "athena":
        raise ValueError(
            "Add Files strategy requires EMR Serverless as the execution engine. "
            "Athena is not supported for add_files.")
    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("draft", "ready"): conn.close(); return None
    _run_sql(conn, "UPDATE migration_plans SET name=?, description=?, migration_type=?, max_table_size_gb=?, owners=? WHERE id=?",
        (request.name, request.description, json.dumps(request.migration_types), request.max_table_size_gb, json.dumps(request.owners), plan_id))
    _commit(conn); conn.close(); return get_plan(plan_id)

def add_table_to_plan(plan_id: str, table: dict) -> Optional[MigrationPlan]:
    conn = get_conn()
    p = _fetchone(conn, "SELECT id, target, migration_type FROM migration_plans WHERE id=?", (plan_id,))
    if not p: conn.close(); return None
    mt = table.get("migration_type", "full_table")

    # Pull source metadata from the inventory if available
    inv = _fetchone(conn,
        "SELECT format, location, aws_account, partitioned FROM tables_inventory "
        "WHERE database_name=? AND table_name=?",
        (table["source_database"], table["source_table"]))

    # Validate against support matrix
    if inv:
        fmt = inv["format"]
        plan_target = p.get("target", "s3_tables")
        plan_types = json.loads(p.get("migration_type", '["rewrite"]'))
        if isinstance(plan_types, str):
            plan_types = [plan_types]

        from app.services.validation_service import get_eligible_types
        allowed_for_format = get_eligible_types(fmt)

        # Check format × migration type compatibility
        compatible = [t for t in plan_types if t in allowed_for_format]
        if not compatible:
            conn.close()
            raise ValueError(
                f"Format '{fmt}' is not compatible with plan migration types "
                f"({', '.join(plan_types)}). "
                f"Format supports: {', '.join(allowed_for_format) if allowed_for_format else 'none'}.")

        # Check iceberg source → iceberg-on-s3 target (same format, no migration needed)
        if fmt == "iceberg" and plan_target == "s3_iceberg":
            conn.close()
            raise ValueError(
                "Table is already in Iceberg format on S3. "
                "Migration to 'Iceberg on S3' target is not needed. "
                "Use 'S3 Tables' target to migrate to managed S3 Tables.")

        # Check morph + S3 Tables restriction
        if plan_target == "s3_tables" and mt == "morph":
            conn.close()
            raise ValueError(
                "Morph strategy is not supported with S3 Tables target. "
                "S3 Tables requires data to be written into the table bucket.")

        # Check morph format restriction
        if mt == "morph" and fmt not in ("parquet", "hudi", "delta"):
            conn.close()
            raise ValueError(
                f"Morph strategy only supports columnar formats (parquet, hudi, delta). "
                f"Got '{fmt}'.")

        # Check add_files format restriction (parquet only)
        if mt == "add_files" and fmt != "parquet":
            conn.close()
            raise ValueError(
                "Add Files strategy only supports partitioned Parquet tables. "
                f"Got '{fmt}'.")

        # Check add_files + unpartitioned restriction
        if mt == "add_files" and not inv["partitioned"]:
            conn.close()
            raise ValueError(
                "Add Files strategy requires a partitioned source table. "
                "Only partitioned Parquet tables are supported.")

        # Check add_files + S3 Tables restriction
        if mt == "add_files" and plan_target == "s3_tables":
            conn.close()
            raise ValueError(
                "Add Files strategy is not supported with S3 Tables target. "
                "S3 Tables requires data to be written into the table bucket. "
                "Use 'Iceberg on S3' target instead.")

        # Check incremental + unpartitioned restriction
        if mt == "incremental" and not inv["partitioned"]:
            conn.close()
            raise ValueError(
                "Incremental strategy requires a partitioned source table. "
                "Use rewrite for unpartitioned tables.")

        # Auto-assign compatible type if 'full_table' placeholder
        if mt == "full_table":
            mt = compatible[0]

    _run_sql(conn,
        "INSERT INTO plan_tables (plan_id,source_database,source_table,"
        "target_database,target_table,migration_type,"
        "source_format,source_location,source_account,source_partitioned) "
        "SELECT ?,?,?,?,?,?,?,?,?,? "
        "WHERE NOT EXISTS ("
        "  SELECT 1 FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?)",
        (plan_id, table["source_database"], table["source_table"],
         table["target_database"], table["target_table"], mt,
         inv["format"] if inv else None,
         inv["location"] if inv else None,
         inv["aws_account"] if inv else None,
         inv["partitioned"] if inv else 0,
         plan_id, table["source_database"], table["source_table"]))
    _commit(conn); conn.close(); return get_plan(plan_id)


def _validate_and_resolve_type(conn, plan_target: str, plan_types: list, table: dict):
    """Validate one table against the plan's support matrix and resolve its
    effective migration type. Returns the resolved migration_type (str) and the
    inventory row (or None). Raises ValueError with a user-facing reason if the
    table is not eligible. Pure DB reads on the passed connection — no writes,
    no commit — so it is safe to call inside a batch transaction.

    This mirrors the checks in add_table_to_plan so the batch path is behavior-
    identical to N single adds.
    """
    mt = table.get("migration_type", "full_table")
    inv = _fetchone(conn,
        "SELECT format, location, aws_account, partitioned FROM tables_inventory "
        "WHERE database_name=? AND table_name=?",
        (table["source_database"], table["source_table"]))
    if not inv:
        return mt, None

    fmt = inv["format"]
    from app.services.validation_service import get_eligible_types
    allowed_for_format = get_eligible_types(fmt)
    compatible = [t for t in plan_types if t in allowed_for_format]
    if not compatible:
        raise ValueError(
            f"Format '{fmt}' is not compatible with plan migration types "
            f"({', '.join(plan_types)}). "
            f"Format supports: {', '.join(allowed_for_format) if allowed_for_format else 'none'}.")
    if fmt == "iceberg" and plan_target == "s3_iceberg":
        raise ValueError(
            "Table is already in Iceberg format on S3. "
            "Migration to 'Iceberg on S3' target is not needed. "
            "Use 'S3 Tables' target to migrate to managed S3 Tables.")
    if plan_target == "s3_tables" and mt == "morph":
        raise ValueError(
            "Morph strategy is not supported with S3 Tables target. "
            "S3 Tables requires data to be written into the table bucket.")
    if mt == "morph" and fmt not in ("parquet", "hudi", "delta"):
        raise ValueError(
            f"Morph strategy only supports columnar formats (parquet, hudi, delta). "
            f"Got '{fmt}'.")
    if mt == "add_files" and fmt != "parquet":
        raise ValueError(
            "Add Files strategy only supports partitioned Parquet tables. "
            f"Got '{fmt}'.")
    if mt == "add_files" and not inv["partitioned"]:
        raise ValueError(
            "Add Files strategy requires a partitioned source table. "
            "Only partitioned Parquet tables are supported.")
    if mt == "add_files" and plan_target == "s3_tables":
        raise ValueError(
            "Add Files strategy is not supported with S3 Tables target. "
            "S3 Tables requires data to be written into the table bucket. "
            "Use 'Iceberg on S3' target instead.")
    if mt == "incremental" and not inv["partitioned"]:
        raise ValueError(
            "Incremental strategy requires a partitioned source table. "
            "Use rewrite for unpartitioned tables.")
    # Managed Migration (AWS S3 Tables Migration API): Parquet source (already
    # gated by format eligibility) AND an S3 Tables target only.
    if mt == "s3tables_managed":
        if fmt != "parquet":
            raise ValueError(
                "Managed Migration only supports Hive-style Parquet sources. "
                f"Got '{fmt}'.")
        if plan_target != "s3_tables":
            raise ValueError(
                "Managed Migration requires an 'S3 Tables' target. "
                f"This plan targets '{plan_target}'.")
    if mt == "full_table":
        mt = compatible[0]
    return mt, inv


def add_tables_to_plan_batch(plan_id: str, tables: list[dict]) -> dict:
    """Add many tables to a plan in ONE transaction and return a minimal summary.

    For each table: validate against the plan's support matrix, resolve the
    migration type, and INSERT (idempotent — skips if already present). Per-table
    failures are collected and skipped; they do not abort the others. Returns
    {"added": int, "skipped": [{"table": "db.tbl", "reason": str}]} — deliberately
    NOT the hydrated plan, so the response cost is O(1) instead of reloading and
    live-status-checking every table on every call.
    """
    conn = get_conn()
    p = _fetchone(conn,
        "SELECT id, target, migration_type FROM migration_plans WHERE id=?",
        (plan_id,))
    if not p:
        conn.close()
        return None

    plan_target = p.get("target", "s3_tables")
    plan_types = json.loads(p.get("migration_type", '["rewrite"]'))
    if isinstance(plan_types, str):
        plan_types = [plan_types]

    added = 0
    skipped: list[dict] = []
    for table in tables:
        label = f"{table.get('source_database')}.{table.get('source_table')}"
        try:
            mt, inv = _validate_and_resolve_type(conn, plan_target, plan_types, table)
            cur = _run_sql(conn,
                "INSERT INTO plan_tables (plan_id,source_database,source_table,"
                "target_database,target_table,migration_type,"
                "source_format,source_location,source_account,source_partitioned) "
                "SELECT ?,?,?,?,?,?,?,?,?,? "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?)",
                (plan_id, table["source_database"], table["source_table"],
                 table["target_database"], table["target_table"], mt,
                 inv["format"] if inv else None,
                 inv["location"] if inv else None,
                 inv["aws_account"] if inv else None,
                 inv["partitioned"] if inv else 0,
                 plan_id, table["source_database"], table["source_table"]))
            if cur.rowcount and cur.rowcount > 0:
                added += 1
            else:
                skipped.append({"table": label, "reason": "already in plan"})
        except ValueError as e:
            skipped.append({"table": label, "reason": str(e)})
        except Exception as e:  # pragma: no cover - defensive per-row guard
            log.warning("Batch add failed for %s: %s", label, e)
            skipped.append({"table": label, "reason": str(e)[:200]})

    # Single commit for the whole batch.
    _commit(conn)
    conn.close()
    return {"added": added, "skipped": skipped}


def remove_table_from_plan(plan_id: str, source_db: str, source_table: str) -> Optional[MigrationPlan]:
    conn = get_conn()
    p = _fetchone(conn, "SELECT id FROM migration_plans WHERE id=?", (plan_id,))
    if not p: conn.close(); return None
    # Get pipeline_id before deleting
    t = _fetchone(conn, "SELECT pipeline_id FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?",
                  (plan_id, source_db, source_table))
    # Delete the pipeline in the engine. The plan_tables row is only removed once
    # the engine confirms the pipeline is gone (or completed, so nothing runs).
    if t and t.get("pipeline_id"):
        engine_url = _get_engine_url()
        _engine_action(engine_url, t["pipeline_id"], "cancel")
        status, err = _engine_action(engine_url, t["pipeline_id"], "delete")
        if not _DELETE_OK(status):
            conn.close()
            raise EngineActionError("Remove not confirmed: " + _failure_text(
                f"{source_db}.{source_table}", status, err))
    _run_sql(conn, "DELETE FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?", (plan_id, source_db, source_table))

    # Re-derive the plan status from the REMAINING tables. Removing a table can
    # change the aggregate — most importantly, deleting the one failed table
    # from a plan whose status got stuck at 'failed' should return the plan to
    # 'in_progress' so the remaining finalize-eligible tables (rewrite=staged,
    # incremental/morph/add_files=in_sync) can be finalized. The old logic only
    # handled the all-completed→completed case and left a stuck 'failed' plan
    # untouched (and sync_all_active_plans skips 'failed' plans, so it never
    # self-healed either). Delegate to the single source of truth used by the
    # periodic sync so the rules stay consistent.
    remaining = _fetchall(conn, "SELECT status FROM plan_tables WHERE plan_id=?", (plan_id,))
    if remaining:
        from app.services.pipeline_sync import _update_plan_status
        _update_plan_status(conn, plan_id)

    _commit(conn); conn.close(); return get_plan(plan_id)

def apply_recommendations(plan_id: str) -> dict:
    """Apply agent-recommended migration types to all tables in a plan that have recommendations."""
    conn = get_conn()
    cur = _run_sql(conn,
        "UPDATE plan_tables SET migration_type = recommended_type "
        "WHERE plan_id = ? AND recommended_type IS NOT NULL AND recommended_type != migration_type",
        (plan_id,))
    _commit(conn)
    updated = cur.rowcount
    conn.close()
    return {"plan_id": plan_id, "tables_updated": updated}


def update_table_migration_type(plan_id: str, source_db: str, source_table: str, migration_type: str) -> Optional[MigrationPlan]:
    conn = get_conn()
    cur = _run_sql(conn, "UPDATE plan_tables SET migration_type=? WHERE plan_id=? AND source_database=? AND source_table=?", (migration_type, plan_id, source_db, source_table))
    _commit(conn)
    if cur.rowcount == 0: conn.close(); return None
    conn.close(); return get_plan(plan_id)


def update_table_spec(plan_id: str, source_db: str, source_table: str, spec_json: dict) -> Optional[MigrationPlan]:
    """Update a table's spec fields from a full spec JSON dict. Source fields are ignored."""
    conn = get_conn()
    p = _fetchone(conn, "SELECT id FROM migration_plans WHERE id=?", (plan_id,))
    if not p:
        conn.close()
        return None

    sets = []
    params = []

    tgt = spec_json.get("target", {})
    strat = spec_json.get("strategy", {})
    ip = spec_json.get("iceberg_properties", {})

    # Target fields
    if tgt.get("type"):
        sets.append("target_type=?"); params.append(tgt["type"])
    if tgt.get("database"):
        sets.append("target_database=?"); params.append(tgt["database"])
    if tgt.get("table_name"):
        sets.append("target_table=?"); params.append(tgt["table_name"])
    if tgt.get("location"):
        sets.append("target_location=?"); params.append(tgt["location"])
    if tgt.get("table_bucket_arn"):
        sets.append("target_bucket_arn=?"); params.append(tgt["table_bucket_arn"])

    # Strategy
    if strat.get("type"):
        sets.append("migration_type=?"); params.append(strat["type"])
    if strat.get("batch_size") is not None:
        sets.append("batch_size=?"); params.append(int(strat["batch_size"]))
    if strat.get("sync_interval") is not None:
        sets.append("sync_interval=?"); params.append(int(strat["sync_interval"]))

    # Iceberg properties
    if ip.get("partition_spec") is not None:
        sets.append("partition_spec=?"); params.append(json.dumps(_normalize_partition_spec(ip["partition_spec"])))
    if ip.get("sort_order") is not None:
        sets.append("sort_order=?"); params.append(json.dumps(ip["sort_order"]))
    if ip.get("table_properties") is not None:
        sets.append("table_properties=?"); params.append(json.dumps(ip["table_properties"]))

    if not sets:
        conn.close()
        return get_plan(plan_id)

    params.extend([plan_id, source_db, source_table])
    # Safety: sets list contains only hardcoded 'column=?' patterns (never user input)
    assert all(s.endswith("=?") for s in sets), f"Unsafe SET clause: {sets}"
    cur = _run_sql(conn, f"UPDATE plan_tables SET {', '.join(sets)} WHERE plan_id=? AND source_database=? AND source_table=?", tuple(params)) # nosec B608
    _commit(conn)
    if cur.rowcount == 0:
        conn.close()
        return None
    conn.close()
    return get_plan(plan_id)

def finalize_plan(plan_id: str) -> Optional[MigrationPlan]:
    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("draft", "ready"): conn.close(); return None
    _run_sql(conn, "UPDATE migration_plans SET status='staged' WHERE id=?", (plan_id,))
    _run_sql(conn, "UPDATE plan_tables SET status='staged' WHERE plan_id=?", (plan_id,))
    _commit(conn); conn.close(); return get_plan(plan_id)


def _build_spec(t: dict, plan: dict) -> dict:
    """Build a migration spec dict from a plan_tables row and plan row."""
    spec = {
        "version": "1.0",
        "source": {
            "database": t["source_database"],
            "table_name": t["source_table"],
            "format": t.get("source_format") or "parquet",
            "location": t.get("source_location") or f"s3://unknown/{t['source_database']}/{t['source_table']}/",
            "aws_account_id": t.get("source_account") or "",
            "partitioned": bool(t.get("source_partitioned", 0)),
        },
        "target": {
            "type": t.get("target_type") or plan.get("target", "s3_tables"),
            "database": t["target_database"],
            "table_name": t["target_table"],
        },
        "strategy": {
            "type": t.get("migration_type") or "rewrite",
            "execution_engine": plan.get("execution_engine", "athena"),
            "swap_tables": bool(plan.get("swap_tables", 0)),
            "batch_size": t.get("batch_size") or 5,
            "sync_interval": t.get("sync_interval") or 3600,
        },
        "iceberg_properties": {},
    }
    if spec["strategy"]["type"] == "s3tables_managed":
        from app.services.settings_service import get_setting
        spec["strategy"]["unmatched_source_fields_policy"] = (
            get_setting("APP_S3TABLES_UNMATCHED_FIELDS_POLICY") or "fail")
        spec["strategy"]["incompatible_values_policy"] = (
            get_setting("APP_S3TABLES_INCOMPATIBLE_VALUES_POLICY") or "fail")
    pk = t.get("source_partition_keys")
    if pk:
        try:
            parsed_pk = json.loads(pk) if isinstance(pk, str) else pk
            if parsed_pk:
                # Only pass name and type — the engine's PartitionKey only accepts these fields
                spec["source"]["partition_keys"] = [
                    {"name": p["name"], "type": p.get("type", "string")}
                    for p in parsed_pk
                ]
                # Ensure partitioned is true if we have partition keys
                spec["source"]["partitioned"] = True
        except (json.JSONDecodeError, TypeError):
            pass
    if t.get("source_compression"):
        spec["source"]["compression"] = t["source_compression"]
    target_location = t.get("target_location") or ""
    if not target_location and spec["target"]["type"] == "s3_iceberg":
        src_loc = spec["source"]["location"].rstrip("/")
        base = src_loc.rsplit("/", 1)[0] if "/" in src_loc else src_loc
        target_location = f"{base}/{t['target_table']}/"
    if target_location:
        spec["target"]["location"] = target_location
    if t.get("target_bucket_arn"):
        spec["target"]["table_bucket_arn"] = t["target_bucket_arn"]
    elif plan.get("table_bucket_arn"):
        spec["target"]["table_bucket_arn"] = plan["table_bucket_arn"]
    ps = t.get("partition_spec")
    if ps:
        try:
            parsed = json.loads(ps) if isinstance(ps, str) else ps
            if parsed:
                # Normalize to ensure canonical format (handles agent format)
                spec["iceberg_properties"]["partition_spec"] = _normalize_partition_spec(parsed)
        except (json.JSONDecodeError, TypeError):
            pass
    so = t.get("sort_order")
    if so:
        try:
            parsed = json.loads(so) if isinstance(so, str) else so
            if parsed:
                spec["iceberg_properties"]["sort_order"] = parsed
        except (json.JSONDecodeError, TypeError):
            pass
    tp = t.get("table_properties")
    if tp:
        try:
            parsed = json.loads(tp) if isinstance(tp, str) else tp
            if parsed:
                spec["iceberg_properties"]["table_properties"] = parsed
        except (json.JSONDecodeError, TypeError):
            pass
    # Always include default table_properties if none were set
    if "table_properties" not in spec["iceberg_properties"]:
        spec["iceberg_properties"]["table_properties"] = {
            "format-version": "2",
            "write.parquet.compression-codec": "zstd",
        }
    # Inject the user-configured Athena settings so the engine's Athena queries
    # (CTAS / INSERT / etc.) use them instead of the engine's deploy-time env
    # defaults. Without this a Settings-only configuration (e.g. a different
    # region/workgroup/output location than the engine was deployed with) never
    # reaches the strategy's AthenaClient, causing "No output location provided".
    # Only include non-empty values so we never override an engine default with
    # a blank. Mirrors the fix already applied to validation.
    from app.services.settings_service import get_setting
    athena_cfg = {
        k: v for k, v in {
            "region": get_setting("ENGINE_ATHENA_REGION") or "",
            "workgroup": get_setting("ENGINE_ATHENA_WORKGROUP") or "",
            "output_location": get_setting("ENGINE_ATHENA_OUTPUT_LOCATION") or "",
        }.items() if v
    }
    if athena_cfg:
        spec["strategy"]["athena"] = athena_cfg
    # Inject the user-configured EMR Serverless settings so the engine submits
    # jobs to the right REGIONAL application. EMR Serverless applications are
    # region-scoped and identified by an application_id unique to one region;
    # without this the engine falls back to its deploy-time env/config default
    # (region us-east-1, empty app-id) and StartJobRun fails with
    # ResourceNotFoundException ("Application does not exist"). Mirrors the
    # athena block above. Only non-empty values are included so we never
    # override an engine default with a blank.
    emr_cfg = {
        k: v for k, v in {
            "application_id": get_setting("ENGINE_EMR_APPLICATION_ID") or "",
            "execution_role_arn": get_setting("ENGINE_EMR_EXECUTION_ROLE_ARN") or "",
            "region": get_setting("ENGINE_EMR_REGION") or "",
        }.items() if v
    }
    if emr_cfg:
        spec["strategy"]["emr"] = emr_cfg
    # Pin the source region explicitly. Glue Data Catalogs are REGIONAL, and the
    # engine's Glue GetTable (schema fetch during create-table) must target the
    # region the source catalog actually lives in — otherwise it fails with
    # EntityNotFoundException when the engine's default region differs. The
    # authoritative source region is where inventory was discovered, i.e. the
    # source-refresh region (APP_SOURCE_REFRESH_REGION), NOT the Athena
    # region. Without this the engine falls back to a default (us-east-1).
    from app.services.source_service import _refresh_region
    source_region = _refresh_region()
    if source_region:
        spec["source"]["region"] = source_region
    # Target region: an S3 Tables bucket ARN encodes its own region
    # (arn:aws:s3tables:<region>:...), which is authoritative for the target.
    # Fall back to the source region otherwise.
    target_region = ""
    tba = spec["target"].get("table_bucket_arn", "")
    if tba.startswith("arn:aws:s3tables:"):
        parts = tba.split(":")
        if len(parts) > 3 and parts[3]:
            target_region = parts[3]
    if not target_region:
        target_region = source_region
    if target_region:
        spec["target"]["region"] = target_region
    return spec


def _validate_spec(spec: dict, table_row: dict) -> str | None:
    """Validate a spec before submission to the engine.

    Returns an error message string if invalid, or None if OK.
    """
    src = spec.get("source", {})
    tgt = spec.get("target", {})
    strat = spec.get("strategy", {})

    # Source must have a real location (not the placeholder)
    location = src.get("location", "")
    if not location or "s3://unknown/" in location:
        return "Source location is missing. Run AI analysis or manually set the S3 path."

    # Source must have a format
    fmt = src.get("format", "")
    if not fmt:
        return "Source format is missing."

    # Target must have a database and table name
    if not tgt.get("database") or not tgt.get("table_name"):
        return "Target database/table is missing."

    # Target type must be set
    if not tgt.get("type"):
        return "Target type (s3_tables or s3_iceberg) is missing."

    # S3 Tables requires a bucket ARN
    if tgt.get("type") == "s3_tables" and not tgt.get("table_bucket_arn"):
        return "S3 Tables target requires a table_bucket_arn."

    # S3 Iceberg requires a target location
    if tgt.get("type") == "s3_iceberg" and not tgt.get("location"):
        return "Iceberg on S3 target requires a location."

    # Strategy type must be valid
    valid_types = ("rewrite", "incremental", "morph", "add_files", "s3tables_managed")
    if strat.get("type") not in valid_types:
        return f"Invalid strategy type '{strat.get('type')}'. Must be one of: {', '.join(valid_types)}."

    # Incremental requires partitioned source
    if strat.get("type") == "incremental":
        if not src.get("partitioned") and not src.get("partition_keys"):
            return "Incremental strategy requires a partitioned source table."

    # Morph only supports columnar formats
    if strat.get("type") == "morph":
        if fmt not in ("parquet", "hudi", "delta"):
            return f"Morph only supports parquet/hudi/delta. Got '{fmt}'."

    # Add Files requires partitioned parquet + EMR Serverless
    if strat.get("type") == "add_files":
        if fmt != "parquet":
            return f"Add Files only supports parquet. Got '{fmt}'."
        if not src.get("partitioned") and not src.get("partition_keys"):
            return "Add Files strategy requires a partitioned source table."
        if strat.get("execution_engine") == "athena":
            return "Add Files strategy requires EMR Serverless (Athena not supported)."
        if tgt.get("type") == "s3_tables":
            return "Morph is not supported with S3 Tables target (requires data in S3 Tables bucket)."

    # Managed Migration (S3 Tables Migration API): parquet source into S3 Tables
    if strat.get("type") == "s3tables_managed":
        if fmt != "parquet":
            return f"Managed Migration only supports parquet. Got '{fmt}'."
        if tgt.get("type") != "s3_tables":
            return "Managed Migration requires an S3 Tables target."

    return None


def execute_plan(plan_id: str) -> Optional[MigrationPlan]:
    """Execute a staged plan: submit specs to the engine, capture pipeline IDs."""

    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status, target, table_bucket_arn, execution_engine FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("staged",):
        conn.close()
        return None

    engine_url = _get_engine_url()
    tables = _fetchall(conn,
        "SELECT * FROM plan_tables WHERE plan_id=?", (plan_id,))

    errors = []
    for t in tables:
        spec = _build_spec(t, p)

        # Pre-submission validation: catch fatal gaps before sending to the engine
        validation_err = _validate_spec(spec, t)
        if validation_err:
            err = f"{t['source_database']}.{t['source_table']}: {validation_err}"
            errors.append(err)
            _run_sql(conn,
                "UPDATE plan_tables SET status='failed', error_message=? "
                "WHERE plan_id=? AND source_database=? AND source_table=?",
                (validation_err, plan_id, t["source_database"], t["source_table"]))
            continue

        # Submit to the engine
        try:
            resp = engine_client.post(
                f"{engine_url}/api/pipeline/submit",
                json=spec,
                timeout=30.0,
            )
            if resp.status_code in (200, 201):
                pipeline = resp.json()
                pid = pipeline.get("id", "")
                migration_type = t.get("migration_type") or "rewrite"

                if migration_type in ("incremental", "morph", "add_files", "s3tables_managed"):
                    # Start these pipelines immediately. For s3tables_managed,
                    # /start runs the strategy's submit() which issues
                    # start_table_migration — without this the AWS migration
                    # never starts and the pipeline sits at 'pending' (which
                    # maps to out_of_sync). initial_load is the correct app
                    # status until the reconcile poll refines it.
                    try:
                        engine_client.post(
                            f"{engine_url}/api/pipeline/{pid}/start",
                            timeout=30.0,
                        )
                        _run_sql(conn,
                            "UPDATE plan_tables SET pipeline_id=?, status='initial_load' "
                            "WHERE plan_id=? AND source_database=? AND source_table=?",
                            (pid, plan_id, t["source_database"], t["source_table"]))
                    except Exception:
                        # Pipeline created but start failed — still mark as initial_load
                        _run_sql(conn,
                            "UPDATE plan_tables SET pipeline_id=?, status='initial_load' "
                            "WHERE plan_id=? AND source_database=? AND source_table=?",
                            (pid, plan_id, t["source_database"], t["source_table"]))
                else:
                    # Rewrite pipelines stay in staged — started on finalize
                    _run_sql(conn,
                        "UPDATE plan_tables SET pipeline_id=?, status='staged' "
                        "WHERE plan_id=? AND source_database=? AND source_table=?",
                        (pid, plan_id, t["source_database"], t["source_table"]))
            else:
                err = resp.text[:200]
                errors.append(f"{t['source_database']}.{t['source_table']}: {err}")
                _run_sql(conn,
                    "UPDATE plan_tables SET status='failed', error_message=? "
                    "WHERE plan_id=? AND source_database=? AND source_table=?",
                    (err, plan_id, t["source_database"], t["source_table"]))
        except Exception as e:
            err_msg = str(e)[:200]
            errors.append(f"{t['source_database']}.{t['source_table']}: {err_msg}")
            _run_sql(conn,
                "UPDATE plan_tables SET status='failed', error_message=? "
                "WHERE plan_id=? AND source_database=? AND source_table=?",
                (err_msg, plan_id, t["source_database"], t["source_table"]))

    # Update plan status
    if errors:
        log.warning(f"[execute_plan] {len(errors)} table(s) failed:")
        for e in errors:
            log.warning(f"  {e}")
    new_status = "in_progress"
    _run_sql(conn, "UPDATE migration_plans SET status=? WHERE id=?", (new_status, plan_id))
    _commit(conn)
    conn.close()
    return get_plan(plan_id)


def finalize_migration(plan_id: str) -> Optional[MigrationPlan]:
    """Finalize an in-progress migration (cutover).

    For each table type:
    - Rewrite: starts the pipeline (executes the CTAS)
    - Incremental/Morph: must be in_sync before finalizing

    All types: after completion, the original table will be dropped and the
    new Iceberg table renamed to the original name. (Validation logic TBD.)
    """

    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("in_progress",):
        conn.close()
        return None

    engine_url = _get_engine_url()

    # Get all tables eligible for finalization:
    # - rewrite tables in 'staged' status (need to start)
    # - incremental/morph/add_files tables in 'in_sync' status (ready for cutover)
    # - s3tables_managed tables once the initial load or a sync has completed
    #   (initial_load_complete / sync_complete) — managed has no in_sync state.
    #   sync_failed is ALSO finalizable: the table is already migrated; only the
    #   last (optional) sync failed, which does not invalidate the migrated data.
    eligible_tables = _fetchall(conn,
        "SELECT * FROM plan_tables WHERE plan_id=? AND ("
        "(migration_type='rewrite' AND status='staged') OR "
        "(migration_type IN ('incremental','morph','add_files') AND status='in_sync') OR "
        "(migration_type='s3tables_managed' AND status IN ('initial_load_complete','sync_complete','sync_failed')))",
        (plan_id,))

    if not eligible_tables:
        conn.close()
        return get_plan(plan_id)

    errors = []
    for t in eligible_tables:
        migration_type = t.get("migration_type", "rewrite")
        pid = t.get("pipeline_id")

        if migration_type == "rewrite":
            # Start the rewrite pipeline
            if not pid:
                errors.append(f"{t['source_database']}.{t['source_table']}: no pipeline_id")
                continue
            try:
                resp = engine_client.post(
                    f"{engine_url}/api/pipeline/{pid}/start",
                    timeout=30.0,
                )
                if resp.status_code in (200, 201):
                    started = resp.json()
                    if started.get("status") == "failed":
                        err_msg = started.get("error_message", "Pipeline failed on start")
                        _run_sql(conn,
                            "UPDATE plan_tables SET status='failed', error_message=? "
                            "WHERE plan_id=? AND source_database=? AND source_table=?",
                            (err_msg, plan_id, t["source_database"], t["source_table"]))
                        errors.append(f"{t['source_database']}.{t['source_table']}: {err_msg}")
                    else:
                        _run_sql(conn,
                            "UPDATE plan_tables SET status='executing' "
                            "WHERE plan_id=? AND source_database=? AND source_table=?",
                            (plan_id, t["source_database"], t["source_table"]))
                else:
                    err = resp.text[:200]
                    errors.append(f"{t['source_database']}.{t['source_table']}: {err}")
                    _run_sql(conn,
                        "UPDATE plan_tables SET status='failed', error_message=? "
                        "WHERE plan_id=? AND source_database=? AND source_table=?",
                        (err, plan_id, t["source_database"], t["source_table"]))
            except Exception as e:
                err_msg = str(e)[:200]
                errors.append(f"{t['source_database']}.{t['source_table']}: {err_msg}")
                _run_sql(conn,
                    "UPDATE plan_tables SET status='failed', error_message=? "
                    "WHERE plan_id=? AND source_database=? AND source_table=?",
                    (err_msg, plan_id, t["source_database"], t["source_table"]))
        else:
            # Incremental/Morph/Add_files — already in_sync, perform cutover
            pid = t.get("pipeline_id")
            swap_enabled = bool(p.get("swap_tables", 0))

            if not pid:
                errors.append(f"{t['source_database']}.{t['source_table']}: no pipeline_id")
                _run_sql(conn,
                    "UPDATE plan_tables SET status='failed', error_message=? "
                    "WHERE plan_id=? AND source_database=? AND source_table=?",
                    ("No pipeline_id", plan_id, t["source_database"], t["source_table"]))
                continue

            # Managed Migration owns the target table inside the S3 Tables
            # bucket — there is no Glue table to drop/rename, so the swap step
            # is skipped. Its cutover happens entirely via /complete below,
            # which the engine routes to complete_table_migration.
            if swap_enabled and migration_type != "s3tables_managed":
                # Swap tables: drop original, rename Iceberg table to original name.
                # Cutover is a prerequisite for completion — if it fails, do NOT
                # mark the pipeline completed (that would misrepresent state).
                try:
                    swap_resp = engine_client.post(
                        f"{engine_url}/api/pipeline/{pid}/swap-tables",
                        timeout=30.0)
                    if swap_resp.status_code not in (200, 201):
                        err = swap_resp.text[:200]
                        errors.append(f"{t['source_database']}.{t['source_table']}: table swap failed: {err}")
                        _run_sql(conn,
                            "UPDATE plan_tables SET status='failed', error_message=? "
                            "WHERE plan_id=? AND source_database=? AND source_table=?",
                            (f"Table swap failed: {err}", plan_id, t["source_database"], t["source_table"]))
                        continue
                except Exception as e:
                    err_msg = f"Table swap error: {str(e)[:200]}"
                    errors.append(f"{t['source_database']}.{t['source_table']}: {err_msg}")
                    _run_sql(conn,
                        "UPDATE plan_tables SET status='failed', error_message=? "
                        "WHERE plan_id=? AND source_database=? AND source_table=?",
                        (err_msg, plan_id, t["source_database"], t["source_table"]))
                    continue
            else:
                log.warning(f"[finalize] Table swap disabled — completing "
                      f"{t['source_database']}.{t['source_table']} without swap")

            # Authoritatively mark the in-sync pipeline as completed in the engine.
            # This is the source of truth for the pipeline status; if it fails,
            # the table is marked failed rather than claiming false completion.
            try:
                resp = engine_client.post(f"{engine_url}/api/pipeline/{pid}/complete", timeout=15.0)
                if resp.status_code not in (200, 201):
                    err = f"Failed to complete pipeline: {resp.text[:200]}"
                    errors.append(f"{t['source_database']}.{t['source_table']}: {err}")
                    _run_sql(conn,
                        "UPDATE plan_tables SET status='failed', error_message=? "
                        "WHERE plan_id=? AND source_database=? AND source_table=?",
                        (err, plan_id, t["source_database"], t["source_table"]))
                    continue
            except Exception as e:
                err = f"Failed to complete pipeline: {str(e)[:200]}"
                errors.append(f"{t['source_database']}.{t['source_table']}: {err}")
                _run_sql(conn,
                    "UPDATE plan_tables SET status='failed', error_message=? "
                    "WHERE plan_id=? AND source_database=? AND source_table=?",
                    (err, plan_id, t["source_database"], t["source_table"]))
                continue

            # Only record completion the engine actually confirms. If the engine
            # reports another status, leave the table as it was so finalize can
            # be run again; the plan stays in progress until every table is done.
            try:
                engine_status = (resp.json() or {}).get("status")
            except ValueError:
                engine_status = None
            if engine_status != "completed":
                errors.append(
                    f"{t['source_database']}.{t['source_table']}: engine reports "
                    f"status '{engine_status}' after complete; not marked completed")
                continue
            _run_sql(conn,
                "UPDATE plan_tables SET status='completed' "
                "WHERE plan_id=? AND source_database=? AND source_table=?",
                (plan_id, t["source_database"], t["source_table"]))

    if errors:
        log.warning(f"[finalize_migration] {len(errors)} table(s) had issues:")
        for e in errors:
            log.warning(f"  {e}")

    # Check if all tables are now completed — if so, mark plan as completed
    all_tables = _fetchall(conn, "SELECT status FROM plan_tables WHERE plan_id=?", (plan_id,))
    all_completed = all(t["status"] == "completed" for t in all_tables)
    if all_completed and all_tables:
        _run_sql(conn, "UPDATE migration_plans SET status='completed' WHERE id=?", (plan_id,))

    _commit(conn)
    conn.close()
    return get_plan(plan_id)


def cancel_plan(plan_id: str) -> Optional[MigrationPlan]:
    """Cancel a plan and cancel all its pipelines in the engine.

    A table is marked cancelled only once the engine confirms its pipeline is no
    longer active. The plan is marked cancelled only when every table is.
    Otherwise EngineActionError lists the tables that could not be cancelled
    and the plan keeps its status so the action can be retried.
    """
    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("draft", "ready", "staged", "in_progress", "paused"):
        conn.close()
        return None
    engine_url = _get_engine_url()
    tables = _fetchall(conn,
        "SELECT source_database, source_table, pipeline_id FROM plan_tables WHERE plan_id=?",
        (plan_id,))
    failures, done = [], 0
    for t in tables:
        label = f"{t['source_database']}.{t['source_table']}"
        pid = t.get("pipeline_id")
        status = None
        if pid:
            status, err = _engine_action(engine_url, pid, "cancel")
            if not _CANCEL_OK(status):
                failures.append(_failure_text(label, status, err))
                continue
        done += 1
        # A table that already finished stays completed; everything else that
        # the engine confirmed (or never had a pipeline) is cancelled.
        new_status = "completed" if status == "completed" else "cancelled"
        _run_sql(conn,
            "UPDATE plan_tables SET status=? "
            "WHERE plan_id=? AND source_database=? AND source_table=?",
            (new_status, plan_id, t["source_database"], t["source_table"]))
    if not failures:
        _run_sql(conn, "UPDATE migration_plans SET status='cancelled' WHERE id=?", (plan_id,))
    _commit(conn)
    conn.close()
    _raise_if_failed("Cancel", done, len(tables), failures)
    return get_plan(plan_id)
def archive_plan(plan_id: str) -> Optional[MigrationPlan]:
    """Archive a completed migration. It stays in the DB but is hidden from the UI."""
    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] != "completed":
        conn.close()
        return None
    _run_sql(conn, "UPDATE migration_plans SET archived=1 WHERE id=?", (plan_id,))
    _commit(conn)
    conn.close()
    return get_plan(plan_id)


def replan(plan_id: str) -> Optional[MigrationPlan]:
    """Move a cancelled or failed migration back to draft so it can be re-planned."""
    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("cancelled", "failed"):
        conn.close()
        return None
    _run_sql(conn, "UPDATE migration_plans SET status='draft' WHERE id=?", (plan_id,))
    _run_sql(conn, "UPDATE plan_tables SET status='draft', pipeline_id=NULL, error_message=NULL WHERE plan_id=?", (plan_id,))
    _commit(conn)
    conn.close()
    return get_plan(plan_id)


def pause_plan(plan_id: str) -> Optional[MigrationPlan]:
    """Pause a plan — pauses all pipelines in the engine.

    The plan is marked paused only when the engine confirms no pipeline is still
    running. Otherwise EngineActionError lists the tables that did not pause.
    """
    conn = get_conn()
    p = _fetchone(conn, "SELECT id, status FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("in_progress",):
        conn.close()
        return None
    engine_url = _get_engine_url()
    tables = _fetchall(conn,
        "SELECT source_database, source_table, pipeline_id FROM plan_tables "
        "WHERE plan_id=? AND pipeline_id IS NOT NULL AND pipeline_id != ''",
        (plan_id,))
    failures, done = [], 0
    for t in tables:
        status, err = _engine_action(engine_url, t["pipeline_id"], "pause")
        if _PAUSE_OK(status):
            done += 1
        else:
            failures.append(_failure_text(
                f"{t['source_database']}.{t['source_table']}", status, err))
    # Only the plan-level status is stored; table statuses come from the engine.
    if not failures:
        _run_sql(conn, "UPDATE migration_plans SET status='paused' WHERE id=?", (plan_id,))
        _commit(conn)
    conn.close()
    _raise_if_failed("Pause", done, len(tables), failures)
    return get_plan(plan_id)
def resume_plan(plan_id: str) -> Optional[MigrationPlan]:
    """Resume a paused plan — resumes all pipelines in the engine.
    If a pipeline doesn't exist in the engine (creation failed previously),
    re-submits the spec to create it.

    The plan returns to in_progress only when every table resumed (or was
    re-submitted). Otherwise it stays paused and EngineActionError lists the
    tables that did not resume.
    """
    conn = get_conn()
    p = _fetchone(conn, "SELECT * FROM migration_plans WHERE id=?", (plan_id,))
    if not p or p["status"] not in ("paused",):
        conn.close()
        return None
    engine_url = _get_engine_url()
    tables = _fetchall(conn,
        "SELECT * FROM plan_tables WHERE plan_id=?", (plan_id,))
    failures, done = [], 0
    for t in tables:
        label = f"{t['source_database']}.{t['source_table']}"
        pid = t.get("pipeline_id") or ""
        migration_type = t.get("migration_type") or "rewrite"
        if pid:
            status, err = _engine_action(engine_url, pid, "resume")
            if status == "deleted":
                pid = ""  # Pipeline gone — fall through to re-submit
            elif _RESUME_OK(status):
                done += 1
                continue
            else:
                failures.append(_failure_text(label, status, err))
                continue
        # Pipeline doesn't exist — re-submit the spec
        spec = _build_spec(t, p)
        validation_err = _validate_spec(spec, t)
        if validation_err:
            failures.append(f"{label}: {validation_err}")
            continue
        try:
            resp = engine_client.post(f"{engine_url}/api/pipeline/submit", json=spec, timeout=30.0)
        except httpx.RequestError as e:
            failures.append(f"{label}: re-submit failed, engine unreachable ({e})")
            continue
        if resp.status_code not in (200, 201):
            failures.append(f"{label}: re-submit failed ({resp.text[:200]})")
            continue
        new_pid = (resp.json() or {}).get("id", "")
        # Store the new pipeline_id (the ONLY thing we write locally)
        _run_sql(conn,
            "UPDATE plan_tables SET pipeline_id=? "
            "WHERE plan_id=? AND source_database=? AND source_table=?",
            (new_pid, plan_id, t["source_database"], t["source_table"]))
        done += 1
        # Start non-rewrite pipelines immediately
        if migration_type in ("incremental", "morph", "add_files"):
            try:
                engine_client.post(f"{engine_url}/api/pipeline/{new_pid}/start", timeout=30.0)
            except httpx.RequestError as e:
                log.debug("Best-effort start failed for %s: %s", new_pid, e)
    if not failures:
        _run_sql(conn, "UPDATE migration_plans SET status='in_progress' WHERE id=?", (plan_id,))
    _commit(conn)
    conn.close()
    _raise_if_failed("Resume", done, len(tables), failures)
    return get_plan(plan_id)
def pause_table(plan_id: str, source_db: str, source_table: str) -> Optional[MigrationPlan]:
    """Pause a table — calls the engine and raises EngineActionError unless the
    engine confirms it."""
    conn = get_conn()
    t = _fetchone(conn, "SELECT pipeline_id FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?",
                  (plan_id, source_db, source_table))
    conn.close()
    if t and t.get("pipeline_id"):
        status, err = _engine_action(_get_engine_url(), t["pipeline_id"], "pause")
        if not _PAUSE_OK(status):
            raise EngineActionError(
                "Pause not confirmed: " + _failure_text(f"{source_db}.{source_table}", status, err))
    return get_plan(plan_id)
def resume_table(plan_id: str, source_db: str, source_table: str) -> Optional[MigrationPlan]:
    """Resume a table — calls the engine and raises EngineActionError unless the
    engine confirms it."""
    conn = get_conn()
    t = _fetchone(conn, "SELECT pipeline_id FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?",
                  (plan_id, source_db, source_table))
    conn.close()
    if t and t.get("pipeline_id"):
        status, err = _engine_action(_get_engine_url(), t["pipeline_id"], "resume")
        if not _RESUME_OK(status):
            raise EngineActionError(
                "Resume not confirmed: " + _failure_text(f"{source_db}.{source_table}", status, err))
    return get_plan(plan_id)
def cancel_table(plan_id: str, source_db: str, source_table: str) -> Optional[MigrationPlan]:
    """Cancel a table — calls the engine and raises EngineActionError unless the
    engine confirms it."""
    conn = get_conn()
    t = _fetchone(conn, "SELECT pipeline_id FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?",
                  (plan_id, source_db, source_table))
    conn.close()
    if t and t.get("pipeline_id"):
        status, err = _engine_action(_get_engine_url(), t["pipeline_id"], "cancel")
        if not _CANCEL_OK(status):
            raise EngineActionError(
                "Cancel not confirmed: " + _failure_text(f"{source_db}.{source_table}", status, err))
    return get_plan(plan_id)
def resume_table_from_failure(plan_id: str, source_db: str, source_table: str) -> Optional[MigrationPlan]:
    """Resume a failed table from its failed batch (preserves completed batches)."""

    conn = get_conn()
    t = _fetchone(conn,
        "SELECT pipeline_id, migration_type, status FROM plan_tables "
        "WHERE plan_id=? AND source_database=? AND source_table=?",
        (plan_id, source_db, source_table))
    if not t or t["status"] != "failed":
        conn.close()
        return None

    pid = t.get("pipeline_id")
    migration_type = t.get("migration_type", "rewrite")
    engine_url = _get_engine_url()

    if not pid:
        conn.close()
        return None

    # Only incremental/morph support resume from batch — rewrite is all-or-nothing
    if migration_type == "rewrite":
        conn.close()
        raise ValueError("Rewrite pipelines cannot be resumed from failure. Use Retry instead.")

    try:
        resp = engine_client.post(
            f"{engine_url}/api/pipeline/{pid}/resume-from-failure",
            timeout=30.0)
        if resp.status_code in (200, 201):
            # Managed Migration starts a FRESH migration on resume-from-failure,
            # so it re-enters initial load (the sync poll refines it shortly).
            # Other non-rewrite strategies resume their sync loop (out_of_sync).
            if migration_type == "rewrite":
                new_status = "executing"
            elif migration_type == "s3tables_managed":
                new_status = "initial_load"
            else:
                new_status = "out_of_sync"
            _run_sql(conn,
                "UPDATE plan_tables SET status=?, error_message=NULL "
                "WHERE plan_id=? AND source_database=? AND source_table=?",
                (new_status, plan_id, source_db, source_table))
            _commit(conn)
            conn.close()
            return get_plan(plan_id)
        else:
            conn.close()
            raise ValueError(f"Resume failed: {resp.text[:200]}")
    except ValueError:
        raise
    except Exception as e:
        conn.close()
        raise ValueError(f"Resume failed: {str(e)[:200]}")


def retry_table(plan_id: str, source_db: str, source_table: str) -> Optional[MigrationPlan]:
    """Retry a failed table by restarting its pipeline in the engine.

    Uses the engine's /retry endpoint which preserves batch history (previous
    failed attempts remain visible) and re-submits the pipeline from scratch.
    If the pipeline no longer exists, falls back to a fresh submission.
    """

    conn = get_conn()
    t = _fetchone(conn,
        "SELECT pipeline_id, migration_type, status FROM plan_tables "
        "WHERE plan_id=? AND source_database=? AND source_table=?",
        (plan_id, source_db, source_table))
    if not t or t["status"] != "failed":
        conn.close()
        return None

    pid = t.get("pipeline_id")
    migration_type = t.get("migration_type", "rewrite")
    engine_url = _get_engine_url()

    if not pid:
        # No pipeline was ever created (failed during spec validation).
        # Fall through to fresh submission below.
        pass
    elif migration_type == "add_files":
        # add_files always does a fresh submission — the /retry endpoint preserves
        # old batches which causes stale-batch failures with DynamoDB eventual consistency.
        # Delete the old pipeline and fall through to fresh submission.
        try:
            engine_client.delete(f"{engine_url}/api/pipeline/{pid}", timeout=10.0)
        except httpx.RequestError as e:
            log.debug("Best-effort delete failed for %s: %s", pid, e)
        pid = None  # Force fresh submission
    else:
        try:
            # Try the retry endpoint (preserves batch history)
            resp = engine_client.post(f"{engine_url}/api/pipeline/{pid}/retry", timeout=30.0)

            if resp.status_code in (200, 201):
                result = resp.json()
                new_status = result.get("status", "")
                if new_status == "failed":
                    error_msg = result.get("error_message", "Pipeline failed on retry")
                    _run_sql(conn,
                        "UPDATE plan_tables SET status='failed', error_message=? "
                        "WHERE plan_id=? AND source_database=? AND source_table=?",
                        (error_msg, plan_id, source_db, source_table))
                    _commit(conn); conn.close()
                    raise ValueError(error_msg)
                new_table_status = "executing" if migration_type == "rewrite" else "initial_load"
                _run_sql(conn,
                    "UPDATE plan_tables SET status=?, error_message=NULL "
                    "WHERE plan_id=? AND source_database=? AND source_table=?",
                    (new_table_status, plan_id, source_db, source_table))
                _commit(conn); conn.close()
                return get_plan(plan_id)

            elif resp.status_code == 404:
                pass  # Fall through to fresh submission below

            else:
                error_msg = resp.text[:200]
                _run_sql(conn,
                    "UPDATE plan_tables SET status='failed', error_message=? "
                    "WHERE plan_id=? AND source_database=? AND source_table=?",
                    (error_msg, plan_id, source_db, source_table))
                _commit(conn); conn.close()
                raise ValueError(f"Retry failed: {error_msg}")

        except ValueError:
            raise
        except Exception as e:
            conn.close()
            raise ValueError(f"Retry failed: {str(e)[:200]}")

    # Fresh submission — pipeline doesn't exist or was not found
    full_t = _fetchone(conn,
        "SELECT * FROM plan_tables WHERE plan_id=? AND source_database=? AND source_table=?",
        (plan_id, source_db, source_table))
    p = _fetchone(conn, "SELECT target, table_bucket_arn, execution_engine FROM migration_plans WHERE id=?", (plan_id,))

    spec = _build_spec(full_t, p)
    validation_err = _validate_spec(spec, full_t)
    if validation_err:
        _run_sql(conn,
            "UPDATE plan_tables SET status='failed', error_message=? "
            "WHERE plan_id=? AND source_database=? AND source_table=?",
            (validation_err, plan_id, source_db, source_table))
        _commit(conn); conn.close()
        raise ValueError(validation_err)

    submit_resp = engine_client.post(
        f"{engine_url}/api/pipeline/submit", json=spec, timeout=30.0)

    if submit_resp.status_code not in (200, 201):
        conn.close()
        raise ValueError(f"Re-submit failed: {submit_resp.text[:200]}")

    new_pipeline = submit_resp.json()
    new_pid = new_pipeline.get("id", "")

    # Start the new pipeline
    start_resp = engine_client.post(
        f"{engine_url}/api/pipeline/{new_pid}/start", timeout=30.0)
    if start_resp.status_code in (200, 201):
        started = start_resp.json()
        if started.get("status") == "failed":
            error_msg = started.get("error_message", "Pipeline failed on start")
            _run_sql(conn,
                "UPDATE plan_tables SET pipeline_id=?, status='failed', error_message=? "
                "WHERE plan_id=? AND source_database=? AND source_table=?",
                (new_pid, error_msg, plan_id, source_db, source_table))
            _commit(conn); conn.close()
            raise ValueError(error_msg)
        new_table_status = "executing" if migration_type == "rewrite" else "initial_load"
    else:
        new_table_status = "initial_load"

    _run_sql(conn,
        "UPDATE plan_tables SET pipeline_id=?, status=?, error_message=NULL "
        "WHERE plan_id=? AND source_database=? AND source_table=?",
        (new_pid, new_table_status, plan_id, source_db, source_table))

    # Start non-rewrite pipelines immediately
    if migration_type in ("incremental", "morph", "add_files"):
        try:
            engine_client.post(f"{engine_url}/api/pipeline/{new_pid}/start", timeout=30.0)
        except httpx.RequestError as e:
            log.debug("Best-effort start failed for %s: %s", new_pid, e)

    _commit(conn); conn.close()
    return get_plan(plan_id)


def delete_plan(plan_id: str) -> bool:
    """Delete a plan and clean up its pipelines in the engine.

    Nothing is deleted unless the engine confirms every pipeline is gone (or
    already completed, which leaves nothing running). Otherwise
    EngineActionError lists the pipelines that are still active, so deleting
    the plan cannot orphan running work.
    """
    conn = get_conn()
    p = _fetchone(conn, "SELECT id FROM migration_plans WHERE id=?", (plan_id,))
    if not p: conn.close(); return False
    engine_url = _get_engine_url()
    tables = _fetchall(conn,
        "SELECT source_database, source_table, pipeline_id FROM plan_tables "
        "WHERE plan_id=? AND pipeline_id IS NOT NULL AND pipeline_id != ''",
        (plan_id,))
    failures, done = [], 0
    for t in tables:
        pid = t["pipeline_id"]
        # Cancel first (delete is only allowed once a pipeline is inactive).
        _engine_action(engine_url, pid, "cancel")
        status, err = _engine_action(engine_url, pid, "delete")
        if _DELETE_OK(status):
            done += 1
        else:
            failures.append(_failure_text(
                f"{t['source_database']}.{t['source_table']}", status, err))
    if failures:
        conn.close()
        _raise_if_failed("Delete", done, len(tables), failures)
    _run_sql(conn, "DELETE FROM plan_tables WHERE plan_id=?", (plan_id,))
    _run_sql(conn, "DELETE FROM migration_plans WHERE id=?", (plan_id,))
    _commit(conn); conn.close(); return True
def get_tables_in_plans() -> dict[str, str]:
    conn = get_conn()
    rows = _fetchall(conn, "SELECT pt.source_database, pt.source_table, mp.name, mp.id FROM plan_tables pt JOIN migration_plans mp ON pt.plan_id = mp.id")
    conn.close()
    result = {}
    for r in rows:
        key = f"{r['source_database']}.{r['source_table']}"
        result[key] = json.dumps({"name": r["name"], "id": r["id"]})
    return result


def _normalize_partition_spec(raw_spec: list) -> list:
    """Normalize partition_spec from agent format to the canonical engine format.

    Agent returns: {"column": "dt", "transform": "bucket(16)", "reason": "..."}
    The engine expects: {"source_column": "dt", "transform": "bucket", "num_buckets": 16}
    """
    if not raw_spec or not isinstance(raw_spec, list):
        return raw_spec
    normalized = []
    for item in raw_spec:
        if not isinstance(item, dict):
            continue
        entry = {}
        # Normalize field name: "column" → "source_column"
        entry["source_column"] = item.get("source_column") or item.get("column") or item.get("name") or ""
        # Parse embedded transform params: "bucket(16)" → transform="bucket", num_buckets=16
        transform = item.get("transform", "identity")
        bucket_match = re.match(r'^bucket[\(\[](\d+)[\)\]]$', transform)
        truncate_match = re.match(r'^truncate[\(\[](\d+)[\)\]]$', transform)
        if bucket_match:
            entry["transform"] = "bucket"
            entry["num_buckets"] = int(bucket_match.group(1))
        elif truncate_match:
            entry["transform"] = "truncate"
            entry["width"] = int(truncate_match.group(1))
        else:
            entry["transform"] = transform
            if item.get("num_buckets") is not None:
                entry["num_buckets"] = int(item["num_buckets"])
            if item.get("width") is not None:
                entry["width"] = int(item["width"])
        normalized.append(entry)
    return normalized


def store_table_analysis(plan_id: str, source_db: str, source_table: str, analysis: dict) -> None:
    """Store analysis results for a table in a plan."""
    conn = get_conn()
    sets = []
    params = []
    # Spec fields
    for key in ("source_format", "source_location", "source_account",
                "source_compression", "target_type", "target_location",
                "target_bucket_arn"):
        if key in analysis and analysis[key] is not None:
            sets.append(f"{key}=?")
            params.append(analysis[key])
    if "source_partitioned" in analysis:
        sets.append("source_partitioned=?")
        params.append(1 if analysis["source_partitioned"] else 0)
    # JSON fields
    for key in ("source_partition_keys", "partition_spec", "sort_order"):
        if key in analysis and analysis[key] is not None:
            val = analysis[key]
            # Normalize partition_spec from agent format to canonical format
            if key == "partition_spec" and isinstance(val, list):
                val = _normalize_partition_spec(val)
            sets.append(f"{key}=?")
            params.append(json.dumps(val))
    # Agent reasoning fields
    for key in ("recommended_type", "recommended_batch_size",
                "estimated_size_gb", "estimated_cost_usd",
                "estimated_time_min", "analysis_summary", "analysis_status"):
        if key in analysis and analysis[key] is not None:
            sets.append(f"{key}=?")
            params.append(analysis[key])
    # Schema evolution fields
    if "schema_compatible" in analysis and analysis["schema_compatible"] is not None:
        sets.append("schema_compatible=?")
        params.append(1 if analysis["schema_compatible"] else 0)
    if "schema_issues" in analysis and analysis["schema_issues"] is not None:
        sets.append("schema_issues=?")
        params.append(json.dumps(analysis["schema_issues"]))
    if not sets:
        conn.close()
        return
    params.extend([plan_id, source_db, source_table])
    # Safety: sets list contains only hardcoded 'column=?' patterns (never user input)
    assert all(s.endswith("=?") for s in sets), f"Unsafe SET clause: {sets}"
    _run_sql(conn, f"UPDATE plan_tables SET {', '.join(sets)} WHERE plan_id=? AND source_database=? AND source_table=?", tuple(params)) # nosec B608
    _commit(conn)
    conn.close()
