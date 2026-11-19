# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validation results repository.

Stores the outcome of on-demand data-validation runs, one row per
(pipeline_id, run_id). Full history is retained; the UI displays the latest run
per pipeline with its timestamp. Mirrors the dual-backend (SQL + DynamoDB)
pattern of CostRepo — the SQL backend is used in local/dev, DynamoDB in a
deployed environment.

Writers:
  - The engine writes results for Athena-based validation (results returned
    synchronously to the engine process).
  - The EMR validation Spark job (spark/validate.py) writes rows directly to
    the DynamoDB table (per-pipeline, as each completes). That path therefore
    only applies when ENGINE_DB_BACKEND=dynamodb (EMR validation is a
    deployed-AWS scenario; there is no EMR against a local SQLite backend).

results_json shape (list of per-check results):
  [{"check_id","name","status","source_value","target_value","detail"}]
status values: "passed" | "failed" | "error" | "skipped"
overall_status: "passed" | "failed" | "error" | "running"
"""

import os
import threading
import json
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)
_ensure_lock = threading.Lock()

_DB_BACKEND = os.getenv("ENGINE_DB_BACKEND", "sql")
_REGION = os.getenv("ENGINE_DYNAMODB_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
_ENDPOINT_URL = os.getenv("ENGINE_DYNAMODB_ENDPOINT", "")
_DEFAULT_VALIDATION_TABLE = "ima-validation-results"


def validation_table_name() -> str:
    """The DynamoDB validation table, resolved on every call.

    The app can push ENGINE_VALIDATION_TABLE from its Settings page at
    runtime, and the engine passes the same name to the EMR validation job, so
    the engine and the job always read and write the same table.
    """
    return os.getenv("ENGINE_VALIDATION_TABLE", "") or _DEFAULT_VALIDATION_TABLE


class ValidationRepo:
    """Per-pipeline validation-run history. SQL and DynamoDB backends."""

    def __init__(self):
        if _DB_BACKEND == "dynamodb":
            self._init_dynamo()
        else:
            self._init_sql()

    # ── init ──

    def _init_sql(self):
        from .connection import get_conn, run_sql, commit
        conn = get_conn()
        run_sql(conn, """CREATE TABLE IF NOT EXISTS validation_results (
            run_id TEXT PRIMARY KEY,
            pipeline_id TEXT NOT NULL,
            tier TEXT NOT NULL,
            engine TEXT,
            overall_status TEXT NOT NULL,
            results_json TEXT,
            run_at TEXT NOT NULL,
            completed_at TEXT,
            triggered_by TEXT)""")
        # Index for latest/history lookups by pipeline.
        run_sql(conn, """CREATE INDEX IF NOT EXISTS idx_validation_pipeline
            ON validation_results (pipeline_id, run_at)""")
        commit(conn)
        conn.close()

    def _init_dynamo(self):
        import boto3
        from ..utils.aws import BOTO_CONFIG
        kwargs = {"region_name": _REGION}
        if _ENDPOINT_URL:
            kwargs["endpoint_url"] = _ENDPOINT_URL
        self._dynamo = boto3.resource("dynamodb", config=BOTO_CONFIG, **kwargs)
        self._dynamo_kwargs = kwargs
        self._ready_tables: set[str] = set()
        self._ensure_dynamo_table(validation_table_name())

    @property
    def _table(self):
        """Handle for the current validation table (created if missing)."""
        name = validation_table_name()
        if name not in self._ready_tables:
            self._ensure_dynamo_table(name)
        return self._dynamo.Table(name)

    def _ensure_dynamo_table(self, name: str) -> None:
        import boto3
        with _ensure_lock:
            if name in self._ready_tables:
                return
            table = self._dynamo.Table(name)
            try:
                table.load()
                self._ready_tables.add(name)
                return
            except Exception:
                pass
            client = boto3.client("dynamodb", **self._dynamo_kwargs)
            try:
                client.create_table(
                    TableName=name,
                    # HASH pipeline_id + RANGE run_id gives cheap per-pipeline
                    # history queries and a natural "latest" via run_at sort.
                    KeySchema=[
                        {"AttributeName": "pipeline_id", "KeyType": "HASH"},
                        {"AttributeName": "run_id", "KeyType": "RANGE"},
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "pipeline_id", "AttributeType": "S"},
                        {"AttributeName": "run_id", "AttributeType": "S"},
                    ],
                    BillingMode="PAY_PER_REQUEST")
                table.wait_until_exists()
                log.info("Created DynamoDB validation table: %s", name)
            except client.exceptions.ResourceInUseException:
                pass
            self._ready_tables.add(name)

    # ── writes ──

    def create_run(self, run_id: str, pipeline_id: str, tier: str,
                   engine: str = "", triggered_by: str = "") -> dict:
        """Create a run row in 'running' state (results filled in later)."""
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "tier": tier,
            "engine": engine,
            "overall_status": "running",
            "results_json": "[]",
            "run_at": now,
            "completed_at": None,
            "triggered_by": triggered_by,
        }
        if _DB_BACKEND == "dynamodb":
            self._put_dynamo(row)
        else:
            self._put_sql(row)
        return row

    def update_run(self, run_id: str, pipeline_id: str,
                   overall_status: str, results: list) -> None:
        """Fill in results and final status for a completed run."""
        now = datetime.now(timezone.utc).isoformat()
        if _DB_BACKEND == "dynamodb":
            self._update_dynamo(run_id, pipeline_id, overall_status, results, now)
        else:
            self._update_sql(run_id, overall_status, results, now)

    def _put_sql(self, row):
        from .connection import get_conn, run_sql, commit
        conn = get_conn()
        run_sql(conn,
            "INSERT OR REPLACE INTO validation_results "
            "(run_id, pipeline_id, tier, engine, overall_status, "
            "results_json, run_at, completed_at, triggered_by) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (row["run_id"], row["pipeline_id"], row["tier"], row["engine"],
             row["overall_status"], row["results_json"], row["run_at"],
             row["completed_at"], row["triggered_by"]))
        commit(conn)
        conn.close()

    def _update_sql(self, run_id, overall_status, results, now):
        from .connection import get_conn, run_sql, commit
        conn = get_conn()
        run_sql(conn,
            "UPDATE validation_results SET overall_status=?, results_json=?, "
            "completed_at=? WHERE run_id=?",
            (overall_status, json.dumps(results), now, run_id))
        commit(conn)
        conn.close()

    def _put_dynamo(self, row):
        item = dict(row)
        # DynamoDB does not accept None; store empty string for completed_at.
        item["completed_at"] = item.get("completed_at") or ""
        try:
            self._table.put_item(Item=item)
        except Exception as e:
            log.warning("Failed to create validation run in DynamoDB: %s", e)

    def _update_dynamo(self, run_id, pipeline_id, overall_status, results, now):
        try:
            self._table.update_item(
                Key={"pipeline_id": pipeline_id, "run_id": run_id},
                UpdateExpression=(
                    "SET overall_status = :st, results_json = :rj, "
                    "completed_at = :ca"),
                ExpressionAttributeValues={
                    ":st": overall_status,
                    ":rj": json.dumps(results),
                    ":ca": now,
                })
        except Exception as e:
            log.warning("Failed to update validation run in DynamoDB: %s", e)

    # ── reads ──

    def get_run(self, run_id: str, pipeline_id: str = "") -> dict | None:
        if _DB_BACKEND == "dynamodb":
            return self._get_run_dynamo(run_id, pipeline_id)
        return self._get_run_sql(run_id)

    def _get_run_sql(self, run_id):
        from .connection import get_conn, fetchone
        conn = get_conn()
        row = fetchone(conn,
            "SELECT * FROM validation_results WHERE run_id=?", (run_id,))
        conn.close()
        return self._deser(dict(row)) if row else None

    def _get_run_dynamo(self, run_id, pipeline_id):
        # DynamoDB needs the full key; when pipeline_id is unknown, fall back to
        # a scan (run detail lookups are rare/admin, not hot-path).
        try:
            if pipeline_id:
                resp = self._table.get_item(
                    Key={"pipeline_id": pipeline_id, "run_id": run_id})
                item = resp.get("Item")
                return self._deser(item) if item else None
            from boto3.dynamodb.conditions import Attr
            from .dynamo_paging import scan_first
            item = scan_first(self._table, FilterExpression=Attr("run_id").eq(run_id))
            return self._deser(item) if item else None
        except Exception:
            return None

    def list_history(self, pipeline_id: str) -> list[dict]:
        """All runs for a pipeline, newest first."""
        if _DB_BACKEND == "dynamodb":
            return self._history_dynamo(pipeline_id)
        return self._history_sql(pipeline_id)

    def _history_sql(self, pipeline_id):
        from .connection import get_conn, fetchall
        conn = get_conn()
        rows = fetchall(conn,
            "SELECT * FROM validation_results WHERE pipeline_id=? "
            "ORDER BY run_at DESC", (pipeline_id,))
        conn.close()
        return [self._deser(dict(r)) for r in rows]

    def _history_dynamo(self, pipeline_id):
        from boto3.dynamodb.conditions import Key
        try:
            from .dynamo_paging import query_all
            items = [self._deser(i) for i in query_all(
                self._table, KeyConditionExpression=Key("pipeline_id").eq(pipeline_id))]
            items.sort(key=lambda r: r.get("run_at", ""), reverse=True)
            return items
        except Exception:
            return []

    def get_latest(self, pipeline_id: str) -> dict | None:
        """Most recent run for a pipeline (by run_at)."""
        history = self.list_history(pipeline_id)
        return history[0] if history else None

    # ── helpers ──

    @staticmethod
    def _deser(row: dict) -> dict:
        """Normalize a stored row: parse results_json, empty completed_at→None."""
        if row is None:
            return None
        out = dict(row)
        rj = out.get("results_json")
        if isinstance(rj, str):
            try:
                out["results"] = json.loads(rj)
            except (json.JSONDecodeError, TypeError):
                out["results"] = []
        elif isinstance(rj, list):
            out["results"] = rj
        else:
            out["results"] = []
        out.pop("results_json", None)
        if out.get("completed_at") == "":
            out["completed_at"] = None
        return out
