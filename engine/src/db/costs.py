# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pipeline cost tracking repository."""

import os
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# Default pricing (us-east-1 on-demand)
_ATHENA_PRICE_PER_TB = float(os.getenv("ENGINE_ATHENA_PRICE_PER_TB", "5.0"))
_EMR_VCPU_PRICE = float(os.getenv("ENGINE_EMR_VCPU_PRICE_PER_HOUR", "0.052624"))
_EMR_MEMORY_PRICE = float(os.getenv("ENGINE_EMR_MEMORY_PRICE_PER_GB_HOUR", "0.0057785"))
_EMR_STORAGE_PRICE = float(os.getenv("ENGINE_EMR_STORAGE_PRICE_PER_GB_HOUR", "0.000111"))

_DB_BACKEND = os.getenv("ENGINE_DB_BACKEND", "sql")
_REGION = os.getenv("ENGINE_DYNAMODB_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
_ENDPOINT_URL = os.getenv("ENGINE_DYNAMODB_ENDPOINT", "")
_COSTS_TABLE = os.getenv("ENGINE_COSTS_TABLE", "ima-costs")


class CostRepo:
    """Tracks compute costs per pipeline. Supports SQL and DynamoDB backends."""

    def __init__(self):
        if _DB_BACKEND == "dynamodb":
            self._init_dynamo()
        else:
            self._init_sql()

    def _init_sql(self):
        from .connection import get_conn, run_sql, commit
        conn = get_conn()
        run_sql(conn, """CREATE TABLE IF NOT EXISTS pipeline_costs (
            pipeline_id TEXT PRIMARY KEY,
            athena_bytes_scanned BIGINT DEFAULT 0,
            athena_query_count INTEGER DEFAULT 0,
            athena_cost_usd REAL DEFAULT 0,
            emr_vcpu_hours REAL DEFAULT 0,
            emr_memory_gb_hours REAL DEFAULT 0,
            emr_storage_gb_hours REAL DEFAULT 0,
            emr_cost_usd REAL DEFAULT 0,
            total_cost_usd REAL DEFAULT 0,
            updated_at TEXT NOT NULL)""")
        commit(conn)
        conn.close()

    def _init_dynamo(self):
        import boto3
        from ..utils.aws import BOTO_CONFIG
        kwargs = {"region_name": _REGION}
        if _ENDPOINT_URL:
            kwargs["endpoint_url"] = _ENDPOINT_URL
        self._dynamo = boto3.resource("dynamodb", config=BOTO_CONFIG, **kwargs)
        self._table = self._dynamo.Table(_COSTS_TABLE)
        # Create table if needed
        try:
            self._table.load()
        except Exception:
            client = boto3.client("dynamodb", **kwargs)
            try:
                client.create_table(
                    TableName=_COSTS_TABLE,
                    KeySchema=[{"AttributeName": "pipeline_id", "KeyType": "HASH"}],
                    AttributeDefinitions=[{"AttributeName": "pipeline_id", "AttributeType": "S"}],
                    BillingMode="PAY_PER_REQUEST")
                self._table.wait_until_exists()
                log.info("Created DynamoDB costs table: %s", _COSTS_TABLE)
            except client.exceptions.ResourceInUseException:
                pass

    def record_athena_query(self, pipeline_id: str,
                            bytes_scanned: int) -> dict:
        """Record cost from an Athena query completion."""
        billed_bytes = max(bytes_scanned, 10 * 1024 * 1024)  # 10MB minimum
        cost = (billed_bytes / (1024 ** 4)) * _ATHENA_PRICE_PER_TB
        now = datetime.now(timezone.utc).isoformat()

        if _DB_BACKEND == "dynamodb":
            return self._record_athena_dynamo(pipeline_id, billed_bytes, cost, now)
        return self._record_athena_sql(pipeline_id, billed_bytes, cost, now)

    def _record_athena_sql(self, pipeline_id, billed_bytes, cost, now):
        from .connection import get_conn, run_sql, fetchone, commit
        conn = get_conn()
        existing = fetchone(conn,
            "SELECT * FROM pipeline_costs WHERE pipeline_id=?",
            (pipeline_id,))
        if existing:
            new_bytes = existing["athena_bytes_scanned"] + billed_bytes
            new_count = existing["athena_query_count"] + 1
            new_athena_cost = (new_bytes / (1024 ** 4)) * _ATHENA_PRICE_PER_TB
            new_total = new_athena_cost + (existing["emr_cost_usd"] or 0)
            run_sql(conn,
                "UPDATE pipeline_costs SET athena_bytes_scanned=?, "
                "athena_query_count=?, athena_cost_usd=?, "
                "total_cost_usd=?, updated_at=? WHERE pipeline_id=?",
                (new_bytes, new_count, new_athena_cost, new_total, now, pipeline_id))
        else:
            run_sql(conn,
                "INSERT INTO pipeline_costs (pipeline_id, "
                "athena_bytes_scanned, athena_query_count, athena_cost_usd, "
                "total_cost_usd, updated_at) VALUES (?,?,?,?,?,?)",
                (pipeline_id, billed_bytes, 1, cost, cost, now))
        commit(conn)
        conn.close()
        return {"pipeline_id": pipeline_id, "bytes_scanned": billed_bytes, "cost_usd": cost}

    def _record_athena_dynamo(self, pipeline_id, billed_bytes, cost, now):
        from decimal import Decimal
        try:
            self._table.update_item(
                Key={"pipeline_id": pipeline_id},
                UpdateExpression=(
                    "SET athena_bytes_scanned = if_not_exists(athena_bytes_scanned, :zero) + :bytes, "
                    "athena_query_count = if_not_exists(athena_query_count, :zero) + :one, "
                    "athena_cost_usd = if_not_exists(athena_cost_usd, :zerof) + :cost, "
                    "total_cost_usd = if_not_exists(total_cost_usd, :zerof) + :cost, "
                    "updated_at = :now"),
                ExpressionAttributeValues={
                    ":bytes": billed_bytes,
                    ":one": 1,
                    ":zero": 0,
                    ":zerof": Decimal("0"),
                    ":cost": Decimal(str(round(cost, 6))),
                    ":now": now,
                })
        except Exception as e:
            log.warning("Failed to record Athena cost in DynamoDB: %s", e)
        return {"pipeline_id": pipeline_id, "bytes_scanned": billed_bytes, "cost_usd": cost}

    def record_emr_job(self, pipeline_id: str,
                       vcpu_hours: float = 0,
                       memory_gb_hours: float = 0,
                       storage_gb_hours: float = 0) -> dict:
        """Record cost from an EMR Serverless job completion."""
        cost = (vcpu_hours * _EMR_VCPU_PRICE +
                memory_gb_hours * _EMR_MEMORY_PRICE +
                storage_gb_hours * _EMR_STORAGE_PRICE)
        now = datetime.now(timezone.utc).isoformat()

        if _DB_BACKEND == "dynamodb":
            return self._record_emr_dynamo(pipeline_id, vcpu_hours,
                                           memory_gb_hours, storage_gb_hours, cost, now)
        return self._record_emr_sql(pipeline_id, vcpu_hours,
                                    memory_gb_hours, storage_gb_hours, cost, now)

    def _record_emr_sql(self, pipeline_id, vcpu_hours, memory_gb_hours,
                        storage_gb_hours, cost, now):
        from .connection import get_conn, run_sql, fetchone, commit
        conn = get_conn()
        existing = fetchone(conn,
            "SELECT * FROM pipeline_costs WHERE pipeline_id=?",
            (pipeline_id,))
        if existing:
            new_vcpu = existing["emr_vcpu_hours"] + vcpu_hours
            new_mem = existing["emr_memory_gb_hours"] + memory_gb_hours
            new_stor = existing["emr_storage_gb_hours"] + storage_gb_hours
            new_emr_cost = (new_vcpu * _EMR_VCPU_PRICE +
                            new_mem * _EMR_MEMORY_PRICE +
                            new_stor * _EMR_STORAGE_PRICE)
            new_total = (existing["athena_cost_usd"] or 0) + new_emr_cost
            run_sql(conn,
                "UPDATE pipeline_costs SET emr_vcpu_hours=?, "
                "emr_memory_gb_hours=?, emr_storage_gb_hours=?, "
                "emr_cost_usd=?, total_cost_usd=?, updated_at=? "
                "WHERE pipeline_id=?",
                (new_vcpu, new_mem, new_stor, new_emr_cost, new_total, now, pipeline_id))
        else:
            run_sql(conn,
                "INSERT INTO pipeline_costs (pipeline_id, "
                "emr_vcpu_hours, emr_memory_gb_hours, emr_storage_gb_hours, "
                "emr_cost_usd, total_cost_usd, updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (pipeline_id, vcpu_hours, memory_gb_hours, storage_gb_hours, cost, cost, now))
        commit(conn)
        conn.close()
        return {"pipeline_id": pipeline_id, "vcpu_hours": vcpu_hours,
                "memory_gb_hours": memory_gb_hours, "cost_usd": cost}

    def _record_emr_dynamo(self, pipeline_id, vcpu_hours, memory_gb_hours,
                           storage_gb_hours, cost, now):
        from decimal import Decimal
        try:
            self._table.update_item(
                Key={"pipeline_id": pipeline_id},
                UpdateExpression=(
                    "SET emr_vcpu_hours = if_not_exists(emr_vcpu_hours, :zerof) + :vcpu, "
                    "emr_memory_gb_hours = if_not_exists(emr_memory_gb_hours, :zerof) + :mem, "
                    "emr_storage_gb_hours = if_not_exists(emr_storage_gb_hours, :zerof) + :stor, "
                    "emr_cost_usd = if_not_exists(emr_cost_usd, :zerof) + :cost, "
                    "total_cost_usd = if_not_exists(total_cost_usd, :zerof) + :cost, "
                    "updated_at = :now"),
                ExpressionAttributeValues={
                    ":vcpu": Decimal(str(round(vcpu_hours, 4))),
                    ":mem": Decimal(str(round(memory_gb_hours, 4))),
                    ":stor": Decimal(str(round(storage_gb_hours, 4))),
                    ":zerof": Decimal("0"),
                    ":cost": Decimal(str(round(cost, 6))),
                    ":now": now,
                })
        except Exception as e:
            log.warning("Failed to record EMR cost in DynamoDB: %s", e)
        return {"pipeline_id": pipeline_id, "vcpu_hours": vcpu_hours,
                "memory_gb_hours": memory_gb_hours, "cost_usd": cost}

    def get(self, pipeline_id: str) -> dict | None:
        """Get cost record for a pipeline."""
        if _DB_BACKEND == "dynamodb":
            return self._get_dynamo(pipeline_id)
        return self._get_sql(pipeline_id)

    def _get_sql(self, pipeline_id):
        from .connection import get_conn, fetchone
        conn = get_conn()
        row = fetchone(conn,
            "SELECT * FROM pipeline_costs WHERE pipeline_id=?",
            (pipeline_id,))
        conn.close()
        return dict(row) if row else None

    def _get_dynamo(self, pipeline_id):
        try:
            resp = self._table.get_item(Key={"pipeline_id": pipeline_id})
            item = resp.get("Item")
            if not item:
                return None
            return {k: float(v) if hasattr(v, 'is_finite') else v
                    for k, v in item.items()}
        except Exception:
            return None

    def get_plan_costs(self, pipeline_ids: list[str]) -> dict:
        """Aggregate costs across multiple pipelines (plan level)."""
        if not pipeline_ids:
            return {"athena_bytes_scanned": 0, "athena_query_count": 0,
                    "athena_cost_usd": 0, "emr_vcpu_hours": 0,
                    "emr_cost_usd": 0, "total_cost_usd": 0}
        totals = {
            "athena_bytes_scanned": 0, "athena_query_count": 0,
            "athena_cost_usd": 0.0,
            "emr_vcpu_hours": 0.0, "emr_memory_gb_hours": 0.0,
            "emr_storage_gb_hours": 0.0, "emr_cost_usd": 0.0,
            "total_cost_usd": 0.0,
        }
        for pid in pipeline_ids:
            r = self.get(pid)
            if r:
                for k in totals:
                    totals[k] += r.get(k, 0) or 0
        return totals
