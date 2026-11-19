# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""DynamoDB implementations for the engine repositories.

Tables:
  ima-pipelines:
    PK: id (String)
    GSI status-index: PK=status, SK=created_at

  ima-batches:
    PK: pipeline_id (String)
    SK: batch_number (Number)

  ima-progress:
    PK: pipeline_id (String)
    SK: partition_key#partition_value (String)
"""

import json
import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr

from . import status_guard
from ..utils.aws import BOTO_CONFIG
from .dynamo_paging import query_all, scan_all, scan_first

log = logging.getLogger(__name__)

_REGION = os.getenv("ENGINE_DYNAMODB_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
_ENDPOINT_URL = os.getenv("ENGINE_DYNAMODB_ENDPOINT", "")  # For DynamoDB Local
_PIPELINE_TABLE = os.getenv("ENGINE_PIPELINE_TABLE", "ima-pipelines")
_BATCH_TABLE = os.getenv("ENGINE_BATCH_TABLE", "ima-batches")
_PROGRESS_TABLE = os.getenv("ENGINE_PROGRESS_TABLE", "ima-progress")


def _get_resource():
    kwargs = {"region_name": _REGION}
    if _ENDPOINT_URL:
        kwargs["endpoint_url"] = _ENDPOINT_URL
    # Shared botocore config (50-connection pool, bounded timeouts, adaptive
    # retries): one repo is shared by every reconcile worker thread.
    return boto3.resource("dynamodb", config=BOTO_CONFIG, **kwargs)


def _decimal_to_int(obj):
    """Convert Decimal values from DynamoDB to int/float."""
    if isinstance(obj, dict):
        return {k: _decimal_to_int(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_decimal_to_int(i) for i in obj]
    elif isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    return obj



# ─── Pipeline Repository ─────────────────────────────────────────────────────

class PipelineRepo:
    """DynamoDB-backed pipeline repository."""

    def __init__(self):
        self._table = _get_resource().Table(_PIPELINE_TABLE)

    def exists(self, source_database: str, source_table: str,
               source_account: str) -> Optional[dict]:
        # No Limit: DynamoDB applies Limit before the filter, so Limit=1 would
        # only ever look at one item. scan_first pages until it finds a match.
        item = scan_first(self._table, FilterExpression=(
            Attr("source_database").eq(source_database) &
            Attr("source_table").eq(source_table) &
            Attr("source_account").eq(source_account)
        ))
        return _decimal_to_int(item) if item else None

    def create_from_spec(self, spec_dict: dict) -> dict:
        src = spec_dict["source"]
        tgt = spec_dict["target"]
        strat = spec_dict["strategy"]
        ip = spec_dict.get("iceberg_properties", {})

        existing = self.exists(src["database"], src["table_name"], src["aws_account_id"])
        if existing:
            raise ValueError(
                f"Pipeline already exists for "
                f"{src['database']}.{src['table_name']} "
                f"(account {src['aws_account_id']}). "
                f"Existing pipeline ID: {existing['id']}")

        pid = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        item = {
            "id": pid,
            "source_database": src["database"],
            "source_table": src["table_name"],
            "source_format": src["format"],
            "source_location": src["location"],
            "source_account": src.get("aws_account_id", ""),
            "source_region": src.get("region", "us-east-1"),
            "source_catalog": src.get("catalog", "glue"),
            "source_partitioned": 1 if src.get("partitioned") else 0,
            "source_partition_keys": json.dumps(src.get("partition_keys", [])),
            "source_compression": src.get("compression", ""),
            "target_type": tgt["type"],
            "target_database": tgt["database"],
            "target_table": tgt["table_name"],
            "target_location": tgt.get("location", ""),
            "target_bucket_arn": tgt.get("table_bucket_arn", ""),
            "target_catalog": tgt.get("catalog", "glue"),
            "target_account": tgt.get("aws_account_id", ""),
            "target_region": tgt.get("region", "us-east-1"),
            "migration_type": strat["type"],
            "execution_engine": strat.get("execution_engine", "athena"),
            "iam_role_arn": strat.get("iam_role_arn", ""),
            "partition_spec": json.dumps(ip.get("partition_spec", [])),
            "sort_order": json.dumps(ip.get("sort_order", [])),
            "schema_overrides": json.dumps(ip.get("schema_overrides", [])),
            "table_properties": json.dumps(ip.get("table_properties", {})),
            "spec_json": json.dumps(spec_dict),
            "status": "pending",
            "sync_status": "pending",
            "emr_job_run_id": "",
            "error_message": "",
            "created_at": now,
            "updated_at": now,
            "started_at": "",
            "completed_at": "",
            "partitions_total": 0,
            "partitions_completed": 0,
            "bytes_total": 0,
            "bytes_processed": 0,
            "rows_total": 0,
            "rows_processed": 0,
        }
        self._table.put_item(Item=item)
        return self.get(pid)

    def get(self, pipeline_id: str, consistent: bool = False) -> Optional[dict]:
        response = self._table.get_item(
            Key={"id": pipeline_id}, ConsistentRead=consistent)
        item = response.get("Item")
        return _decimal_to_int(item) if item else None

    def list_all(self, status: str = None) -> list[dict]:
        if status:
            items = query_all(
                self._table,
                IndexName="status-index",
                KeyConditionExpression=Key("status").eq(status),
                ScanIndexForward=False,
            )
        else:
            items = scan_all(self._table)
        # Sort by created_at descending
        items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return [_decimal_to_int(i) for i in items]

    def update_status(self, pipeline_id: str, status: str,
                      error_message: str = None,
                      reset_started_at: bool = False,
                      expected_status: str = None,
                      force: bool = False) -> Optional[dict]:
        """Set a pipeline's status, refusing writes that would clobber state.

        See db/status_guard.py: completed/cancelled are sticky unless `force`,
        and the write is compare-and-set when an expected status is given (or
        set by the reconcile context). A refused write returns the pipeline as
        it currently is.
        """
        expected = expected_status or status_guard.expected_status(pipeline_id)
        now = datetime.now(timezone.utc).isoformat()
        update_expr = "SET #s = :s, updated_at = :now"
        expr_names = {"#s": "status"}
        expr_values = {":s": status, ":now": now}
        if status == "running":
            # Only stamp started_at on the FIRST transition into running. The
            # managed-migration reconcile loop re-sets status to "running" on
            # every poll; unconditionally writing started_at would keep resetting
            # it to "now". Preserve the original once set, unless the caller
            # explicitly restarts the run (reset_started_at). Mirrors the SQL
            # PipelineRepo.update_status behaviour. (Items are created with
            # started_at="", so if_not_exists() cannot replace this read.)
            if reset_started_at:
                update_expr += ", started_at = :ts"
                expr_values[":ts"] = now
            else:
                existing = self.get(pipeline_id)
                if not (existing and existing.get("started_at")):
                    update_expr += ", started_at = :ts"
                    expr_values[":ts"] = now
        elif status in ("completed", "failed"):
            update_expr += ", completed_at = :ts"
            expr_values[":ts"] = now
        if error_message is not None:
            update_expr += ", error_message = :err"
            expr_values[":err"] = error_message

        conditions = []
        if not force:
            conditions.append(
                "(attribute_not_exists(#s) OR NOT (#s IN (:term0, :term1)))")
            expr_values[":term0"], expr_values[":term1"] = sorted(
                status_guard.TERMINAL_STATUSES)
        if expected:
            conditions.append("#s = :expected")
            expr_values[":expected"] = expected
        kwargs = {}
        if conditions:
            kwargs["ConditionExpression"] = " AND ".join(conditions)
        try:
            resp = self._table.update_item(
                Key={"id": pipeline_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=expr_names,
                ExpressionAttributeValues=expr_values,
                ReturnValues="ALL_NEW",
                **kwargs,
            )
        except self._table.meta.client.exceptions.ConditionalCheckFailedException:
            current = self.get(pipeline_id, consistent=True)
            log.info("Pipeline %s: status write '%s' refused (now '%s'%s)",
                     pipeline_id[:8], status, (current or {}).get("status"),
                     f", expected '{expected}'" if expected else "")
            return current
        status_guard.record_write(pipeline_id, status)
        item = resp.get("Attributes")
        return _decimal_to_int(item) if item else self.get(pipeline_id)

    def update_sync_status(self, pipeline_id: str, sync_status: str) -> Optional[dict]:
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"id": pipeline_id},
            UpdateExpression="SET sync_status = :ss, updated_at = :now",
            ExpressionAttributeValues={":ss": sync_status, ":now": now},
        )
        return self.get(pipeline_id)

    def update_metadata(self, pipeline_id: str, key: str, value: str) -> None:
        """Store arbitrary metadata on the pipeline's spec_json."""
        item = self.get(pipeline_id)
        if not item:
            return
        spec = item.get("spec_json") or {}
        if isinstance(spec, str):
            spec = json.loads(spec)
        spec.setdefault("_metadata", {})[key] = value
        self._table.update_item(
            Key={"id": pipeline_id},
            UpdateExpression="SET spec_json = :spec",
            ExpressionAttributeValues={":spec": json.dumps(spec)},
        )

    def update_emr_job_run_id(self, pipeline_id: str, job_run_id: str) -> Optional[dict]:
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"id": pipeline_id},
            UpdateExpression="SET emr_job_run_id = :jid, updated_at = :now",
            ExpressionAttributeValues={":jid": job_run_id, ":now": now},
        )
        return self.get(pipeline_id)

    def update_progress(self, pipeline_id: str, bytes_processed: int = 0,
                        rows_processed: int = 0, partitions_completed: int = 0,
                        partitions_total: int = None) -> Optional[dict]:
        now = datetime.now(timezone.utc).isoformat()
        update_expr = "SET bytes_processed = :bp, rows_processed = :rp, partitions_completed = :pc, updated_at = :now"
        expr_values = {":bp": bytes_processed, ":rp": rows_processed, ":pc": partitions_completed, ":now": now}
        if partitions_total is not None:
            update_expr += ", partitions_total = :pt"
            expr_values[":pt"] = partitions_total
        self._table.update_item(
            Key={"id": pipeline_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
        )
        return self.get(pipeline_id)

    def delete(self, pipeline_id: str) -> bool:
        # Delete related batches and progress
        batch_repo = BatchRepo()
        progress_repo = ProgressRepo()
        for b in batch_repo.list_by_pipeline(pipeline_id):
            batch_repo._table.delete_item(Key={"pipeline_id": pipeline_id, "batch_number": b["batch_number"]})
        for p in progress_repo.list_by_pipeline(pipeline_id):
            progress_repo._table.delete_item(Key={"pipeline_id": pipeline_id, "sk": p["sk"]})
        self._table.delete_item(Key={"id": pipeline_id})
        return True



# ─── Batch Repository ─────────────────────────────────────────────────────────

class BatchRepo:
    """DynamoDB-backed batch repository."""

    def __init__(self):
        self._table = _get_resource().Table(_BATCH_TABLE)
        self._last_pipeline_id = ""  # Cache from last get call for mark_* compat

    def create(self, pipeline_id: str, batch_number: int,
               partitions: list[str], bytes_total: int = 0) -> dict:
        self._last_pipeline_id = pipeline_id
        now = datetime.now(timezone.utc).isoformat()
        item = {
            "pipeline_id": pipeline_id,
            "batch_number": batch_number,
            "id": batch_number,  # Compat with SQL version
            "status": "pending",
            "partitions": json.dumps(partitions),
            "partition_count": len(partitions),
            "bytes_total": bytes_total,
            "bytes_processed": 0,
            "emr_job_run_id": "",
            "error_message": "",
            "created_at": now,
            "started_at": "",
            "completed_at": "",
        }
        self._table.put_item(Item=item)
        return self.get_by_number(pipeline_id, batch_number)

    def get_by_number(self, pipeline_id: str, batch_number: int) -> Optional[dict]:
        response = self._table.get_item(
            Key={"pipeline_id": pipeline_id, "batch_number": batch_number})
        item = response.get("Item")
        return _decimal_to_int(item) if item else None

    def get_running(self, pipeline_id: str) -> Optional[dict]:
        self._last_pipeline_id = pipeline_id
        items = query_all(
            self._table,
            KeyConditionExpression=Key("pipeline_id").eq(pipeline_id),
            FilterExpression=Attr("status").eq("running"),
        )
        if not items:
            return None
        # Return the MOST RECENT running batch (highest batch_number) so Managed
        # Migration phase detection reflects the sync batch, not a lingering
        # earlier one. Mirrors the SQL BatchRepo ordering.
        latest = max(items, key=lambda b: int(b.get("batch_number", 0)))
        return _decimal_to_int(latest)

    def mark_running(self, batch_id: int, emr_job_run_id: str,
                     pipeline_id: str = "") -> None:
        pid = pipeline_id or self._last_pipeline_id
        if not pid:
            log.warning("BatchRepo.mark_running: no pipeline_id available")
            return
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pid, "batch_number": batch_id},
            UpdateExpression="SET #s = :s, emr_job_run_id = :jid, started_at = :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "running", ":jid": emr_job_run_id, ":now": now},
        )

    def mark_completed(self, batch_id: int, bytes_processed: int = 0,
                       pipeline_id: str = "",
                       started_at: str = None,
                       completed_at: str = None) -> None:
        # started_at/completed_at let callers record the job's ACTUAL execution
        # window (e.g. Athena timestamps) instead of the reconcile observation
        # time, keeping fast-batch durations accurate. See BatchRepo (SQL) for
        # the full rationale.
        pid = pipeline_id or self._last_pipeline_id
        if not pid:
            log.warning("BatchRepo.mark_completed: no pipeline_id available")
            return
        now = datetime.now(timezone.utc).isoformat()
        completed = completed_at or now
        expr = "SET #s = :s, bytes_processed = :bp, completed_at = :ca"
        values = {":s": "completed", ":bp": bytes_processed, ":ca": completed}
        if started_at:
            expr += ", started_at = :sa"
            values[":sa"] = started_at
        self._table.update_item(
            Key={"pipeline_id": pid, "batch_number": batch_id},
            UpdateExpression=expr,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=values,
        )

    def mark_failed(self, batch_id: int, error_message: str = "",
                    pipeline_id: str = "") -> None:
        pid = pipeline_id or self._last_pipeline_id
        if not pid:
            log.warning("BatchRepo.mark_failed: no pipeline_id available")
            return
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pid, "batch_number": batch_id},
            UpdateExpression="SET #s = :s, error_message = :err, completed_at = :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "failed", ":err": error_message, ":now": now},
        )

    def list_by_pipeline(self, pipeline_id: str, status: str = None) -> list[dict]:
        self._last_pipeline_id = pipeline_id
        kwargs = {"KeyConditionExpression": Key("pipeline_id").eq(pipeline_id), "ScanIndexForward": True}
        if status:
            kwargs["FilterExpression"] = Attr("status").eq(status)
        return [_decimal_to_int(i) for i in query_all(self._table, **kwargs)]

    def get_summary(self, pipeline_id: str) -> dict:
        all_batches = self.list_by_pipeline(pipeline_id)
        summary = {}
        for b in all_batches:
            s = b["status"]
            if s not in summary:
                summary[s] = {"count": 0, "partitions": 0}
            summary[s]["count"] += 1
            summary[s]["partitions"] += b.get("partition_count", 0)
        return summary

    def reset_failed_to_pending(self, pipeline_id: str) -> int:
        failed = self.list_by_pipeline(pipeline_id, status="failed")
        for b in failed:
            self._table.update_item(
                Key={"pipeline_id": pipeline_id, "batch_number": b["batch_number"]},
                UpdateExpression="SET #s = :s, error_message = :e, emr_job_run_id = :j, started_at = :sa, completed_at = :ca",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "pending", ":e": "", ":j": "", ":sa": "", ":ca": ""},
            )
        return len(failed)

    def delete_from_number(self, pipeline_id: str, min_batch_number: int) -> int:
        """Delete all batches with batch_number >= min_batch_number.

        Used on retry-from-scratch to clear leftover data batches so a
        re-submission starts with a clean slate (batch 0 is recreated by
        the strategy). Returns the number of batches deleted.
        """
        deleted = 0
        for b in self.list_by_pipeline(pipeline_id):
            if b["batch_number"] >= min_batch_number:
                self._table.delete_item(
                    Key={"pipeline_id": pipeline_id,
                         "batch_number": b["batch_number"]})
                deleted += 1
        return deleted



# ─── Progress Repository ──────────────────────────────────────────────────────

class ProgressRepo:
    """DynamoDB-backed progress repository."""

    def __init__(self):
        self._table = _get_resource().Table(_PROGRESS_TABLE)

    def _make_sk(self, partition_key: str, partition_value: str) -> str:
        return f"{partition_key}#{partition_value}"

    def upsert(self, pipeline_id: str, partition_key: str,
               partition_value: str, objects_total: int = 0,
               bytes_total: int = 0) -> dict:
        sk = self._make_sk(partition_key, partition_value)
        now = datetime.now(timezone.utc).isoformat()
        self._table.put_item(Item={
            "pipeline_id": pipeline_id,
            "sk": sk,
            "partition_key": partition_key,
            "partition_value": partition_value,
            "status": "pending",
            "objects_total": objects_total,
            "objects_processed": 0,
            "bytes_total": bytes_total,
            "bytes_processed": 0,
            "manifest_file": "",
            "snapshot_id": 0,
            "started_at": "",
            "completed_at": "",
            "error_message": "",
            "updated_at": now,
        })
        return self.get(pipeline_id, partition_key, partition_value)

    def get(self, pipeline_id: str, partition_key: str,
            partition_value: str) -> Optional[dict]:
        sk = self._make_sk(partition_key, partition_value)
        response = self._table.get_item(Key={"pipeline_id": pipeline_id, "sk": sk})
        item = response.get("Item")
        return _decimal_to_int(item) if item else None

    def list_by_pipeline(self, pipeline_id: str, status: str = None) -> list[dict]:
        kwargs = {"KeyConditionExpression": Key("pipeline_id").eq(pipeline_id)}
        if status:
            kwargs["FilterExpression"] = Attr("status").eq(status)
        return [_decimal_to_int(i) for i in query_all(self._table, **kwargs)]

    def mark_running(self, progress_id, pipeline_id: str = "") -> None:
        # progress_id is the SK for DynamoDB
        if not pipeline_id:
            return
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pipeline_id, "sk": progress_id},
            UpdateExpression="SET #s = :s, started_at = :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "running", ":now": now},
        )

    def mark_completed(self, progress_id, pipeline_id: str = "",
                       objects_processed: int = 0, bytes_processed: int = 0,
                       manifest_file: str = "", snapshot_id: int = 0) -> None:
        if not pipeline_id:
            return
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pipeline_id, "sk": progress_id},
            UpdateExpression="SET #s = :s, objects_processed = :op, bytes_processed = :bp, manifest_file = :mf, snapshot_id = :sid, completed_at = :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "completed", ":op": objects_processed, ":bp": bytes_processed, ":mf": manifest_file, ":sid": snapshot_id, ":now": now},
        )

    def mark_failed(self, progress_id, pipeline_id: str = "",
                    error_message: str = "") -> None:
        if not pipeline_id:
            return
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pipeline_id, "sk": progress_id},
            UpdateExpression="SET #s = :s, error_message = :err, completed_at = :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "failed", ":err": error_message, ":now": now},
        )

    def compute_sync_status(self, pipeline_id: str) -> str:
        all_progress = self.list_by_pipeline(pipeline_id)
        if not all_progress:
            return "pending"
        counts = {}
        for p in all_progress:
            s = p["status"]
            counts[s] = counts.get(s, 0) + 1
        if counts.get("failed", 0) > 0:
            return "failed"
        if counts.get("running", 0) > 0:
            return "syncing"
        if counts.get("pending", 0) > 0:
            return "pending_sync"
        return "in_sync"


# ─── Table Creation (for setup/testing) ───────────────────────────────────────

def create_tables_if_not_exist():
    """Create all DynamoDB tables for the engine (idempotent)."""
    kwargs = {"region_name": _REGION}
    if _ENDPOINT_URL:
        kwargs["endpoint_url"] = _ENDPOINT_URL
    client = boto3.client("dynamodb", **kwargs)

    tables = [
        {
            "TableName": _PIPELINE_TABLE,
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "status", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "status-index",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
        },
        {
            "TableName": _BATCH_TABLE,
            "KeySchema": [
                {"AttributeName": "pipeline_id", "KeyType": "HASH"},
                {"AttributeName": "batch_number", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pipeline_id", "AttributeType": "S"},
                {"AttributeName": "batch_number", "AttributeType": "N"},
            ],
        },
        {
            "TableName": _PROGRESS_TABLE,
            "KeySchema": [
                {"AttributeName": "pipeline_id", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pipeline_id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
        },
    ]

    for table_def in tables:
        name = table_def["TableName"]
        try:
            client.describe_table(TableName=name)
            log.info("DynamoDB table %s already exists", name)
            continue
        except client.exceptions.ResourceNotFoundException:
            pass

        create_args = {**table_def, "BillingMode": "PAY_PER_REQUEST"}
        if "GlobalSecondaryIndexes" not in create_args:
            # Remove GSI-related attribute definitions if no GSI
            pass
        client.create_table(**create_args)
        # Newly created tables sit in CREATING status for a few seconds.
        # Wait until the table is ACTIVE before returning, otherwise
        # immediate Scan/Query calls can raise ResourceNotFoundException.
        client.get_waiter("table_exists").wait(TableName=name)
        log.info("Created DynamoDB table %s", name)

    # Object tracking and snapshot tables have their own schema (GSIs / TTL)
    # defined in their modules — create them here too so init is complete.
    try:
        from .object_tracking_dynamo import ObjectTrackingRepo
        ObjectTrackingRepo.create_table_if_not_exists()
    except Exception as e:
        log.warning("Object tracking table creation skipped/failed: %s", e)
    try:
        from .snapshot_tracking import SnapshotTrackingRepo
        SnapshotTrackingRepo.create_table_if_not_exists()
    except Exception as e:
        log.warning("Snapshot tracking table creation skipped/failed: %s", e)
