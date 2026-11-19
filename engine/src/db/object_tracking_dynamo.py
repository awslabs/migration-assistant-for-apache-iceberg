# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Object tracking repository — DynamoDB implementation.

Table: ima-object-tracking
  PK: pipeline_id (String)
  SK: received_at#key (String) — ensures uniqueness and ordering
  GSI: status-pipeline-index
    PK: pipeline_id#status (String)
    SK: received_at (String)
  TTL: expires_at (Number) — auto-delete processed events after 7 days

Events are written by a Lambda function triggered by S3 notifications.
The engine reconcile loop reads pending events and processes them.
"""

import json
import os
import logging
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Key

from ..utils.aws import BOTO_CONFIG
from .dynamo_paging import count_all

log = logging.getLogger(__name__)

_TTL_DAYS = 7


def _table_name() -> str:
    """Resolve the tracking table name at call time (respects settings pushed after startup)."""
    return os.getenv("ENGINE_TRACKING_TABLE", "ima-object-tracking")


def _region() -> str:
    return os.getenv("ENGINE_TRACKING_REGION",
                     os.getenv("AWS_DEFAULT_REGION",
                               os.getenv("AWS_REGION", "us-east-1")))


def _endpoint_url() -> str:
    return os.getenv("ENGINE_DYNAMODB_ENDPOINT", "")  # For DynamoDB Local


def _get_table():
    kwargs = {"region_name": _region()}
    ep = _endpoint_url()
    if ep:
        kwargs["endpoint_url"] = ep
    dynamodb = boto3.resource("dynamodb", config=BOTO_CONFIG, **kwargs)
    return dynamodb.Table(_table_name())


def _make_sk(received_at: str, key: str) -> str:
    """Build sort key: received_at#key."""
    return f"{received_at}#{key}"


def _make_gsi_pk(pipeline_id: str, status: str) -> str:
    """Build GSI partition key: pipeline_id#status."""
    return f"{pipeline_id}#{status}"


class ObjectTrackingRepo:
    """DynamoDB-backed object tracking repository.

    Drop-in replacement for the SQL-based ObjectTrackingRepo.
    """

    def __init__(self):
        pass

    @property
    def _table(self):
        # Resolve lazily so table name/region reflect settings pushed at runtime
        return _get_table()

    def record_event(self, pipeline_id: str, bucket: str, key: str,
                     event_type: str, size_bytes: int = 0,
                     etag: str = "", last_modified: str = "",
                     partition_values: dict = None) -> dict:
        """Record a new S3 object event."""
        now = datetime.now(timezone.utc).isoformat()
        expires_at = int((datetime.now(timezone.utc) + timedelta(days=_TTL_DAYS)).timestamp())

        item = {
            "pipeline_id": pipeline_id,
            "sk": _make_sk(now, key),
            "bucket": bucket,
            "key": key,
            "event_type": event_type,
            "size_bytes": size_bytes,
            "etag": etag or "",
            "last_modified": last_modified or "",
            "partition_values": json.dumps(partition_values) if partition_values else "",
            "received_at": now,
            "processed_at": "",
            "status": "pending",
            "gsi_pk": _make_gsi_pk(pipeline_id, "pending"),
            "expires_at": expires_at,
        }

        self._table.put_item(Item=item)
        return {"pipeline_id": pipeline_id, "bucket": bucket,
                "key": key, "event_type": event_type, "status": "pending"}

    def get_pending(self, pipeline_id: str,
                    limit: int = 1000) -> list[dict]:
        """Get unprocessed events for a pipeline using the GSI."""
        try:
            response = self._table.query(
                IndexName="status-pipeline-index",
                KeyConditionExpression=Key("gsi_pk").eq(_make_gsi_pk(pipeline_id, "pending")),
                Limit=limit,
                ScanIndexForward=True,  # oldest first
            )
        except Exception as e:
            # Fallback: query by pipeline_id PK and filter by status
            # (slower, but works if GSI doesn't exist yet)
            if "specified index" in str(e).lower() or "ValidationException" in str(type(e).__name__):
                log.warning("GSI 'status-pipeline-index' not found, using fallback query")
                from boto3.dynamodb.conditions import Attr
                response = self._table.query(
                    KeyConditionExpression=Key("pipeline_id").eq(pipeline_id),
                    FilterExpression=Attr("status").eq("pending"),
                    Limit=limit,
                    ScanIndexForward=True,
                )
            else:
                raise
        items = response.get("Items", [])
        # Normalize for compatibility with SQL version
        results = []
        for item in items:
            row = dict(item)
            row["id"] = row["sk"]  # Use SK as the event identifier
            pv = row.get("partition_values", "")
            if pv and isinstance(pv, str):
                try:
                    row["partition_values"] = json.loads(pv)
                except (json.JSONDecodeError, TypeError):
                    row["partition_values"] = None
            else:
                row["partition_values"] = None
            results.append(row)
        return results

    def mark_processed(self, event_id: str, pipeline_id: str = "") -> None:
        """Mark an event as processed.

        event_id is the SK value. pipeline_id needed for the primary key.
        """
        if not pipeline_id:
            # Try to extract pipeline_id from context — caller should provide it
            log.warning("mark_processed called without pipeline_id")
            return

        now = datetime.now(timezone.utc).isoformat()
        expires_at = int((datetime.now(timezone.utc) + timedelta(days=_TTL_DAYS)).timestamp())

        self._table.update_item(
            Key={"pipeline_id": pipeline_id, "sk": event_id},
            UpdateExpression="SET #s = :s, processed_at = :pa, gsi_pk = :gsi, expires_at = :ttl",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "processed",
                ":pa": now,
                ":gsi": _make_gsi_pk(pipeline_id, "processed"),
                ":ttl": expires_at,
            },
        )

    def mark_failed(self, event_id: str, pipeline_id: str = "",
                    error: str = "") -> None:
        """Mark an event as failed."""
        if not pipeline_id:
            return

        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pipeline_id, "sk": event_id},
            UpdateExpression="SET #s = :s, processed_at = :pa, gsi_pk = :gsi",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "failed",
                ":pa": now,
                ":gsi": _make_gsi_pk(pipeline_id, "failed"),
            },
        )

    def reset_failed(self, pipeline_id: str) -> int:
        """Reset all failed events for a pipeline back to pending for retry."""
        # Query failed events
        try:
            response = self._table.query(
                IndexName="status-pipeline-index",
                KeyConditionExpression=Key("gsi_pk").eq(_make_gsi_pk(pipeline_id, "failed")),
                Limit=1000,
            )
        except Exception:
            # Fallback if GSI doesn't exist
            from boto3.dynamodb.conditions import Attr
            response = self._table.query(
                KeyConditionExpression=Key("pipeline_id").eq(pipeline_id),
                FilterExpression=Attr("status").eq("failed"),
                Limit=1000,
            )
        items = response.get("Items", [])
        for item in items:
            self._table.update_item(
                Key={"pipeline_id": pipeline_id, "sk": item["sk"]},
                UpdateExpression="SET #s = :s, processed_at = :pa, gsi_pk = :gsi",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": "pending",
                    ":pa": "",
                    ":gsi": _make_gsi_pk(pipeline_id, "pending"),
                },
            )
        return len(items)

    def get_stats(self, pipeline_id: str) -> dict:
        """Get event counts by status for a pipeline."""
        counts = {}
        for status in ("pending", "processed", "failed"):
            try:
                count = count_all(
                    self._table.query,
                    IndexName="status-pipeline-index",
                    KeyConditionExpression=Key("gsi_pk").eq(_make_gsi_pk(pipeline_id, status)),
                )
            except Exception:
                # Fallback if GSI doesn't exist
                from boto3.dynamodb.conditions import Attr
                count = count_all(
                    self._table.query,
                    KeyConditionExpression=Key("pipeline_id").eq(pipeline_id),
                    FilterExpression=Attr("status").eq(status),
                )
            if count > 0:
                counts[status] = count
        return counts

    @classmethod
    def create_table_if_not_exists(cls):
        """Create the DynamoDB table (for setup/testing). Idempotent, waits until ACTIVE."""
        table_name = _table_name()
        kwargs = {"region_name": _region()}
        ep = _endpoint_url()
        if ep:
            kwargs["endpoint_url"] = ep
        client = boto3.client("dynamodb", **kwargs)

        try:
            client.describe_table(TableName=table_name)
            log.info("DynamoDB table %s already exists", table_name)
            return
        except client.exceptions.ResourceNotFoundException:
            pass

        client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "pipeline_id", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pipeline_id", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
                {"AttributeName": "received_at", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "status-pipeline-index",
                    "KeySchema": [
                        {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                        {"AttributeName": "received_at", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        # Wait until ACTIVE so immediate queries don't hit ResourceNotFoundException
        client.get_waiter("table_exists").wait(TableName=table_name)
        # TTL must be set after creation (can't be set while CREATING)
        try:
            client.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={"AttributeName": "expires_at", "Enabled": True})
        except Exception as e:
            log.warning("Failed to enable TTL on %s: %s", table_name, e)
        log.info("Created DynamoDB table %s", table_name)
