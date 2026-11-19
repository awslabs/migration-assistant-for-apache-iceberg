# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Snapshot tracking for Iceberg-to-S3Tables incremental migrations.

Table: ima-snapshots
  PK: pipeline_id (String)
  Attributes:
    snapshot_before: initial snapshot ID at migration start
    snapshot_after_load: snapshot ID after initial load completes
    snapshot_cursor: current high-water mark for iterative sync
    phase: current phase (preparation, initial_load, catch_up, converging, in_sync)
    last_sync_at: timestamp of last successful sync
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3

from ..utils.aws import BOTO_CONFIG

log = logging.getLogger(__name__)

def _table_name() -> str:
    """Resolve the snapshot table name at call time (respects settings pushed after startup)."""
    return os.getenv("ENGINE_SNAPSHOT_TABLE", "ima-snapshots")


def _region() -> str:
    return os.getenv("ENGINE_DYNAMODB_REGION",
                     os.getenv("AWS_DEFAULT_REGION",
                               os.getenv("AWS_REGION", "us-east-1")))


def _endpoint_url() -> str:
    return os.getenv("ENGINE_DYNAMODB_ENDPOINT", "")


def _get_table():
    kwargs = {"region_name": _region()}
    ep = _endpoint_url()
    if ep:
        kwargs["endpoint_url"] = ep
    dynamodb = boto3.resource("dynamodb", config=BOTO_CONFIG, **kwargs)
    return dynamodb.Table(_table_name())


class SnapshotTrackingRepo:
    """Tracks Iceberg snapshot cursors for incremental migrations."""

    def __init__(self):
        pass

    @property
    def _table(self):
        # Resolve lazily so table name/region reflect settings pushed at runtime
        return _get_table()

    def create(self, pipeline_id: str, snapshot_before: str,
               committed_at_before: str = "") -> dict:
        """Initialize snapshot tracking for a pipeline."""
        now = datetime.now(timezone.utc).isoformat()
        item = {
            "pipeline_id": pipeline_id,
            "snapshot_before": snapshot_before,
            "committed_at_before": committed_at_before,
            "snapshot_after_load": "",
            "committed_at_after_load": "",
            "snapshot_cursor": "",
            "committed_at_cursor": "",
            "phase": "preparation",
            "convergence_iterations": 0,
            "last_sync_at": "",
            "created_at": now,
            "updated_at": now,
        }
        self._table.put_item(Item=item)
        return item

    def get(self, pipeline_id: str) -> Optional[dict]:
        """Get snapshot tracking state for a pipeline."""
        response = self._table.get_item(Key={"pipeline_id": pipeline_id})
        return response.get("Item")

    def update_phase(self, pipeline_id: str, phase: str) -> None:
        """Update the current phase."""
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pipeline_id},
            UpdateExpression="SET phase = :p, updated_at = :now",
            ExpressionAttributeValues={":p": phase, ":now": now},
        )

    def set_snapshot_after_load(self, pipeline_id: str,
                                snapshot_after_load: str,
                                committed_at_after_load: str = "") -> None:
        """Record the snapshot ID after initial load completes."""
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pipeline_id},
            UpdateExpression="SET snapshot_after_load = :s, snapshot_cursor = :s, "
                            "committed_at_after_load = :ca, committed_at_cursor = :ca, "
                            "phase = :p, updated_at = :now",
            ExpressionAttributeValues={
                ":s": snapshot_after_load, ":ca": committed_at_after_load,
                ":p": "catch_up", ":now": now},
        )

    def update_cursor(self, pipeline_id: str, snapshot_cursor: str,
                      phase: str = "in_sync",
                      committed_at_cursor: str = "") -> None:
        """Update the snapshot cursor (high-water mark)."""
        now = datetime.now(timezone.utc).isoformat()
        self._table.update_item(
            Key={"pipeline_id": pipeline_id},
            UpdateExpression="SET snapshot_cursor = :c, committed_at_cursor = :ca, "
                            "phase = :p, last_sync_at = :now, updated_at = :now",
            ExpressionAttributeValues={
                ":c": snapshot_cursor, ":ca": committed_at_cursor,
                ":p": phase, ":now": now},
        )

    def increment_convergence(self, pipeline_id: str) -> int:
        """Increment convergence iteration counter. Returns new count."""
        response = self._table.update_item(
            Key={"pipeline_id": pipeline_id},
            UpdateExpression="SET convergence_iterations = convergence_iterations + :one",
            ExpressionAttributeValues={":one": 1},
            ReturnValues="UPDATED_NEW",
        )
        return int(response.get("Attributes", {}).get("convergence_iterations", 0))

    def reset_convergence(self, pipeline_id: str) -> None:
        """Reset convergence counter (when entering in_sync)."""
        self._table.update_item(
            Key={"pipeline_id": pipeline_id},
            UpdateExpression="SET convergence_iterations = :zero",
            ExpressionAttributeValues={":zero": 0},
        )

    def delete(self, pipeline_id: str) -> None:
        """Delete snapshot tracking for a pipeline."""
        self._table.delete_item(Key={"pipeline_id": pipeline_id})

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
            KeySchema=[{"AttributeName": "pipeline_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pipeline_id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        # Wait until ACTIVE so immediate reads/writes don't hit ResourceNotFoundException
        client.get_waiter("table_exists").wait(TableName=table_name)
        log.info("Created DynamoDB table %s", table_name)
