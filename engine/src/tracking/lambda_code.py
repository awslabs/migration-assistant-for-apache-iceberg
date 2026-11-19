# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Template for the S3 event tracking Lambda function.

Each pipeline gets its own Lambda that watches a specific S3 prefix.
Events are written to the object_tracking table in the engine database.
"""

LAMBDA_CODE_TEMPLATE = '''
import json
import os
import urllib.parse
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3

# Environment variables set by the tracking manager
PIPELINE_ID = os.environ["PIPELINE_ID"]
TABLE_NAME = os.environ.get("TRACKING_TABLE", "ima-object-tracking")
TTL_DAYS = 7


def handler(event, context):
    """Process S3 event notifications and write to DynamoDB tracking table."""
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200, "body": "No records"}

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)
    now = datetime.now(timezone.utc).isoformat()
    expires_at = int((datetime.now(timezone.utc) + timedelta(days=TTL_DAYS)).timestamp())

    with table.batch_writer() as batch:
        for record in records:
            event_name = record.get("eventName", "")
            s3_info = record.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name", "")
            key = urllib.parse.unquote_plus(s3_info.get("object", {}).get("key", ""))
            size = s3_info.get("object", {}).get("size", 0)
            etag = s3_info.get("object", {}).get("eTag", "")

            if not bucket or not key:
                continue

            # Determine event type
            if "ObjectCreated" in event_name:
                event_type = "created"
            elif "ObjectRemoved" in event_name:
                event_type = "deleted"
            else:
                event_type = "unknown"

            # Extract partition values from Hive-style path
            partition_values = {}
            for part in key.split("/"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    partition_values[k] = v

            item = {
                "pipeline_id": PIPELINE_ID,
                "sk": f"{now}#{key}",
                "bucket": bucket,
                "key": key,
                "event_type": event_type,
                "size_bytes": size or 0,
                "etag": etag or "",
                "last_modified": "",
                "partition_values": json.dumps(partition_values) if partition_values else "",
                "received_at": now,
                "processed_at": "",
                "status": "pending",
                "gsi_pk": f"{PIPELINE_ID}#pending",
                "expires_at": expires_at,
            }
            batch.put_item(Item=item)

    return {"statusCode": 200, "body": f"Recorded {len(records)} events"}
'''
