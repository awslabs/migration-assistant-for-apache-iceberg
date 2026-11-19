# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CloudTrail tools for access pattern analysis."""

from datetime import datetime, timedelta
from strands import tool
from tools.aws import get_client


@tool
def get_s3_access_patterns(
    bucket: str, prefix: str, days_back: int = 30
) -> dict:
    """Analyze CloudTrail for recent S3 access to identify who reads this data.

    Looks up GetObject and SelectObjectContent events to find
    IAM roles/users that have accessed the S3 objects.

    Args:
        bucket: S3 bucket name
        prefix: S3 key prefix
        days_back: How many days back to search (max 90)
    """
    ct = get_client("cloudtrail")
    start = datetime.utcnow() - timedelta(days=min(days_back, 90))

    try:
        resp = ct.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "ResourceType", "AttributeValue": "AWS::S3::Object"}
            ],
            StartTime=start,
            EndTime=datetime.utcnow(),
            MaxResults=50,
        )
        accessors = {}
        for event in resp.get("Events", []):
            for resource in event.get("Resources", []):
                rn = resource.get("ResourceName", "")
                if bucket in rn and prefix in rn:
                    user = event.get("Username", "unknown")
                    if user not in accessors:
                        accessors[user] = {
                            "count": 0,
                            "last_access": None,
                            "event_names": set(),
                        }
                    accessors[user]["count"] += 1
                    accessors[user]["last_access"] = event["EventTime"].isoformat()
                    accessors[user]["event_names"].add(event.get("EventName", ""))

        for v in accessors.values():
            v["event_names"] = list(v["event_names"])

        return {
            "bucket": bucket,
            "prefix": prefix,
            "days_searched": days_back,
            "unique_accessors": len(accessors),
            "accessors": accessors,
        }
    except Exception as e:
        return {"error": str(e), "note": "CloudTrail lookup may require specific permissions."}


@tool
def get_glue_table_access(database: str, table_name: str, days_back: int = 30) -> dict:
    """Check CloudTrail for Glue GetTable/GetPartitions calls to see who queries this table.

    Args:
        database: Glue database name
        table_name: Table name
        days_back: Days to look back
    """
    ct = get_client("cloudtrail")
    start = datetime.utcnow() - timedelta(days=min(days_back, 90))

    try:
        resp = ct.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "EventName", "AttributeValue": "GetTable"}
            ],
            StartTime=start,
            EndTime=datetime.utcnow(),
            MaxResults=50,
        )
        accessors = {}
        for event in resp.get("Events", []):
            cloud_trail_event = event.get("CloudTrailEvent", "")
            if database in cloud_trail_event and table_name in cloud_trail_event:
                user = event.get("Username", "unknown")
                if user not in accessors:
                    accessors[user] = {"count": 0, "last_access": None}
                accessors[user]["count"] += 1
                accessors[user]["last_access"] = event["EventTime"].isoformat()

        return {
            "database": database,
            "table_name": table_name,
            "days_searched": days_back,
            "unique_accessors": len(accessors),
            "accessors": accessors,
        }
    except Exception as e:
        return {"error": str(e)}
