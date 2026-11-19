# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Object tracking repository — records S3 PUT/DELETE events."""

import json
from datetime import datetime, timezone

from .connection import get_conn, run_sql, fetchall, commit


class ObjectTrackingRepo:
    """CRUD for the object_tracking table.

    Records are created by a Lambda function triggered by S3 events.
    The migration engine processes them and updates status.
    """

    def record_event(self, pipeline_id: str, bucket: str, key: str,
                     event_type: str, size_bytes: int = 0,
                     etag: str = "", last_modified: str = "",
                     partition_values: dict = None) -> dict:
        """Record a new S3 object event (PUT or DELETE)."""
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        pv = json.dumps(partition_values) if partition_values else None
        run_sql(conn,
            "INSERT INTO object_tracking "
            "(pipeline_id,bucket,key,event_type,size_bytes,etag,"
            "last_modified,partition_values,received_at,status) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (pipeline_id, bucket, key, event_type, size_bytes,
             etag, last_modified, pv, now, "pending"))
        commit(conn)
        conn.close()
        return {"pipeline_id": pipeline_id, "bucket": bucket,
                "key": key, "event_type": event_type, "status": "pending"}

    def get_pending(self, pipeline_id: str,
                    limit: int = 1000) -> list[dict]:
        """Get unprocessed events for a pipeline."""
        conn = get_conn()
        rows = fetchall(conn,
            "SELECT * FROM object_tracking "
            "WHERE pipeline_id=? AND status='pending' "
            "ORDER BY received_at ASC LIMIT ?",
            (pipeline_id, limit))
        conn.close()
        return rows

    def mark_processed(self, event_id: int, pipeline_id: str = "") -> None:
        """Mark an event as processed."""
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "UPDATE object_tracking SET status='processed', "
            "processed_at=? WHERE id=?", (now, event_id))
        commit(conn)
        conn.close()

    def mark_failed(self, event_id: int, pipeline_id: str = "",
                    error: str = "") -> None:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "UPDATE object_tracking SET status='failed', "
            "processed_at=? WHERE id=?", (now, event_id))
        commit(conn)
        conn.close()

    def reset_failed(self, pipeline_id: str) -> int:
        """Reset all failed events for a pipeline back to pending for retry."""
        conn = get_conn()
        cur = run_sql(conn,
            "UPDATE object_tracking SET status='pending', processed_at=NULL "
            "WHERE pipeline_id=? AND status='failed'", (pipeline_id,))
        commit(conn)
        count = cur.rowcount
        conn.close()
        return count

    def get_stats(self, pipeline_id: str) -> dict:
        """Get event counts by status for a pipeline."""
        conn = get_conn()
        rows = fetchall(conn,
            "SELECT status, COUNT(*) as cnt FROM object_tracking "
            "WHERE pipeline_id=? GROUP BY status", (pipeline_id,))
        conn.close()
        return {r["status"]: r["cnt"] for r in rows}
