# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Progress repository — tracks per-partition progress."""

from datetime import datetime, timezone
from typing import Optional

from .connection import get_conn, run_sql, fetchall, fetchone, commit


class ProgressRepo:
    """CRUD for the migration_progress table.

    Tracks how objects have been processed at the partition level
    and updates the sync status in the pipeline table.
    """

    def upsert(self, pipeline_id: str, partition_key: str,
               partition_value: str, objects_total: int = 0,
               bytes_total: int = 0) -> dict:
        """Create or update a partition progress record."""
        conn = get_conn()
        existing = fetchone(conn,
            "SELECT id FROM migration_progress "
            "WHERE pipeline_id=? AND partition_key=? AND partition_value=?",
            (pipeline_id, partition_key, partition_value))
        if existing:
            run_sql(conn,
                "UPDATE migration_progress SET objects_total=?, "
                "bytes_total=? WHERE id=?",
                (objects_total, bytes_total, existing["id"]))
        else:
            run_sql(conn,
                "INSERT INTO migration_progress "
                "(pipeline_id,partition_key,partition_value,"
                "status,objects_total,bytes_total) "
                "VALUES (?,?,?,?,?,?)",
                (pipeline_id, partition_key, partition_value,
                 "pending", objects_total, bytes_total))
        commit(conn)
        conn.close()
        return self.get(pipeline_id, partition_key, partition_value)

    def get(self, pipeline_id: str, partition_key: str,
            partition_value: str) -> Optional[dict]:
        conn = get_conn()
        row = fetchone(conn,
            "SELECT * FROM migration_progress "
            "WHERE pipeline_id=? AND partition_key=? AND partition_value=?",
            (pipeline_id, partition_key, partition_value))
        conn.close()
        return row

    def list_by_pipeline(self, pipeline_id: str,
                         status: str = None) -> list[dict]:
        conn = get_conn()
        sql = "SELECT * FROM migration_progress WHERE pipeline_id=?"
        params = [pipeline_id]
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY partition_value"
        rows = fetchall(conn, sql, tuple(params))
        conn.close()
        return rows

    def mark_running(self, progress_id: int) -> None:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "UPDATE migration_progress SET status='running', "
            "started_at=? WHERE id=?", (now, progress_id))
        commit(conn)
        conn.close()

    def mark_completed(self, progress_id: int,
                       objects_processed: int = 0,
                       bytes_processed: int = 0,
                       manifest_file: str = "",
                       snapshot_id: int = 0) -> None:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "UPDATE migration_progress SET status='completed', "
            "objects_processed=?, bytes_processed=?, "
            "manifest_file=?, snapshot_id=?, completed_at=? "
            "WHERE id=?",
            (objects_processed, bytes_processed, manifest_file,
             snapshot_id, now, progress_id))
        commit(conn)
        conn.close()

    def mark_failed(self, progress_id: int,
                    error_message: str = "") -> None:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "UPDATE migration_progress SET status='failed', "
            "error_message=?, completed_at=? WHERE id=?",
            (error_message, now, progress_id))
        commit(conn)
        conn.close()

    def compute_sync_status(self, pipeline_id: str) -> str:
        """Compute the overall sync status for a pipeline.

        Returns:
          - 'in_sync':      all partitions completed, no pending objects
          - 'pending_sync': some partitions pending or objects unprocessed
          - 'syncing':      some partitions running
          - 'failed':       any partition failed
          - 'pending':      no progress records yet
        """
        conn = get_conn()
        rows = fetchall(conn,
            "SELECT status, COUNT(*) as cnt FROM migration_progress "
            "WHERE pipeline_id=? GROUP BY status", (pipeline_id,))
        conn.close()

        if not rows:
            return "pending"

        counts = {r["status"]: r["cnt"] for r in rows}
        if counts.get("failed", 0) > 0:
            return "failed"
        if counts.get("running", 0) > 0:
            return "syncing"
        if counts.get("pending", 0) > 0:
            return "pending_sync"
        return "in_sync"
