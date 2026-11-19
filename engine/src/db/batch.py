# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Incremental batch repository — tracks batched partition loads."""

import json
from datetime import datetime, timezone
from typing import Optional

from .connection import get_conn, run_sql, fetchall, fetchone, commit


class BatchRepo:
    """CRUD for the incremental_batch table."""

    def create(self, pipeline_id: str, batch_number: int,
               partitions: list[str], bytes_total: int = 0) -> dict:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "INSERT INTO incremental_batch "
            "(pipeline_id, batch_number, status, partitions, "
            "partition_count, bytes_total, created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (pipeline_id, batch_number, "pending",
             json.dumps(partitions), len(partitions),
             bytes_total, now))
        commit(conn)
        conn.close()
        return self.get_by_number(pipeline_id, batch_number)

    def get_by_number(self, pipeline_id: str,
                      batch_number: int) -> Optional[dict]:
        conn = get_conn()
        row = fetchone(conn,
            "SELECT * FROM incremental_batch "
            "WHERE pipeline_id=? AND batch_number=?",
            (pipeline_id, batch_number))
        conn.close()
        return row

    def get_running(self, pipeline_id: str) -> Optional[dict]:
        """Get the currently running batch.

        Orders by batch_number DESC so the MOST RECENT running batch wins if
        more than one is ever running at once. This matters for Managed
        Migration phase detection: a sync batch has a higher batch_number than
        the initial-load batch, and the current phase must reflect the sync.
        """
        conn = get_conn()
        row = fetchone(conn,
            "SELECT * FROM incremental_batch "
            "WHERE pipeline_id=? AND status='running' "
            "ORDER BY batch_number DESC LIMIT 1", (pipeline_id,))
        conn.close()
        return row

    def mark_running(self, batch_id: int, emr_job_run_id: str,
                     pipeline_id: str = "") -> None:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        if pipeline_id:
            run_sql(conn,
                "UPDATE incremental_batch SET status='running', "
                "emr_job_run_id=?, started_at=? WHERE pipeline_id=? AND batch_number=?",
                (emr_job_run_id, now, pipeline_id, batch_id))
        else:
            run_sql(conn,
                "UPDATE incremental_batch SET status='running', "
                "emr_job_run_id=?, started_at=? WHERE id=?",
                (emr_job_run_id, now, batch_id))
        commit(conn)
        conn.close()

    def mark_completed(self, batch_id: int,
                       bytes_processed: int = 0,
                       pipeline_id: str = "",
                       started_at: Optional[str] = None,
                       completed_at: Optional[str] = None) -> None:
        """Mark a batch completed.

        started_at / completed_at may be supplied to record the job's ACTUAL
        execution window (e.g. Athena's SubmissionDateTime / CompletionDateTime)
        instead of the reconcile-loop observation time. This keeps batch
        durations accurate for fast queries that finish inside a single
        reconcile interval — otherwise both timestamps collapse onto poll ticks
        and every batch appears to take ~one interval (default 30s).

        When started_at is None the existing value is preserved; when
        completed_at is None it defaults to now().
        """
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        completed = completed_at or now
        # Use fully static query strings (no interpolation) so there is no
        # dynamically-constructed SQL. started_at is only overwritten when an
        # accurate value is provided; otherwise the submit-time value on the row
        # is preserved. Values are always passed as bound parameters.
        if pipeline_id:
            if started_at:
                run_sql(conn,
                    "UPDATE incremental_batch SET status='completed', "
                    "bytes_processed=?, completed_at=?, started_at=? "
                    "WHERE pipeline_id=? AND batch_number=?",
                    (bytes_processed, completed, started_at, pipeline_id, batch_id))
            else:
                run_sql(conn,
                    "UPDATE incremental_batch SET status='completed', "
                    "bytes_processed=?, completed_at=? "
                    "WHERE pipeline_id=? AND batch_number=?",
                    (bytes_processed, completed, pipeline_id, batch_id))
        else:
            if started_at:
                run_sql(conn,
                    "UPDATE incremental_batch SET status='completed', "
                    "bytes_processed=?, completed_at=?, started_at=? WHERE id=?",
                    (bytes_processed, completed, started_at, batch_id))
            else:
                run_sql(conn,
                    "UPDATE incremental_batch SET status='completed', "
                    "bytes_processed=?, completed_at=? WHERE id=?",
                    (bytes_processed, completed, batch_id))
        commit(conn)
        conn.close()

    def mark_failed(self, batch_id: int,
                    error_message: str = "",
                    pipeline_id: str = "") -> None:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        if pipeline_id:
            run_sql(conn,
                "UPDATE incremental_batch SET status='failed', "
                "error_message=?, completed_at=? WHERE pipeline_id=? AND batch_number=?",
                (error_message, now, pipeline_id, batch_id))
        else:
            run_sql(conn,
                "UPDATE incremental_batch SET status='failed', "
                "error_message=?, completed_at=? WHERE id=?",
                (error_message, now, batch_id))
        commit(conn)
        conn.close()

    def list_by_pipeline(self, pipeline_id: str,
                         status: str = None) -> list[dict]:
        conn = get_conn()
        sql = ("SELECT * FROM incremental_batch "
               "WHERE pipeline_id=?")
        params = [pipeline_id]
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY batch_number ASC"
        rows = fetchall(conn, sql, tuple(params))
        conn.close()
        return rows

    def get_summary(self, pipeline_id: str) -> dict:
        """Get batch counts by status."""
        conn = get_conn()
        rows = fetchall(conn,
            "SELECT status, COUNT(*) as cnt, "
            "SUM(partition_count) as partitions "
            "FROM incremental_batch "
            "WHERE pipeline_id=? GROUP BY status",
            (pipeline_id,))
        conn.close()
        return {r["status"]: {"count": r["cnt"],
                              "partitions": r["partitions"] or 0}
                for r in rows}

    def reset_failed_to_pending(self, pipeline_id: str) -> int:
        """Reset all failed batches back to pending for resume."""
        conn = get_conn()
        cur = run_sql(conn,
            "UPDATE incremental_batch SET status='pending', "
            "error_message=NULL, emr_job_run_id='', "
            "started_at=NULL, completed_at=NULL "
            "WHERE pipeline_id=? AND status='failed'",
            (pipeline_id,))
        commit(conn)
        count = cur.rowcount
        conn.close()
        return count

    def delete_from_number(self, pipeline_id: str,
                           min_batch_number: int) -> int:
        """Delete all batches with batch_number >= min_batch_number.

        Used on retry-from-scratch to clear leftover data batches so a
        re-submission starts with a clean slate (batch 0 is recreated by
        the strategy). Returns the number of batches deleted.
        """
        conn = get_conn()
        cur = run_sql(conn,
            "DELETE FROM incremental_batch "
            "WHERE pipeline_id=? AND batch_number>=?",
            (pipeline_id, min_batch_number))
        commit(conn)
        count = cur.rowcount
        conn.close()
        return count
