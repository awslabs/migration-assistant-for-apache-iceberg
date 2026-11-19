# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pipeline repository."""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from .connection import get_conn, run_sql, fetchall, fetchone, commit
from . import status_guard

log = logging.getLogger(__name__)


class PipelineRepo:
    """CRUD for the migration_pipeline table."""

    def exists(self, source_database: str, source_table: str,
               source_account: str) -> Optional[dict]:
        """Check if a pipeline already exists for this source."""
        conn = get_conn()
        row = fetchone(conn,
            "SELECT * FROM migration_pipeline "
            "WHERE source_database=? AND source_table=? AND source_account=?",
            (source_database, source_table, source_account))
        conn.close()
        return row

    def create_from_spec(self, spec_dict: dict) -> dict:
        """Create a pipeline record from a MigrationSpec dict."""
        src = spec_dict["source"]
        tgt = spec_dict["target"]
        strat = spec_dict["strategy"]
        ip = spec_dict.get("iceberg_properties", {})

        # Uniqueness check
        existing = self.exists(
            src["database"], src["table_name"], src["aws_account_id"])
        if existing:
            raise ValueError(
                f"Pipeline already exists for "
                f"{src['database']}.{src['table_name']} "
                f"(account {src['aws_account_id']}). "
                f"Existing pipeline ID: {existing['id']}")

        conn = get_conn()
        pid = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        run_sql(conn,
            "INSERT INTO migration_pipeline "
            "(id, source_database, source_table, source_format, "
            "source_location, source_account, source_region, "
            "source_catalog, source_partitioned, source_partition_keys, "
            "source_compression, "
            "target_type, target_database, target_table, "
            "target_location, target_bucket_arn, target_catalog, "
            "target_account, target_region, "
            "migration_type, execution_engine, iam_role_arn, "
            "partition_spec, sort_order, schema_overrides, "
            "table_properties, spec_json, "
            "status, sync_status, "
            "created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (pid,
             src["database"], src["table_name"], src["format"],
             src["location"], src["aws_account_id"],
             src.get("region", "us-east-1"),
             src.get("catalog", "glue"),
             1 if src.get("partitioned") else 0,
             json.dumps(src.get("partition_keys", [])),
             src.get("compression", ""),
             tgt["type"], tgt["database"], tgt["table_name"],
             tgt.get("location", ""),
             tgt.get("table_bucket_arn", ""),
             tgt.get("catalog", "glue"),
             tgt.get("aws_account_id", ""),
             tgt.get("region", "us-east-1"),
             strat["type"],
             strat.get("execution_engine", "athena"),
             strat.get("iam_role_arn", ""),
             json.dumps(ip.get("partition_spec", [])),
             json.dumps(ip.get("sort_order", [])),
             json.dumps(ip.get("schema_overrides", [])),
             json.dumps(ip.get("table_properties", {})),
             json.dumps(spec_dict),
             "pending", "pending",
             now, now))
        commit(conn)
        conn.close()
        return self.get(pid)

    def get(self, pipeline_id: str, consistent: bool = False) -> Optional[dict]:
        # `consistent` matches the DynamoDB repo's signature; SQL reads are
        # always consistent.
        conn = get_conn()
        row = fetchone(conn, "SELECT * FROM migration_pipeline WHERE id=?",
                       (pipeline_id,))
        conn.close()
        return row

    def list_all(self, status: str = None) -> list[dict]:
        conn = get_conn()
        sql = "SELECT * FROM migration_pipeline WHERE 1=1"
        params = []
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC"
        rows = fetchall(conn, sql, tuple(params) if params else None)
        conn.close()
        return rows

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
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        sets = ["status=?", "updated_at=?"]
        params = [status, now]
        if status == "running":
            # Only stamp started_at on the FIRST transition into running. The
            # managed-migration reconcile loop re-sets status to "running" on
            # every poll, so unconditionally writing started_at here would keep
            # resetting it to "now" — making the Started field always show the
            # current time. Preserve the original start timestamp once set,
            # unless the caller explicitly restarts the run (reset_started_at).
            if reset_started_at:
                sets.append("started_at=?")
                params.append(now)
            else:
                # Set once, in the same statement (no pre-read). started_at
                # is NULL until first set; TIMESTAMPTZ on Postgres, so no ''
                # comparison here.
                sets.append("started_at=COALESCE(started_at, ?)")
                params.append(now)
        elif status in ("completed", "failed"):
            sets.append("completed_at=?")
            params.append(now)
        if error_message is not None:
            sets.append("error_message=?")
            params.append(error_message)
        where = ["id=?"]
        params.append(pipeline_id)
        if not force:
            terminal = sorted(status_guard.TERMINAL_STATUSES)
            where.append(
                f"(status IS NULL OR status NOT IN ({','.join('?' * len(terminal))}))")
            params.extend(terminal)
        if expected:
            where.append("status=?")
            params.append(expected)
        cur = run_sql(conn,
            f"UPDATE migration_pipeline SET {','.join(sets)} "  # nosec B608
            f"WHERE {' AND '.join(where)}",
            tuple(params))
        changed = cur.rowcount
        commit(conn)
        conn.close()
        current = self.get(pipeline_id)
        if changed == 0 and current is not None:
            log.info("Pipeline %s: status write '%s' refused (now '%s'%s)",
                     pipeline_id[:8], status, current.get("status"),
                     f", expected '{expected}'" if expected else "")
            return current
        status_guard.record_write(pipeline_id, status)
        return current

    def update_sync_status(self, pipeline_id: str,
                           sync_status: str) -> Optional[dict]:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "UPDATE migration_pipeline SET sync_status=?, updated_at=? "
            "WHERE id=?", (sync_status, now, pipeline_id))
        commit(conn)
        conn.close()
        return self.get(pipeline_id)

    def update_metadata(self, pipeline_id: str, key: str, value: str) -> None:
        """Store an arbitrary metadata key-value on the pipeline's spec_json."""
        conn = get_conn()
        row = fetchone(conn,
            "SELECT spec_json FROM migration_pipeline WHERE id=?",
            (pipeline_id,))
        if row:
            spec = row.get("spec_json") or "{}"
            if isinstance(spec, str):
                spec = json.loads(spec)
            spec.setdefault("_metadata", {})[key] = value
            run_sql(conn,
                "UPDATE migration_pipeline SET spec_json=? WHERE id=?",
                (json.dumps(spec), pipeline_id))
            commit(conn)
        conn.close()

    def update_emr_job_run_id(self, pipeline_id: str,
                              job_run_id: str) -> Optional[dict]:
        """Store the EMR Serverless job run ID on the pipeline."""
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        run_sql(conn,
            "UPDATE migration_pipeline SET emr_job_run_id=?, updated_at=? "
            "WHERE id=?", (job_run_id, now, pipeline_id))
        commit(conn)
        conn.close()
        return self.get(pipeline_id)

    def update_progress(self, pipeline_id: str,
                        bytes_processed: int = 0,
                        rows_processed: int = 0,
                        partitions_completed: int = 0,
                        partitions_total: int = None) -> Optional[dict]:
        conn = get_conn()
        now = datetime.now(timezone.utc).isoformat()
        if partitions_total is not None:
            run_sql(conn,
                "UPDATE migration_pipeline SET bytes_processed=?, "
                "rows_processed=?, partitions_completed=?, partitions_total=?, updated_at=? "
                "WHERE id=?",
                (bytes_processed, rows_processed, partitions_completed,
                 partitions_total, now, pipeline_id))
        else:
            run_sql(conn,
                "UPDATE migration_pipeline SET bytes_processed=?, "
                "rows_processed=?, partitions_completed=?, updated_at=? "
                "WHERE id=?",
                (bytes_processed, rows_processed, partitions_completed,
                 now, pipeline_id))
        commit(conn)
        conn.close()
        return self.get(pipeline_id)

    def delete(self, pipeline_id: str) -> bool:
        """Delete a pipeline record and its related tracking data."""
        conn = get_conn()
        run_sql(conn, "DELETE FROM incremental_batch WHERE pipeline_id=?",
                (pipeline_id,))
        run_sql(conn, "DELETE FROM migration_progress WHERE pipeline_id=?",
                (pipeline_id,))
        run_sql(conn, "DELETE FROM object_tracking WHERE pipeline_id=?",
                (pipeline_id,))
        run_sql(conn, "DELETE FROM migration_pipeline WHERE id=?",
                (pipeline_id,))
        commit(conn)
        conn.close()
        return True
