# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Database connection and schema initialization."""

import os
import sqlite3
from pathlib import Path



SQLITE_PATH = Path(__file__).parent.parent.parent / "data" / "engine.db"


def _load_config() -> dict:
    """Settings from environment variables, with local-development defaults."""
    return {
        "host": os.getenv("ENGINE_DB_HOST", "localhost"),
        "port": int(os.getenv("ENGINE_DB_PORT", 5432)),
        "name": os.getenv("ENGINE_DB_NAME", "ima_engine"),
        "user": os.getenv("ENGINE_DB_USER", "ima_admin"),
        "password": os.getenv("ENGINE_DB_PASSWORD", ""),
        "engine": os.getenv("ENGINE_DB_ENGINE", "postgres"),
    }


_config = _load_config()
_is_pg = _config["engine"] == "postgres"

# ── Postgres connection pool ─────────────────────────────────────────────
# Every repo method does get_conn() ... conn.close(). Under the enlarged
# reconcile pool (ENGINE_RECONCILE_WORKERS) plus the API, opening a fresh
# psycopg2 connection per call churns connections and can hit Aurora's
# max_connections. We pool connections and make the returned connection's
# close() hand it back to the pool instead of tearing down the socket, so no
# repo call site has to change.
#
# Sizing: ENGINE_DB_POOL_MAX should be >= reconcile workers + API headroom,
# and must stay under the DB's max_connections. Default 60.
_DB_POOL_MIN = int(os.getenv("ENGINE_DB_POOL_MIN", "1"))
_DB_POOL_MAX = int(os.getenv("ENGINE_DB_POOL_MAX", "60"))
_pg_pool = None
_pg_pool_lock = None


def _get_pg_pool():
    global _pg_pool, _pg_pool_lock
    if _pg_pool_lock is None:
        import threading
        _pg_pool_lock = threading.Lock()
    if _pg_pool is None:
        with _pg_pool_lock:
            if _pg_pool is None:
                from psycopg2.pool import ThreadedConnectionPool
                _pg_pool = ThreadedConnectionPool(
                    _DB_POOL_MIN, _DB_POOL_MAX,
                    host=_config["host"],
                    port=_config["port"],
                    dbname=_config["name"],
                    user=_config["user"],
                    password=_config["password"],
                    sslmode=_config.get("sslmode", "require"),
                )
    return _pg_pool


class _PooledConnection:
    """Proxy that returns its psycopg2 connection to the pool on close().

    Preserves the existing get_conn()/conn.close() contract: repos keep calling
    close(), but the underlying connection is recycled rather than destroyed.
    All other attribute access (cursor, commit, rollback, ...) passes through.
    """

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn
        self._returned = False

    def close(self):
        if not self._returned:
            self._returned = True
            try:
                self._pool.putconn(self._conn)
            except Exception:
                # If returning fails, discard the connection so the pool can
                # replace it rather than leaking a bad handle.
                try:
                    self._pool.putconn(self._conn, close=True)
                except Exception:
                    pass

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_conn():
    """Get a database connection.

    Postgres connections come from a shared pool and are returned to it on
    close(); SQLite opens a fresh connection per call (file-based, cheap).
    """
    if _is_pg:
        pool = _get_pg_pool()
        conn = pool.getconn()
        conn.autocommit = True
        return _PooledConnection(pool, conn)
    else:
        SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(SQLITE_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn


def run_sql(conn, sql: str, params=None):
    """Execute SQL with placeholder conversion."""
    if _is_pg:
        sql = sql.replace("?", "%s")
    cur = conn.cursor()
    cur.execute(sql, params) if params else cur.execute(sql)
    return cur


def fetchall(conn, sql: str, params=None) -> list[dict]:
    cur = run_sql(conn, sql, params)
    if _is_pg:
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    else:
        return [dict(row) for row in cur.fetchall()]


def fetchone(conn, sql: str, params=None) -> dict | None:
    cur = run_sql(conn, sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    if _is_pg:
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    else:
        return dict(row)


def commit(conn):
    if not _is_pg:
        conn.commit()


def init_db():
    """Create the migration tracking tables."""
    conn = get_conn()

    if _is_pg:
        # ── Migration Pipeline ──
        run_sql(conn, """CREATE TABLE IF NOT EXISTS migration_pipeline (
            id TEXT PRIMARY KEY,
            source_database TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_format TEXT NOT NULL,
            source_location TEXT NOT NULL,
            source_account TEXT NOT NULL DEFAULT '',
            source_region TEXT NOT NULL DEFAULT 'us-east-1',
            source_catalog TEXT NOT NULL DEFAULT 'glue',
            source_partitioned BOOLEAN NOT NULL DEFAULT FALSE,
            source_partition_keys JSONB DEFAULT '[]',
            source_compression TEXT DEFAULT '',
            target_type TEXT NOT NULL DEFAULT 's3_tables',
            target_database TEXT NOT NULL,
            target_table TEXT NOT NULL,
            target_location TEXT DEFAULT '',
            target_bucket_arn TEXT DEFAULT '',
            target_catalog TEXT DEFAULT 'glue',
            target_account TEXT DEFAULT '',
            target_region TEXT DEFAULT 'us-east-1',
            migration_type TEXT NOT NULL,
            execution_engine TEXT DEFAULT 'athena',
            iam_role_arn TEXT DEFAULT '',
            partition_spec JSONB DEFAULT '[]',
            sort_order JSONB DEFAULT '[]',
            schema_overrides JSONB DEFAULT '[]',
            table_properties JSONB DEFAULT '{}',
            spec_json JSONB,
            status TEXT NOT NULL DEFAULT 'pending',
            sync_status TEXT NOT NULL DEFAULT 'pending',
            emr_job_run_id TEXT DEFAULT '',
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            partitions_total INTEGER DEFAULT 0,
            partitions_completed INTEGER DEFAULT 0,
            bytes_total BIGINT DEFAULT 0,
            bytes_processed BIGINT DEFAULT 0,
            rows_total BIGINT DEFAULT 0,
            rows_processed BIGINT DEFAULT 0,
            UNIQUE(source_database, source_table, source_account))""")

        # Partial index for the reconcile loop's hot query
        # (list_all(status='running')). Without it, every sweep does a full-table
        # scan of migration_pipeline that grows with total pipeline history.
        run_sql(conn, """CREATE INDEX IF NOT EXISTS idx_pipeline_running
            ON migration_pipeline (status) WHERE status='running'""")

        # ── Object Tracking ──
        run_sql(conn, """CREATE TABLE IF NOT EXISTS object_tracking (
            id BIGSERIAL PRIMARY KEY,
            pipeline_id TEXT NOT NULL REFERENCES migration_pipeline(id),
            bucket TEXT NOT NULL,
            key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            size_bytes BIGINT DEFAULT 0,
            etag TEXT,
            last_modified TIMESTAMPTZ,
            partition_values JSONB,
            received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'pending')""")

        run_sql(conn, """CREATE INDEX IF NOT EXISTS idx_ot_pipeline_status
            ON object_tracking (pipeline_id, status)""")
        run_sql(conn, """CREATE INDEX IF NOT EXISTS idx_ot_bucket_key
            ON object_tracking (bucket, key)""")

        # ── Migration Progress ──
        run_sql(conn, """CREATE TABLE IF NOT EXISTS migration_progress (
            id BIGSERIAL PRIMARY KEY,
            pipeline_id TEXT NOT NULL REFERENCES migration_pipeline(id),
            partition_key TEXT,
            partition_value TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            objects_total INTEGER DEFAULT 0,
            objects_processed INTEGER DEFAULT 0,
            bytes_total BIGINT DEFAULT 0,
            bytes_processed BIGINT DEFAULT 0,
            manifest_file TEXT,
            snapshot_id BIGINT,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            error_message TEXT,
            UNIQUE(pipeline_id, partition_key, partition_value))""")

        run_sql(conn, """CREATE INDEX IF NOT EXISTS idx_mp_pipeline_status
            ON migration_progress (pipeline_id, status)""")

        # ── Incremental Batch Tracking ──
        run_sql(conn, """CREATE TABLE IF NOT EXISTS incremental_batch (
            id BIGSERIAL PRIMARY KEY,
            pipeline_id TEXT NOT NULL REFERENCES migration_pipeline(id),
            batch_number INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            partitions JSONB NOT NULL DEFAULT '[]',
            partition_count INTEGER NOT NULL DEFAULT 0,
            emr_job_run_id TEXT DEFAULT '',
            bytes_total BIGINT DEFAULT 0,
            bytes_processed BIGINT DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            UNIQUE(pipeline_id, batch_number))""")

        run_sql(conn, """CREATE INDEX IF NOT EXISTS idx_ib_pipeline_status
            ON incremental_batch (pipeline_id, status)""")

    else:
        # SQLite DDL
        run_sql(conn, """CREATE TABLE IF NOT EXISTS migration_pipeline (
            id TEXT PRIMARY KEY,
            source_database TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_format TEXT NOT NULL,
            source_location TEXT NOT NULL,
            source_account TEXT NOT NULL DEFAULT '',
            source_region TEXT NOT NULL DEFAULT 'us-east-1',
            source_catalog TEXT NOT NULL DEFAULT 'glue',
            source_partitioned INTEGER NOT NULL DEFAULT 0,
            source_partition_keys TEXT DEFAULT '[]',
            source_compression TEXT DEFAULT '',
            target_type TEXT NOT NULL DEFAULT 's3_tables',
            target_database TEXT NOT NULL,
            target_table TEXT NOT NULL,
            target_location TEXT DEFAULT '',
            target_bucket_arn TEXT DEFAULT '',
            target_catalog TEXT DEFAULT 'glue',
            target_account TEXT DEFAULT '',
            target_region TEXT DEFAULT 'us-east-1',
            migration_type TEXT NOT NULL,
            execution_engine TEXT DEFAULT 'athena',
            iam_role_arn TEXT DEFAULT '',
            partition_spec TEXT DEFAULT '[]',
            sort_order TEXT DEFAULT '[]',
            schema_overrides TEXT DEFAULT '[]',
            table_properties TEXT DEFAULT '{}',
            spec_json TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            sync_status TEXT NOT NULL DEFAULT 'pending',
            emr_job_run_id TEXT DEFAULT '',
            error_message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            partitions_total INTEGER DEFAULT 0,
            partitions_completed INTEGER DEFAULT 0,
            bytes_total INTEGER DEFAULT 0,
            bytes_processed INTEGER DEFAULT 0,
            rows_total INTEGER DEFAULT 0,
            rows_processed INTEGER DEFAULT 0,
            UNIQUE(source_database, source_table, source_account))""")

        # Index for the reconcile loop's hot query (list_all(status='running')).
        run_sql(conn, """CREATE INDEX IF NOT EXISTS idx_pipeline_status
            ON migration_pipeline (status)""")

        run_sql(conn, """CREATE TABLE IF NOT EXISTS object_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL REFERENCES migration_pipeline(id),
            bucket TEXT NOT NULL,
            key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            size_bytes INTEGER DEFAULT 0,
            etag TEXT,
            last_modified TEXT,
            partition_values TEXT,
            received_at TEXT NOT NULL,
            processed_at TEXT,
            status TEXT NOT NULL DEFAULT 'pending')""")

        run_sql(conn, """CREATE TABLE IF NOT EXISTS migration_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL REFERENCES migration_pipeline(id),
            partition_key TEXT,
            partition_value TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            objects_total INTEGER DEFAULT 0,
            objects_processed INTEGER DEFAULT 0,
            bytes_total INTEGER DEFAULT 0,
            bytes_processed INTEGER DEFAULT 0,
            manifest_file TEXT,
            snapshot_id INTEGER,
            started_at TEXT,
            completed_at TEXT,
            error_message TEXT,
            UNIQUE(pipeline_id, partition_key, partition_value))""")

        run_sql(conn, """CREATE TABLE IF NOT EXISTS incremental_batch (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id TEXT NOT NULL REFERENCES migration_pipeline(id),
            batch_number INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            partitions TEXT NOT NULL DEFAULT '[]',
            partition_count INTEGER NOT NULL DEFAULT 0,
            emr_job_run_id TEXT DEFAULT '',
            bytes_total INTEGER DEFAULT 0,
            bytes_processed INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            UNIQUE(pipeline_id, batch_number))""")

    commit(conn)
    conn.close()
    print(f"Engine DB initialized ({_config['engine']})")
