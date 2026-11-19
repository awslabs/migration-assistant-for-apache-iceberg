# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Database layer supporting SQLite (local) and Aurora PostgreSQL (production).

Set DB_TYPE=aurora to use Aurora PostgreSQL, otherwise defaults to SQLite.

Aurora authentication is IAM-only — there is no password-based auth. The
application connects using a short-lived IAM authentication token generated
via the task/execution role. No database password is ever read, stored, or
sent by this application.

Prerequisite: the database user (DB_USER) must have the `rds_iam` role granted
(`GRANT rds_iam TO <user>`) as a one-time DB setup step, and the runtime IAM
role must have `rds-db:connect` permission for that user.
"""
import logging
import os
import sqlite3
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

DB_TYPE = os.getenv("DB_TYPE", "sqlite")
SQLITE_PATH = Path(__file__).parent / "data" / "app.db"

# Aurora PostgreSQL settings (IAM auth only — no password)
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "ima")
DB_USER = os.getenv("DB_USER", "imaadmin")
DB_REGION = os.getenv("DB_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

_is_pg = DB_TYPE == "aurora"

# IAM token cache (valid for 15 minutes, refresh at 12)
_iam_token_cache = {"token": "", "expires_at": 0.0}


def _get_iam_token() -> str:
    """Generate an IAM authentication token for Aurora PostgreSQL.

    Tokens are cached and refreshed 3 minutes before expiry.
    """
    now = time.time()
    if _iam_token_cache["token"] and now < _iam_token_cache["expires_at"]:
        return _iam_token_cache["token"]

    import boto3
    client = boto3.client("rds", region_name=DB_REGION)
    token = client.generate_db_auth_token(
        DBHostname=DB_HOST, Port=DB_PORT, DBUsername=DB_USER, Region=DB_REGION
    )
    _iam_token_cache["token"] = token
    _iam_token_cache["expires_at"] = now + 720  # refresh after 12 minutes
    return token


def get_conn():
    """Get a database connection.

    For Aurora, authentication is IAM-only: a short-lived IAM auth token is
    used as the connection credential over a required TLS connection. There is
    no password-based authentication path.
    """
    if _is_pg:
        import psycopg2

        token = _get_iam_token()
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=token, sslmode="require",
        )
        conn.autocommit = True
        return conn
    else:
        SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(SQLITE_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn


def _run_sql(conn, sql, params=None):
    """Execute SQL, converting ? to %s for PostgreSQL."""
    if _is_pg:
        sql = sql.replace("?", "%s")
    cur = conn.cursor()
    cur.execute(sql, params) if params else cur.execute(sql)
    return cur


def _fetchall(conn, sql, params=None):
    """Fetch all rows as list of dicts."""
    cur = _run_sql(conn, sql, params)
    if _is_pg:
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    else:
        return [dict(row) for row in cur.fetchall()]


def _fetchone(conn, sql, params=None):
    """Fetch one row as dict."""
    cur = _run_sql(conn, sql, params)
    row = cur.fetchone()
    if row is None:
        return None
    if _is_pg:
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))
    else:
        return dict(row)


def _commit(conn):
    """Commit for SQLite only (Aurora uses autocommit)."""
    if not _is_pg:
        conn.commit()


def _safe_add_column(conn, table: str, column: str, col_type: str):
    """Add a column to a table if it doesn't already exist. Silently ignores if exists.

    This is an idempotent schema migration: ALTER TABLE ADD COLUMN raises a
    "duplicate column" error (a different exception type on SQLite vs
    PostgreSQL) when the column is already present, which is the expected
    no-op case. Swallowing it is intentional.
    """
    try:
        _run_sql(conn, f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except Exception:  # nosec B110 - idempotent migration: column already exists
        log.debug("Column %s.%s already exists; skipping add", table, column)


def init_db():
    """Create tables and seed data."""
    conn = get_conn()

    if _is_pg:
        # PostgreSQL DDL
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY, name TEXT NOT NULL,
            password TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'viewer')""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS sessions (
            token_hash TEXT PRIMARY KEY,
            username TEXT NOT NULL REFERENCES users(username),
            created_at DOUBLE PRECISION NOT NULL,
            expires_at DOUBLE PRECISION NOT NULL,
            last_seen_at DOUBLE PRECISION NOT NULL)""")
        _run_sql(conn, "CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username)")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS groups (
            name TEXT PRIMARY KEY, description TEXT DEFAULT '')""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS group_members (
            id SERIAL PRIMARY KEY,
            group_name TEXT NOT NULL REFERENCES groups(name),
            username TEXT NOT NULL REFERENCES users(username),
            role TEXT NOT NULL DEFAULT 'viewer',
            UNIQUE(group_name, username))""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS tables_inventory (
            id SERIAL PRIMARY KEY,
            database_name TEXT NOT NULL, table_name TEXT NOT NULL,
            format TEXT NOT NULL, location TEXT NOT NULL,
            aws_account TEXT NOT NULL DEFAULT '123456789012',
            size_bytes BIGINT NOT NULL,
            partitioned INTEGER NOT NULL DEFAULT 0,
            owner TEXT NOT NULL DEFAULT '',
            UNIQUE(database_name, table_name))""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS migration_plans (
            id TEXT PRIMARY KEY, name TEXT NOT NULL,
            description TEXT DEFAULT '', status TEXT DEFAULT 'draft',
            target TEXT DEFAULT 's3_tables',
            migration_type TEXT DEFAULT '["full_table"]',
            max_table_size_gb INTEGER,
            table_bucket_arn TEXT DEFAULT '',
            owners TEXT DEFAULT '[]',
            notification_emails TEXT DEFAULT '[]',
            archived INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, created_by TEXT NOT NULL)""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS plan_tables (
            id SERIAL PRIMARY KEY,
            plan_id TEXT NOT NULL REFERENCES migration_plans(id),
            source_database TEXT NOT NULL, source_table TEXT NOT NULL,
            target_database TEXT NOT NULL, target_table TEXT NOT NULL,
            migration_type TEXT NOT NULL DEFAULT 'full_table',
            status TEXT NOT NULL DEFAULT 'pending',
            source_format TEXT,
            source_location TEXT,
            source_account TEXT,
            source_partitioned INTEGER NOT NULL DEFAULT 0,
            source_partition_keys TEXT DEFAULT '[]',
            source_compression TEXT,
            target_type TEXT,
            target_location TEXT,
            target_bucket_arn TEXT,
            partition_spec TEXT DEFAULT '[]',
            sort_order TEXT DEFAULT '[]',
            pipeline_id TEXT,
            recommended_type TEXT,
            recommended_batch_size INTEGER,
            estimated_size_gb REAL,
            estimated_cost_usd REAL,
            estimated_time_min REAL,
            analysis_summary TEXT,
            analysis_status TEXT NOT NULL DEFAULT 'pending',
            error_message TEXT,
            schema_compatible INTEGER,
            schema_issues TEXT DEFAULT '[]',
            UNIQUE(plan_id, source_database, source_table))""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY, name TEXT NOT NULL,
            source_type TEXT NOT NULL DEFAULT 'aws_glue_data_catalog',
            aws_account_id TEXT NOT NULL,
            iam_role_arn TEXT DEFAULT '',
            default_group TEXT DEFAULT '',
            db_filter_mode TEXT DEFAULT 'exclude',
            db_filter_list TEXT DEFAULT '',
            last_refreshed_at TEXT,
            last_refresh_summary TEXT,
            created_at TEXT NOT NULL, created_by TEXT NOT NULL)""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS inventory_refresh_log (
            id SERIAL PRIMARY KEY,
            source_id TEXT NOT NULL,
            database_name TEXT,
            table_name TEXT,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL,
            created_at TEXT NOT NULL)""")
    else:
        # SQLite DDL
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY, name TEXT NOT NULL,
            password TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'viewer')""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS sessions (
            token_hash TEXT PRIMARY KEY,
            username TEXT NOT NULL REFERENCES users(username),
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            last_seen_at REAL NOT NULL)""")
        _run_sql(conn, "CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions(username)")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS groups (
            name TEXT PRIMARY KEY, description TEXT DEFAULT '')""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS group_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT NOT NULL REFERENCES groups(name),
            username TEXT NOT NULL REFERENCES users(username),
            role TEXT NOT NULL DEFAULT 'viewer',
            UNIQUE(group_name, username))""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS tables_inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            database_name TEXT NOT NULL, table_name TEXT NOT NULL,
            format TEXT NOT NULL, location TEXT NOT NULL,
            aws_account TEXT NOT NULL DEFAULT '123456789012',
            size_bytes INTEGER NOT NULL,
            partitioned INTEGER NOT NULL DEFAULT 0,
            owner TEXT NOT NULL DEFAULT '',
            UNIQUE(database_name, table_name))""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS migration_plans (
            id TEXT PRIMARY KEY, name TEXT NOT NULL,
            description TEXT DEFAULT '', status TEXT DEFAULT 'draft',
            target TEXT DEFAULT 's3_tables',
            migration_type TEXT DEFAULT '["full_table"]',
            max_table_size_gb INTEGER,
            table_bucket_arn TEXT DEFAULT '',
            owners TEXT DEFAULT '[]',
            notification_emails TEXT DEFAULT '[]',
            archived INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, created_by TEXT NOT NULL)""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS plan_tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id TEXT NOT NULL REFERENCES migration_plans(id),
            source_database TEXT NOT NULL, source_table TEXT NOT NULL,
            target_database TEXT NOT NULL, target_table TEXT NOT NULL,
            migration_type TEXT NOT NULL DEFAULT 'full_table',
            status TEXT NOT NULL DEFAULT 'pending',
            source_format TEXT,
            source_location TEXT,
            source_account TEXT,
            source_partitioned INTEGER NOT NULL DEFAULT 0,
            source_partition_keys TEXT DEFAULT '[]',
            source_compression TEXT,
            target_type TEXT,
            target_location TEXT,
            target_bucket_arn TEXT,
            partition_spec TEXT DEFAULT '[]',
            sort_order TEXT DEFAULT '[]',
            pipeline_id TEXT,
            recommended_type TEXT,
            recommended_batch_size INTEGER,
            estimated_size_gb REAL,
            estimated_cost_usd REAL,
            estimated_time_min REAL,
            analysis_summary TEXT,
            analysis_status TEXT NOT NULL DEFAULT 'pending',
            error_message TEXT,
            schema_compatible INTEGER,
            schema_issues TEXT DEFAULT '[]',
            UNIQUE(plan_id, source_database, source_table))""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY, name TEXT NOT NULL,
            source_type TEXT NOT NULL DEFAULT 'aws_glue_data_catalog',
            aws_account_id TEXT NOT NULL,
            iam_role_arn TEXT DEFAULT '',
            default_group TEXT DEFAULT '',
            db_filter_mode TEXT DEFAULT 'exclude',
            db_filter_list TEXT DEFAULT '',
            last_refreshed_at TEXT,
            last_refresh_summary TEXT,
            created_at TEXT NOT NULL, created_by TEXT NOT NULL)""")
        _run_sql(conn, """CREATE TABLE IF NOT EXISTS inventory_refresh_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            database_name TEXT,
            table_name TEXT,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL,
            created_at TEXT NOT NULL)""")

    # ── Schema migrations (must run BEFORE the seed so new columns exist) ──
    # must_change_password: forces a password change on first login. Seeded
    # accounts (and admin resets) set this to 1; it is cleared once the user
    # sets a password of their own (kill the default admin/admin credential).
    _safe_add_column(conn, "users", "must_change_password", "INTEGER NOT NULL DEFAULT 0")
    # totp_secret / mfa_enabled: TOTP multi-factor authentication.
    _safe_add_column(conn, "users", "totp_secret", "TEXT")
    _safe_add_column(conn, "users", "mfa_enabled", "INTEGER NOT NULL DEFAULT 0")

    # Seed essential data only (admin user + admins group)
    row = _fetchone(conn, "SELECT COUNT(*) as cnt FROM users")
    if row and row["cnt"] == 0:
        import bcrypt
        # The bootstrap admin password is provided out-of-band via the
        # APP_BOOTSTRAP_ADMIN_PASSWORD env var. If unset, we fall back to a
        # random, non-guessable password that is logged once to the server log so
        # the operator can retrieve it — we NEVER seed a static "admin" password.
        # Either way the account is flagged must_change_password=1 so the first
        # login is forced to set a new, policy-compliant password.
        import secrets as _secrets
        import string as _string
        bootstrap_pw = os.getenv("APP_BOOTSTRAP_ADMIN_PASSWORD", "")
        generated = False
        if not bootstrap_pw:
            _alphabet = _string.ascii_letters + _string.digits + "!@#$%^&*"
            bootstrap_pw = "".join(_secrets.choice(_alphabet) for _ in range(24))
            generated = True
        default_pw_hash = bcrypt.hashpw(bootstrap_pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        _run_sql(
            conn,
            "INSERT INTO users (username, name, password, role, must_change_password) "
            "VALUES (?,?,?,?,?)",
            ("admin", "Admin User", default_pw_hash, "admin", 1),
        )
        if generated:
            # NEVER log the generated password: application logs are shipped to
            # CloudWatch (and persisted/broadly readable), so writing a live
            # credential there exposes it. In a real deployment this branch is
            # not taken — CloudFormation always injects
            # APP_BOOTSTRAP_ADMIN_PASSWORD from Secrets Manager (the else
            # branch), and operators retrieve it from Secrets Manager. This
            # generated fallback exists only for LOCAL dev, where there is no
            # Secrets Manager. Write the one-time password to an owner-only
            # (0600) file beside the local DB and log only its PATH, so the
            # local operator can retrieve it out-of-band without the secret ever
            # entering the log stream.
            secret_path = SQLITE_PATH.parent / "bootstrap-admin-password.txt"
            wrote_file = False
            try:
                secret_path.parent.mkdir(parents=True, exist_ok=True)
                # Create with 0600 from the start (avoid a world-readable window).
                fd = os.open(str(secret_path),
                             os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                try:
                    os.write(fd, (bootstrap_pw + "\n").encode("utf-8"))
                finally:
                    os.close(fd)
                os.chmod(secret_path, 0o600)
                wrote_file = True
            except OSError as e:
                log.error(
                    "Seeded initial admin account with a generated one-time "
                    "password, but could not write it to %s (%s). Set "
                    "APP_BOOTSTRAP_ADMIN_PASSWORD and re-seed, or reset the "
                    "admin password out-of-band.", secret_path, e)
            if wrote_file:
                log.warning(
                    "Seeded initial admin account with a generated one-time "
                    "password, written to %s (owner-readable only). Retrieve it "
                    "there, log in, and change it on first login. For non-local "
                    "environments set APP_BOOTSTRAP_ADMIN_PASSWORD instead.",
                    secret_path)
        else:
            log.warning(
                "Seeded initial admin account from APP_BOOTSTRAP_ADMIN_PASSWORD "
                "— you must change it on first login.")

    row = _fetchone(conn, "SELECT COUNT(*) as cnt FROM groups")
    if row and row["cnt"] == 0:
        _run_sql(conn, "INSERT INTO groups VALUES (?,?)", ("admins", "System administrators — cannot be deleted"))
        _run_sql(conn, "INSERT INTO group_members (group_name,username,role) VALUES (?,?,?)", ("admins", "admin", "manage"))

    # Schema migrations: add columns that may not exist in older databases
    _safe_add_column(conn, "plan_tables", "schema_compatible", "INTEGER")
    _safe_add_column(conn, "plan_tables", "schema_issues", "TEXT DEFAULT '[]'")
    _safe_add_column(conn, "plan_tables", "batch_size", "INTEGER DEFAULT 5")
    _safe_add_column(conn, "plan_tables", "sync_interval", "INTEGER DEFAULT 3600")
    _safe_add_column(conn, "plan_tables", "table_properties", "TEXT DEFAULT '{}'")
    _safe_add_column(conn, "migration_plans", "execution_engine", "TEXT DEFAULT 'athena'")
    _safe_add_column(conn, "migration_plans", "swap_tables", "INTEGER DEFAULT 0")
    _safe_add_column(conn, "users", "email", "TEXT DEFAULT ''")
    _safe_add_column(conn, "users", "theme_preference", "TEXT DEFAULT 'system'")
    _safe_add_column(conn, "sources", "last_refresh_summary", "TEXT")
    # Notification preferences table
    _run_sql(conn, """CREATE TABLE IF NOT EXISTS notification_preferences (
            username TEXT PRIMARY KEY REFERENCES users(username),
            pipeline_started INTEGER NOT NULL DEFAULT 0,
            pipeline_completed INTEGER NOT NULL DEFAULT 1,
            pipeline_failed INTEGER NOT NULL DEFAULT 1,
            batch_failed INTEGER NOT NULL DEFAULT 1,
            sync_out_of_sync INTEGER NOT NULL DEFAULT 1,
            plan_completed INTEGER NOT NULL DEFAULT 1)""")

    _commit(conn)
    conn.close()
    log.info("Database initialized (%s)", DB_TYPE)
