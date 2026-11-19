# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Settings service — persists app and engine configuration overrides."""
from app.database import get_conn, _fetchall, _fetchone, _run_sql, _commit


# ── Single AWS region ───────────────────────────────────────────────────────
# One setting (APP_REGION) drives every per-service region. The keys below
# are no longer individually configurable: get_setting() resolves each of them
# to the global region, and the engine-side keys are pushed to the engine
# whenever the global region changes (see region_payload()).
REGION_KEY = "APP_REGION"

# Region keys the ENGINE reads from its environment (pushed to the engine).
ENGINE_REGION_KEYS = (
    "ENGINE_ATHENA_REGION",
    "ENGINE_EMR_REGION",
    "ENGINE_TRACKING_REGION",
    "ENGINE_S3TABLES_REGION",
    "ENGINE_XTABLE_LAMBDA_REGION",
)
# Region keys only the APP reads (resolved locally; nothing to push).
APP_REGION_KEYS = (
    "APP_SOURCE_REFRESH_REGION",
    "APP_NOTIFICATIONS_REGION",
)
DERIVED_REGION_KEYS = ENGINE_REGION_KEYS + APP_REGION_KEYS


# ── Single logs bucket ──────────────────────────────────────────────────────
# One bucket (ENGINE_LOGS_BUCKET) holds all of the engine's operational output,
# separated by prefix. The per-purpose location settings below are no longer
# individually configurable: get_setting() derives each from the bucket, and
# logs_payload() pushes the derived values to the engine.
#
#   s3://<bucket>/athena-query-results/   Athena query results
#   s3://<bucket>/emr-logs/               EMR Serverless job logs
#   s3://<bucket>/migration-reports/      Managed Migration compatibility/migration reports
#
# Keep these in step with the engine stack (workgroup output location, EMR log
# URI, and the S3 Tables migration role's ReportsWrite grant) and with the
# engine's default reports prefix (strategies/s3tables_managed.py).
LOGS_BUCKET_KEY = "ENGINE_LOGS_BUCKET"
ATHENA_RESULTS_PREFIX = "athena-query-results/"
EMR_LOGS_PREFIX = "emr-logs/"
MIGRATION_REPORTS_PREFIX = "migration-reports"  # engine default; not pushed (read at import)

LOGS_DERIVED_KEYS = {
    "ENGINE_ATHENA_OUTPUT_LOCATION": lambda b: f"s3://{b}/{ATHENA_RESULTS_PREFIX}",
    "ENGINE_EMR_LOG_URI": lambda b: f"s3://{b}/{EMR_LOGS_PREFIX}",
    # The engine appends MIGRATION_REPORTS_PREFIX itself and builds the ARN.
    "ENGINE_S3TABLES_REPORTS_BUCKET": lambda b: b,
}

import re as _re
_BUCKET_NAME_RE = _re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def normalize_bucket_name(value) -> str:
    """Reduce a bucket name / s3:// URI / bucket ARN to the bare bucket name."""
    v = (value or "").strip()
    if v.startswith("arn:aws:s3:::"):
        v = v[len("arn:aws:s3:::"):]
    elif v.startswith("s3://"):
        v = v[len("s3://"):]
    return v.split("/", 1)[0].strip()


def is_valid_bucket_name(name: str) -> bool:
    """True for a plausible S3 bucket name (3-63 chars: a-z, 0-9, '.', '-')."""
    return bool(_BUCKET_NAME_RE.match(name or ""))


def _default_region() -> str:
    """Region used when APP_REGION has no database override or env value.

    Falls back to the app's own region so a deployed app (whose task runs in the
    stack's region) works without any manual entry, then to us-east-1.
    """
    import os
    return (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
            or "us-east-1")


# Registry of known settings with metadata for the UI
SETTINGS_REGISTRY = [
    # General (single AWS region for every service — Athena, EMR, tracking, sources, ...)
    {"key": REGION_KEY, "label": "AWS Region", "default": "us-east-1", "group": "General", "description": "Defaults to the app's own region."},
    {"key": LOGS_BUCKET_KEY, "label": "Logs Bucket", "default": "", "group": "General", "description": "S3 Bucket for migration engine logs. Enter bucket name only."},
    # Engine
    {"key": "ENGINE_URL", "label": "Engine URL", "default": "http://localhost:8001", "group": "Engine", "description": "URL of the migration engine"},
    {"key": "AGENT_URL", "label": "AI Agent ARN/URL", "default": "http://localhost:8080/invocations", "group": "Engine", "description": "AgentCore runtime ARN (arn:aws:bedrock-agentcore:...) or local URL for dev"},
    {"key": "APP_TARGET_PREFIX", "label": "Target Table Prefix", "default": "ima__", "group": "Engine", "description": "Prefix for target Iceberg table names"},
    # Reconciler
    {"key": "ENGINE_RECONCILE_INTERVAL_SECONDS", "label": "Reconcile Interval (seconds)", "default": "30", "group": "Reconciler", "description": "How often the reconcile loop checks running pipelines"},
    {"key": "ENGINE_MAX_RUNNING_PIPELINES", "label": "Max Running Pipelines", "default": "200", "group": "Reconciler", "description": "Maximum pipelines running at once; extra pipelines wait as queued. Keep below your EMR Serverless and Athena concurrency quotas"},
    {"key": "ENGINE_INCREMENTAL_INTERVAL_SECONDS", "label": "Incremental Sync Interval (seconds)", "default": "3600", "group": "Reconciler", "description": "Interval between incremental sync cycles"},
    {"key": "ENGINE_MORPH_INTERVAL_SECONDS", "label": "Morph Sync Interval (seconds)", "default": "3600", "group": "Reconciler", "description": "Interval between morph event reconciliation"},
    # Athena
    {"key": "ENGINE_ATHENA_WORKGROUP", "label": "Athena Workgroup", "default": "primary", "group": "Athena", "description": "Athena workgroup for query execution"},
    # EMR Serverless
    {"key": "ENGINE_EMR_APPLICATION_ID", "label": "EMR Application ID", "default": "", "group": "EMR Serverless", "description": "EMR Serverless application ID"},
    {"key": "ENGINE_EMR_EXECUTION_ROLE_ARN", "label": "EMR Execution Role ARN", "default": "", "group": "EMR Serverless", "description": "IAM role ARN for EMR job execution"},
    {"key": "ENGINE_EMR_SCRIPTS_URI", "label": "EMR Scripts URI", "default": "", "group": "EMR Serverless", "description": "S3 URI where Spark scripts are stored (e.g. s3://bucket-name). Falls back to Log URI if not set."},
    # Tracking
    {"key": "ENGINE_TRACKING_LAMBDA_ROLE_ARN", "label": "Tracking Lambda Role ARN", "default": "", "group": "Tracking", "description": "IAM role for S3 event tracking Lambda functions"},
    {"key": "ENGINE_TRACKING_TABLE", "label": "Tracking DynamoDB Table", "default": "", "group": "Tracking", "description": "DynamoDB table the S3 event tracker writes to. Leave empty on a deployed stack (the engine already has it); set it when the engine runs elsewhere, e.g. locally (<env>-object-tracking-<region>)"},
    # Morph
    {"key": "ENGINE_MORPH_WORKERS", "label": "Morph Worker Threads", "default": "32", "group": "Morph", "description": "Number of parallel threads for Parquet footer reads"},
    {"key": "ENGINE_XTABLE_LAMBDA_ARN", "label": "XTable Lambda ARN", "default": "", "group": "Morph", "description": "ARN of the Lambda function for Hudi/Delta → Iceberg conversion via Apache XTable"},

    # Managed Migration (AWS S3 Tables Migration API)
    {"key": "ENGINE_S3TABLES_MIGRATION_ROLE_ARN", "label": "Managed Migration Role ARN", "default": "", "group": "Managed Migration", "description": "IAM role ARN the S3 Tables Migration API assumes to read source data and write target S3 Tables (trusts migration.s3tables.amazonaws.com)"},
    {"key": "APP_S3TABLES_UNMATCHED_FIELDS_POLICY", "label": "Unmatched Source Fields Policy", "default": "fail", "group": "Managed Migration", "description": "Source fields with no matching target column: fail stops the migration, drop skips them", "options": ["fail", "drop"]},
    {"key": "APP_S3TABLES_INCOMPATIBLE_VALUES_POLICY", "label": "Incompatible Values Policy", "default": "fail", "group": "Managed Migration", "description": "Source values that cannot be represented in the target type: fail stops the migration, setNull writes NULL", "options": ["fail", "setNull"]},

    # Validation
    {"key": "ENGINE_VALIDATION_WORKERS", "label": "Validation Worker Threads", "default": "5", "group": "Validation", "description": "Max Athena validation pipelines run concurrently by the engine"},
    {"key": "ENGINE_VALIDATION_TABLE", "label": "Validation DynamoDB Table", "default": "", "group": "Validation", "description": "DynamoDB table for validation results, used by the engine and the EMR validation job. Leave empty on a deployed stack (the engine already has it); set it when the engine runs elsewhere, e.g. locally (<env>-validation-results-<region>)"},
    # Notifications
    {"key": "APP_NOTIFICATIONS_ENABLED", "label": "Notifications Enabled", "default": "false", "group": "Notifications", "description": "Emit pipeline status events to EventBridge (true/false)", "options": ["true", "false"]},
    {"key": "APP_NOTIFICATIONS_EVENTBRIDGE_BUS", "label": "EventBridge Bus Name", "default": "default", "group": "Notifications", "description": "Name of the EventBridge event bus to publish notification events to"},
    {"key": "APP_NOTIFICATIONS_EVENTS", "label": "Enabled Events", "default": "pipeline_completed,pipeline_failed,sync_out_of_sync,plan_completed", "group": "Notifications", "description": "Comma-separated event types to emit: pipeline_completed, pipeline_failed, sync_out_of_sync, plan_completed"},

    # Logging
    {"key": "ENGINE_LOG_LEVEL", "label": "Log Level", "default": "INFO", "group": "Logging", "description": "Logging level: DEBUG, INFO, WARNING, ERROR"},
    {"key": "ENGINE_LOG_FILE", "label": "Log File Path", "default": "", "group": "Logging", "description": "Path to write log file (empty = stdout only)"},
]


def _ensure_table(conn):
    """Create settings table if it doesn't exist."""
    from app.database import _run_sql
    _run_sql(conn, """CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL)""")


def list_settings() -> list[dict]:
    """Get all settings with their current values (DB override or default)."""
    import os
    conn = get_conn()
    _ensure_table(conn)
    rows = _fetchall(conn, "SELECT key, value, updated_at FROM settings")
    conn.close()

    overrides = {r["key"]: {"value": r["value"], "updated_at": r["updated_at"]} for r in rows}

    result = []
    for s in SETTINGS_REGISTRY:
        key = s["key"]
        if key in overrides:
            current = overrides[key]["value"]
            source = "database"
            updated_at = overrides[key]["updated_at"]
        elif os.getenv(key):
            current = os.getenv(key, "")
            source = "environment"
            updated_at = None
        elif key == REGION_KEY:
            # Show the effective fallback (the app's own region), not a static
            # placeholder, so the UI reflects what services will really use.
            current = _default_region()
            source = "default"
            updated_at = None
        else:
            current = s["default"]
            source = "default"
            updated_at = None
        result.append({
            "key": key,
            "label": s["label"],
            "value": current,
            "default": s["default"],
            "group": s["group"],
            "description": s["description"],
            "source": source,
            "updated_at": updated_at,
            "options": s.get("options"),
        })
    return result


def get_setting(key: str) -> str | None:
    """Get a single setting value. Returns DB override > env var > default.

    Per-service region keys (Athena, EMR, tracking, source refresh, ...) are not
    independently configurable: they always resolve to the single global region
    (APP_REGION). Any stale per-service override left in the database from
    before the consolidation is deliberately ignored.
    """
    import os
    if key in DERIVED_REGION_KEYS:
        return get_setting(REGION_KEY)
    if key in LOGS_DERIVED_KEYS:
        # Athena results / EMR logs / migration reports all derive from the one
        # logs bucket. If it isn't set, fall back to whatever the environment
        # provides for the specific key (e.g. the stack default) so an unset
        # bucket never forces a blank over a working configuration.
        bucket = normalize_bucket_name(get_setting(LOGS_BUCKET_KEY))
        if bucket:
            return LOGS_DERIVED_KEYS[key](bucket)
        return os.getenv(key, "")
    conn = get_conn()
    _ensure_table(conn)
    row = _fetchone(conn, "SELECT value FROM settings WHERE key=?", (key,))
    conn.close()
    if row:
        return row["value"]
    env_val = os.getenv(key)
    if env_val is not None:
        return env_val
    if key == REGION_KEY:
        return _default_region()
    for s in SETTINGS_REGISTRY:
        if s["key"] == key:
            return s["default"]
    return None


def region_payload() -> dict[str, str]:
    """Settings to push to the engine whenever the global region changes.

    Maps the global APP_REGION plus every engine-side per-service region
    key to the one effective region, so the engine (which reads the per-service
    keys from its environment) follows a single value.
    """
    region = get_setting(REGION_KEY)
    if not region:
        return {}
    return {k: region for k in (REGION_KEY, *ENGINE_REGION_KEYS)}


def logs_payload() -> dict[str, str]:
    """Settings to push to the engine whenever the logs bucket changes.

    Maps the logs bucket to the Athena output location, EMR log URI and
    migration-reports bucket the engine reads from its environment. Empty when
    no logs bucket is configured (so nothing is overwritten with a blank).
    """
    bucket = normalize_bucket_name(get_setting(LOGS_BUCKET_KEY))
    if not bucket:
        return {}
    payload = {LOGS_BUCKET_KEY: bucket}
    payload.update({k: derive(bucket) for k, derive in LOGS_DERIVED_KEYS.items()})
    return payload


def update_setting(key: str, value: str) -> dict:
    """Update a setting value in the database."""
    from datetime import datetime, timezone
    conn = get_conn()
    _ensure_table(conn)
    now = datetime.now(timezone.utc).isoformat()
    # Upsert
    existing = _fetchone(conn, "SELECT key FROM settings WHERE key=?", (key,))
    if existing:
        _run_sql(conn, "UPDATE settings SET value=?, updated_at=? WHERE key=?", (value, now, key))
    else:
        _run_sql(conn, "INSERT INTO settings (key, value, updated_at) VALUES (?,?,?)", (key, value, now))
    _commit(conn)
    conn.close()
    return {"key": key, "value": value, "updated_at": now}


def delete_setting(key: str) -> bool:
    """Remove a setting override (reverts to env var or default)."""
    conn = get_conn()
    _ensure_table(conn)
    _run_sql(conn, "DELETE FROM settings WHERE key=?", (key,))
    _commit(conn)
    conn.close()
    return True
