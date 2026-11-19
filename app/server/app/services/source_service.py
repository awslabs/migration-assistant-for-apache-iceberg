# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Source management service."""
import json
import logging
import uuid
from datetime import datetime
from typing import Optional
from app.database import get_conn, _fetchall, _fetchone, _run_sql, _commit
from app.models import SourceInfo, CreateSourceRequest

log = logging.getLogger(__name__)


def _get_iceberg_partition_info(s3_client, metadata_location: str) -> tuple[bool, int]:
    """Fetch Iceberg metadata JSON from S3 and determine partition status.

    Returns:
        (is_partitioned, partition_field_count)
    """
    try:
        # Parse s3://bucket/path from metadata_location
        if not metadata_location.startswith("s3://"):
            return (False, 0)
        path = metadata_location[5:]
        bucket = path.split("/", 1)[0]
        key = path.split("/", 1)[1]

        resp = s3_client.get_object(Bucket=bucket, Key=key)
        metadata = json.loads(resp["Body"].read())

        # Get default partition spec
        default_spec_id = metadata.get("default-spec-id", 0)
        partition_specs = metadata.get("partition-specs", [])

        for spec in partition_specs:
            if spec.get("spec-id") == default_spec_id:
                fields = spec.get("fields", [])
                return (len(fields) > 0, len(fields))

        # Fallback: check if any spec has fields
        if partition_specs:
            fields = partition_specs[0].get("fields", [])
            return (len(fields) > 0, len(fields))

        return (False, 0)
    except Exception:
        # If we can't read metadata, fall back to unpartitioned
        return (False, 0)


def list_sources() -> list[SourceInfo]:
    conn = get_conn()
    rows = _fetchall(conn, "SELECT * FROM sources ORDER BY created_at DESC")
    conn.close()
    return [SourceInfo(**r) for r in rows]


def get_source(source_id: str) -> Optional[SourceInfo]:
    conn = get_conn()
    row = _fetchone(conn, "SELECT * FROM sources WHERE id=?", (source_id,))
    conn.close()
    return SourceInfo(**row) if row else None


def create_source(request: CreateSourceRequest, username: str = "admin") -> SourceInfo:
    source_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    conn = get_conn()
    _run_sql(conn,
        "INSERT INTO sources (id,name,source_type,aws_account_id,iam_role_arn,default_group,db_filter_mode,db_filter_list,created_at,created_by) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (source_id, request.name, request.source_type, request.aws_account_id, request.iam_role_arn, request.default_group, request.db_filter_mode, request.db_filter_list, now, username))
    _commit(conn)
    conn.close()
    return get_source(source_id)


def delete_source(source_id: str) -> bool:
    conn = get_conn()
    cur = _run_sql(conn, "DELETE FROM sources WHERE id=?", (source_id,))
    _commit(conn)
    deleted = cur.rowcount > 0
    conn.close()
    return deleted


def update_source(source_id: str, iam_role_arn: str, default_group: str) -> Optional[SourceInfo]:
    conn = get_conn()
    cur = _run_sql(conn, "UPDATE sources SET iam_role_arn=?, default_group=? WHERE id=?",
        (iam_role_arn, default_group, source_id))
    _commit(conn)
    if cur.rowcount == 0:
        conn.close()
        return None
    conn.close()
    return get_source(source_id)


def _refresh_region() -> str:
    """AWS region for the Glue/S3 clients used during source refresh.

    Glue Data Catalogs are REGIONAL. Without an explicit region the boto3
    client falls back to the task's default region, so a source table created
    in a different region is silently never discovered (no error, no log). This
    lets operators point the refresh at the region where their source catalog
    lives via the APP_SOURCE_REFRESH_REGION setting, defaulting to the
    task's own region (AWS_REGION / AWS_DEFAULT_REGION), then us-east-1.
    """
    import os
    try:
        from app.services.settings_service import get_setting
        override = get_setting("APP_SOURCE_REFRESH_REGION")
    except Exception:
        override = None
    return (override
            or os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
            or "us-east-1")


def _log_refresh_error(conn, source_id: str, db_name: str, table_name: str, error_type: str, error_msg: str):
    """Log a refresh error to the inventory_refresh_log table."""
    now = datetime.now().isoformat()
    _run_sql(conn,
        "INSERT INTO inventory_refresh_log (source_id,database_name,table_name,error_type,error_message,created_at) VALUES (?,?,?,?,?,?)",
        (source_id, db_name, table_name, error_type, error_msg[:2000], now))


def get_refresh_log(source_id: str, limit: int = 100) -> list[dict]:
    """Fetch the most recent refresh error log entries for a source."""
    conn = get_conn()
    rows = _fetchall(conn,
        "SELECT id,source_id,database_name,table_name,error_type,error_message,created_at "
        "FROM inventory_refresh_log WHERE source_id=? ORDER BY created_at DESC LIMIT ?",
        (source_id, limit))
    conn.close()
    return rows


def refresh_source(source_id: str) -> Optional[dict]:
    """Refresh a source by fetching databases/tables from Glue Data Catalog."""
    source = get_source(source_id)
    if not source:
        return None

    import boto3

    # Glue Data Catalogs are regional — pin the clients to the configured
    # refresh region so a source catalog in a non-default region is discovered.
    region = _refresh_region()

    # Get Glue + S3 clients — assume role if ARN provided, otherwise use default chain
    if source.iam_role_arn:
        try:
            sts = boto3.client("sts")
            assumed = sts.assume_role(
                RoleArn=source.iam_role_arn,
                RoleSessionName="ima-glue-refresh",
            )
            creds = assumed["Credentials"]
            client_kwargs = dict(
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            )
            glue = boto3.client("glue", region_name=region, **client_kwargs)
            s3 = boto3.client("s3", region_name=region, **client_kwargs)
        except Exception as e:
            error_str = str(e)
            if "AccessDenied" in error_str or "is not authorized" in error_str:
                raise PermissionError(
                    f"Cannot assume IAM role '{source.iam_role_arn}'. "
                    f"Ensure the ECS task role has sts:AssumeRole permission and the target role's "
                    f"trust policy allows this account. Detail: {error_str[:300]}"
                )
            raise RuntimeError(f"Failed to assume role '{source.iam_role_arn}': {error_str[:500]}")
    else:
        glue = boto3.client("glue", region_name=region)
        s3 = boto3.client("s3", region_name=region)

    # Parse filter list
    filter_list = [s.strip().lower() for s in source.db_filter_list.split(",") if s.strip()] if source.db_filter_list else []
    filter_mode = source.db_filter_mode

    conn = get_conn()
    databases = []
    tables_added = 0
    tables_updated = 0
    errors_count = 0
    # Track every (database, table) seen this refresh so we can detect tables
    # that were deleted from the source Glue catalog since the last refresh.
    seen_tables: set[tuple[str, str]] = set()

    # Fetch all databases
    try:
        paginator = glue.get_paginator("get_databases")
        for page in paginator.paginate(CatalogId=source.aws_account_id):
            for db in page["DatabaseList"]:
                db_name = db["Name"].lower()
                if filter_mode == "include" and filter_list:
                    if db_name not in filter_list:
                        continue
                elif filter_mode == "exclude" and filter_list:
                    if db_name in filter_list:
                        continue
                databases.append(db["Name"])
    except Exception as e:
        _log_refresh_error(conn, source_id, "", "", "get_databases", str(e))
        _commit(conn)
        conn.close()
        error_str = str(e)
        # Surface access errors clearly so the UI can display them
        if "AccessDeniedException" in error_str or "is not authorized" in error_str:
            raise PermissionError(
                f"Access denied when calling Glue GetDatabases for account {source.aws_account_id}. "
                f"Ensure the ECS task role has glue:GetDatabases permission, or the IAM role ARN "
                f"'{source.iam_role_arn}' is assumable and has Glue access. Detail: {error_str[:300]}"
            )
        raise RuntimeError(f"Failed to list databases from Glue: {error_str[:500]}")

    # Fetch tables for each database
    for db_name in databases:
        try:
            tbl_paginator = glue.get_paginator("get_tables")
            for page in tbl_paginator.paginate(CatalogId=source.aws_account_id, DatabaseName=db_name):
                for tbl in page["TableList"]:
                    try:
                        table_name = tbl["Name"]
                        seen_tables.add((db_name, table_name))
                        sd = tbl.get("StorageDescriptor", {})
                        input_format = (sd.get("InputFormat", "") + " " + sd.get("OutputFormat", "")).lower()
                        serde = sd.get("SerdeInfo", {}).get("SerializationLibrary", "").lower()
                        location = sd.get("Location", "")
                        params = tbl.get("Parameters", {})

                        # Detect format
                        fmt = None
                        if "iceberg" in params.get("table_type", "").lower() or "iceberg" in serde:
                            fmt = "iceberg"
                        elif "delta" in input_format or "delta" in serde or params.get("spark.sql.sources.provider", "").lower() == "delta":
                            fmt = "delta"
                        elif "symlinktextinputformat" in input_format:
                            # Symlink manifest tables are typically Delta tables registered for Athena/Presto
                            fmt = "delta"
                        elif "hudi" in input_format or "hudi" in serde:
                            fmt = "hudi"
                        elif "orc" in input_format or "orc" in serde:
                            fmt = "orc"
                        elif "avro" in input_format or "avro" in serde:
                            fmt = "avro"
                        elif "json" in input_format or "json" in serde:
                            fmt = "json"
                        elif "csv" in input_format or "csv" in serde or "opencsv" in serde:
                            fmt = "csv"
                        elif "parquet" in input_format or "parquet" in serde:
                            fmt = "parquet"

                        # Fallback: check the 'classification' table parameter
                        # (set by AWS Glue Crawlers and manual registrations).
                        if not fmt:
                            classification = params.get("classification", "").lower()
                            if classification in ("csv", "json", "parquet", "orc",
                                                  "avro", "hudi", "delta", "iceberg"):
                                fmt = classification

                        # Final fallback: TextInputFormat + LazySimpleSerDe with
                        # a comma delimiter is a common CSV registration pattern
                        # that doesn't contain "csv" in the class names.
                        if not fmt:
                            serde_params = sd.get("SerdeInfo", {}).get("Parameters", {})
                            if ("textinputformat" in input_format
                                    and "lazysimple" in serde
                                    and serde_params.get("field.delim") in (",", "\t", "|", ";")):
                                fmt = "csv"

                        # If nothing matched, default to parquet (most common
                        # table format in Glue-registered data lakes).
                        if not fmt:
                            fmt = "parquet"

                        size_bytes = int(params.get("sizeKey", 0)) or int(params.get("totalSize", 0)) or 0
                        partition_keys = tbl.get("PartitionKeys", [])
                        is_partitioned = 1 if len(partition_keys) > 0 else 0

                        # For Iceberg tables, partitioning is in the metadata, not Glue PartitionKeys
                        if fmt == "iceberg":
                            metadata_location = params.get("metadata_location", "")
                            if metadata_location:
                                iceberg_partitioned, _ = _get_iceberg_partition_info(s3, metadata_location)
                                is_partitioned = 1 if iceberg_partitioned else 0

                        owner = source.default_group or ""

                        existing = _fetchone(conn,
                            "SELECT id FROM tables_inventory WHERE database_name=? AND table_name=?",
                            (db_name, table_name))

                        if existing:
                            _run_sql(conn,
                                "UPDATE tables_inventory SET format=?, location=?, aws_account=?, size_bytes=?, partitioned=? WHERE database_name=? AND table_name=?",
                                (fmt, location, source.aws_account_id, size_bytes, is_partitioned, db_name, table_name))
                            tables_updated += 1
                        else:
                            _run_sql(conn,
                                "INSERT INTO tables_inventory (database_name,table_name,format,location,aws_account,size_bytes,partitioned,owner) VALUES (?,?,?,?,?,?,?,?)",
                                (db_name, table_name, fmt, location, source.aws_account_id, size_bytes, is_partitioned, owner))
                            tables_added += 1
                    except Exception as e:
                        table_name = tbl.get("Name", "unknown")
                        _log_refresh_error(conn, source_id, db_name, table_name, "process_table", str(e))
                        errors_count += 1
        except Exception as e:
            _log_refresh_error(conn, source_id, db_name, "", "get_tables", str(e))
            errors_count += 1

    # ── Detect and remove tables deleted from the source Glue catalog ──
    # Only consider inventory rows for this source's account AND within the
    # databases we actually scanned this run (so filtered-out DBs are untouched).
    # A table already referenced by a plan is NOT deleted — it's reported as
    # orphaned so the plan owner can decide what to do.
    tables_removed = 0
    tables_orphaned = 0
    orphaned_details = []
    if databases:
        placeholders = ",".join(["?"] * len(databases))
        inventory_rows = _fetchall(conn,
            f"SELECT database_name, table_name FROM tables_inventory " # nosec B608
            f"WHERE aws_account=? AND database_name IN ({placeholders})",
            tuple([source.aws_account_id] + list(databases)))

        for row in inventory_rows:
            key = (row["database_name"], row["table_name"])
            if key in seen_tables:
                continue  # still present in the source catalog
            # Table is gone from Glue. Check whether a plan references it.
            in_plan = _fetchone(conn,
                "SELECT 1 FROM plan_tables WHERE source_database=? AND source_table=? LIMIT 1",
                (row["database_name"], row["table_name"]))
            if in_plan:
                tables_orphaned += 1
                orphaned_details.append(f"{row['database_name']}.{row['table_name']}")
                log.warning(
                    "Source table %s.%s was deleted from Glue but is referenced by a "
                    "migration plan — leaving in inventory (orphaned)",
                    row["database_name"], row["table_name"])
                continue
            _run_sql(conn,
                "DELETE FROM tables_inventory WHERE database_name=? AND table_name=?",
                (row["database_name"], row["table_name"]))
            tables_removed += 1

    # Update last_refreshed_at and persist refresh summary
    now = datetime.now().isoformat()
    summary = json.dumps({
        "databases_scanned": len(databases),
        "tables_added": tables_added,
        "tables_updated": tables_updated,
        "tables_removed": tables_removed,
        "tables_orphaned": tables_orphaned,
        "errors": errors_count,
    })
    _run_sql(conn, "UPDATE sources SET last_refreshed_at=?, last_refresh_summary=? WHERE id=?",
             (now, summary, source_id))
    _commit(conn)
    conn.close()

    return {
        "source_id": source_id,
        "databases_scanned": len(databases),
        "tables_added": tables_added,
        "tables_updated": tables_updated,
        "tables_removed": tables_removed,
        "tables_orphaned": tables_orphaned,
        "orphaned_tables": orphaned_details,
        "errors": errors_count,
        "refreshed_at": now,
    }
