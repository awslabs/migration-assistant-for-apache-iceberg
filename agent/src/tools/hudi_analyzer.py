# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Hudi table metadata analysis tools.

Reads Hudi-specific metadata from S3 (.hoodie/ directory) to provide
accurate table type detection, active writer inference, schema evolution
awareness, and effective size estimation for Merge-on-Read tables.

This module supplements the generic S3/Glue analyzers with Hudi-native
intelligence that cannot be derived from Glue catalog metadata alone.
"""

import json
from datetime import datetime, timezone

from strands import tool
from tools.aws import get_client


def _parse_s3_uri(s3_path: str) -> tuple:
    """Parse s3://bucket/prefix into (bucket, prefix) with trailing slash."""
    path = s3_path.replace("s3://", "")
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    return bucket, prefix


def _read_s3_object(bucket: str, key: str) -> str | None:
    """Read a small S3 object and return its content as string."""
    s3 = get_client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read().decode("utf-8")
    except Exception:
        return None


def _list_hoodie_timeline(bucket: str, hoodie_prefix: str, max_keys: int = 200) -> list:
    """List files in the .hoodie/ timeline directory.

    Returns list of dicts with 'key', 'size', 'last_modified' for each
    timeline file (commits, inflight, rollbacks, etc.).
    """
    s3 = get_client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    files = []

    for page in paginator.paginate(Bucket=bucket, Prefix=hoodie_prefix, PaginationConfig={"MaxItems": max_keys}):
        for obj in page.get("Contents", []):
            filename = obj["Key"].rsplit("/", 1)[-1] if "/" in obj["Key"] else obj["Key"]
            # Skip archived/ subdirectory for timeline listing
            relative = obj["Key"][len(hoodie_prefix):]
            if relative.startswith("archived/"):
                continue
            files.append({
                "key": obj["Key"],
                "filename": filename,
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            })

    return files


def _parse_hoodie_properties(content: str) -> dict:
    """Parse hoodie.properties file (Java properties format) into dict."""
    props = {}
    if not content:
        return props
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            props[key.strip()] = value.strip()
    return props


def _classify_timeline_files(timeline_files: list) -> dict:
    """Classify Hudi timeline files into commits, inflight, and other categories.

    Hudi timeline file naming: <timestamp>.<action>.<state>
    Actions: commit, deltacommit, compaction, clean, rollback, savepoint
    States: (none=completed), inflight, requested
    """
    commits = []
    delta_commits = []
    inflight = []
    compactions = []
    cleans = []
    rollbacks = []

    for f in timeline_files:
        name = f["filename"]
        parts = name.split(".")

        if len(parts) < 2:
            continue

        timestamp_str = parts[0]
        action = parts[1] if len(parts) > 1 else ""
        state = parts[2] if len(parts) > 2 else "completed"

        entry = {
            "timestamp": timestamp_str,
            "action": action,
            "state": state,
            "last_modified": f["last_modified"],
            "size": f["size"],
            "key": f["key"],
        }

        if state == "inflight" or state == "requested":
            inflight.append(entry)
        elif action == "commit":
            commits.append(entry)
        elif action == "deltacommit":
            delta_commits.append(entry)
        elif action == "compaction":
            compactions.append(entry)
        elif action == "clean":
            cleans.append(entry)
        elif action == "rollback":
            rollbacks.append(entry)

    return {
        "commits": sorted(commits, key=lambda x: x["timestamp"], reverse=True),
        "delta_commits": sorted(delta_commits, key=lambda x: x["timestamp"], reverse=True),
        "inflight": inflight,
        "compactions": sorted(compactions, key=lambda x: x["timestamp"], reverse=True),
        "cleans": cleans,
        "rollbacks": rollbacks,
    }


def _parse_commit_metadata(bucket: str, commit_key: str) -> dict | None:
    """Parse a Hudi commit metadata file (JSON) to extract write stats.

    Commit files contain partitionToWriteStats with bytes written,
    records inserted/updated, and file IDs.
    """
    content = _read_s3_object(bucket, commit_key)
    if not content:
        return None

    try:
        data = json.loads(content)
        total_bytes_written = 0
        total_records_written = 0
        total_records_updated = 0
        total_files_written = 0
        partitions_written = set()

        partition_stats = data.get("partitionToWriteStats", {})
        for partition, stats_list in partition_stats.items():
            partitions_written.add(partition)
            for stat in stats_list:
                total_bytes_written += stat.get("totalWriteBytes", 0)
                total_records_written += stat.get("numInserts", 0)
                total_records_updated += stat.get("numUpdateWrites", 0)
                total_files_written += 1

        return {
            "total_bytes_written": total_bytes_written,
            "total_records_written": total_records_written,
            "total_records_updated": total_records_updated,
            "total_files_written": total_files_written,
            "partitions_written": len(partitions_written),
        }
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def _count_file_types(bucket: str, prefix: str) -> dict:
    """Count base files vs log files in a Hudi table's data directory.

    Base files: .parquet (CoW and MoR base)
    Log files: .log.* files (MoR delta logs)

    Returns breakdown of counts and sizes for each type.
    """
    s3 = get_client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    base_count = 0
    base_size = 0
    log_count = 0
    log_size = 0
    other_count = 0
    other_size = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]

            # Skip .hoodie/ metadata directory
            if "/.hoodie/" in key or key.endswith("/.hoodie"):
                continue
            # Skip zero-byte markers
            if size == 0:
                continue
            # Skip partition metadata
            filename = key.rsplit("/", 1)[-1] if "/" in key else key
            if filename == ".hoodie_partition_metadata":
                continue
            if filename.startswith("."):
                continue

            if ".log." in filename or filename.endswith(".log"):
                log_count += 1
                log_size += size
            elif filename.endswith(".parquet"):
                base_count += 1
                base_size += size
            else:
                other_count += 1
                other_size += size

    return {
        "base_files": {"count": base_count, "size_bytes": base_size},
        "log_files": {"count": log_count, "size_bytes": log_size},
        "other_files": {"count": other_count, "size_bytes": other_size},
        "total_s3_bytes": base_size + log_size + other_size,
    }


@tool
def get_hudi_table_metadata(s3_location: str) -> dict:
    """Analyze a Hudi table by reading its .hoodie/ metadata directory.

    Extracts table type (CoW/MoR), commit timeline activity, active writer
    detection, compaction state, schema version, and effective data size
    (separating base files from log files for MoR tables).

    This provides Hudi-native intelligence beyond what Glue catalog metadata
    can offer. Use this for any table detected as Hudi format.

    Args:
        s3_location: S3 URI of the Hudi table root (e.g. s3://bucket/db/table/)
    """
    bucket, prefix = _parse_s3_uri(s3_location)
    hoodie_prefix = prefix + ".hoodie/"

    # --- Step 1: Read hoodie.properties for table configuration ---
    props_key = hoodie_prefix + "hoodie.properties"
    props_content = _read_s3_object(bucket, props_key)

    if props_content is None:
        return {
            "s3_location": s3_location,
            "is_hudi_table": False,
            "error": "No .hoodie/hoodie.properties found. This may not be a Hudi table.",
        }

    properties = _parse_hoodie_properties(props_content)

    table_type = properties.get("hoodie.table.type", "UNKNOWN")
    table_name = properties.get("hoodie.table.name", "")
    table_version = properties.get("hoodie.table.version", "")
    base_file_format = properties.get("hoodie.table.base.file.format", "parquet")
    key_generator = properties.get("hoodie.table.keygenerator.class", "")
    record_key_fields = properties.get("hoodie.datasource.write.recordkey.field", "")
    partition_fields = properties.get("hoodie.datasource.write.partitionpath.field", "")
    precombine_field = properties.get("hoodie.datasource.write.precombine.field", "")

    is_cow = table_type == "COPY_ON_WRITE"
    is_mor = table_type == "MERGE_ON_READ"

    # --- Step 2: Read timeline to determine activity ---
    timeline_files = _list_hoodie_timeline(bucket, hoodie_prefix)
    classified = _classify_timeline_files(timeline_files)

    # Active writer detection
    has_inflight = len(classified["inflight"]) > 0
    recent_commits = classified["commits"] + classified["delta_commits"]
    recent_commits.sort(key=lambda x: x["timestamp"], reverse=True)

    last_commit_time = None
    last_commit_days_ago = None
    has_active_writers = has_inflight

    if recent_commits:
        # Parse Hudi timestamp format: YYYYMMDDHHmmss or similar
        latest = recent_commits[0]
        last_commit_time = latest["last_modified"]
        if isinstance(last_commit_time, datetime):
            now = datetime.now(timezone.utc)
            last_commit_days_ago = (now - last_commit_time).days
            # Consider active if committed within 60 days
            if last_commit_days_ago < 60:
                has_active_writers = True

    # Compaction state (MoR specific)
    pending_compactions = [c for c in classified["compactions"] if c["state"] in ("requested", "inflight")]

    # --- Step 3: Parse latest commit for write stats ---
    latest_commit_stats = None
    if recent_commits and recent_commits[0].get("size", 0) > 0:
        latest_commit_stats = _parse_commit_metadata(bucket, recent_commits[0]["key"])

    # --- Step 4: Count base files vs log files ---
    file_breakdown = _count_file_types(bucket, prefix)

    # Effective size: for CoW it's total, for MoR it's base files
    # (log files will be compacted during migration)
    if is_mor:
        effective_size_bytes = file_breakdown["base_files"]["size_bytes"]
        total_s3_bytes = file_breakdown["total_s3_bytes"]
        log_overhead_pct = (
            file_breakdown["log_files"]["size_bytes"] / total_s3_bytes * 100
            if total_s3_bytes > 0 else 0
        )
    else:
        effective_size_bytes = file_breakdown["total_s3_bytes"]
        total_s3_bytes = file_breakdown["total_s3_bytes"]
        log_overhead_pct = 0

    # --- Step 5: Schema info ---
    # Try to read the schema from hoodie properties or auxiliary file
    schema_info = {
        "record_key_fields": [f.strip() for f in record_key_fields.split(",") if f.strip()],
        "partition_fields": [f.strip() for f in partition_fields.split(",") if f.strip()],
        "precombine_field": precombine_field or None,
        "key_generator": key_generator.rsplit(".", 1)[-1] if key_generator else None,
    }

    # --- Build result ---
    result = {
        "s3_location": s3_location,
        "is_hudi_table": True,
        "table_type": table_type,
        "table_type_display": "Copy-on-Write" if is_cow else "Merge-on-Read" if is_mor else table_type,
        "table_name": table_name,
        "table_version": table_version,
        "base_file_format": base_file_format,
        "properties": properties,
        "schema_info": schema_info,
        "timeline": {
            "total_commits": len(classified["commits"]),
            "total_delta_commits": len(classified["delta_commits"]),
            "inflight_operations": len(classified["inflight"]),
            "total_compactions": len(classified["compactions"]),
            "pending_compactions": len(pending_compactions),
            "total_cleans": len(cleans) if (cleans := classified["cleans"]) else 0,
            "total_rollbacks": len(classified["rollbacks"]),
            "last_commit_days_ago": last_commit_days_ago,
        },
        "active_writers": {
            "has_active_writers": has_active_writers,
            "has_inflight_operations": has_inflight,
            "last_commit_days_ago": last_commit_days_ago,
            "detection_method": "hoodie_timeline",
        },
        "file_breakdown": {
            "base_files_count": file_breakdown["base_files"]["count"],
            "base_files_size_bytes": file_breakdown["base_files"]["size_bytes"],
            "base_files_size_gb": round(file_breakdown["base_files"]["size_bytes"] / (1024**3), 3),
            "log_files_count": file_breakdown["log_files"]["count"],
            "log_files_size_bytes": file_breakdown["log_files"]["size_bytes"],
            "log_files_size_gb": round(file_breakdown["log_files"]["size_bytes"] / (1024**3), 3),
            "log_overhead_pct": round(log_overhead_pct, 1),
            "total_s3_bytes": total_s3_bytes,
            "total_s3_gb": round(total_s3_bytes / (1024**3), 3),
            "effective_size_bytes": effective_size_bytes,
            "effective_size_gb": round(effective_size_bytes / (1024**3), 3),
        },
        "latest_commit_stats": latest_commit_stats,
        "migration_guidance": _build_migration_guidance(
            table_type=table_type,
            is_cow=is_cow,
            is_mor=is_mor,
            has_active_writers=has_active_writers,
            pending_compactions=len(pending_compactions),
            log_overhead_pct=log_overhead_pct,
            file_breakdown=file_breakdown,
        ),
    }

    return result


def _build_migration_guidance(
    table_type: str,
    is_cow: bool,
    is_mor: bool,
    has_active_writers: bool,
    pending_compactions: int,
    log_overhead_pct: float,
    file_breakdown: dict,
) -> dict:
    """Build Hudi-specific migration guidance based on table state."""
    guidance = {
        "morph_eligible": False,
        "morph_reason": "",
        "requires_compaction_first": False,
        "effective_size_note": "",
        "writer_coordination_needed": has_active_writers,
        "recommendations": [],
    }

    if is_cow:
        # CoW tables are morph-eligible since they're just Parquet files
        # with Hudi metadata alongside
        guidance["morph_eligible"] = True
        guidance["morph_reason"] = (
            "Copy-on-Write table uses standard Parquet base files. "
            "Morph can register these files directly as Iceberg without rewriting."
        )
        guidance["effective_size_note"] = (
            "All data is in base Parquet files — total S3 size equals effective data size."
        )
        if has_active_writers:
            guidance["recommendations"].append(
                "Pause writers before morph to avoid data loss from in-flight commits."
            )
        guidance["recommendations"].append(
            "After morph, Hudi metadata (.hoodie/) can be archived but should be "
            "retained for rollback until the Iceberg table is validated."
        )

    elif is_mor:
        # MoR tables cannot be morphed — log files must be compacted first
        guidance["morph_eligible"] = False
        guidance["morph_reason"] = (
            "Merge-on-Read table has log files containing uncompacted deltas. "
            "These cannot be registered as Iceberg data files. Rewrite is required."
        )

        if pending_compactions > 0:
            guidance["requires_compaction_first"] = True
            guidance["recommendations"].append(
                f"Run pending compactions ({pending_compactions} pending) before migration "
                "to reduce log file overhead and ensure data consistency."
            )

        if log_overhead_pct > 30:
            guidance["requires_compaction_first"] = True
            guidance["recommendations"].append(
                f"Log files represent {log_overhead_pct:.0f}% of total storage. "
                "Compacting before migration will reduce data to rewrite and "
                "ensure the latest record versions are materialized."
            )

        guidance["effective_size_note"] = (
            f"Base files: {file_breakdown['base_files']['size_bytes'] / (1024**3):.1f} GB, "
            f"Log files: {file_breakdown['log_files']['size_bytes'] / (1024**3):.1f} GB. "
            "Migration rewrites base files and applies log deltas — effective output "
            "size will be approximately the base file size."
        )

        if has_active_writers:
            guidance["recommendations"].append(
                "Coordinate writer cutover: stop Hudi writes → run final compaction → "
                "migrate to Iceberg → redirect writers to Iceberg."
            )

        guidance["recommendations"].append(
            "Use the record key fields as the basis for Iceberg's row-level "
            "equality delete tracking if incremental migration is chosen."
        )

    else:
        guidance["morph_reason"] = f"Unknown Hudi table type: {table_type}. Rewrite recommended."
        guidance["recommendations"].append("Unknown table type — default to full rewrite.")

    return guidance


@tool
def discover_hudi_partitions(s3_location: str) -> dict:
    """Discover Hudi partitions directly from S3 by scanning for
    .hoodie_partition_metadata marker files.

    Use this when Glue partition metadata may be stale or incomplete.
    Hudi writes a .hoodie_partition_metadata file in each partition
    directory, providing a reliable source of truth.

    Args:
        s3_location: S3 URI of the Hudi table root (e.g. s3://bucket/db/table/)
    """
    s3 = get_client("s3")
    bucket, prefix = _parse_s3_uri(s3_location)

    # Search for .hoodie_partition_metadata files
    paginator = s3.get_paginator("list_objects_v2")
    partitions = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.rsplit("/", 1)[-1] if "/" in key else key
            if filename == ".hoodie_partition_metadata":
                # Extract partition path relative to table root
                relative = key[len(prefix):]
                partition_path = relative.rsplit("/", 1)[0] if "/" in relative else ""
                if partition_path and partition_path != ".hoodie":
                    partitions.append({
                        "partition_path": partition_path,
                        "marker_key": key,
                    })

    seen = set()
    unique_partitions = []
    for p in partitions:
        if p["partition_path"] not in seen:
            seen.add(p["partition_path"])
            unique_partitions.append(p)
    unique_partitions.sort(key=lambda x: x["partition_path"])

    return {
        "s3_location": s3_location,
        "partition_count": len(unique_partitions),
        "partitions": unique_partitions,
        "discovery_method": "hoodie_partition_metadata_markers",
        "note": (
            f"Found {len(unique_partitions)} partitions via .hoodie_partition_metadata files. "
            "This reflects the actual Hudi partition state regardless of Glue catalog freshness."
        ),
    }


@tool
def get_hudi_compaction_status(s3_location: str) -> dict:
    """Check the compaction state of a Hudi Merge-on-Read table.

    Reports pending, in-progress, and completed compactions from the
    Hudi timeline. Useful for determining if compaction should be run
    before migrating to Iceberg.

    Args:
        s3_location: S3 URI of the Hudi table root
    """
    bucket, prefix = _parse_s3_uri(s3_location)
    hoodie_prefix = prefix + ".hoodie/"

    props_content = _read_s3_object(bucket, hoodie_prefix + "hoodie.properties")
    if not props_content:
        return {
            "s3_location": s3_location,
            "error": "Not a Hudi table — no .hoodie/hoodie.properties found.",
        }

    properties = _parse_hoodie_properties(props_content)
    table_type = properties.get("hoodie.table.type", "UNKNOWN")

    if table_type == "COPY_ON_WRITE":
        return {
            "s3_location": s3_location,
            "table_type": "COPY_ON_WRITE",
            "compaction_applicable": False,
            "note": "Copy-on-Write tables do not use compaction. All data is in base files.",
        }

    # List timeline for compaction-related files
    timeline_files = _list_hoodie_timeline(bucket, hoodie_prefix)
    classified = _classify_timeline_files(timeline_files)

    # File breakdown to assess log overhead
    file_breakdown = _count_file_types(bucket, prefix)
    total_bytes = file_breakdown["total_s3_bytes"]
    log_bytes = file_breakdown["log_files"]["size_bytes"]
    log_pct = (log_bytes / total_bytes * 100) if total_bytes > 0 else 0

    pending = [c for c in classified["compactions"] if c["state"] in ("requested", "inflight")]
    completed = [c for c in classified["compactions"] if c["state"] == "completed"]

    urgency = "none"
    urgency_reason = ""
    if len(pending) > 0 and log_pct > 30:
        urgency = "high"
        urgency_reason = (
            f"{len(pending)} pending compaction(s) and log files represent "
            f"{log_pct:.0f}% of storage. Compact before migration."
        )
    elif log_pct > 50:
        urgency = "high"
        urgency_reason = (
            f"Log files represent {log_pct:.0f}% of storage — "
            "significant uncompacted deltas. Compact before migration."
        )
    elif log_pct > 20:
        urgency = "medium"
        urgency_reason = (
            f"Log files represent {log_pct:.0f}% of storage. "
            "Compaction recommended before migration for optimal results."
        )
    elif len(pending) > 0:
        urgency = "low"
        urgency_reason = (
            f"{len(pending)} pending compaction(s) but log overhead is low ({log_pct:.0f}%). "
            "Compaction is optional before migration."
        )

    return {
        "s3_location": s3_location,
        "table_type": table_type,
        "compaction_applicable": True,
        "pending_compactions": len(pending),
        "completed_compactions": len(completed),
        "log_files_count": file_breakdown["log_files"]["count"],
        "log_files_size_gb": round(log_bytes / (1024**3), 3),
        "log_overhead_pct": round(log_pct, 1),
        "base_files_size_gb": round(file_breakdown["base_files"]["size_bytes"] / (1024**3), 3),
        "compaction_urgency": urgency,
        "urgency_reason": urgency_reason,
        "recommendation": (
            "Run compaction before migration to ensure all deltas are materialized "
            "into base files. This produces the cleanest Iceberg output."
            if urgency in ("high", "medium")
            else "Compaction state is acceptable for migration."
        ),
    }
