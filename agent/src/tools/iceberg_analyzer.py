# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Iceberg table metadata analysis tools.

Reads Iceberg metadata from S3 (metadata.json, manifest lists, manifests)
to provide accurate size estimation, partition counting, and file distribution
analysis — without requiring expensive S3 ListObjectsV2 calls.

This module supplements the generic S3/Glue analyzers with Iceberg-native
intelligence derived directly from the table metadata layer.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

from strands import tool
from tools.aws import get_client

logger = logging.getLogger(__name__)


def _read_s3_json(bucket: str, key: str) -> dict | None:
    """Read a JSON file from S3."""
    s3 = get_client("s3")
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception:
        return None


def _parse_s3_uri(s3_path: str) -> tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key)."""
    path = s3_path.replace("s3://", "")
    parts = path.split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def _read_avro_manifest(bucket: str, key: str) -> list[dict]:
    """Read an Iceberg Avro file (manifest-list or manifest) and extract entries.

    For manifest-list files: returns entries with manifest_path, added_data_files_count, etc.
    For manifest files: returns entries with file_path, file_size_in_bytes, partition, etc.
    """
    try:
        import fastavro
        import io
        s3 = get_client("s3")
        resp = s3.get_object(Bucket=bucket, Key=key)
        # fastavro needs a seekable file-like object
        data = io.BytesIO(resp["Body"].read())
        reader = fastavro.reader(data)
        entries = []
        for record in reader:
            # Manifest-list entries have 'manifest_path'
            if "manifest_path" in record:
                entries.append({
                    "manifest_path": record.get("manifest_path", ""),
                    "manifest_length": record.get("manifest_length", 0),
                    "added_data_files_count": record.get("added_data_files_count", 0),
                    "existing_data_files_count": record.get("existing_data_files_count", 0),
                    "content": record.get("content", 0),
                })
            # Data manifest entries have 'data_file' nested struct
            elif "data_file" in record:
                df = record["data_file"]
                entries.append({
                    "file_path": df.get("file_path", ""),
                    "file_size_in_bytes": df.get("file_size_in_bytes", 0),
                    "record_count": df.get("record_count", 0),
                    "partition": df.get("partition", {}),
                })
            # Some formats flatten the data_file fields at top level
            elif "file_path" in record:
                entries.append({
                    "file_path": record.get("file_path", ""),
                    "file_size_in_bytes": record.get("file_size_in_bytes", 0),
                    "record_count": record.get("record_count", 0),
                    "partition": record.get("partition", {}),
                })
        return entries
    except ImportError:
        return []
    except Exception:
        logger.warning("Failed to read Avro file s3://%s/%s", bucket, key, exc_info=True)
        return []


@tool
def get_iceberg_table_stats(database: str, table_name: str) -> dict:
    """Get Iceberg table size, file count, and partition count from metadata.

    Reads the Iceberg metadata.json to extract exact statistics without
    listing S3 objects. Much faster and cheaper than S3 enumeration.

    Returns: total_size_bytes, total_size_gb, total_data_files,
    total_records, partition_count, file_size_distribution,
    last_updated_ms, and snapshot info.

    Args:
        database: Glue database name
        table_name: Table name
    """
    # Step 1: Get metadata_location from Glue
    glue = get_client("glue")
    try:
        resp = glue.get_table(DatabaseName=database, Name=table_name)
        params = resp["Table"].get("Parameters", {})
        metadata_location = params.get("metadata_location", "")
    except Exception as e:
        return {"error": f"Failed to get table from Glue: {e}"}

    if not metadata_location:
        return {"error": "No metadata_location in table parameters. Is this an Iceberg table?"}

    # Step 2: Read metadata.json
    bucket, key = _parse_s3_uri(metadata_location)
    metadata = _read_s3_json(bucket, key)
    if not metadata:
        return {"error": f"Failed to read metadata at {metadata_location}"}

    # Step 3: Find current snapshot
    current_snapshot_id = metadata.get("current-snapshot-id")
    snapshots = metadata.get("snapshots", [])
    current_snapshot = None
    for snap in snapshots:
        if snap.get("snapshot-id") == current_snapshot_id:
            current_snapshot = snap
            break

    if not current_snapshot:
        return {
            "database": database,
            "table_name": table_name,
            "total_size_bytes": 0,
            "total_size_gb": 0.0,
            "total_data_files": 0,
            "total_records": 0,
            "partition_count": 0,
            "note": "No current snapshot found (empty table).",
        }

    # Step 4: Extract stats from snapshot summary
    summary = current_snapshot.get("summary", {})
    total_size = int(summary.get("total-files-size", 0))
    total_files = int(summary.get("total-data-files", 0))
    total_records = int(summary.get("total-records", 0))
    last_updated_ms = current_snapshot.get("timestamp-ms", 0)

    # Step 5: Get partition spec to determine partition field count
    default_spec_id = metadata.get("default-spec-id", 0)
    partition_specs = metadata.get("partition-specs", [])
    partition_fields = []
    for spec in partition_specs:
        if spec.get("spec-id") == default_spec_id:
            partition_fields = spec.get("fields", [])
            break

    is_partitioned = len(partition_fields) > 0

    # Step 6: Read manifest list to count distinct partitions and get file size distribution
    manifest_list_path = current_snapshot.get("manifest-list", "")
    partition_count = 0
    file_sizes = []
    per_partition_sizes = defaultdict(int)

    if manifest_list_path and is_partitioned:
        ml_bucket, ml_key = _parse_s3_uri(manifest_list_path)
        manifest_list_entries = _read_avro_manifest(ml_bucket, ml_key)

        if manifest_list_entries:
            # manifest_list_entries contain manifest_path → read each manifest for data files
            all_data_entries = []
            for ml_entry in manifest_list_entries:
                manifest_path = ml_entry.get("manifest_path", "")
                if manifest_path:
                    m_bucket, m_key = _parse_s3_uri(manifest_path)
                    data_entries = _read_avro_manifest(m_bucket, m_key)
                    all_data_entries.extend(data_entries)

            if all_data_entries:
                partition_values_set = set()
                for entry in all_data_entries:
                    file_sizes.append(entry.get("file_size_in_bytes", 0))
                    # Build partition tuple for distinct counting
                    part = entry.get("partition", {})
                    if part:
                        part_key = json.dumps(part, sort_keys=True)
                        partition_values_set.add(part_key)
                        per_partition_sizes[part_key] += entry.get("file_size_in_bytes", 0)
                partition_count = len(partition_values_set)

    # If we couldn't read manifests (no fastavro), estimate from summary
    if partition_count == 0 and is_partitioned:
        # Fallback: use total-equality-deletes or just note that count is unavailable
        # Some catalogs expose partition count in properties
        partition_count_str = summary.get("total-partitions")
        if partition_count_str:
            partition_count = int(partition_count_str)

    # Step 7: Compute file size distribution
    file_size_stats = {}
    if file_sizes:
        file_sizes_sorted = sorted(file_sizes)
        n = len(file_sizes_sorted)
        avg_size = sum(file_sizes_sorted) / n
        file_size_stats = {
            "avg_file_size_mb": round(avg_size / (1024 * 1024), 2),
            "min_file_size_mb": round(file_sizes_sorted[0] / (1024 * 1024), 2),
            "max_file_size_mb": round(file_sizes_sorted[-1] / (1024 * 1024), 2),
            "p10_mb": round(file_sizes_sorted[int(n * 0.1)] / (1024 * 1024), 2),
            "p50_mb": round(file_sizes_sorted[int(n * 0.5)] / (1024 * 1024), 2),
            "p90_mb": round(file_sizes_sorted[int(n * 0.9)] / (1024 * 1024), 2),
            "pct_under_16mb": round(sum(1 for s in file_sizes_sorted if s < 16 * 1024 * 1024) / n * 100, 1),
        }
    elif total_files > 0 and total_size > 0:
        avg_size = total_size / total_files
        file_size_stats = {
            "avg_file_size_mb": round(avg_size / (1024 * 1024), 2),
            "note": "Estimated from summary (manifest details unavailable).",
        }

    last_modified_days_ago = None
    if last_updated_ms:
        last_dt = datetime.fromtimestamp(last_updated_ms / 1000, tz=timezone.utc)
        delta = datetime.now(timezone.utc) - last_dt
        last_modified_days_ago = delta.days

    return {
        "database": database,
        "table_name": table_name,
        "total_size_bytes": total_size,
        "total_size_gb": round(total_size / (1024**3), 3),
        "total_data_files": total_files,
        "total_records": total_records,
        "is_partitioned": is_partitioned,
        "partition_count": partition_count,
        "partition_fields": [
            {"name": f.get("name", ""), "transform": f.get("transform", "identity"),
             "source_id": f.get("source-id")}
            for f in partition_fields
        ],
        "file_size_stats": file_size_stats,
        "last_modified_days_ago": last_modified_days_ago,
        "last_updated_ms": last_updated_ms,
        "snapshot_id": str(current_snapshot_id),
        "snapshot_count": len(snapshots),
        "method": "iceberg_metadata",
        "confidence": "high",
        "note": f"Stats derived from Iceberg metadata ({len(snapshots)} snapshot(s)). No S3 listing required.",
    }


@tool
def get_iceberg_partition_details(database: str, table_name: str) -> dict:
    """Get detailed partition information from Iceberg metadata.

    Returns the partition spec (transforms and source columns), partition count,
    and per-partition size distribution for skew analysis.

    This is the Iceberg-native alternative to get_glue_partitions (which returns
    empty for Iceberg tables since they don't use Hive-style partitioning).

    Args:
        database: Glue database name
        table_name: Table name
    """
    # Step 1: Get metadata_location from Glue
    glue = get_client("glue")
    try:
        resp = glue.get_table(DatabaseName=database, Name=table_name)
        params = resp["Table"].get("Parameters", {})
        metadata_location = params.get("metadata_location", "")
    except Exception as e:
        return {"error": f"Failed to get table from Glue: {e}"}

    if not metadata_location:
        return {"error": "No metadata_location. Is this an Iceberg table?"}

    # Step 2: Read metadata.json
    bucket, key = _parse_s3_uri(metadata_location)
    metadata = _read_s3_json(bucket, key)
    if not metadata:
        return {"error": f"Failed to read metadata at {metadata_location}"}

    # Step 3: Get partition spec
    default_spec_id = metadata.get("default-spec-id", 0)
    partition_specs = metadata.get("partition-specs", [])
    partition_fields = []
    for spec in partition_specs:
        if spec.get("spec-id") == default_spec_id:
            partition_fields = spec.get("fields", [])
            break

    if not partition_fields:
        return {
            "database": database,
            "table_name": table_name,
            "is_partitioned": False,
            "partition_spec": [],
            "partition_count": 0,
            "note": "Table is unpartitioned.",
        }

    # Step 4: Resolve source-id to column names
    # Handle both V1 (schema.fields) and V2 (schemas[current-schema-id].fields)
    schema_fields = metadata.get("schema", {}).get("fields", [])
    if not schema_fields:
        current_schema_id = metadata.get("current-schema-id", 0)
        for s in metadata.get("schemas", []):
            if s.get("schema-id") == current_schema_id:
                schema_fields = s.get("fields", [])
                break
        if not schema_fields and metadata.get("schemas"):
            schema_fields = metadata["schemas"][-1].get("fields", [])

    field_id_map = {f["id"]: f["name"] for f in schema_fields}

    partition_spec = []
    for pf in partition_fields:
        source_id = pf.get("source-id")
        source_column = field_id_map.get(source_id, pf.get("name", "unknown"))
        partition_spec.append({
            "source_column": source_column,
            "transform": pf.get("transform", "identity"),
            "field_name": pf.get("name", ""),
            "field_id": pf.get("field-id"),
        })

    # Step 5: Count partitions from manifests
    current_snapshot_id = metadata.get("current-snapshot-id")
    current_snapshot = None
    for snap in metadata.get("snapshots", []):
        if snap.get("snapshot-id") == current_snapshot_id:
            current_snapshot = snap
            break

    partition_count = 0
    partition_sizes = {}
    if current_snapshot:
        manifest_list_path = current_snapshot.get("manifest-list", "")
        if manifest_list_path:
            ml_bucket, ml_key = _parse_s3_uri(manifest_list_path)
            manifest_list_entries = _read_avro_manifest(ml_bucket, ml_key)

            all_data_entries = []
            for ml_entry in manifest_list_entries:
                mp = ml_entry.get("manifest_path", "")
                if mp:
                    m_bucket, m_key = _parse_s3_uri(mp)
                    all_data_entries.extend(_read_avro_manifest(m_bucket, m_key))

            if all_data_entries:
                part_sizes = defaultdict(int)
                for entry in all_data_entries:
                    part = entry.get("partition", {})
                    if part:
                        pk = json.dumps(part, sort_keys=True)
                        part_sizes[pk] += entry.get("file_size_in_bytes", 0)
                partition_count = len(part_sizes)

                # Compute skew
                if partition_count >= 2:
                    sizes = sorted(part_sizes.values())
                    total = sum(sizes)
                    median = sizes[len(sizes) // 2]
                    mean = total / len(sizes)
                    partition_sizes = {
                        "count": partition_count,
                        "total_gb": round(total / (1024**3), 3),
                        "min_gb": round(sizes[0] / (1024**3), 3),
                        "max_gb": round(sizes[-1] / (1024**3), 3),
                        "median_gb": round(median / (1024**3), 3),
                        "mean_gb": round(mean / (1024**3), 3),
                        "max_to_median_ratio": round(sizes[-1] / median, 2) if median > 0 else 0,
                    }

        # Fallback from snapshot summary
        if partition_count == 0:
            summary = current_snapshot.get("summary", {})
            tp = summary.get("total-partitions")
            if tp:
                partition_count = int(tp)

    return {
        "database": database,
        "table_name": table_name,
        "is_partitioned": True,
        "partition_spec": partition_spec,
        "partition_count": partition_count,
        "partition_size_distribution": partition_sizes if partition_sizes else None,
        "note": f"Partition info from Iceberg metadata. {partition_count} distinct partition value(s).",
    }
