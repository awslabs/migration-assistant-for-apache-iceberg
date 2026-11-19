# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""S3 tools for analyzing table data files."""

import math
from collections import defaultdict
from strands import tool
from tools.aws import get_client
from tools.glue_analyzer import get_glue_table

# Threshold for considering Glue catalog stats "fresh enough" (bytes divergence)
_STALENESS_THRESHOLD = 0.20  # 20% divergence triggers full estimation


@tool
def list_s3_objects(s3_path: str, max_keys: int = 20) -> dict:
    """List objects in an S3 path to understand file layout.

    Args:
        s3_path: S3 URI like s3://bucket/prefix
        max_keys: Max number of objects to return
    """
    s3 = get_client("s3")
    bucket, prefix = s3_path.replace("s3://", "").split("/", 1)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    objects = []
    total_size = 0
    for obj in resp.get("Contents", []):
        objects.append({
            "key": obj["Key"],
            "size_bytes": obj["Size"],
            "last_modified": obj["LastModified"].isoformat(),
        })
        total_size += obj["Size"]
    return {
        "bucket": bucket,
        "prefix": prefix,
        "object_count": resp.get("KeyCount", 0),
        "is_truncated": resp.get("IsTruncated", False),
        "total_size_bytes": total_size,
        "objects": objects,
    }


@tool
def get_s3_object_sample(s3_path: str) -> dict:
    """Get size statistics from a sample of files in an S3 path.

    Samples up to 100 objects to compute average file size,
    total count estimate, and size distribution.

    Args:
        s3_path: S3 URI like s3://bucket/prefix
    """
    s3 = get_client("s3")
    bucket, prefix = s3_path.replace("s3://", "").split("/", 1)

    paginator = s3.get_paginator("list_objects_v2")
    sizes = []
    total_count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": 500}):
        for obj in page.get("Contents", []):
            if obj["Size"] > 0:  # skip directory markers
                sizes.append(obj["Size"])
                total_count += 1

    if not sizes:
        return {"error": "No data files found", "s3_path": s3_path}

    sizes.sort()
    avg_size = sum(sizes) / len(sizes)
    total_size = sum(sizes)

    return {
        "s3_path": s3_path,
        "files_sampled": len(sizes),
        "total_files_found": total_count,
        "total_size_bytes": total_size,
        "total_size_gb": round(total_size / (1024**3), 3),
        "avg_file_size_bytes": round(avg_size),
        "avg_file_size_mb": round(avg_size / (1024**2), 2),
        "min_file_size_bytes": sizes[0],
        "max_file_size_bytes": sizes[-1],
        "median_file_size_bytes": sizes[len(sizes) // 2],
    }


def _parse_s3_uri(s3_path: str) -> tuple:
    """Parse s3://bucket/prefix into (bucket, prefix).

    Ensures the prefix ends with '/' to avoid matching sibling prefixes.
    For example, prefix 'db/events' would also match 'db/events_archive/'
    without the trailing slash.
    """
    path = s3_path.replace("s3://", "")
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    # Ensure trailing slash to scope listing to this table only
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"
    return bucket, prefix


def _count_objects_fast(bucket: str, prefix: str, partition_depth: int = 0) -> dict:
    """Count data files, sum total size, and collect per-partition sizes.

    Iterates through all objects once to collect file count, total size in
    bytes, per-partition size distribution, and timestamp bounds. This
    eliminates the need for separate sampling — the full traversal gives
    exact numbers.

    Filters out common non-data files like _SUCCESS markers, .crc files,
    Spark commit metadata, and directory markers (zero-byte objects).

    Derives partition membership by extracting the partition path at the
    correct depth relative to the table prefix. For a table at prefix
    "db/table/" with partition_depth=2 (e.g., year/month), a file at
    "db/table/year=2024/month=01/part-00000.parquet" yields partition
    "year=2024/month=01/".

    Args:
        bucket: S3 bucket name.
        prefix: S3 prefix for the table root (should end with /).
        partition_depth: Number of partition key levels. If 0, no partition
            counting is performed (unpartitioned table).

    Returns:
        dict with 'file_count', 'total_size_bytes', 'file_sizes' (list of
        individual file sizes in bytes), 'partition_count',
        'partition_sizes' (dict of partition_path → bytes),
        'newest_modified', and 'oldest_modified'.
    """
    s3 = get_client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    total = 0
    total_size = 0
    file_sizes = []
    partition_sizes = defaultdict(int)
    newest_modified = None
    oldest_modified = None

    # Normalize prefix so we can reliably strip it from keys
    norm_prefix = prefix if prefix.endswith("/") else prefix + "/"

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if _is_data_file(obj["Key"], obj["Size"]):
                total += 1
                total_size += obj["Size"]
                file_sizes.append(obj["Size"])

                mod = obj["LastModified"]
                if newest_modified is None or mod > newest_modified:
                    newest_modified = mod
                if oldest_modified is None or mod < oldest_modified:
                    oldest_modified = mod

                if partition_depth > 0:
                    # Strip the table prefix to get the relative path
                    relative = obj["Key"][len(norm_prefix):]
                    # Split into path segments and take the first N as the partition
                    segments = relative.split("/")
                    if len(segments) > partition_depth:
                        partition_path = "/".join(segments[:partition_depth])
                        partition_sizes[partition_path] += obj["Size"]

    return {
        "file_count": total,
        "total_size_bytes": total_size,
        "file_sizes": file_sizes,
        "partition_count": len(partition_sizes) if partition_depth > 0 else 0,
        "partition_sizes": dict(partition_sizes),
        "newest_modified": newest_modified,
        "oldest_modified": oldest_modified,
    }


def _compute_partition_skew(partition_sizes: dict) -> dict | None:
    """Compute skew metrics from a partition size distribution.

    Returns None if fewer than 2 partitions (skew is meaningless).

    Metrics:
    - coefficient_of_variation (CV): std_dev / mean. CV > 1.0 indicates
      high skew; CV < 0.3 indicates relatively even distribution.
    - max_to_median_ratio: how much larger the biggest partition is vs
      the median. Ratios > 3x suggest a hot partition.
    - top_partitions: the N largest partitions by size (for surfacing
      to the user).
    - is_skewed: boolean flag (CV > 0.5 or max/median > 3).

    Args:
        partition_sizes: dict of partition_path → total_bytes

    Returns:
        dict with skew metrics, or None if not enough partitions.
    """
    if len(partition_sizes) < 2:
        return None

    sizes = sorted(partition_sizes.values())
    n = len(sizes)
    mean = sum(sizes) / n
    median = sizes[n // 2]

    variance = sum((s - mean) ** 2 for s in sizes) / n
    std_dev = math.sqrt(variance)

    cv = std_dev / mean if mean > 0 else 0.0

    max_size = sizes[-1]
    min_size = sizes[0]
    max_to_median = max_size / median if median > 0 else float("inf")

    is_skewed = cv > 0.5 or max_to_median > 3.0

    # Top N partitions by size (for user visibility)
    top_n = min(5, n)
    sorted_partitions = sorted(partition_sizes.items(), key=lambda x: x[1], reverse=True)
    top_partitions = [
        {
            "partition": p,
            "size_gb": round(s / (1024**3), 3),
            "pct_of_total": round(s / sum(sizes) * 100, 1),
        }
        for p, s in sorted_partitions[:top_n]
    ]

    return {
        "is_skewed": is_skewed,
        "coefficient_of_variation": round(cv, 3),
        "max_to_median_ratio": round(max_to_median, 2),
        "min_partition_gb": round(min_size / (1024**3), 3),
        "max_partition_gb": round(max_size / (1024**3), 3),
        "median_partition_gb": round(median / (1024**3), 3),
        "mean_partition_gb": round(mean / (1024**3), 3),
        "top_partitions": top_partitions,
    }


def _compute_file_size_skew(file_sizes: list) -> dict | None:
    """Compute file size distribution and skew metrics.

    Returns None if fewer than 2 files (distribution is meaningless).

    Metrics:
    - percentiles (p10, p25, p50, p75, p90): size distribution in MB
    - coefficient_of_variation (CV): std_dev / mean. CV > 1.0 indicates
      highly variable file sizes.
    - pct_under_16mb: percentage of files below the 16 MB Iceberg
      optimal threshold. High values indicate a small-file problem
      that morph would preserve.
    - pct_over_256mb: percentage of files above 256 MB. High values
      may indicate under-partitioned writes or lack of compaction limits.
    - is_skewed: boolean flag (CV > 0.5 or >20% of files under 16 MB
      while median is above 16 MB — i.e., bimodal distribution).

    Args:
        file_sizes: list of file sizes in bytes (from _count_objects_fast)

    Returns:
        dict with file size distribution metrics, or None if < 2 files.
    """
    if len(file_sizes) < 2:
        return None

    sizes = sorted(file_sizes)
    n = len(sizes)
    total = sum(sizes)
    mean = total / n

    # Percentile helper (nearest-rank method)
    def percentile(pct):
        idx = max(0, min(int(pct / 100 * n) - 1, n - 1))
        return sizes[idx]

    p10 = percentile(10)
    p25 = percentile(25)
    p50 = percentile(50)  # median
    p75 = percentile(75)
    p90 = percentile(90)

    variance = sum((s - mean) ** 2 for s in sizes) / n
    std_dev = math.sqrt(variance)
    cv = std_dev / mean if mean > 0 else 0.0

    threshold_16mb = 16 * 1024 * 1024  # 16 MB in bytes
    threshold_256mb = 256 * 1024 * 1024  # 256 MB in bytes
    count_under_16mb = sum(1 for s in sizes if s < threshold_16mb)
    count_over_256mb = sum(1 for s in sizes if s > threshold_256mb)
    pct_under_16mb = count_under_16mb / n * 100
    pct_over_256mb = count_over_256mb / n * 100

    # Detect skew: high CV, or bimodal (many small files but median is healthy)
    is_bimodal = pct_under_16mb > 20 and p50 >= threshold_16mb
    is_skewed = cv > 0.5 or is_bimodal

    return {
        "is_skewed": is_skewed,
        "coefficient_of_variation": round(cv, 3),
        "p10_mb": round(p10 / (1024**2), 3),
        "p25_mb": round(p25 / (1024**2), 3),
        "p50_mb": round(p50 / (1024**2), 3),
        "p75_mb": round(p75 / (1024**2), 3),
        "p90_mb": round(p90 / (1024**2), 3),
        "min_mb": round(sizes[0] / (1024**2), 3),
        "max_mb": round(sizes[-1] / (1024**2), 3),
        "mean_mb": round(mean / (1024**2), 3),
        "pct_under_16mb": round(pct_under_16mb, 1),
        "pct_over_256mb": round(pct_over_256mb, 1),
        "total_files": n,
    }


def _is_data_file(key: str, size: int) -> bool:
    """Determine if an S3 object is a data file vs metadata/marker."""
    if size == 0:
        return False

    filename = key.rsplit("/", 1)[-1] if "/" in key else key

    # Skip common marker/metadata files
    skip_prefixes = ("_SUCCESS", "_committed_", "_started_", ".")
    if filename.startswith(skip_prefixes):
        return False

    skip_suffixes = (".crc", ".metadata", ".stats")
    if filename.endswith(skip_suffixes):
        return False

    # Skip known non-data filenames common with CSV tables
    skip_names = ("manifest.json", "manifest.csv", "_schema", "schema.json",
                  "_col_name", "_column_names")
    if filename in skip_names or filename.lower() in skip_names:
        return False

    # Skip metadata and staging directories
    skip_dirs = ("/_metadata/", "/_common_metadata/", "/_temporary/",
                 "/_spark_metadata/", "/.hoodie/", "/.hive-staging")
    if any(d in key for d in skip_dirs):
        return False

    return True


@tool
def estimate_table_size(database: str, table_name: str) -> dict:
    """Estimate table size by enumerating all data files under the table prefix.

    Strategy:
    1. Fetch table metadata via get_glue_table
    2. Check Glue catalog stats — use as a fast-path if fresh
    3. Enumerate all data files via _count_objects_fast to get exact size
    4. Compare against catalog stats to determine freshness

    Args:
        database: Glue database name
        table_name: Table name
    """
    # Use the existing get_glue_table tool to fetch metadata
    table_meta = get_glue_table(database=database, table_name=table_name)

    if "error" in table_meta:
        return {"error": f"Failed to get table metadata: {table_meta['error']}"}

    params = table_meta.get("parameters", {})
    location = table_meta.get("location", "")
    partition_keys = table_meta.get("partition_keys", [])
    is_partitioned = len(partition_keys) > 0

    if not location:
        return {"error": "Table has no S3 location in StorageDescriptor"}

    bucket, prefix = _parse_s3_uri(location)

    # --- Step 1: Check Glue catalog stats ---
    catalog_size_bytes = None
    catalog_size_str = params.get("totalSize") or params.get("rawDataSize") or params.get("spark.sql.statistics.totalSize")
    if catalog_size_str:
        try:
            catalog_size_bytes = int(catalog_size_str)
        except (ValueError, TypeError):
            pass

    # --- Step 2: Full enumeration for exact size ---
    partition_depth = len(partition_keys) if is_partitioned else 0
    count_result = _count_objects_fast(bucket, prefix, partition_depth=partition_depth)
    total_file_count = count_result["file_count"]
    total_size_bytes = count_result["total_size_bytes"]
    partition_count = count_result["partition_count"]
    partition_sizes = count_result.get("partition_sizes", {})
    newest_modified = count_result.get("newest_modified")
    oldest_modified = count_result.get("oldest_modified")

    partition_skew = _compute_partition_skew(partition_sizes) if partition_sizes else None

    file_sizes = count_result.get("file_sizes", [])
    file_size_skew = _compute_file_size_skew(file_sizes) if file_sizes else None

    last_modified_days_ago = None
    data_span_days = None
    if newest_modified:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        last_modified_days_ago = (now - newest_modified).days
        if oldest_modified:
            data_span_days = (newest_modified - oldest_modified).days

    if total_file_count == 0:
        # No data files found — fall back to catalog if available
        if catalog_size_bytes and catalog_size_bytes > 0:
            return {
                "database": database,
                "table_name": table_name,
                "estimated_size_bytes": catalog_size_bytes,
                "estimated_size_gb": round(catalog_size_bytes / (1024**3), 3),
                "method": "glue_catalog_only",
                "confidence": "low",
                "partition_count": 0,
                "note": "No S3 data files found at location. Using catalog stats only.",
            }
        return {"error": "No data files found at S3 location", "location": location}

    avg_file_size = total_size_bytes / total_file_count

    # --- Step 3: Compare with catalog stats if available ---
    if catalog_size_bytes and catalog_size_bytes > 0:
        reference = max(total_size_bytes, catalog_size_bytes)
        divergence = abs(total_size_bytes - catalog_size_bytes) / reference

        if divergence <= _STALENESS_THRESHOLD:
            # Catalog is fresh — use the larger of the two (catalog only
            # goes stale in one direction: table grows, stats stay the same)
            best_estimate = max(catalog_size_bytes, total_size_bytes)
            return {
                "database": database,
                "table_name": table_name,
                "estimated_size_bytes": best_estimate,
                "estimated_size_gb": round(best_estimate / (1024**3), 3),
                "method": "glue_catalog_verified",
                "confidence": "high",
                "is_partitioned": is_partitioned,
                "partition_count": partition_count,
                "partition_skew": partition_skew,
                "file_size_skew": file_size_skew,
                "details": {
                    "total_files": total_file_count,
                    "avg_file_size_mb": round(avg_file_size / (1024**2), 4),
                    "avg_file_size_kb": round(avg_file_size / 1024, 1),
                    "last_modified_days_ago": last_modified_days_ago,
                    "data_span_days": data_span_days,
                    "catalog_size_bytes": catalog_size_bytes,
                    "s3_enumerated_size_bytes": total_size_bytes,
                    "divergence_pct": round(divergence * 100, 1),
                },
                "note": "Catalog stats verified against full S3 enumeration. Divergence within threshold.",
            }

        # Catalog is stale — use the enumerated value (ground truth)
        return {
            "database": database,
            "table_name": table_name,
            "estimated_size_bytes": total_size_bytes,
            "estimated_size_gb": round(total_size_bytes / (1024**3), 3),
            "method": "s3_enumerated",
            "confidence": "high",
            "is_partitioned": is_partitioned,
            "partition_count": partition_count,
            "partition_skew": partition_skew,
            "file_size_skew": file_size_skew,
            "details": {
                "total_files": total_file_count,
                "avg_file_size_mb": round(avg_file_size / (1024**2), 4),
                "avg_file_size_kb": round(avg_file_size / 1024, 1),
                "last_modified_days_ago": last_modified_days_ago,
                "data_span_days": data_span_days,
                "catalog_size_bytes": catalog_size_bytes,
                "catalog_divergence_pct": round(divergence * 100, 1),
            },
            "note": (
                f"Catalog stats stale ({divergence*100:.0f}% divergence). "
                f"Size from full S3 enumeration of {total_file_count} data files."
            ),
        }

    # --- No catalog stats — return enumerated total directly ---
    return {
        "database": database,
        "table_name": table_name,
        "estimated_size_bytes": total_size_bytes,
        "estimated_size_gb": round(total_size_bytes / (1024**3), 3),
        "method": "s3_enumerated",
        "confidence": "high",
        "is_partitioned": is_partitioned,
        "partition_count": partition_count,
        "partition_skew": partition_skew,
        "file_size_skew": file_size_skew,
        "details": {
            "total_files": total_file_count,
            "avg_file_size_mb": round(avg_file_size / (1024**2), 4),
            "avg_file_size_kb": round(avg_file_size / 1024, 1),
            "last_modified_days_ago": last_modified_days_ago,
            "data_span_days": data_span_days,
        },
        "note": (
            f"No catalog stats available. Size from full S3 enumeration "
            f"of {total_file_count} data files."
        ),
    }
