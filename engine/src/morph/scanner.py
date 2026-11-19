# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""S3 scanner — discovers files and reads Parquet footers for metadata.

Adapted from iceberg-translator. Walks a Hive-style S3 prefix,
reads Parquet footers for schema and column stats, and returns
FileMetadata records.

Assumes all data files under the prefix are Parquet. Zero-byte files
are skipped. Footer read failures are raised as errors (pipeline should
fail rather than silently commit files with missing metadata).
"""

import logging
import re
from dataclasses import dataclass, field

import pyarrow.parquet as pq
import pyarrow.fs as pafs

log = logging.getLogger(__name__)

_PARTITION_RE = re.compile(r"([^/]+)=([^/]+)")


@dataclass
class FileMetadata:
    """Metadata for a single discovered file."""
    file_path: str
    partition_values: dict[str, str]
    record_count: int
    file_size_bytes: int
    status: str = "ACTIVE"
    column_sizes: dict[int, int] = field(default_factory=dict)
    value_counts: dict[int, int] = field(default_factory=dict)
    null_value_counts: dict[int, int] = field(default_factory=dict)
    lower_bounds: dict[int, bytes] = field(default_factory=dict)
    upper_bounds: dict[int, bytes] = field(default_factory=dict)


def parse_partition_values(file_path: str, prefix: str,
                           partition_columns: list[str] = None
                           ) -> dict[str, str]:
    """Extract partition key=value pairs from a Hive-style S3 path."""
    relative = file_path[len(prefix):] if file_path.startswith(prefix) else file_path
    values = {}
    for part in relative.split("/"):
        m = _PARTITION_RE.fullmatch(part)
        if m:
            k, v = m.group(1), m.group(2)
            if not partition_columns or k in partition_columns:
                values[k] = v
    return values


# Reused filesystem handles. pyarrow's S3FileSystem/LocalFileSystem are
# thread-safe, and constructing an S3FileSystem per footer read is expensive
# and connection-hungry — at high morph concurrency that churn is a real cost.
# Cache one of each and share across all footer reads.
_s3_fs = None
_local_fs = None
_fs_lock = None


def _get_fs(file_path: str):
    global _s3_fs, _local_fs, _fs_lock
    if _fs_lock is None:
        import threading
        _fs_lock = threading.Lock()
    if file_path.startswith("s3://"):
        if _s3_fs is None:
            with _fs_lock:
                if _s3_fs is None:
                    _s3_fs = pafs.S3FileSystem()
        return _s3_fs, file_path[len("s3://"):]
    if _local_fs is None:
        with _fs_lock:
            if _local_fs is None:
                _local_fs = pafs.LocalFileSystem()
    return _local_fs, file_path


def list_data_files(s3_prefix: str,
                    region: str = "us-east-1") -> list[dict]:
    """List all data files under an S3 prefix, skipping zero-byte files.

    Assumes all files are Parquet (no extension filtering).
    Skips common non-data markers (_SUCCESS, _metadata, etc.)
    and zero-byte objects.
    """
    without_scheme = s3_prefix[len("s3://"):]
    bucket, _, key_prefix = without_scheme.partition("/")
    from ..utils.aws import get_client
    s3 = get_client("s3", region)
    paginator = s3.get_paginator("list_objects_v2")

    # Prefixes/filenames to skip (common Hive/Spark markers)
    _SKIP_PREFIXES = ("_SUCCESS", "_metadata", "_common_metadata",
                      ".hive-staging", "_temporary", "_spark_metadata")

    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]

            # Skip zero-byte files
            if size == 0:
                continue

            # Skip non-data marker files and hidden dot-prefix files
            basename = key.rsplit("/", 1)[-1] if "/" in key else key
            if any(basename.startswith(p) for p in _SKIP_PREFIXES):
                continue
            if basename.startswith("."):
                continue

            # Skip directories (keys ending with /)
            if key.endswith("/"):
                continue

            # Skip Iceberg/Delta metadata files that may exist under
            # nested target table locations
            if "/metadata/" in key or "/_delta_log/" in key:
                continue

            # Skip non-parquet files (avro manifests, json metadata, etc.)
            # that could exist in nested Iceberg table directories
            if basename.endswith((".avro", ".json", ".crc")):
                continue

            files.append({
                "key": key,
                "size": size,
                "full_path": f"s3://{bucket}/{key}",
            })

    log.info("Found %d data files under %s", len(files), s3_prefix)
    return files


def read_parquet_footer(file_path: str) -> tuple[int, dict]:
    """Read a Parquet file footer.

    Returns (record_count, column_stats_dict).
    Raises on failure — caller should handle the error.

    The column stats dict contains:
      - column_sizes: {col_idx: size_in_bytes}
      - value_counts: {col_idx: count}
      - null_value_counts: {col_idx: null_count}
      - lower_bounds: {col_idx: raw_bytes} (where available)
      - upper_bounds: {col_idx: raw_bytes} (where available)

    INT96 timestamp columns are skipped for min/max bounds since
    their physical representation is not compatible with Iceberg.
    """
    fs, path = _get_fs(file_path)
    pf = pq.ParquetFile(fs.open_input_file(path))
    metadata = pf.metadata

    record_count = metadata.num_rows

    # Extract per-column statistics from row group metadata
    column_sizes: dict[int, int] = {}
    value_counts: dict[int, int] = {}
    null_value_counts: dict[int, int] = {}
    lower_bounds: dict[int, bytes] = {}
    upper_bounds: dict[int, bytes] = {}

    # Track min/max across row groups for each column
    col_mins: dict[int, object] = {}
    col_maxs: dict[int, object] = {}

    for rg_idx in range(metadata.num_row_groups):
        rg = metadata.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            # Accumulate sizes across row groups
            column_sizes[col_idx] = (
                column_sizes.get(col_idx, 0) + col.total_compressed_size)
            value_counts[col_idx] = (
                value_counts.get(col_idx, 0) + rg.num_rows)

            if col.is_stats_set and col.statistics:
                stats = col.statistics
                null_value_counts[col_idx] = (
                    null_value_counts.get(col_idx, 0)
                    + (stats.null_count if stats.has_null_count else 0))

                # Extract min/max — skip INT96 physical types
                if stats.has_min_max and stats.physical_type != "INT96":
                    try:
                        cur_min = stats.min
                        cur_max = stats.max
                        if col_idx not in col_mins or cur_min < col_mins[col_idx]:
                            col_mins[col_idx] = cur_min
                        if col_idx not in col_maxs or cur_max > col_maxs[col_idx]:
                            col_maxs[col_idx] = cur_max
                    except (TypeError, ArithmeticError):
                        # Comparison failed (e.g., incompatible types)
                        pass
            else:
                null_value_counts.setdefault(col_idx, 0)

    # Serialize min/max to bytes for Iceberg DataFile format.
    # Iceberg uses the raw Parquet encoding for bounds, which is what
    # min_raw/max_raw provides. However, we only have the decoded values
    # from aggregation across row groups. For single-row-group files
    # we can use min_raw/max_raw directly. For multi-row-group files
    # with aggregated values, we skip bounds (pyiceberg needs raw bytes).
    if metadata.num_row_groups == 1:
        rg = metadata.row_group(0)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            if (col.is_stats_set and col.statistics
                    and col.statistics.has_min_max
                    and col.statistics.physical_type != "INT96"):
                try:
                    lower_bounds[col_idx] = col.statistics.min_raw
                    upper_bounds[col_idx] = col.statistics.max_raw
                except Exception as e:
                    # Some column types don't expose usable raw min/max; skip
                    # bounds for this column rather than failing the scan.
                    log.debug("Skipping min/max bounds for column %s: %s", col_idx, e)

    stats_dict = {
        "column_sizes": column_sizes,
        "value_counts": value_counts,
        "null_value_counts": null_value_counts,
    }
    if lower_bounds:
        stats_dict["lower_bounds"] = lower_bounds
    if upper_bounds:
        stats_dict["upper_bounds"] = upper_bounds

    return record_count, stats_dict


def read_parquet_schema(file_path: str) -> dict:
    """Read a Parquet file's Arrow schema as an ordered {column_name: pa.DataType} map.

    Used to reconcile a Glue-catalog-derived table schema against the actual
    physical types in the data files. Raises on read failure.
    """
    fs, path = _get_fs(file_path)
    pf = pq.ParquetFile(fs.open_input_file(path))
    arrow_schema = pf.schema_arrow
    return {arrow_schema.field(i).name: arrow_schema.field(i).type
            for i in range(len(arrow_schema))}


def data_file_candidates(s3_prefix: str, region: str = "us-east-1",
                         limit: int = 5) -> list[str]:
    """Return up to `limit` candidate data-file paths under a prefix.

    Filters out Hudi/Delta metadata and log files that share the table prefix
    so callers sampling a Parquet file don't pick a non-Parquet object. The
    caller should still tolerate a read failure on any individual candidate.
    """
    out = []
    for f in list_data_files(s3_prefix, region):
        key = f["key"]
        base = key.rsplit("/", 1)[-1] if "/" in key else key
        # Skip Hudi metadata / log files and Delta logs.
        if "/.hoodie/" in f"/{key}" or ".hoodie_partition_metadata" in base:
            continue
        if base.startswith(".hoodie") or ".log." in base or base.endswith(".log"):
            continue
        if "/_delta_log/" in f"/{key}":
            continue
        # Prefer files that look like Parquet base files.
        out.append(f["full_path"])
        if len(out) >= limit:
            break
    return out

