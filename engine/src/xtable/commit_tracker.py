# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Commit tracking for Hudi and Delta Lake tables.

Uses native Python libraries (hudi-rs, delta-rs) to read source table
commit timestamps without needing JVM or external compute.

Used by the morph reconciler to detect when new commits have landed
and the Iceberg metadata needs an incremental XTable sync.
"""

import logging
import os

log = logging.getLogger(__name__)


def get_latest_hudi_commit(table_base_path: str,
                           region: str = "us-east-1") -> str | None:
    """Get the latest completed commit timestamp from a Hudi table.

    Uses the hudi-rs Python binding to read the timeline.
    Falls back to S3 listing of .hoodie/ if the library is unavailable.

    Returns ISO timestamp string or None if no commits found.
    """
    # Try native hudi-rs library first
    try:
        from hudi import HudiTableBuilder

        storage_options = {"region": region}
        if os.getenv("AWS_PROFILE"):
            storage_options["profile"] = os.getenv("AWS_PROFILE")

        table = (HudiTableBuilder
                 .from_base_uri(table_base_path)
                 .with_storage_options(storage_options)
                 .build())

        # Get timeline — latest commit instant
        # hudi-rs exposes timeline as part of table metadata
        timeline = table.get_timeline()
        if timeline:
            # Timeline instants are sorted; last is most recent
            latest = timeline[-1]
            # Instant format: YYYYMMDDHHmmss or similar
            return str(latest)
        return None

    except ImportError:
        log.debug("hudi package not available, falling back to S3 listing")
    except Exception as e:
        log.debug("hudi-rs timeline read failed: %s, falling back to S3", e)

    # Fallback: list .hoodie/ directory for commit files
    return _get_latest_hudi_commit_s3(table_base_path, region)


def _get_latest_hudi_commit_s3(table_base_path: str,
                                region: str) -> str | None:
    """Fallback: read latest Hudi commit from S3 .hoodie/ directory.

    Hudi stores completed commits as files like:
      .hoodie/20240315120000.commit
      .hoodie/20240315130000.deltacommit

    We list these and find the most recent one.
    """
    import boto3

    path = table_base_path.rstrip("/")
    if not path.startswith("s3://"):
        return None

    without_scheme = path[5:]
    bucket, _, prefix = without_scheme.partition("/")
    hoodie_prefix = f"{prefix}/.hoodie/" if prefix else ".hoodie/"

    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")

    latest_commit = None
    for page in paginator.paginate(Bucket=bucket, Prefix=hoodie_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            basename = key.rsplit("/", 1)[-1]
            # Match commit files: <timestamp>.commit or <timestamp>.deltacommit
            if basename.endswith((".commit", ".deltacommit")):
                commit_ts = basename.split(".")[0]
                if latest_commit is None or commit_ts > latest_commit:
                    latest_commit = commit_ts

    return latest_commit


def get_latest_delta_version(table_base_path: str,
                             region: str = "us-east-1") -> int | None:
    """Get the latest version number from a Delta Lake table.

    Uses the deltalake (delta-rs) Python binding.

    Returns version number (int) or None if table can't be read.
    """
    try:
        from deltalake import DeltaTable

        storage_options = {"region": region}
        if os.getenv("AWS_PROFILE"):
            storage_options["profile"] = os.getenv("AWS_PROFILE")
        if os.getenv("AWS_DEFAULT_REGION"):
            storage_options["region"] = os.getenv("AWS_DEFAULT_REGION")

        dt = DeltaTable(table_base_path, storage_options=storage_options)
        return dt.version()

    except ImportError:
        log.debug("deltalake package not available, falling back to S3")
    except Exception as e:
        log.debug("deltalake version read failed: %s, falling back to S3", e)

    # Fallback: list _delta_log/ directory
    return _get_latest_delta_version_s3(table_base_path, region)


def _get_latest_delta_version_s3(table_base_path: str,
                                  region: str) -> int | None:
    """Fallback: read latest Delta version from _delta_log/ directory.

    Delta stores commits as numbered JSON files:
      _delta_log/00000000000000000000.json
      _delta_log/00000000000000000001.json
    """
    import boto3

    path = table_base_path.rstrip("/")
    if not path.startswith("s3://"):
        return None

    without_scheme = path[5:]
    bucket, _, prefix = without_scheme.partition("/")
    log_prefix = f"{prefix}/_delta_log/" if prefix else "_delta_log/"

    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")

    max_version = -1
    for page in paginator.paginate(Bucket=bucket, Prefix=log_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            basename = key.rsplit("/", 1)[-1]
            if basename.endswith(".json") and basename[:-5].isdigit():
                version = int(basename[:-5])
                if version > max_version:
                    max_version = version

    return max_version if max_version >= 0 else None


def has_new_commits(source_format: str, table_base_path: str,
                    last_synced_commit: str | None,
                    region: str = "us-east-1") -> tuple[bool, str | None]:
    """Check if the source table has commits newer than last_synced_commit.

    Args:
        source_format: "hudi" or "delta"
        table_base_path: S3 path to the source table
        last_synced_commit: The commit marker from the last successful sync
                           (Hudi: timestamp string, Delta: version number as string)
        region: AWS region

    Returns:
        (has_new, latest_commit) — whether new commits exist and the current latest
    """
    if source_format.lower() == "hudi":
        latest = get_latest_hudi_commit(table_base_path, region)
        if latest is None:
            return False, None
        if last_synced_commit is None:
            return True, latest
        return latest > last_synced_commit, latest

    elif source_format.lower() == "delta":
        latest = get_latest_delta_version(table_base_path, region)
        if latest is None:
            return False, None
        if last_synced_commit is None:
            return True, str(latest)
        try:
            return latest > int(last_synced_commit), str(latest)
        except (ValueError, TypeError):
            return True, str(latest)

    return False, None


# ── Affected-partition discovery (for full-partition-rewrite sync) ──────────
#
# For the incremental strategy's commit-based sync we rewrite the partitions
# that changed since the last synced commit (a full rewrite of each affected
# partition — NOT row-level CDC, which is a later enhancement). These helpers
# return the list of partition-value maps that were touched between a commit
# cursor and the latest commit, plus the latest commit marker. When the precise
# set can't be determined they return ALL_PARTITIONS so the caller rewrites the
# whole table — heavier, but always correct.

# Sentinel: caller should rewrite every partition (couldn't scope precisely).
ALL_PARTITIONS = "__ALL_PARTITIONS__"


def get_affected_partitions_since(source_format: str, table_base_path: str,
                                  last_synced_commit: str | None,
                                  region: str = "us-east-1"
                                  ) -> tuple[list[dict] | str, str | None]:
    """Partitions touched since ``last_synced_commit`` for a hudi/delta table.

    Returns ``(affected, latest_commit)`` where ``affected`` is either:
      * a list of ``{column: value}`` dicts (the partitions to rewrite), or
      * :data:`ALL_PARTITIONS` when the precise set can't be determined (or the
        source is unpartitioned) — the caller then rewrites the whole table.

    ``latest_commit`` is the new cursor to persist after a successful sync.
    Full-partition-rewrite semantics: we only need WHICH partitions changed;
    the sync reads their CURRENT state from the source and overwrites them in
    the target, so we do not need per-row change data here.
    """
    fmt = source_format.lower()
    if fmt == "hudi":
        return _hudi_affected_partitions(table_base_path, last_synced_commit, region)
    if fmt == "delta":
        return _delta_affected_partitions(table_base_path, last_synced_commit, region)
    return ALL_PARTITIONS, None


def _hudi_affected_partitions(table_base_path: str,
                              last_synced_commit: str | None,
                              region: str) -> tuple[list[dict] | str, str | None]:
    """Hudi: partitions written by commits after ``last_synced_commit``.

    Uses hudi-rs incremental read metadata when available; falls back to the
    ALL_PARTITIONS sentinel (whole-table rewrite) if the library or the
    partition detail is unavailable.
    """
    latest = get_latest_hudi_commit(table_base_path, region)
    if latest is None:
        return [], None  # no commits at all → nothing to sync
    try:
        from hudi import HudiTableBuilder

        storage_options = {"region": region}
        if os.getenv("AWS_PROFILE"):
            storage_options["profile"] = os.getenv("AWS_PROFILE")
        table = (HudiTableBuilder
                 .from_base_uri(table_base_path)
                 .with_storage_options(storage_options)
                 .build())

        # Incremental read of file slices changed after the cursor. hudi-rs
        # exposes the changed file groups' partition paths; derive the distinct
        # partition-value maps from them.
        begin = last_synced_commit or ""
        file_slices = table.get_file_slices_between(begin, latest) \
            if hasattr(table, "get_file_slices_between") else None
        if file_slices is None:
            # No incremental API available on this hudi-rs build.
            log.debug("hudi-rs incremental slice API unavailable; "
                      "rewriting all partitions")
            return ALL_PARTITIONS, latest
        parts = _distinct_partition_maps(
            getattr(fs, "partition_path", None) for fs in file_slices)
        if not parts:
            return ALL_PARTITIONS, latest
        return parts, latest
    except ImportError:
        log.debug("hudi package unavailable; rewriting all partitions")
        return ALL_PARTITIONS, latest
    except Exception as e:
        log.debug("hudi incremental partition scan failed (%s); "
                  "rewriting all partitions", e)
        return ALL_PARTITIONS, latest


def _delta_affected_partitions(table_base_path: str,
                               last_synced_commit: str | None,
                               region: str) -> tuple[list[dict] | str, str | None]:
    """Delta: partitions touched by versions after ``last_synced_commit``.

    Reads the Change Data Feed metadata (add/remove actions carry
    partitionValues) between ``startingVersion = last_synced_commit + 1`` and
    the latest version. Falls back to ALL_PARTITIONS when CDF/partition detail
    is unavailable.
    """
    latest = get_latest_delta_version(table_base_path, region)
    if latest is None:
        return [], None
    latest_str = str(latest)
    try:
        from deltalake import DeltaTable

        storage_options = {"region": region}
        if os.getenv("AWS_DEFAULT_REGION"):
            storage_options["region"] = os.getenv("AWS_DEFAULT_REGION")
        dt = DeltaTable(table_base_path, storage_options=storage_options)

        try:
            start_version = int(last_synced_commit) + 1 if last_synced_commit else 0
        except (ValueError, TypeError):
            start_version = 0
        if start_version > latest:
            return [], latest_str  # already current

        # Use the change-data-feed to list partitions touched in the range.
        cdf = dt.load_cdf(starting_version=start_version, ending_version=latest) \
            if hasattr(dt, "load_cdf") else None
        if cdf is None:
            log.debug("deltalake CDF API unavailable; rewriting all partitions")
            return ALL_PARTITIONS, latest_str
        # cdf is a pyarrow RecordBatchReader; collect the partition columns.
        part_cols = dt.metadata().partition_columns
        if not part_cols:
            return ALL_PARTITIONS, latest_str  # unpartitioned → whole table
        tbl = cdf.read_all()
        cols = {c: tbl.column(c).to_pylist() for c in part_cols if c in tbl.column_names}
        if not cols:
            return ALL_PARTITIONS, latest_str
        rows = zip(*[cols[c] for c in part_cols if c in cols])
        parts = _distinct_partition_maps(
            {c: v for c, v in zip(part_cols, row)} for row in rows)
        if not parts:
            return [], latest_str
        return parts, latest_str
    except ImportError:
        log.debug("deltalake package unavailable; rewriting all partitions")
        return ALL_PARTITIONS, latest_str
    except Exception as e:
        log.debug("delta CDF partition scan failed (%s); rewriting all partitions", e)
        return ALL_PARTITIONS, latest_str


def _distinct_partition_maps(items) -> list[dict]:
    """Normalize an iterable of partition descriptors into distinct
    ``{column: value}`` dicts.

    Accepts either dicts (Delta partitionValues) or Hive-style path strings
    like ``region=us-east-1/event_date=2024-01-03`` (Hudi partition paths).
    """
    seen = set()
    out: list[dict] = []
    for it in items:
        if it is None:
            continue
        if isinstance(it, dict):
            pm = {str(k): (str(v) if v is not None else None) for k, v in it.items()}
        elif isinstance(it, str):
            pm = {}
            for seg in it.strip("/").split("/"):
                if "=" in seg:
                    k, _, v = seg.partition("=")
                    pm[k] = v
            if not pm:
                continue
        else:
            continue
        key = tuple(sorted(pm.items()))
        if key not in seen:
            seen.add(key)
            out.append(pm)
    return out
