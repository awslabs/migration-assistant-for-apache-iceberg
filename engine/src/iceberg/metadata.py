# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Iceberg metadata access via PyIceberg.

Provides direct access to Iceberg table metadata (snapshots, partition specs,
data files) without running Athena queries. Used by the Iceberg incremental
strategy for snapshot tracking and change detection.
"""

import logging
import os
from typing import Optional

from pyiceberg.catalog import load_catalog

log = logging.getLogger(__name__)

_CATALOG_NAME = os.getenv("ENGINE_ICEBERG_CATALOG", "glue")
_CATALOG_CONFIG = {
    "type": "glue",
    "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
}


def _get_catalog():
    """Get the PyIceberg catalog (Glue-backed)."""
    return load_catalog(_CATALOG_NAME, **_CATALOG_CONFIG)


def _load_table_with_fallback(catalog, database: str, table_name: str):
    """Load an Iceberg table, trying multiple identifier formats.
    
    PyIceberg Glue catalog may need different identifier formats depending
    on how the table was registered.
    """
    # Try standard format first: database.table_name
    identifiers = [
        f"{database}.{table_name}",
        (database, table_name),
    ]
    last_error = None
    for identifier in identifiers:
        try:
            return catalog.load_table(identifier)
        except Exception as e:
            last_error = e
            log.debug("load_table(%s) failed: %s", identifier, e)
            continue

    raise last_error or RuntimeError(f"Could not load table {database}.{table_name}")


class IcebergMetadataReader:
    """Read Iceberg table metadata directly via PyIceberg."""

    def __init__(self):
        self._catalog = _get_catalog()

    def get_latest_snapshot(self, database: str, table_name: str) -> Optional[dict]:
        """Get the latest snapshot ID and committed_at timestamp.
        
        Returns: {"snapshot_id": "123456", "committed_at": "2024-01-15 10:00:00.000"}
        or None if no snapshots exist.
        """
        try:
            log.debug("Loading Iceberg table: %s.%s", database, table_name)
            table = _load_table_with_fallback(self._catalog, database, table_name)
        except Exception as e:
            log.error("Failed to load table %s.%s from catalog: %s (%s)",
                      database, table_name, e, type(e).__name__)
            return None

        try:
            # Try current_snapshot() first, fall back to iterating snapshots
            snapshot = table.current_snapshot()
            if snapshot is None:
                # Some PyIceberg versions need metadata refresh or
                # current_snapshot() may not work — try snapshots list
                all_snapshots = list(table.snapshots())
                log.debug("Table %s.%s: current_snapshot() returned None, "
                          "snapshots list has %d entries",
                          database, table_name, len(all_snapshots))
                if not all_snapshots:
                    log.warning("Table %s.%s has no snapshots (empty table?)",
                                database, table_name)
                    return None
                # Use the last snapshot (most recent by position)
                snapshot = all_snapshots[-1]

            # Convert timestamp_ms to ISO-like string for Athena TIMESTAMP compatibility
            committed_at = ""
            if snapshot.timestamp_ms:
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(snapshot.timestamp_ms / 1000.0, tz=timezone.utc)
                committed_at = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # millisecond precision
            log.debug("Table %s.%s: latest snapshot=%s committed_at=%s",
                      database, table_name, snapshot.snapshot_id, committed_at)
            return {
                "snapshot_id": str(snapshot.snapshot_id),
                "committed_at": committed_at,
            }
        except Exception as e:
            log.error("Failed to read snapshot metadata for %s.%s: %s (%s)",
                      database, table_name, e, type(e).__name__)
            return None

    def get_partition_spec(self, database: str, table_name: str) -> list[dict]:
        """Get the current partition spec of a table.
        
        Returns list of: [{"source_column": "event_date", "transform": "day"}]
        """
        try:
            table = _load_table_with_fallback(self._catalog, database, table_name)
            spec = table.spec()
            schema = table.schema()

            result = []
            for field in spec.fields:
                source_field = schema.find_field(field.source_id)
                col_name = source_field.name if source_field else f"col_{field.source_id}"
                transform = str(field.transform)
                result.append({
                    "source_column": col_name,
                    "transform": transform,
                })
            return result
        except Exception as e:
            log.error("Failed to get partition spec for %s.%s: %s",
                      database, table_name, e)
            return []

    def has_deletes_between(self, database: str, table_name: str,
                            from_snapshot_id: int, to_snapshot_id: int) -> bool:
        """Check if any delete files were written between two snapshots.
        
        Returns True if there are delete operations (not append-only).
        """
        try:
            table = _load_table_with_fallback(self._catalog, database, table_name)
            snapshots = list(table.snapshots())

            # Find timestamps for from and to snapshots
            from_ts = None
            to_ts = None
            for snap in snapshots:
                if snap.snapshot_id == from_snapshot_id:
                    from_ts = snap.timestamp_ms or 0
                if snap.snapshot_id == to_snapshot_id:
                    to_ts = snap.timestamp_ms or 0

            log.debug("has_deletes_between: from=%s (ts=%s), to=%s (ts=%s), "
                      "total_snapshots=%d",
                      from_snapshot_id, from_ts, to_snapshot_id, to_ts,
                      len(snapshots))

            # If we can't find the from_snapshot in the list, it may have been
            # expired/compacted — assume deletes for safety
            if from_ts is None:
                log.warning("has_deletes_between: from_snapshot %s not found in "
                            "snapshot list (expired?), assuming deletes",
                            from_snapshot_id)
                return True

            # Check all snapshots that are AFTER from_snapshot (by timestamp or position)
            # Include to_snapshot itself but exclude from_snapshot
            for snap in snapshots:
                # Skip the from_snapshot itself
                if snap.snapshot_id == from_snapshot_id:
                    continue
                # Include if timestamp is after from_snapshot
                # (or if we can't determine timestamps, include all except from)
                snap_ts = snap.timestamp_ms or 0
                if from_ts is not None and snap_ts <= from_ts:
                    continue
                if to_ts is not None and snap_ts > to_ts:
                    continue

                summary = snap.summary
                # Handle different PyIceberg summary types
                if summary is None:
                    continue
                # Convert to dict if it's a Summary object
                if not isinstance(summary, dict):
                    try:
                        summary = dict(summary)
                    except (TypeError, ValueError):
                        summary = {}

                deleted_files = int(summary.get("deleted-data-files") or 0)
                deleted_records = int(summary.get("deleted-records") or 0)
                operation = str(summary.get("operation") or "")

                log.debug("  snapshot %s: op=%s, deleted_files=%d, deleted_records=%d, "
                          "summary_keys=%s",
                          snap.snapshot_id, operation, deleted_files, deleted_records,
                          list(summary.keys())[:8])

                if deleted_files > 0 or deleted_records > 0:
                    return True
                # Also check operation type directly
                if operation in ("delete", "overwrite", "replace"):
                    return True

            return False
        except Exception as e:
            log.warning("Failed to check deletes for %s.%s: %s (%s)",
                        database, table_name, e, type(e).__name__)
            return True  # Assume deletes if we can't check (safer)

    def get_new_data_files(self, database: str, table_name: str,
                           from_snapshot_id: int, to_snapshot_id: int) -> list[str]:
        """Get file paths of data files added between two snapshots.
        
        Used for append-only sync to INSERT only new files.
        """
        try:
            table = _load_table_with_fallback(self._catalog, database, table_name)
            # Get files at to_snapshot
            to_scan = table.scan(snapshot_id=to_snapshot_id)
            to_files = set()
            for task in to_scan.plan_files():
                to_files.add(task.file.file_path)

            # Get files at from_snapshot
            from_scan = table.scan(snapshot_id=from_snapshot_id)
            from_files = set()
            for task in from_scan.plan_files():
                from_files.add(task.file.file_path)

            # New files = in to but not in from
            new_files = to_files - from_files
            return list(new_files)
        except Exception as e:
            log.error("Failed to get new data files for %s.%s: %s",
                      database, table_name, e)
            return []

    def get_affected_partitions(self, database: str, table_name: str,
                                from_snapshot_id: int, to_snapshot_id: int,
                                partition_column: str) -> list[dict]:
        """Get partition values that changed between two snapshots.
        
        Returns list of dicts: [{"value": "54", "transform": "year", "source_column": "event_date"}]
        with the raw transformed value and transform type for proper SQL generation.
        """
        try:
            table = _load_table_with_fallback(self._catalog, database, table_name)

            # Get the partition spec to understand field mapping
            spec = table.spec()
            schema = table.schema()

            # Find the partition field index, name, and transform
            partition_field_idx = None
            partition_field_name = None
            partition_transform = "identity"
            for i, field in enumerate(spec.fields):
                source_field = schema.find_field(field.source_id)
                if source_field and source_field.name == partition_column:
                    partition_field_idx = i
                    partition_field_name = field.name
                    partition_transform = str(field.transform)
                    break

            if partition_field_idx is None:
                log.warning("Partition column '%s' not found in spec for %s.%s. "
                            "Spec fields: %s",
                            partition_column, database, table_name,
                            [(f.name, schema.find_field(f.source_id).name 
                              if schema.find_field(f.source_id) else '?')
                             for f in spec.fields])
                return []

            log.debug("Partition lookup: col=%s, field_idx=%d, field_name=%s, transform=%s",
                      partition_column, partition_field_idx, partition_field_name,
                      partition_transform)

            def _extract_partition_value(pv):
                """Extract partition value from a PyIceberg partition Record."""
                if pv is None:
                    return None
                try:
                    if hasattr(pv, '__getitem__'):
                        try:
                            val = pv[partition_field_idx]
                            if val is not None:
                                return str(val)
                        except (IndexError, KeyError, TypeError):
                            pass
                except TypeError:
                    pass
                if partition_field_name and hasattr(pv, partition_field_name):
                    val = getattr(pv, partition_field_name)
                    if val is not None:
                        return str(val)
                return None

            # Get files at both snapshots
            to_scan = table.scan(snapshot_id=to_snapshot_id)
            from_scan = table.scan(snapshot_id=from_snapshot_id)

            to_files = {}
            sample_logged = False
            for task in to_scan.plan_files():
                val = _extract_partition_value(task.file.partition)
                if not sample_logged and task.file.partition is not None:
                    log.debug("Sample partition record (to): type=%s, value=%s, "
                              "extracted=%s",
                              type(task.file.partition).__name__,
                              repr(task.file.partition)[:200], val)
                    sample_logged = True
                if val is not None:
                    to_files[task.file.file_path] = val

            from_files = {}
            for task in from_scan.plan_files():
                val = _extract_partition_value(task.file.partition)
                if val is not None:
                    from_files[task.file.file_path] = val

            # Affected partitions: files added or removed
            new_file_paths = set(to_files.keys()) - set(from_files.keys())
            removed_file_paths = set(from_files.keys()) - set(to_files.keys())

            affected_values = set()
            for fp in new_file_paths:
                if to_files.get(fp):
                    affected_values.add(to_files[fp])
            for fp in removed_file_paths:
                if from_files.get(fp):
                    affected_values.add(from_files[fp])

            log.info("Affected partitions for %s.%s (col=%s, transform=%s): "
                     "to_files=%d, from_files=%d, new=%d, removed=%d, affected=%s",
                     database, table_name, partition_column, partition_transform,
                     len(to_files), len(from_files),
                     len(new_file_paths), len(removed_file_paths),
                     sorted(affected_values)[:10])

            # Return structured results with transform info
            return [{"value": v, "transform": partition_transform,
                     "source_column": partition_column}
                    for v in sorted(affected_values)]
        except Exception as e:
            log.error("Failed to get affected partitions for %s.%s: %s (%s)",
                      database, table_name, e, type(e).__name__)
            return []
