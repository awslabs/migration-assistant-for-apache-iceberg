# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Thin boto3 wrapper for the S3 Tables Migration API.

Centralizes the six operations used by the `s3tables_managed` strategy and
normalizes their responses into plain dicts the strategy can consume. No state
projection here (that lives in core/s3tables_state_map.py) and no Engine DB
access — this module is a pure API client, mirroring engines/athena.py and
engines/emr.py.

Operations (boto3 `s3tables` client, botocore >= 1.43.124):
  start_table_migration, sync_table_migration, get_table_migration,
  list_table_migrations, update_table_migration, complete_table_migration.

The migration API runs its own state machine; callers poll get_table_migration
and project the returned `state` onto engine status (see s3tables_state_map).
"""

import os

# Default source data-file format + iceberg knobs for the managed migration.
_DEFAULT_ICEBERG_FORMAT_VERSION = 2
_DEFAULT_COMPRESSION = "zstd"
# API-accepted compression codecs (start_table_migration properties).
_VALID_COMPRESSION = {"zstd", "snappy", "gzip", "uncompressed"}
# API-accepted migrationPolicies values (dataMigration.migrationPolicies).
# All enum values are lowerCamelCase per the current API.
#   unmatchedSourceFieldsPolicy: how to handle source fields absent from the
#       destination schema — "fail" (stop) or "drop" (omit them).
#   incompatibleValuesPolicy: how to handle values that don't fit the target
#       type — "fail" (stop) or "setNull" (write null).
# MigrationSourceType: the only source type the API accepts today.
SOURCE_TYPE = "glueTable"
_VALID_UNMATCHED = {"fail", "drop"}
_VALID_INCOMPATIBLE = {"fail", "setNull"}
_DEFAULT_UNMATCHED = "fail"
_DEFAULT_INCOMPATIBLE = "fail"


def _region() -> str:
    return os.getenv(
        "ENGINE_S3TABLES_REGION",
        os.getenv("ENGINE_ATHENA_REGION",
                  os.getenv("AWS_DEFAULT_REGION", "us-east-1")))


def region_from_arn(arn: str) -> str:
    """Extract the region field from an AWS ARN, or "" if not parseable.

    ARN format: arn:partition:service:region:account-id:resource. For an S3
    Tables bucket this is arn:aws:s3tables:<region>:<account>:bucket/<name>.
    The S3 Tables Migration API requires the caller's region to match the
    region of the target table bucket, so this lets callers pin the client
    to the resource's own region instead of relying on env vars.
    """
    parts = (arn or "").split(":")
    # parts[3] is the region segment; guard against short/malformed ARNs.
    if len(parts) >= 4 and parts[0] == "arn" and parts[3]:
        return parts[3]
    return ""


def account_from_arn(arn: str) -> str:
    """Extract the account-id field from an AWS ARN, or "" if not parseable.

    ARN format: arn:partition:service:region:account-id:resource. Used to
    resolve the caller's account id (from the target bucket / migration role
    ARN) for the Glue source's `catalogId` — the S3 Tables Migration API reads
    the source table through the CALLER's Glue catalog, so glueCatalogId must
    be the caller's account, not the source data account.
    """
    parts = (arn or "").split(":")
    # parts[4] is the account-id segment; guard against short/malformed ARNs.
    if len(parts) >= 5 and parts[0] == "arn" and parts[4]:
        return parts[4]
    return ""


def _display_request(req: dict) -> dict:
    """Return the request as-is for display/capture.

    The request members already match the wire form, so no normalization
    is needed. Kept as
    a thin indirection so call sites don't need to change if a future rename is
    reintroduced.
    """
    return dict(req)


class S3TablesMigrationClient:
    """Thin wrapper around the S3 Tables Migration API (boto3 `s3tables`)."""

    def __init__(self, region: str = ""):
        region = region or _region()
        from ..utils.aws import get_client
        self._client = get_client("s3tables", region)

    # ── request builders (pure; separated so they're unit-testable) ──

    @staticmethod
    def build_source(database: str, table: str, catalog_id: str = "") -> dict:
        """The `source` member for a Glue table (the engine's Hive-Parquet lane).

        The source type is a separate top-level request member
        (SOURCE_TYPE below), not part of this structure.
        """
        glue = {"databaseName": database, "tableName": table}
        if catalog_id:
            glue["catalogId"] = catalog_id
        return {"glueTable": glue}

    @staticmethod
    def build_destination_table(table_bucket_arn: str, namespace: str,
                                name: str, compression: str = "",
                                iceberg_format_version: int = 0) -> dict:
        """destinationTable for start_table_migration (bucket + ns + name).

        The destination is deliberately minimal: the service infers the schema
        and the partitioning from the Glue source. The API has no partitionSpec
        or schema member (a table cannot be repartitioned or re-schematized as
        part of a migration), and sorting during migration is unsupported, so
        none of those are sent.
        """
        codec = (compression or _DEFAULT_COMPRESSION).lower()
        if codec not in _VALID_COMPRESSION:
            codec = _DEFAULT_COMPRESSION
        dest: dict = {
            # Strip stray whitespace — a pasted ARN with a trailing space or
            # newline looks fine in the UI but fails the API's ARN regex.
            "tableBucketARN": (table_bucket_arn or "").strip(),
            "namespace": namespace,
            "name": name,
            "properties": {
                "writeFormat": "parquet",
                "icebergFormatVersion": int(iceberg_format_version
                                            or _DEFAULT_ICEBERG_FORMAT_VERSION),
                "compressionCodec": codec,
            },
        }
        return dest

    @staticmethod
    def build_data_migration(
            unmatched_source_fields_policy: str = _DEFAULT_UNMATCHED,
            incompatible_values_policy: str = _DEFAULT_INCOMPATIBLE) -> dict:
        """dataMigration for start_table_migration.

        - migrationPolicies: how the service handles schema/value conflicts
          found during planning (replaces the old dataTypeTransforms):
            * unmatchedSourceFieldsPolicy: "fail" | "drop"
            * incompatibleValuesPolicy:    "fail" | "setNull"
          Values are lowerCamelCase per the current API.

        maintenanceSettings is intentionally not sent: migration compaction is
        binpack-only (the service default); `auto`, `sort` and `z-order` are
        rejected, and the compaction block has no member for sort keys.
        """
        unmatched = unmatched_source_fields_policy or _DEFAULT_UNMATCHED
        if unmatched not in _VALID_UNMATCHED:
            unmatched = _DEFAULT_UNMATCHED
        incompatible = incompatible_values_policy or _DEFAULT_INCOMPATIBLE
        if incompatible not in _VALID_INCOMPATIBLE:
            incompatible = _DEFAULT_INCOMPATIBLE
        data_migration: dict = {
            "migrationPolicies": {
                "unmatchedSourceFieldsPolicy": unmatched,
                "incompatibleValuesPolicy": incompatible,
            },
        }
        return data_migration

    @staticmethod
    def build_output(reports_bucket_arn: str, prefix: str) -> dict:
        """migrationOutput: one general-purpose bucket, one prefix per report.

        The API takes `{bucket, prefixes}`: every report goes to the same bucket
        (an S3 bucket ARN) and each of the three reports gets its own key prefix.
        The service fills in `key` / `checksumCRC64NVME` per report on the
        GetTableMigration response once a report is written.

          <prefix>/table-compatibility/    written by Planning, before any copy
          <prefix>/object-compatibility/   written by Planning, one row per object
          <prefix>/object-migration/       written once data has been copied
        """
        bucket = (reports_bucket_arn or "").strip()
        if not bucket.startswith("arn:aws"):
            raise ValueError(
                "migrationOutput.bucket must be an S3 bucket ARN "
                f"(arn:aws:s3:::<name>), got {reports_bucket_arn!r}")
        base = prefix.rstrip("/")
        return {
            "bucket": bucket,
            "prefixes": {
                "tableCompatibilityReport": f"{base}/table-compatibility/",
                "objectCompatibilityReport": f"{base}/object-compatibility/",
                "objectMigrationReport": f"{base}/object-migration/",
            },
        }

    # ── operations ──

    def ensure_namespace(self, table_bucket_arn: str, namespace: str) -> bool:
        """Create the target namespace in the table bucket if it is missing.

        StartTableMigration requires the destination namespace to exist. Returns
        True if the namespace was created, False if it already existed. Needs
        s3tables:GetNamespace and s3tables:CreateNamespace on the table bucket.
        """
        from botocore.exceptions import ClientError
        try:
            self._client.get_namespace(
                tableBucketARN=table_bucket_arn, namespace=namespace)
            return False
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "NotFoundException":
                raise
        try:
            self._client.create_namespace(
                tableBucketARN=table_bucket_arn, namespace=[namespace])
        except ClientError as e:
            # Lost a race with another pipeline creating the same namespace.
            if e.response.get("Error", {}).get("Code") != "ConflictException":
                raise
            return False
        return True

    @staticmethod
    def build_start_request(role_arn: str, destination_table: dict, source: dict,
                            output: dict, data_migration: dict | None = None,
                            review_migration_plan: bool = True) -> dict:
        """The StartTableMigration request. Also used to record the payload that
        is shown on the pipeline detail page, so what is shown is what is sent.

        Members (per the current API): roleARN, destinationTable, sourceType,
        source, migrationOutput (all required), dataMigration, reviewMigrationPlan.
        """
        if not output:
            raise ValueError("start_table_migration requires migrationOutput "
                             "(a reports bucket must be configured)")
        req: dict = {
            "roleARN": role_arn,
            "destinationTable": destination_table,
            "sourceType": SOURCE_TYPE,
            "source": source,
            "reviewMigrationPlan": review_migration_plan,
            "migrationOutput": output,
        }
        if data_migration:
            req["dataMigration"] = data_migration
        return req

    def start(self, role_arn: str, destination_table: dict, source: dict,
              output: dict, data_migration: dict | None = None,
              review_migration_plan: bool = True) -> dict:
        """start_table_migration. Returns {migration_id, state}."""
        req = self.build_start_request(
            role_arn, destination_table, source, output, data_migration,
            review_migration_plan)
        resp = self._client.start_table_migration(**req)
        return {"migration_id": resp.get("migrationId"),
                "state": resp.get("state"),
                # The exact request we assembled, captured so it can be shown
                # (and re-recorded) on the pipeline detail page.
                "request": _display_request(req)}

    def sync(self, role_arn: str, target_table_arn: str, output: dict,
             review_migration_plan: bool = True) -> dict:
        """sync_table_migration (delta). Keyed by the TARGET table ARN.

        Input members (per the current API): roleARN, tableARN,
        migrationOutput (all required), reviewMigrationPlan. SyncTableMigration
        takes no `source` (the service re-reads the source recorded on the table).

        Returns {migration_id, state} for the new (type=sync) migration record.
        """
        if not output:
            raise ValueError("sync_table_migration requires migrationOutput "
                             "(a reports bucket must be configured)")
        req: dict = {
            "roleARN": role_arn,
            "tableARN": target_table_arn,
            "reviewMigrationPlan": review_migration_plan,
            "migrationOutput": output,
        }
        resp = self._client.sync_table_migration(**req)
        return {"migration_id": resp.get("migrationId"),
                "state": resp.get("state"),
                "request": _display_request(req)}

    def get(self, migration_id: str) -> dict:
        """get_table_migration. Returns the normalized detail (see _normalize_get)."""
        resp = self._client.get_table_migration(migrationId=migration_id)
        return self._normalize_get(resp)

    def list(self, table_bucket_arn: str = "", namespace: str = "",
             table_name: str = "", table_arn: str = "", state: str = "",
             has_valid_plan: bool | None = None, max_results: int = 0,
             next_token: str | None = None) -> dict:
        """list_table_migrations. Returns {migrations:[...], next_token}."""
        req: dict = {}
        if table_bucket_arn:
            req["tableBucketARN"] = table_bucket_arn
        if namespace:
            req["namespace"] = namespace
        if table_name:
            req["name"] = table_name
        if table_arn:
            req["tableARN"] = table_arn
        if state:
            req["state"] = state
        if has_valid_plan is not None:
            req["hasValidPlan"] = has_valid_plan
        if max_results:
            req["maxResults"] = max_results
        if next_token:
            req["nextToken"] = next_token
        resp = self._client.list_table_migrations(**req)
        return {"migrations": resp.get("migrations", []),
                "next_token": resp.get("nextToken")}

    def approve(self, migration_id: str, description: str = "") -> dict:
        """Resume a plan-review pause: update_table_migration -> queued."""
        return self._update(migration_id=migration_id,
                            requested_state="queued", description=description)

    def cancel(self, migration_id: str, description: str = "") -> dict:
        """Cancel a migration: update_table_migration -> cancelled."""
        return self._update(migration_id=migration_id,
                            requested_state="cancelled", description=description)

    def _update(self, requested_state: str, migration_id: str = "",
                table_arn: str = "", description: str = "") -> dict:
        """update_table_migration. requested_state is 'queued' or 'cancelled'.

        Accepts migration_id OR table_arn (the API allows either).
        Returns {migration_id, state, table_arn}.
        """
        req: dict = {"requestedState": requested_state}
        if migration_id:
            req["migrationId"] = migration_id
        if table_arn:
            req["tableARN"] = table_arn
        if description:
            req["updateDescription"] = description
        resp = self._client.update_table_migration(**req)
        return {"migration_id": resp.get("migrationId"),
                "state": resp.get("state"),
                "table_arn": resp.get("tableARN")}

    def complete(self, target_table_arn: str) -> dict:
        """complete_table_migration — cutover. Keyed by the TARGET table ARN.

        The API takes no other input (it has no description member)."""
        self._client.complete_table_migration(tableARN=target_table_arn)
        return {"table_arn": target_table_arn, "completed": True}

    # ── response normalization ──

    @staticmethod
    def _normalize_get(resp: dict) -> dict:
        """Flatten the get_table_migration response to the fields the strategy
        needs. Keys are snake_case; missing values are None/empty."""
        ps = resp.get("planningStats") or {}
        ms = resp.get("migrationStats") or {}
        # The response echoes migrationOutput as {bucket, tableCompatibilityReport,
        # objectCompatibilityReport, objectMigrationReport}; each report carries
        # its own prefix, the written `key` (once the report exists), a checksum,
        # and — on the two compatibility reports — `invalidFindings`.
        out = resp.get("migrationOutput") or {}
        bucket = out.get("bucket") or ""
        table_compat = out.get("tableCompatibilityReport") or {}
        object_compat = out.get("objectCompatibilityReport") or {}
        object_migration = out.get("objectMigrationReport") or {}

        def _report_uri(cfg: dict) -> str:
            # `bucket` is an S3 bucket ARN (arn:aws:s3:::<name>) shared by all
            # three reports; normalize to a readable s3://<name>/<key> URI.
            # Only a report the service has actually written has a `key`. The
            # `prefix` is just where we ASKED it to write, so linking to it when
            # no report exists (e.g. the migration failed before planning
            # produced one) would point operators at an empty location.
            key = cfg.get("key") or ""
            if not bucket or not key:
                return ""
            name = bucket.split(":::", 1)[1] if ":::" in bucket else bucket
            return f"s3://{name}/{key}"

        def _blocking_findings() -> dict:
            # Blocking findings per category (access | metadata | data |
            # internal), summed across the two compatibility reports (the table
            # report lists each blocking finding; the object report lists one per
            # blocked object). A category appears only if it blocks the migration.
            totals: dict = {}
            for report in (table_compat, object_compat):
                for category, count in (report.get("invalidFindings") or {}).items():
                    totals[category] = totals.get(category, 0) + (count or 0)
            return totals

        def _iso(v):
            # boto3 parses AWS timestamps into datetime objects. Downstream code
            # stores these (batch timestamps, metadata) — and the DynamoDB
            # backend cannot serialize a raw datetime — so normalize to an ISO
            # string here. Leave already-string / None values as-is.
            from datetime import datetime as _dt
            return v.isoformat() if isinstance(v, _dt) else v

        return {
            "migration_id": resp.get("migrationId"),
            "state": resp.get("state"),
            "type": resp.get("type"),
            "table_arn": resp.get("tableARN"),
            "has_valid_plan": resp.get("hasValidPlan"),
            "created_at": _iso(resp.get("createdAt")),
            "updated_at": _iso(resp.get("stateUpdatedAt")),
            "planning_stats": {
                # New planningStats: objectsScanned (was totalSourceObjectsScanned)
                # and objectsPlanned (was totalObjectsPlannedToMigrate). No more
                # byte counters. Keep `total_objects` as an alias of
                # objectsPlanned so existing consumers (progress denominator,
                # UI) keep working without a key rename.
                "objects_scanned": ps.get("objectsScanned"),
                "objects_planned": ps.get("objectsPlanned"),
                "total_objects": ps.get("objectsPlanned"),
                "partition_count": ps.get("partitionCount"),
                # invalidFindings is reported per compatibility report and
                # aggregated here by category.
                "invalid_findings": _blocking_findings(),
            },
            "migration_stats": {
                # migrationStats exposes object counters only.
                "objects_migrated": ms.get("objectsMigrated"),
                "objects_failed": ms.get("objectsFailedMigration"),
                "objects_pending": ms.get("objectsPendingMigration"),
            },
            # Per-column type changes are not returned inline; they are only in
            # the table compatibility report written to S3.
            "table_compat_report_uri": _report_uri(table_compat),
            "object_compat_report_uri": _report_uri(object_compat),
            "migration_report_uri": _report_uri(object_migration),
            # Structured failure reason ({code, detail}) — the specific reason a
            # migration failed (e.g. sourcePermissions). Present on failure.
            "failure_reason": resp.get("failureReason") or {},
        }
