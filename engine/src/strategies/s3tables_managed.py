# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Managed Migration strategy (`s3tables_managed`).

Delegates Hive-Parquet → S3 Tables migration to the AWS S3 Tables Migration API.
The engine acts as a polling adapter over the API's own state machine — it does
NOT drive data movement (no Athena/EMR/Spark, no S3-event tracker). See the
integration plan (working/s3tables-migration-api-integration-plan.md).

Lifecycle:
  submit()   → start_table_migration(reviewMigrationPlan=true) → store s3tm:<id>
  reconcile()→ get_table_migration → project API state onto status/sync_status,
               capture planning/migration stats + report locations
  approve()  → update_table_migration(queued)  (user action at awaiting_review)
  execute_sync()→ sync_table_migration          (drift re-sync)
  complete() → complete_table_migration          (cutover, via finalize)
  cancel()   → update_table_migration(cancelled)

Implemented on the legacy reconcile() path (NOT registered in
_STATE_MACHINE_STRATEGIES), so the engine's state machine is untouched.
"""

import json
import logging
import os
from datetime import datetime, timezone

from .base import MigrationStrategy
from ..db import PipelineRepo, BatchRepo
from ..engines.s3tables_migration import (
    S3TablesMigrationClient, region_from_arn, account_from_arn)
from ..core.s3tables_state_map import (
    project_state, is_migrated, is_failed, is_cancelled,
    TERMINAL_STATES,
)

log = logging.getLogger(__name__)

# Prefix for compatibility/migration reports inside the common logs bucket
# (alongside athena-query-results/ and emr-logs/). The engine stack's
# S3TablesMigrationRole ReportsWrite grant is scoped to this prefix, so keep the
# two in step if it is ever overridden.
_REPORTS_PREFIX = os.getenv(
    "ENGINE_S3TABLES_REPORTS_PREFIX", "migration-reports")

# Longest failure message we persist. Three S3 report URIs alone are ~350
# characters, so the old 500-character cap cut the last link off mid-URL.
_MAX_FAILURE_MSG = 1500

# GetTableMigration failureReason.code -> what the operator should do next. Only
# the codes that are an access/configuration problem (not a data problem, which
# the compatibility reports explain) get a hint.
_FAILURE_HINTS = {
    "rolePermissions": (
        "The migration role could not be used. Check that it exists, trusts "
        "migration.s3tables.amazonaws.com, and matches ENGINE_S3TABLES_MIGRATION_ROLE_ARN."),
    "sourcePermissions": (
        "The migration role cannot read the source. Grant it Glue read "
        "(GetDatabase/GetTable/GetPartitions) on the source table and S3 read on "
        "the source bucket; if the database is governed by Lake Formation, also "
        "grant it SELECT/DESCRIBE on the table and data-location access."),
    "destinationPermissions": (
        "The migration role cannot write the destination. Check its S3 Tables "
        "permissions on the target table bucket."),
    "reportBucketNotSupported": (
        "The reports bucket must be a general-purpose S3 bucket "
        "(directory buckets are not supported)."),
    "resourceNotFound": (
        "A resource named in the request was not found. Check that the Glue "
        "table, table bucket and role exist in this region."),
}

# Auto-sync cadence for Managed Migration, mirroring the incremental/morph
# behaviour: once the initial load (or a prior sync) has completed, the
# reconcile loop re-runs sync_table_migration on this interval to migrate new
# source data. Per-pipeline override via spec.strategy.sync_interval; global
# default shared with incremental (ENGINE_INCREMENTAL_INTERVAL_SECONDS).
_SYNC_INTERVAL = int(os.getenv("ENGINE_INCREMENTAL_INTERVAL_SECONDS", "3600"))

# Batch label markers for the Managed Migration phase timeline. Batch 0 is
# always the initial load (start_table_migration); each subsequent sync run
# (sync_table_migration) appends a new batch. The frontend formatBatchLabel
# renders these into "Initial load" / "Sync".
_INITIAL_LOAD_MARKER = "__mm_initial_load__"
_SYNC_MARKER = "__mm_sync__"


def _reports_bucket_arn() -> str:
    """S3 bucket ARN for the compatibility/migration reports.

    The migration API's report `bucket` field requires a full S3 bucket ARN
    (arn:aws:s3:::<name>), not a bare bucket name. The setting
    (ENGINE_S3TABLES_REPORTS_BUCKET, falling back to ENGINE_LOGS_BUCKET)
    may be configured as either a plain bucket name OR an already-formed ARN,
    so we normalize to an ARN here and are idempotent if it is already one.
    Empty string if neither is set (validate blocks submit).
    """
    value = (os.getenv("ENGINE_S3TABLES_REPORTS_BUCKET", "")
             or os.getenv("ENGINE_LOGS_BUCKET", "")).strip()
    if not value:
        return ""
    # Already an ARN (any partition, e.g. arn:aws-us-gov:s3:::...) — use as-is.
    if value.startswith("arn:") and ":s3:::" in value:
        return value
    # Otherwise treat it as a bucket name and build the ARN. Strip an s3:// or
    # s3a:// scheme and any path so a value like "s3://my-bucket/prefix" still
    # yields arn:aws:s3:::my-bucket.
    for scheme in ("s3://", "s3a://"):
        if value.startswith(scheme):
            value = value[len(scheme):]
            break
    bucket_name = value.split("/", 1)[0]
    return f"arn:aws:s3:::{bucket_name}" if bucket_name else ""


def _migration_role_arn(pipeline: dict) -> str:
    """The IAM role the migration API assumes.

    Prefers the pipeline's own iam_role_arn (if a plan set one), else the
    engine-wide ENGINE_S3TABLES_MIGRATION_ROLE_ARN env var (injected from the
    dedicated S3TablesMigrationRole stack output). The app does not currently
    inject iam_role_arn into the spec, so the env var is the normal source.
    """
    return (pipeline.get("iam_role_arn")
            or os.getenv("ENGINE_S3TABLES_MIGRATION_ROLE_ARN", ""))


def _caller_account_id(pipeline: dict) -> str:
    """The caller's AWS account id, for the Glue source `catalogId`.

    The S3 Tables Migration API reads the source Glue table through the
    CALLER's catalog and requires glueCatalogId to be the caller's account
    (not the source DATA account). We derive it from resources the caller
    owns — the target table bucket ARN first, then the migration role ARN —
    falling back to the ENGINE_AWS_ACCOUNT_ID env var.
    """
    return (account_from_arn(pipeline.get("target_bucket_arn", ""))
            or account_from_arn(_migration_role_arn(pipeline))
            or os.getenv("ENGINE_AWS_ACCOUNT_ID", ""))


def _parse_json(val):
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (ValueError, TypeError):
            return None
    return val


class S3TablesManagedStrategy(MigrationStrategy):
    """Managed Migration via the S3 Tables Migration API."""

    def __init__(self):
        self._repo = PipelineRepo()
        self._batches = BatchRepo()
        # Default client (env-region) for operations keyed by an opaque
        # migration_id. Region-sensitive operations (start/sync) use a client
        # pinned to the target bucket's own region via _client_for(); see below.
        self._client = S3TablesMigrationClient()

    def _client_for(self, pipeline: dict) -> S3TablesMigrationClient:
        """Return a client pinned to the pipeline's target-bucket region.

        The S3 Tables Migration API rejects StartTableMigration /
        SyncTableMigration when the caller's region differs from the region of
        the target table bucket ("The provided Region does not match the
        Region of the resource"). The target_bucket_arn is the source of truth
        for where the resource lives, so we derive the client region from it
        rather than relying on ENGINE_S3TABLES_REGION / AWS_DEFAULT_REGION
        matching. Falls back to the env-region client when the ARN is absent or
        unparseable.
        """
        region = region_from_arn(pipeline.get("target_bucket_arn", ""))
        if not region:
            return self._client
        return S3TablesMigrationClient(region=region)

    # ── validate ──

    def validate(self, pipeline: dict) -> tuple[bool, str]:
        fmt = (pipeline.get("source_format") or "").lower()
        if fmt != "parquet":
            return False, (
                "Managed Migration only supports Hive-style Parquet sources "
                f"(got '{fmt}').")
        target_type = (pipeline.get("target_type") or "").lower()
        if target_type != "s3_tables":
            return False, (
                "Managed Migration requires an S3 Tables target "
                f"(got '{target_type}').")
        bucket_arn = pipeline.get("target_bucket_arn") or ""
        if not bucket_arn:
            return False, "Target has no table bucket ARN"
        # Must be a real S3 Tables *table bucket* ARN, not a plain bucket name or
        # a general-purpose S3 bucket ARN (arn:aws:s3:::...). The migration API
        # rejects those with "table bucket ARN is not valid".
        if not bucket_arn.startswith("arn:aws:s3tables:") or ":bucket/" not in bucket_arn:
            return False, (
                "Invalid S3 Tables table bucket ARN "
                f"'{bucket_arn}'. Expected "
                "arn:aws:s3tables:<region>:<account>:bucket/<name>")
        if not _migration_role_arn(pipeline):
            return False, (
                "Managed Migration requires a migration IAM role "
                "(set ENGINE_S3TABLES_MIGRATION_ROLE_ARN)")
        # The migration API requires an output (report) configuration, so a
        # reports bucket must be configured. In prod this is wired to the logs
        # bucket via the stack; locally set ENGINE_S3TABLES_REPORTS_BUCKET.
        if not _reports_bucket_arn():
            return False, (
                "Managed Migration requires a reports bucket "
                "(set ENGINE_S3TABLES_REPORTS_BUCKET)")
        return True, "OK"

    # ── submit → start_table_migration ──

    def submit(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        ok, reason = self.validate(pipeline)
        if not ok:
            return self._repo.update_status(pid, "failed", error_message=reason)

        # Phase timeline: batch 0 is the initial load. Create it pending up
        # front (idempotent create doubles as a reset on re-submit/retry).
        self._batches.create(pid, 0, [_INITIAL_LOAD_MARKER])

        # The migration API needs the destination namespace to exist already.
        try:
            if self._client_for(pipeline).ensure_namespace(
                    pipeline["target_bucket_arn"], pipeline["target_database"]):
                log.info("Managed migration %s: created namespace '%s' in %s",
                         pid[:8], pipeline["target_database"],
                         pipeline["target_bucket_arn"])
        except Exception as e:
            log.exception("Managed migration %s: could not ensure namespace '%s'",
                          pid[:8], pipeline["target_database"])
            msg = (f"Could not create target namespace "
                   f"'{pipeline['target_database']}': {e}")[:500]
            self._batches.mark_failed(0, error_message=msg, pipeline_id=pid)
            return self._repo.update_status(pid, "failed", error_message=msg)
        try:
            # Pin the client to the target bucket's region so the API's
            # region-match check on the tableBucketARN passes.
            client = self._client_for(pipeline)
            # glueCatalogId is REQUIRED for a GLUE_TABLE source. It must be the
            # CALLER's account id (the migration reads the source table through
            # the caller's Glue catalog) — NOT the source DATA account from
            # inventory, which the API rejects. Resolve it from the caller's own
            # resources (target bucket / migration role ARN).
            source = client.build_source(
                pipeline["source_database"], pipeline["source_table"],
                catalog_id=_caller_account_id(pipeline))

            table_props = _parse_json(pipeline.get("table_properties")) or {}

            # The destination is minimal on purpose. The service infers the
            # schema and the partitioning from the Glue source, and a migration
            # cannot repartition a table or sort rows while it runs — the API
            # has no partitionSpec / schema member, and the plan's recommended
            # partition transforms and sort keys are not part of the request.
            if self._has_sort_order(pipeline):
                log.info("Managed migration %s: the plan carries a sort order, but "
                         "sorting is not supported during migration; it is not "
                         "sent (migration compaction is binpack-only)", pid[:8])

            destination = client.build_destination_table(
                pipeline["target_bucket_arn"],
                pipeline["target_database"], pipeline["target_table"],
                compression=table_props.get("write.parquet.compression-codec", ""),
                iceberg_format_version=int(table_props.get("format-version", 2) or 2))

            opts = self._managed_options(pipeline)
            data_migration = client.build_data_migration(
                unmatched_source_fields_policy=opts.get(
                    "unmatched_source_fields_policy", "fail"),
                incompatible_values_policy=opts.get(
                    "incompatible_values_policy", "fail"))

            # migrationOutput is required by the API. validate() has already
            # ensured a reports bucket is configured.
            output = client.build_output(
                _reports_bucket_arn(), f"{_REPORTS_PREFIX}/{pid}")

            # Assemble the exact request we're about to send, then persist + log
            # it BEFORE the API call — so a FAILED start (which never returns a
            # result) still leaves the payload on the pipeline for inspection.
            role_arn = _migration_role_arn(pipeline)
            from ..engines.s3tables_migration import _display_request
            req_preview = client.build_start_request(
                role_arn, destination, source, output, data_migration,
                review_migration_plan=True)
            req_display = _display_request(req_preview)
            self._repo.update_metadata(pid, "start_request", json.dumps(req_display))
            log.info("Managed migration %s start_table_migration request: %s",
                     pid[:8], json.dumps(req_display))

            result = client.start(
                role_arn=role_arn,
                destination_table=destination,
                source=source,
                output=output,
                data_migration=data_migration,
                review_migration_plan=True)
        except Exception as e:
            # Log the request payload at ERROR level too, so it is visible when
            # the submit fails even under ENGINE_LOG_LEVEL=ERROR (the info log
            # above is suppressed at that level). The payload is what we need to
            # diagnose "partition spec is not valid" and similar API rejections.
            try:
                log.error("Managed migration %s start_table_migration request "
                          "payload: %s", pid[:8], json.dumps(req_display))
            except NameError:
                pass  # failure occurred before the payload was assembled
            log.exception("Managed migration submit failed for %s", pid[:8])
            self._batches.mark_failed(0, error_message=str(e)[:500], pipeline_id=pid)
            return self._repo.update_status(
                pid, "failed", error_message=f"start_table_migration failed: {e}"[:500])

        migration_id = result.get("migration_id")
        if not migration_id:
            self._batches.mark_failed(
                0, error_message="no migrationId returned", pipeline_id=pid)
            return self._repo.update_status(
                pid, "failed", error_message="start_table_migration returned no migrationId")

        self._repo.update_emr_job_run_id(pid, f"s3tm:{migration_id}")
        # Capture the exact request payload we sent so it can be inspected (and
        # re-recorded) on the pipeline detail page. Stored under
        # spec_json._metadata.start_request.
        if result.get("request"):
            self._repo.update_metadata(
                pid, "start_request", json.dumps(result["request"]))
        # The initial-load batch is now running (the API is planning/migrating).
        self._batches.mark_running(0, f"s3tm:{migration_id}", pipeline_id=pid)
        status, sync_status = project_state(result.get("state"))
        if sync_status:
            self._repo.update_sync_status(pid, sync_status)
        log.info("Managed migration %s started for pipeline %s (state=%s)",
                 migration_id, pid[:8], result.get("state"))
        return self._repo.update_status(pid, status)

    # ── reconcile → get_table_migration + project ──

    def reconcile(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        # A failed sync is a terminal-until-retried state. On sync failure the
        # emr_job_run_id still points at the PREVIOUS (succeeded) migration, so
        # re-polling it here would see `succeeded` and reset sync_status back to
        # initial_load_complete/sync_complete — silently erasing the failure.
        # Leave it failed until an explicit re-sync / resume-from-failure.
        if pipeline.get("sync_status") == "failed":
            return pipeline
        migration_id = self._migration_id(pipeline)
        if not migration_id:
            # Nothing to poll yet (submit not done); leave as-is.
            return pipeline
        try:
            detail = self._client_for(pipeline).get(migration_id)
        except Exception as e:
            log.warning("get_table_migration failed for %s: %s", pid[:8], e)
            return pipeline

        # The reconcile loop works from a snapshot of running pipelines, and
        # finalize can complete (cut over) a pipeline while this poll is in
        # flight. The API still reports the migration as `succeeded` after
        # cutover, so writing from here would flip a completed pipeline back to
        # running. Re-read and stop if it is no longer running.
        current = self._repo.get(pid)
        if current and current.get("status") != "running":
            log.info("Managed migration %s: status is now '%s'; skipping "
                     "reconcile update", pid[:8], current.get("status"))
            return current

        api_state = detail.get("state") or ""

        # Capture metadata + progress regardless of state.
        if detail.get("table_arn"):
            self._repo.update_metadata(pid, "target_table_arn", detail["table_arn"])
        ps = detail.get("planning_stats") or {}
        ms = detail.get("migration_stats") or {}
        self._repo.update_metadata(pid, "planning_stats", json.dumps(ps))
        # Persist migrationStats too (object counters migrated/pending/failed —
        # byte counters were removed from the API) so the UI can report live
        # data-movement progress during queued/running.
        self._repo.update_metadata(pid, "migration_stats", json.dumps(ms))
        # The three reports the service writes (table compatibility and object
        # compatibility during planning, object migration once data is copied).
        # Always write the current value, including empty: a report only has a
        # link once the service has written it, and a stale link saved by an
        # earlier poll (or an earlier retry's migration) must not outlive that.
        for key in ("table_compat_report_uri", "object_compat_report_uri",
                    "migration_report_uri"):
            self._repo.update_metadata(pid, key, detail.get(key) or "")
        if detail.get("failure_reason"):
            self._repo.update_metadata(pid, "failure_reason",
                                       json.dumps(detail["failure_reason"]))

        # Progress denominator is objectsPlanned (objects planned to migrate);
        # total_objects aliases it in _normalize_get. Byte counters are gone, so
        # there is no bytes_processed to report for managed migration.
        total_objects = ps.get("objects_planned") or ps.get("total_objects") or 0
        objects_migrated = ms.get("objects_migrated") or 0
        self._repo.update_progress(
            pid,
            partitions_completed=objects_migrated,
            partitions_total=total_objects or None)

        # Identify the current phase so we can render "Initial load ..." vs
        # "Last sync ..." and drive its status/timestamps. The migration's own
        # `type` ("start" vs "sync") is the authoritative signal and — unlike
        # the running batch — remains correct AFTER the migration succeeds and
        # its batch is closed out (otherwise a re-polled succeeded sync would
        # lose its phase and revert to "initial load completed"). Fall back to
        # the running batch when type is absent.
        running_batch = self._batches.get_running(pid)
        mig_type = (detail.get("type") or "").lower()
        if mig_type == "sync":
            is_sync_phase = True
        elif mig_type == "start":
            is_sync_phase = False
        else:
            is_sync_phase = self._is_sync_batch(running_batch)
        # Prefer the API's own updatedAt for accurate batch timestamps.
        updated_at = detail.get("updated_at") or None

        # Failure: mark the running batch failed, keep the pipeline in the
        # failed sync_status (status stays running so recovery is possible).
        if is_failed(api_state):
            findings = ps.get("invalid_findings") or {}
            objects_failed = ms.get("objects_failed") or 0
            # The API's structured failureReason ({code, detail}) is the
            # specific, actionable reason (e.g. sourcePermissions on a table).
            # Lead with it.
            fr = detail.get("failure_reason") or {}
            fr_code = fr.get("code") or ""
            fr_detail = fr.get("detail") or ""
            parts = [f"Managed migration {api_state}"]
            if fr_code:
                parts.append(
                    f"reason: {fr_code}" + (f" ({fr_detail})" if fr_detail else ""))
                # Plain-language next step for the failure codes that are an
                # access/config problem rather than a data problem.
                hint = _FAILURE_HINTS.get(fr_code)
                if hint:
                    parts.append(hint.rstrip("."))  # parts are joined with ". "
            if findings:
                # Compatibility problems found during planning.
                parts.append(f"compatibility findings: {findings}")
            if objects_failed:
                parts.append(f"{objects_failed} object(s) failed to migrate")
            # Point to the reports (the per-object/column detail lives there) —
            # but only those the service actually wrote.
            written = [
                (label, uri) for label, uri in (
                    ("table compatibility report", detail.get("table_compat_report_uri")),
                    ("object compatibility report", detail.get("object_compat_report_uri")),
                    ("migration report", detail.get("migration_report_uri")))
                if uri]
            for label, uri in written:
                parts.append(f"{label}: {uri}")
            if not written:
                parts.append("no reports were written (the migration failed before "
                             "the service produced any)")
            elif not fr_code and not findings and not objects_failed:
                parts.append("see the compatibility/migration report for details")
            detail_msg = ". ".join(parts)
            log.warning("Managed migration %s failed: %s", pid[:8], detail_msg)
            if running_batch:
                self._batches.mark_failed(
                    running_batch["batch_number"],
                    error_message=detail_msg[:_MAX_FAILURE_MSG], pipeline_id=pid)
            # Phase-aware failure semantics:
            #  - A failed SYNC does NOT invalidate the already-migrated table
            #    (the initial load produced it). Keep status=running and mark
            #    sync_status='sync_failed' so the pipeline stays finalizable and
            #    the sync is simply retried (guarded) on cadence / via resume.
            #  - A failed INITIAL LOAD is a true failure — no target table
            #    exists yet — so status=failed (retry / resume-from-failure can
            #    then restart a fresh start_table_migration).
            already_migrated = bool(self._target_table_arn(pipeline))
            if is_sync_phase and already_migrated:
                self._repo.update_sync_status(pid, "sync_failed")
                return self._repo.update_status(
                    pid, "running", error_message=detail_msg[:_MAX_FAILURE_MSG])
            self._repo.update_sync_status(pid, "failed")
            return self._repo.update_status(pid, "failed", error_message=detail_msg[:_MAX_FAILURE_MSG])

        # Cancellation: mark any running batch failed (it did not finish).
        if is_cancelled(api_state):
            if running_batch:
                self._batches.mark_failed(
                    running_batch["batch_number"],
                    error_message="Migration cancelled", pipeline_id=pid)
            return self._repo.update_status(pid, "cancelled")

        # Data movement succeeded: close out the running batch and record the
        # phase-specific completion state (NOT the generic in_sync). We no
        # longer track source drift, so we surface WHAT completed instead.
        if is_migrated(api_state):
            if running_batch:
                self._batches.mark_completed(
                    running_batch["batch_number"],
                    pipeline_id=pid,
                    completed_at=updated_at)
            sync_status = "sync_complete" if is_sync_phase else "initial_load_complete"
            self._repo.update_sync_status(pid, sync_status)
            self._repo.update_status(pid, "running")
            # Auto-sync: once a load/sync has completed, re-run
            # sync_table_migration on the configured interval (same cadence as
            # incremental/morph) to pick up new source data. Manual "Sync Now"
            # bypasses this via the sync-now route.
            if self._auto_sync_due(pid, pipeline):
                log.info("Managed migration %s: auto-sync interval elapsed, "
                         "triggering sync", pid[:8])
                return self.execute_sync(self._repo.get(pid) or pipeline)
            return self._repo.get(pid)

        # Still working (new/planning/queued/migrating) or awaiting review.
        # project_state is phase-agnostic and maps the early states
        # (new/planning/queued) to "initial_load" and migrating to "syncing".
        # But a SYNC run also passes through new/planning/queued before
        # migrating, so during the sync phase those must show "syncing", not
        # "initial_load" — otherwise a freshly-triggered sync briefly shows
        # "syncing" then flips back to "initial load" while the sync migration
        # is still queuing/planning. Make the in-progress sync_status reflect
        # the phase for ALL active pre-completion states.
        status, sync_status = project_state(api_state)
        if sync_status in ("initial_load", "syncing"):
            sync_status = "syncing" if is_sync_phase else "initial_load"
        if sync_status:
            self._repo.update_sync_status(pid, sync_status)
        return self._repo.update_status(pid, status)

    # ── cancel → update_table_migration(cancelled) ──

    def cancel(self, pipeline: dict) -> dict:
        pid = pipeline["id"]
        migration_id = self._migration_id(pipeline)
        if migration_id:
            try:
                self._client_for(pipeline).cancel(migration_id, description="Cancelled via IMA")
            except Exception as e:
                log.warning("cancel_table_migration failed for %s: %s", pid[:8], e)
        return self._repo.update_status(pid, "cancelled")

    # ── approve (user action at awaiting_review) → update queued ──

    def approve(self, pipeline: dict, username: str = "") -> dict:
        """Resume a plan-review pause. Only valid at awaiting_review."""
        pid = pipeline["id"]
        if pipeline.get("sync_status") != "awaiting_review":
            raise ValueError("Migration is not awaiting review")
        migration_id = self._migration_id(pipeline)
        if not migration_id:
            raise ValueError("No migration id on this pipeline")
        result = self._client_for(pipeline).approve(
            migration_id, description=f"Approved via IMA by {username or '?'}")
        if result.get("table_arn"):
            self._repo.update_metadata(pid, "target_table_arn", result["table_arn"])
        status, sync_status = project_state(result.get("state"))
        if sync_status:
            self._repo.update_sync_status(pid, sync_status)
        log.info("Managed migration %s approved (pipeline %s, state=%s)",
                 migration_id, pid[:8], result.get("state"))
        return self._repo.update_status(pid, status)

    # ── resume (from paused) → re-derive true state from the API ──

    def resume(self, pipeline: dict) -> dict:
        """Resume a paused managed pipeline.

        The AWS migration is not actually pausable in place — pausing only
        flips the engine's display status. Resuming therefore must NOT invent
        batch/state-machine state (there are no batches). We set the pipeline
        back to running and re-project the true migration state from the API so
        the next reconcile shows the real status (migrating/succeeded/awaiting
        review/etc.). If there is no migration yet, fall back to a fresh submit.
        """
        pid = pipeline["id"]
        migration_id = self._migration_id(pipeline)
        if not migration_id:
            # Never actually started — start it now.
            return self.submit(pipeline)
        # Set running first so reconcile treats it as active, then reconcile
        # re-projects the live API state onto status/sync_status.
        self._repo.update_status(pid, "running")
        # Re-read so reconcile sees the running status + current emr_job_run_id.
        current = self._repo.get(pid) or pipeline
        return self.reconcile(current)

    # ── resume-from-failure → start a fresh migration ──

    def resume_from_failure(self, pipeline: dict) -> dict:
        """Recover a failed managed migration. Behavior depends on the phase:

        SYNC failure (initial load already produced a table — captured
        target_table_arn present): the migrated table is still valid, so we do
        NOT restart the whole migration. We RE-ATTEMPT THE SYNC now via
        execute_sync (which sets sync_status='syncing' — a truthful state). The
        previous sync already failed (terminal), so there is no in-flight sync
        to conflict with; execute_sync's _sync_in_flight guard makes this a
        safe no-op in the unlikely event one is somehow still running. We must
        NOT fabricate a 'sync_complete' state here — the last sync did NOT
        succeed.

        INITIAL-LOAD failure (no target table exists): a true failure — restart
        a fresh start_table_migration via submit().
        """
        pid = pipeline["id"]
        target_arn = self._target_table_arn(pipeline)
        spec = _parse_json(pipeline.get("spec_json")) or {}
        has_captured_arn = bool((spec.get("_metadata", {}) or {}).get("target_table_arn"))

        if target_arn and has_captured_arn:
            # Sync failure recovery — clear the dead failed sync and re-run it.
            # Drop the failed sync's migration id, then re-attempt the sync.
            # execute_sync appends a NEW sync batch, so we leave the failed
            # batch in place as history rather than resetting it (which would
            # orphan a never-run 'pending' batch). This does NOT touch the
            # migrated table; it just retries the delta sync.
            self._repo.update_emr_job_run_id(pid, "")
            self._repo.update_status(pid, "running", error_message="")
            current = self._repo.get(pid) or pipeline
            log.info("Managed migration %s: re-attempting failed sync via "
                     "sync_table_migration", pid[:8])
            return self.execute_sync(current)

        # Initial-load failure — no table exists. Restart from scratch.
        self._repo.update_emr_job_run_id(pid, "")
        self._repo.update_sync_status(pid, "")
        # Genuine restart — stamp a new started_at rather than preserving the
        # failed run's timestamp.
        self._repo.update_status(pid, "running", error_message="", reset_started_at=True)
        self._batches.reset_failed_to_pending(pid)
        current = self._repo.get(pid) or pipeline
        log.info("Managed migration %s: re-attempting initial load via "
                 "start_table_migration", pid[:8])
        return self.submit(current)

    # ── complete (cutover, via finalize) → complete_table_migration ──

    def complete(self, pipeline: dict, username: str = "") -> dict:
        """Cutover: release the target table to read-write. Only at in_sync."""
        pid = pipeline["id"]
        target_arn = self._target_table_arn(pipeline)
        if not target_arn:
            raise ValueError("No target table ARN — cannot complete migration")
        self._client_for(pipeline).complete(target_arn)
        log.info("Managed migration cutover complete for pipeline %s (by %s)",
                 pid[:8], username or "?")
        return self._repo.update_status(pid, "completed")

    # ── ongoing sync (drift) ──

    def detect_changes(self, pipeline: dict) -> bool:
        """We keep no source-side tracking; sync IS the detector (plan §8).

        Return True so the state machine's in_sync path would trigger a sync —
        but note this strategy is NOT on the state machine; execute_sync is
        invoked on the sync cadence / manual 'Sync now'. Kept for interface
        completeness.
        """
        return True

    def execute_sync(self, pipeline: dict, batch_number: int = 0) -> dict:
        """Issue sync_table_migration; the API diffs the source and migrates the
        delta. Drift is known afterward from migrationStats.objects_migrated."""
        pid = pipeline["id"]

        # Idempotency guard: never issue a second sync while one is already in
        # flight. AWS rejects a concurrent sync with ConflictException ("A
        # migration is already in progress for the specified table"), which
        # happened when a resume-driven reconcile and a background-loop
        # reconcile both passed the auto-sync gate and each called this. This is
        # the authoritative guard — it protects against ANY overlapping trigger
        # (resume, sync-now, reconcile loop) regardless of ordering.
        if self._sync_in_flight(pipeline):
            log.info("Managed migration %s: a sync/migration is already in "
                     "flight — skipping duplicate sync_table_migration", pid[:8])
            return self._repo.get(pid) or pipeline

        # sync_table_migration is keyed by the REAL target table ARN, which for
        # S3 Tables ends in /table/<uuid> — captured from the migration API on
        # reconcile (spec_json._metadata.target_table_arn). We must use the
        # captured ARN; a reconstructed /table/<namespace>/<name> ARN is NOT a
        # valid S3 Tables table ARN and the sync will fail.
        spec = _parse_json(pipeline.get("spec_json")) or {}
        captured_arn = (spec.get("_metadata", {}) or {}).get("target_table_arn", "")
        target_arn = captured_arn or self._target_table_arn(pipeline)
        if not target_arn:
            msg = ("Cannot sync: target table ARN not yet known. It is captured "
                   "from the migration API once the initial load has been "
                   "reconciled — wait for the initial load to complete.")
            log.warning("Managed sync skipped for %s: %s", pid[:8], msg)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.update_status(pid, "running", error_message=msg)
        # Append a new sync batch to the phase timeline.
        next_batch = self._next_batch_number(pid)
        self._batches.create(pid, next_batch, [_SYNC_MARKER])
        try:
            # Pin to the target table's region (sync is keyed by the regional
            # target table ARN, same region-match constraint as start).
            client = self._client_for(pipeline)
            # SyncTableMigration takes no `source`: the service re-reads the
            # source recorded on the target table during the initial load. We
            # only supply the role, target ARN, and reports.
            # migrationOutput is REQUIRED on sync; with no reports bucket
            # configured build_output() raises and the sync is marked failed
            # with a clear message instead of an opaque API validation error.
            reports_arn = _reports_bucket_arn()
            if not reports_arn:
                raise ValueError("no reports bucket configured "
                                 "(set the Logs Bucket / ENGINE_S3TABLES_REPORTS_BUCKET)")
            output = client.build_output(
                reports_arn, f"{_REPORTS_PREFIX}/{pid}")
            result = client.sync(
                role_arn=_migration_role_arn(pipeline),
                target_table_arn=target_arn,
                output=output,
                # Plan review is for the INITIAL load only. Syncs run
                # unattended — no compatibility-review pause before cutover.
                review_migration_plan=False)
        except Exception as e:
            msg = f"sync_table_migration failed: {e}"
            log.exception("Managed sync failed for %s", pid[:8])
            self._batches.mark_failed(next_batch, error_message=msg[:500], pipeline_id=pid)
            # A sync failure never invalidates the already-migrated table (the
            # initial load produced it before any sync could run). Keep
            # status=running and mark sync_status='sync_failed' so the pipeline
            # stays finalizable and the sync is retried (guarded) on cadence /
            # via resume-from-failure. Persist the error for the UI.
            self._repo.update_sync_status(pid, "sync_failed")
            return self._repo.update_status(pid, "running", error_message=msg[:500])

        migration_id = result.get("migration_id")
        if migration_id:
            self._repo.update_emr_job_run_id(pid, f"s3tm:{migration_id}")
            self._batches.mark_running(next_batch, f"s3tm:{migration_id}", pipeline_id=pid)
        # Capture the most recent sync request payload for inspection.
        if result.get("request"):
            self._repo.update_metadata(
                pid, "sync_request", json.dumps(result["request"]))
        self._repo.update_sync_status(pid, "syncing")
        return self._repo.update_status(pid, "running")

    # ── helpers ──

    @staticmethod
    def _migration_id(pipeline: dict) -> str:
        """Extract the migrationId from emr_job_run_id (s3tm:<id>)."""
        ref = pipeline.get("emr_job_run_id") or ""
        return ref[len("s3tm:"):] if ref.startswith("s3tm:") else ""

    @staticmethod
    def _is_sync_batch(batch: dict | None) -> bool:
        """True if the given batch is a sync run (vs the initial load)."""
        if not batch:
            return False
        parts = _parse_json(batch.get("partitions")) or []
        return bool(parts) and parts[0] == _SYNC_MARKER

    def _sync_in_flight(self, pipeline: dict) -> bool:
        """True if a sync (or any migration) is already in progress for this
        pipeline, so a new sync_table_migration MUST NOT be issued.

        Uses local state only (no AWS call) — three independent signals, any of
        which means "in flight":
          1. sync_status == 'syncing' (a sync was just started).
          2. A running sync batch exists in the phase timeline.
          3. The current migration id maps to a non-terminal API state on the
             most recent reconcile — best-effort via a live get, but we avoid a
             hard AWS dependency: if the get fails we fall back to (1)/(2).
        """
        pid = pipeline["id"]
        # (1) sync_status
        if (pipeline.get("sync_status") or "").lower() == "syncing":
            return True
        # (2) running sync batch
        for b in (self._batches.list_by_pipeline(pid) or []):
            if b.get("status") == "running" and self._is_sync_batch(b):
                return True
        # (3) live API state of the current migration (best-effort). A migration
        # in an ACTIVE state (planning/queued/migrating/…) is still in progress.
        migration_id = self._migration_id(pipeline)
        if migration_id:
            try:
                detail = self._client_for(pipeline).get(migration_id)
                api_state = (detail.get("state") or "").strip()
                if api_state and api_state not in TERMINAL_STATES:
                    return True
            except Exception as e:
                log.debug("Managed migration %s: in-flight get() failed (%s); "
                          "relying on local signals", pid[:8], e)
        return False

    def _next_batch_number(self, pid: str) -> int:
        """Next batch_number for a new sync batch (max existing + 1)."""
        existing = self._batches.list_by_pipeline(pid) or []
        if not existing:
            return 1
        return max(b["batch_number"] for b in existing) + 1

    def _sync_interval(self, pipeline: dict) -> int:
        """Per-pipeline sync interval (seconds) from spec.strategy.sync_interval,
        falling back to the global default."""
        spec = _parse_json(pipeline.get("spec_json")) or {}
        try:
            return int((spec.get("strategy", {}) or {}).get(
                "sync_interval", _SYNC_INTERVAL) or _SYNC_INTERVAL)
        except (ValueError, TypeError):
            return _SYNC_INTERVAL

    def _auto_sync_due(self, pid: str, pipeline: dict) -> bool:
        """True if enough time has elapsed since the last completed load/sync to
        auto-trigger another sync.

        Uses the most recent completed batch's completed_at as the reference —
        NOT the pipeline updated_at, which the reconcile loop re-touches on
        every poll. If no completed batch timestamp is available, do not
        auto-sync (avoids a tight sync loop on missing data).
        """
        interval = self._sync_interval(pipeline)
        batches = self._batches.list_by_pipeline(pid) or []
        completed_times = [
            b.get("completed_at") for b in batches
            if b.get("status") == "completed" and b.get("completed_at")
        ]
        if not completed_times:
            return False
        last = max(completed_times)
        try:
            last_dt = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - last_dt).total_seconds() >= interval
        except (ValueError, TypeError):
            return False

    def _target_table_arn(self, pipeline: dict) -> str:
        """The target table ARN — ONLY the captured value from the migration API.

        The real S3 Tables table ARN ends in /table/<server-assigned-uuid>
        (e.g. .../table/af06aca4-535b-...), which CANNOT be reconstructed from
        the namespace/table name. It is captured on reconcile from the API's
        `tableARN` field (spec_json._metadata.target_table_arn). Returning a
        reconstructed .../table/<namespace>/<table> ARN would be a bogus
        identifier that sync_table_migration / complete_table_migration silently
        misfire on — so we return "" instead and let callers surface a clear
        error.
        """
        spec = _parse_json(pipeline.get("spec_json")) or {}
        return (spec.get("_metadata", {}) or {}).get("target_table_arn", "")

    @staticmethod
    def _managed_options(pipeline: dict) -> dict:
        """Managed-migration options from the spec's strategy block.

        The S3 Tables Migration API's dataMigration.migrationPolicies has two
        lowerCamelCase enums (replacing the old conflictPolicy/typeConversion):
          unmatchedSourceFieldsPolicy: "fail" | "drop"   (default "fail")
          incompatibleValuesPolicy:    "fail" | "setNull" (default "fail")
        The defaults are strict: a schema/value mismatch fails the migration
        unless the user opts into drop / setNull in Settings."""
        spec = _parse_json(pipeline.get("spec_json")) or {}
        strat = spec.get("strategy", {}) or {}
        return {
            "unmatched_source_fields_policy":
                strat.get("unmatched_source_fields_policy", "fail"),
            "incompatible_values_policy":
                strat.get("incompatible_values_policy", "fail"),
        }

    @staticmethod
    def _has_sort_order(pipeline: dict) -> bool:
        """True when the plan carries a non-empty iceberg_properties.sort_order
        (i.e. the agent recommended sort keys). Used only to log that the sort
        order is not sent: sorting is unsupported during migration."""
        spec = _parse_json(pipeline.get("spec_json")) or {}
        so = (spec.get("iceberg_properties", {}) or {}).get("sort_order", [])
        return bool(so)
