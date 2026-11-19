# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pipeline State Machine — unified reconcile logic for all strategies.

Handles common state transitions, batch advancement, job polling, interval
checks, and sync batch tracking. Strategies implement hooks for their
specific work.

State model:
  - pipeline.status: pending → running → completed | failed | cancelled | paused
  - pipeline.sync_status: pending → initial_load → in_sync ⇌ out_of_sync → syncing → in_sync | failed

Batch model:
  - batch.status: pending → running → completed | failed
  - Batches are sequential: next batch starts only after current completes.
"""

import json
import logging
import os
from datetime import datetime, timezone

from ..db import PipelineRepo, BatchRepo
from ..utils.polling import poll_until_complete

log = logging.getLogger(__name__)

_SYNC_INTERVAL = int(os.getenv("ENGINE_INCREMENTAL_INTERVAL_SECONDS", "3600"))


def _has_util(resource_util: dict) -> bool:
    """True if EMR totalResourceUtilization carries any non-zero usage.

    Used to detect the window where a job is terminal but EMR has not yet
    aggregated its utilization (all fields absent/zero), so we can re-fetch
    before recording cost.
    """
    if not resource_util:
        return False
    return any(
        float(resource_util.get(k, 0) or 0) > 0
        for k in ("vCPUHour", "memoryGBHour", "storageGBHour")
    )


class PipelineStateMachine:
    """Common reconcile state machine for all strategies."""

    def __init__(self):
        self._repo = PipelineRepo()
        self._batches = BatchRepo()

    # ═══════════════════════════════════════════════════════════════
    # MAIN RECONCILE ENTRY POINT
    # ═══════════════════════════════════════════════════════════════

    def reconcile(self, pipeline: dict, strategy) -> dict:
        """Advance a running pipeline through its lifecycle.

        Called by the reconcile loop for each running pipeline.
        Delegates work to the strategy via hooks.
        """
        pid = pipeline["id"]
        sync_status = pipeline.get("sync_status", "pending")

        # ── Phase 1: Poll active job ──
        if self._has_active_job(pipeline):
            return self._poll_and_handle_job(pipeline, strategy)

        # ── Phase 1b: One-shot strategies (rewrite) with no active job ──
        # If there's no ongoing sync capability and no job running,
        # check if all batches are done. If so, mark completed.
        if getattr(strategy, '_one_shot', False):
            all_batches = self._batches.list_by_pipeline(pid)
            if not all_batches:
                return pipeline  # Job hasn't been submitted yet
            completed = all(b["status"] == "completed" for b in all_batches)
            if completed:
                if hasattr(strategy, 'on_initial_load_complete'):
                    result = strategy.on_initial_load_complete(pipeline)
                    if result:
                        return result
                return self._repo.update_status(pid, "completed")
            failed = [b for b in all_batches if b["status"] == "failed"]
            if failed:
                return self._repo.update_status(pid, "failed",
                    error_message=failed[0].get("error_message", "Batch failed"))
            return pipeline  # Batches still pending/running — wait

        # ── Phase 2: Initial load (batch-driven) ──
        if sync_status in ("pending", "initial_load"):
            return self._advance_initial_load(pipeline, strategy)

        # ── Phase 3: Handle failed sync (retry after interval) ──
        if sync_status == "failed":
            if not self._interval_elapsed(pipeline):
                return pipeline
            self._repo.update_sync_status(pid, "out_of_sync")
            log.info("Pipeline %s: resetting failed sync for retry", pid[:8])
            return self._repo.get(pid)

        # ── Phase 4: Check for changes (in_sync → out_of_sync) ──
        if sync_status == "in_sync":
            try:
                has_changes = strategy.detect_changes(pipeline)
            except Exception as e:
                log.warning("Pipeline %s: change detection failed: %s", pid[:8], e)
                return pipeline
            if not has_changes:
                return pipeline
            self._repo.update_sync_status(pid, "out_of_sync")
            log.info("Pipeline %s: changes detected, marking out_of_sync", pid[:8])
            sync_status = "out_of_sync"

        # ── Phase 5: Execute sync (out_of_sync → syncing → in_sync) ──
        if sync_status == "out_of_sync":
            if not self._interval_elapsed(pipeline):
                return self._repo.get(pid)
            return self._execute_sync(pipeline, strategy)

        # ── Phase 6: Syncing (poll if there's a job, otherwise wait) ──
        if sync_status == "syncing":
            return pipeline

        return pipeline

    # ═══════════════════════════════════════════════════════════════
    # PHASE 1: JOB POLLING
    # ═══════════════════════════════════════════════════════════════

    def _has_active_job(self, pipeline: dict) -> bool:
        """Check if the pipeline has an active job to poll."""
        return bool(pipeline.get("emr_job_run_id", ""))

    def _poll_and_handle_job(self, pipeline: dict, strategy) -> dict:
        """Poll an active Athena/EMR job and handle completion."""
        pid = pipeline["id"]
        job_ref = pipeline["emr_job_run_id"]

        # Get job status
        state, details, cost_info, timing = self._get_job_state(pipeline, job_ref)
        if state is None:
            return pipeline  # Polling failed, try next cycle

        log.debug("Pipeline %s job %s: %s", pid[:8], job_ref[:20], state)

        if state == "RUNNING":
            return pipeline

        # Record cost on completion (success or failure)
        if cost_info:
            self._record_cost(pid, job_ref, cost_info)

        if state == "SUCCESS":
            return self._handle_job_success(pipeline, strategy, job_ref, timing)
        elif state in ("FAILED", "CANCELLED"):
            return self._handle_job_failure(pipeline, strategy, job_ref, state, details)
        else:
            log.warning("Pipeline %s: unexpected job state '%s' for %s, waiting",
                        pid[:8], state, job_ref[:20])
            return pipeline

    def _get_job_state(self, pipeline: dict, job_ref: str):
        """Poll job status.

        Returns (state, details, cost_info, timing) or (None, None, None, None)
        on error. ``timing`` is a dict with ISO ``started_at`` / ``completed_at``
        reflecting the job's ACTUAL execution window (Athena only, from its
        SubmissionDateTime / CompletionDateTime), or None when unavailable — in
        which case the reconcile observation time is used instead.
        """
        from ..engines.athena import AthenaClient
        from ..engines.emr import EmrServerlessClient

        if job_ref.startswith("athena:"):
            qid = job_ref[7:]
            try:
                # Poll in the SAME region the query was submitted in — the
                # pipeline's Athena config (from Settings), not the engine env
                # default. Otherwise GetQueryExecution runs in the wrong region
                # and reports "QueryExecution ... was not found".
                athena = AthenaClient.from_pipeline(pipeline)
                status = athena.get_query_status(qid)
            except Exception as e:
                log.error("Failed to poll Athena query %s: %s", qid, e)
                return None, None, None, None
            state = status["state"]
            state_map = {"SUCCEEDED": "SUCCESS", "FAILED": "FAILED",
                         "CANCELLED": "CANCELLED", "RUNNING": "RUNNING",
                         "QUEUED": "RUNNING"}
            cost_info = {"type": "athena", "bytes_scanned": status.get("data_scanned_bytes", 0)}
            timing = None
            sub, comp = status.get("submission_time"), status.get("completion_time")
            if sub or comp:
                timing = {"started_at": sub or None, "completed_at": comp or None}
            return state_map.get(state, state), status.get("state_reason", ""), cost_info, timing
        else:
            try:
                # Poll in the SAME region/application the job was submitted to —
                # the pipeline's EMR config (from Settings), not the engine env
                # default. EMR Serverless applications are regional; polling the
                # wrong region reports "Job Run with given ID doesn't exist".
                emr = EmrServerlessClient.from_pipeline(pipeline)
                status = emr.get_job_status(job_ref)
            except Exception as e:
                log.error("Failed to poll EMR job %s: %s", job_ref, e)
                return None, None, None, None
            state = status["state"]
            # EMR Serverless states: SUBMITTED, PENDING, SCHEDULED, RUNNING, SUCCESS, FAILED, CANCELLING, CANCELLED
            # Map pre-running states to RUNNING so the state machine just waits
            if state in ("PENDING", "SCHEDULED", "SUBMITTED"):
                state = "RUNNING"
            resource_util = status.get("total_resource_utilization", {})
            # EMR Serverless populates totalResourceUtilization slightly AFTER
            # a job first flips to a terminal state — it is frequently empty
            # (all-zero) at the exact poll that observes SUCCESS/FAILED. For
            # one-shot strategies (Rewrite) the job ref is cleared immediately
            # after this single terminal poll and never re-polled, so a zero
            # here means the cost is lost forever (multi-batch strategies like
            # Incremental happen to re-poll and catch it, which is why they
            # "work"). When a job is terminal but utilization is still zero,
            # briefly re-poll (via the sanctioned polling helper, not a bare
            # sleep) until the aggregate is populated so cost recording is
            # deterministic across ALL strategies.
            if state in ("SUCCESS", "FAILED", "CANCELLED") and not _has_util(resource_util):
                def _refetch():
                    try:
                        return emr.get_job_status(job_ref)
                    except Exception:
                        return status
                # ~6s max (interval 1.0 x 6), well under the reconcile interval.
                status = poll_until_complete(
                    check=_refetch,
                    is_done=lambda s: _has_util(
                        s.get("total_resource_utilization", {})),
                    interval=1.0, max_attempts=6)
                resource_util = status.get("total_resource_utilization", {})
            cost_info = {
                "type": "emr",
                "vcpu_hours": resource_util.get("vCPUHour", 0),
                "memory_gb_hours": resource_util.get("memoryGBHour", 0),
                "storage_gb_hours": resource_util.get("storageGBHour", 0),
            }
            # EMR jobs run minutes; the ~30s reconcile quantization is
            # negligible there, so we leave EMR timing as the observation time.
            return state, status.get("state_details", ""), cost_info, None

    def _record_cost(self, pid: str, job_ref: str, cost_info: dict):
        """Record compute cost for a completed job."""
        try:
            from ..db import CostRepo
            costs = CostRepo()
            if cost_info["type"] == "athena":
                if cost_info["bytes_scanned"] > 0:
                    costs.record_athena_query(pid, cost_info["bytes_scanned"])
            else:
                vcpu = cost_info.get("vcpu_hours", 0)
                mem = cost_info.get("memory_gb_hours", 0)
                storage = cost_info.get("storage_gb_hours", 0)
                if vcpu > 0 or mem > 0:
                    costs.record_emr_job(pid, vcpu, mem, storage)
        except Exception as e:
            log.warning("Pipeline %s: failed to record cost: %s", pid[:8], e)

    def _handle_job_success(self, pipeline: dict, strategy, job_ref: str,
                            timing: dict = None) -> dict:
        """Handle a successful job completion.

        ``timing`` (when provided, Athena only) carries the job's actual
        started_at / completed_at so the batch records its true execution
        window rather than the reconcile-loop observation time.
        """
        pid = pipeline["id"]

        # Find and complete the running batch
        running_batch = self._batches.get_running(pid)
        if running_batch:
            batch_id = running_batch.get("id") or running_batch.get("batch_number")
            self._batches.mark_completed(
                batch_id, pipeline_id=pid,
                started_at=(timing or {}).get("started_at"),
                completed_at=(timing or {}).get("completed_at"))
            log.info("Pipeline %s: batch %d completed (job %s)",
                     pid[:8], running_batch["batch_number"], job_ref[:20])

        # Clear job reference
        self._repo.update_emr_job_run_id(pid, "")

        # Let strategy handle post-completion logic
        if hasattr(strategy, 'on_job_success'):
            result = strategy.on_job_success(pipeline, running_batch)
            if result is not None:
                return result
            # Strategy returned None — use default behavior below.

        # Default post-success: update progress and advance.
        self._update_batch_progress(pid)

        # Determine if this was a sync batch or an initial-load batch.
        # Sync batches have partitions starting with "__sync__".
        is_sync_batch = False
        if running_batch:
            batch_partitions = running_batch.get("partitions", "[]")
            if isinstance(batch_partitions, str):
                import json as _json
                try:
                    batch_partitions = _json.loads(batch_partitions)
                except (ValueError, TypeError):
                    batch_partitions = [batch_partitions]
            is_sync_batch = any(isinstance(p, str) and p.startswith("__sync__")
                                for p in batch_partitions)

        if is_sync_batch:
            # Sync job completed — transition back to in_sync.
            self._repo.update_sync_status(pid, "in_sync")
            self._repo.update_emr_job_run_id(pid, "")
            log.info("Pipeline %s: sync batch completed, back to in_sync", pid[:8])
            return self._repo.get(pid)

        return self._advance_initial_load(pipeline, strategy)

    def _handle_job_failure(self, pipeline: dict, strategy, job_ref: str,
                            state: str, details: str) -> dict:
        """Handle a failed/cancelled job."""
        pid = pipeline["id"]

        # Mark running batch as failed
        running_batch = self._batches.get_running(pid)
        if running_batch:
            batch_id = running_batch.get("id") or running_batch.get("batch_number")
            self._batches.mark_failed(batch_id, error_message=details[:500], pipeline_id=pid)

        # Clear job reference
        self._repo.update_emr_job_run_id(pid, "")

        # Let strategy handle (some may want to retry, others fail pipeline)
        if hasattr(strategy, 'on_job_failure'):
            return strategy.on_job_failure(pipeline, running_batch, state, details)

        # Default: fail the pipeline
        error_msg = f"Job {state.lower()}: {details[:300]}" if details else f"Job {state.lower()}"
        return self._repo.update_status(pid, "failed", error_message=error_msg)

    # ═══════════════════════════════════════════════════════════════
    # PHASE 2: INITIAL LOAD (BATCH ADVANCEMENT)
    # ═══════════════════════════════════════════════════════════════

    def _advance_initial_load(self, pipeline: dict, strategy) -> dict:
        """Advance through initial load batches sequentially."""
        pid = pipeline["id"]

        # Get all batches
        all_batches = self._batches.list_by_pipeline(pid)
        if not all_batches:
            # No batches yet — strategy hasn't created them (submit still in progress)
            log.debug("Pipeline %s: no batches exist yet, waiting", pid[:8])
            return pipeline

        # Check for running batch — wait for it or execute it
        running = [b for b in all_batches if b["status"] == "running"]
        if running:
            batch = running[0]
            batch_job_ref = batch.get("emr_job_run_id", "")

            # If the batch has its own job ref, set it on the pipeline so Phase 1 can poll it
            if batch_job_ref and batch_job_ref not in ("", "in-process", "retry",
                                                        "pending", "xtable-lambda",
                                                        "xtable-incremental", "sync-in-progress",
                                                        "thread:running"):
                self._repo.update_emr_job_run_id(pid, batch_job_ref)
                return self._repo.get(pid)

            # A batch is running with no external job — strategy does in-process work
            if hasattr(strategy, 'execute_batch'):
                return strategy.execute_batch(pipeline, batch)
            log.debug("Pipeline %s: batch %d running, waiting", pid[:8], batch["batch_number"])
            return pipeline

        # Check for pending batches — submit next one
        # Exclude sync batches (those created by execute_sync, not initial load)
        pending = [b for b in all_batches if b["status"] == "pending"]
        initial_pending = []
        for b in pending:
            partitions = b.get("partitions", "[]")
            if isinstance(partitions, str):
                try:
                    import json as _json
                    partitions = _json.loads(partitions)
                except (ValueError, TypeError):
                    partitions = [partitions]
            is_sync = any(isinstance(p, str) and p.startswith("__sync__") for p in partitions)
            if not is_sync:
                initial_pending.append(b)

        # Check for failed batches BEFORE submitting next — stop immediately on failure
        failed = [b for b in all_batches if b["status"] == "failed"]
        if failed:
            error_msg = failed[0].get("error_message", "Batch failed")
            return self._repo.update_status(pid, "failed", error_message=error_msg)

        if initial_pending:
            next_batch = min(initial_pending, key=lambda b: b["batch_number"])
            if hasattr(strategy, 'submit_batch'):
                return strategy.submit_batch(pipeline, next_batch)
            # Default: mark running and let strategy pick it up next cycle
            self._batches.mark_running(next_batch["batch_number"], "pending",
                                       pipeline_id=pid)
            return self._repo.get(pid)

        # If only sync batches are pending, initial load is done — move to in_sync
        if pending and not initial_pending:
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: initial load done (sync batches pending), in_sync", pid[:8])
            return self._repo.get(pid)

        # Guard against a submit-time race: if the ONLY batch is batch 0
        # and it is still RUNNING (i.e. not yet completed), the strategy may
        # be mid-submit (about to create data batches). Wait for the next
        # cycle rather than jumping to in_sync and stranding those batches.
        # However, if batch 0 is already COMPLETED and is the only batch,
        # this is a legitimate single-batch workflow (e.g. XTable morph) that
        # is genuinely done — proceed to on_initial_load_complete.
        batch_numbers = {b["batch_number"] for b in all_batches}
        if batch_numbers == {0}:
            batch0 = all_batches[0]
            if batch0.get("status") != "completed":
                log.debug("Pipeline %s: only create-table batch exists and is "
                          "not yet completed, waiting for data batches", pid[:8])
                return pipeline

        # All batches completed — transition to in_sync or completed
        if hasattr(strategy, 'on_initial_load_complete'):
            return strategy.on_initial_load_complete(pipeline)

        # Default: mark in_sync for strategies with ongoing sync
        if hasattr(strategy, 'detect_changes'):
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: initial load complete, in_sync", pid[:8])
            return self._repo.get(pid)
        else:
            # One-shot strategy (rewrite) — mark completed
            return self._repo.update_status(pid, "completed")

    # ═══════════════════════════════════════════════════════════════
    # PHASE 6: EXECUTE SYNC
    # ═══════════════════════════════════════════════════════════════

    def _execute_sync(self, pipeline: dict, strategy) -> dict:
        """Execute an ongoing sync operation."""
        pid = pipeline["id"]

        # Create a sync batch for tracking
        existing_batches = self._batches.list_by_pipeline(pid)
        next_batch_num = max((b["batch_number"] for b in existing_batches), default=-1) + 1

        self._repo.update_sync_status(pid, "syncing")

        try:
            result = strategy.execute_sync(pipeline, next_batch_num)

            # If strategy returns a pipeline dict, use it
            if isinstance(result, dict) and "id" in result:
                return result

            # Otherwise assume sync completed synchronously
            self._repo.update_sync_status(pid, "in_sync")
            log.info("Pipeline %s: sync complete, back to in_sync", pid[:8])
            return self._repo.get(pid)

        except Exception as e:
            log.error("Pipeline %s: sync execution failed: %s", pid[:8], e)
            self._repo.update_sync_status(pid, "failed")
            return self._repo.get(pid)

    # ═══════════════════════════════════════════════════════════════
    # SHARED UTILITIES
    # ═══════════════════════════════════════════════════════════════

    def _interval_elapsed(self, pipeline: dict) -> bool:
        """Check if enough time has passed since the last sync for another sync.

        Uses the most recent completed sync batch's `completed_at` timestamp
        as the reference point — NOT `pipeline.updated_at` (which gets touched
        by many operations and doesn't reliably represent "last sync time").
        If no sync batch has ever completed, falls back to the pipeline's
        `started_at` (initial load completion time is a reasonable first-sync
        baseline) or returns True (always allow the first sync).

        Special case: if updated_at is set to the far-past sentinel
        ("2000-01-01") by the manual sync-now endpoint, always returns True
        to bypass the interval (intentional force-sync).
        """
        pid = pipeline["id"]

        # Manual sync-now sets updated_at to far-past as a force signal
        updated_at = pipeline.get("updated_at", "")
        if updated_at and updated_at.startswith("2000-01-01"):
            return True

        # Find the most recent completed sync batch
        all_batches = self._batches.list_by_pipeline(pid)
        last_sync_time = None
        for b in all_batches:
            if b.get("status") != "completed":
                continue
            partitions = b.get("partitions", "[]")
            if isinstance(partitions, str):
                import json as _json
                try:
                    partitions = _json.loads(partitions)
                except (ValueError, TypeError):
                    partitions = [partitions]
            is_sync = any(isinstance(p, str) and p.startswith("__sync__")
                          for p in partitions)
            if is_sync and b.get("completed_at"):
                ts = b["completed_at"]
                if not last_sync_time or ts > last_sync_time:
                    last_sync_time = ts

        # If no sync batch ever completed, use started_at as baseline
        # (reasonable: initial load just finished, first sync can fire
        # after one interval from that point).
        if not last_sync_time:
            last_sync_time = pipeline.get("started_at") or pipeline.get("created_at", "")

        if not last_sync_time:
            return True

        try:
            last = datetime.fromisoformat(last_sync_time.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return (now - last).total_seconds() >= _SYNC_INTERVAL
        except (ValueError, TypeError):
            return True

    def _update_batch_progress(self, pid: str):
        """Update pipeline progress counters from batch summary."""
        summary = self._batches.get_summary(pid)
        completed_count = summary.get("completed", {}).get("count", 0)
        total_count = sum(v["count"] for v in summary.values())
        self._repo.update_progress(
            pid, partitions_completed=completed_count,
            partitions_total=total_count)

    # ═══════════════════════════════════════════════════════════════
    # RESUME / RETRY (called from API routes)
    # ═══════════════════════════════════════════════════════════════

    def resume(self, pipeline_id: str) -> dict:
        """Resume a paused/pending pipeline. Resets failed batches."""
        p = self._repo.get(pipeline_id)
        if not p:
            return {}

        migration_type = p.get("migration_type", "rewrite")

        # Rewrite pipelines stay in 'pending' until finalize triggers them.
        # Resume just un-pauses them back to pending (staged in the app).
        if migration_type == "rewrite":
            prev_status = p.get("status", "")
            if prev_status == "paused":
                # Was it running (CTAS in progress) or just staged (pending)?
                # Check if there's an active job — if so, resume to running
                if p.get("emr_job_run_id"):
                    self._repo.update_status(pipeline_id, "running", error_message="")
                else:
                    self._repo.update_status(pipeline_id, "pending", error_message="")
            log.info("Pipeline %s: rewrite resumed → %s", pipeline_id[:8],
                     "running" if p.get("emr_job_run_id") else "pending")
            return self._repo.get(pipeline_id)

        # Non-rewrite: reset failed batches and resume
        reset_count = self._batches.reset_failed_to_pending(pipeline_id)
        if reset_count:
            log.info("Pipeline %s: reset %d failed batch(es)", pipeline_id[:8], reset_count)

        self._repo.update_status(pipeline_id, "running", error_message="")

        # Determine correct sync_status:
        # - If any initial load batch is incomplete → initial_load
        # - Otherwise → restore to in_sync (detect_changes will catch up)
        # Initial load batches = any batch NOT prefixed with __sync__
        all_batches = self._batches.list_by_pipeline(pipeline_id)

        def _is_sync_batch(b):
            partitions = b.get("partitions", "[]")
            if isinstance(partitions, str):
                try:
                    partitions = json.loads(partitions)
                except (json.JSONDecodeError, TypeError):
                    partitions = [partitions]
            return any(isinstance(p, str) and p.startswith("__sync__") for p in partitions)

        initial_batches = [b for b in all_batches if not _is_sync_batch(b)]
        initial_complete = (
            len(initial_batches) > 0 and
            all(b["status"] == "completed" for b in initial_batches)
        )

        if not initial_batches or not initial_complete:
            self._repo.update_sync_status(pipeline_id, "initial_load")
            if reset_count:
                batch0 = self._batches.get_by_number(pipeline_id, 0)
                if batch0 and batch0.get("status") == "pending":
                    self._batches.mark_running(0, "retry", pipeline_id=pipeline_id)
        else:
            # Initial load done — restore to in_sync (detect_changes will catch up)
            self._repo.update_sync_status(pipeline_id, "in_sync")

        log.info("Pipeline %s: resumed", pipeline_id[:8])
        return self._repo.get(pipeline_id)

    def retry(self, pipeline_id: str) -> dict:
        """Retry a failed pipeline from scratch (re-submit)."""
        # Reset all batches
        self._batches.reset_failed_to_pending(pipeline_id)
        self._repo.update_status(pipeline_id, "running", error_message="")
        self._repo.update_sync_status(pipeline_id, "initial_load")
        self._repo.update_emr_job_run_id(pipeline_id, "")
        return self._repo.get(pipeline_id)
