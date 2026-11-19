# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Background reconcile loop for running pipelines.

Spawned as an asyncio task inside the FastAPI lifespan.
Polls all running pipelines on a fixed interval and advances
their state by checking EMR Serverless job status.
"""

import asyncio
import logging
import os

from .engine import MigrationEngine
from .pipeline_locks import try_pipeline_lock
from ..db import PipelineRepo
from ..db.status_guard import expect_status

log = logging.getLogger(__name__)

# Default 30s, override with ENGINE_RECONCILE_INTERVAL_SECONDS
_INTERVAL = int(os.getenv("ENGINE_RECONCILE_INTERVAL_SECONDS", "30"))

# Size of the thread pool that runs per-pipeline reconciles concurrently.
# Reconcile work is I/O-bound (EMR/Athena/DB calls), so a larger pool lets more
# pipelines advance per sweep. Default 50 comfortably handles 100s of pipelines;
# raise for higher counts, bounded by DB connection-pool size (see db/connection).
# Note: morph loads offload to their own (separately capped) background threads,
# so this pool size does not directly bound morph's footer-read fan-out.
_WORKERS = int(os.getenv("ENGINE_RECONCILE_WORKERS", "50"))

# Returned by a guarded step that did nothing this sweep (lock busy, or the
# pipeline changed since the sweep listed it).
_SKIPPED = object()


def _guarded(repo, pipeline: dict, step):
    """Run `step(fresh_pipeline)` safely against concurrent user actions.

    Takes the pipeline's lock without waiting, re-reads the pipeline with a
    consistent read, skips it if its status moved on since the sweep listed it,
    and runs the step with compare-and-set status writes (db/status_guard.py).
    """
    pid = pipeline["id"]
    with try_pipeline_lock(pid) as acquired:
        if not acquired:
            log.debug("Pipeline %s: user action in progress, skipping this sweep",
                      pid[:8])
            return _SKIPPED
        fresh = repo.get(pid, consistent=True)
        if not fresh or fresh.get("status") != pipeline.get("status"):
            log.debug("Pipeline %s: status changed (%s -> %s) since the sweep "
                      "listed it, skipping", pid[:8], pipeline.get("status"),
                      (fresh or {}).get("status"))
            return _SKIPPED
        with expect_status(pid, fresh["status"]):
            return step(fresh)


async def reconcile_loop():
    """Run the reconcile sweep forever until cancelled."""
    from concurrent.futures import ThreadPoolExecutor

    engine = MigrationEngine()
    repo = PipelineRepo()
    executor = ThreadPoolExecutor(max_workers=_WORKERS)
    _in_progress = set()  # Track pipelines currently being reconciled

    log.info("Reconcile loop started (interval=%ds, workers=%d)", _INTERVAL, _WORKERS)

    loop = asyncio.get_event_loop()

    def _promote_queued():
        """Promote queued pipelines to running while under the concurrency cap.

        Runs in the executor (blocking DB + submit calls). Governs EMR/Athena
        concurrency: only starts as many queued pipelines as there are free
        slots (Phase 3.1 / T16).
        """
        from .governor import available_slots
        slots = available_slots(repo)
        if slots <= 0:
            return
        queued = repo.list_all(status="queued")
        if not queued:
            return
        # Oldest first (list_all returns created_at DESC), so start from the end.
        for p in reversed(queued):
            if slots <= 0:
                break
            try:
                if _guarded(repo, p, engine.submit) is _SKIPPED:
                    continue
                log.info("Pipeline %s: promoted from queued -> running", p["id"][:8])
                slots -= 1
            except Exception:
                log.exception("Failed to promote queued pipeline %s", p["id"][:8])

    while True:
        try:
            # Promote any queued pipelines that now fit under the cap.
            await loop.run_in_executor(executor, _promote_queued)

            running = await loop.run_in_executor(
                executor, lambda: repo.list_all(status="running"))
            if running:
                # Filter out pipelines already being processed from a previous sweep
                to_process = [p for p in running if p["id"] not in _in_progress]
                skipped = len(running) - len(to_process)
                if skipped:
                    log.debug("Reconcile sweep: %d running, %d already in progress, processing %d",
                              len(running), skipped, len(to_process))
                elif to_process:
                    log.info("Reconcile sweep: %d running pipeline(s)", len(running))
                for p in to_process:
                    log.info("  ├─ %s | %s.%s → %s.%s | sync=%s | engine=%s | "
                             "emr_job=%s | partitions=%s/%s | bytes=%s/%s",
                             p["id"][:8],
                             p["source_database"], p["source_table"],
                             p.get("target_database", ""),
                             p.get("target_table", ""),
                             p.get("sync_status", "?"),
                             p.get("execution_engine", ""),
                             p.get("emr_job_run_id", "n/a"),
                             p.get("partitions_completed", 0),
                             p.get("partitions_total", 0),
                             p.get("bytes_processed", 0),
                             p.get("bytes_total", 0))
            else:
                log.debug("Reconcile sweep: no running pipelines")
                to_process = []

            # Reconcile pipelines concurrently
            async def _reconcile_one(pipeline):
                pid = pipeline["id"]
                _in_progress.add(pid)
                source = f"{pipeline['source_database']}.{pipeline['source_table']}"
                try:
                    # No timeout — let long-running morph/XTable complete naturally.
                    # The _in_progress set prevents duplicate processing.
                    updated = await loop.run_in_executor(
                        executor, _guarded, repo, pipeline, engine.reconcile)
                    if updated is _SKIPPED:
                        return
                    if updated is None:
                        log.warning("Pipeline %s (%s): reconcile returned None, skipping",
                                    pid[:8], source)
                        return
                    new_status = updated.get("status", "unknown")
                    if new_status != pipeline["status"]:
                        log.info("Pipeline %s (%s) transitioned: %s -> %s",
                                 pid[:8], source, pipeline["status"], new_status)
                except Exception:
                    log.exception("Reconcile failed for pipeline %s (%s)", pid[:8], source)
                finally:
                    _in_progress.discard(pid)

            # Fire tasks without waiting — they run in background.
            # The _in_progress set prevents the next sweep from re-processing them.
            for p in to_process:
                asyncio.ensure_future(_reconcile_one(p))

        except Exception:
            log.exception("Reconcile sweep failed")

        await asyncio.sleep(_INTERVAL)
