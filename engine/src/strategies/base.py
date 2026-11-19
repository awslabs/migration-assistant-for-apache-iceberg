# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Base migration strategy interface.

Strategies follow an async lifecycle:
  validate → submit → reconcile (polled) → complete/fail
  cancel can be called at any point while running.

The PipelineStateMachine handles common reconcile logic (job polling,
batch advancement, interval checks, sync_status transitions). Strategies
implement hooks for their specific work.
"""

from abc import ABC, abstractmethod
from typing import Optional


class MigrationStrategy(ABC):
    """Abstract base for all migration strategies."""

    @abstractmethod
    def validate(self, pipeline: dict) -> tuple[bool, str]:
        """Validate that the pipeline can be executed.

        Returns (is_valid, reason).
        """

    @abstractmethod
    def submit(self, pipeline: dict) -> dict:
        """Submit the initial work for this pipeline.

        Creates batches, submits initial job.
        Returns updated pipeline dict with job references stored.
        """

    @abstractmethod
    def cancel(self, pipeline: dict) -> dict:
        """Cancel in-flight work for this pipeline.

        Returns updated pipeline dict.
        """

    # ── Optional Hooks (implement for batch-driven strategies) ──

    def execute_batch(self, pipeline: dict, batch: dict) -> dict:
        """Execute an in-process batch (no external job).

        Called when a batch is 'running' but has no emr_job_run_id.
        Used by morph (in-process file scanning) and similar.

        Returns updated pipeline dict.
        """
        return pipeline

    def submit_batch(self, pipeline: dict, batch: dict) -> dict:
        """Submit the next batch as an external job.

        Called when state machine finds a pending batch to advance.
        Strategy should mark the batch running and set emr_job_run_id.

        Returns updated pipeline dict.
        """
        return pipeline

    def on_job_success(self, pipeline: dict, batch: Optional[dict]) -> dict:
        """Hook called after a job completes successfully.

        Strategy can do post-processing (e.g., check results,
        submit dependent work). Default behavior is handled by
        the state machine (update progress, advance to next batch).

        Returns updated pipeline dict.
        """
        # Return None to use default state machine behavior
        return None

    def on_job_failure(self, pipeline: dict, batch: Optional[dict],
                       state: str, details: str) -> dict:
        """Hook called after a job fails.

        Strategy can decide whether to retry or fail the pipeline.
        Return None to use default behavior (fail the pipeline).
        """
        return None

    def on_initial_load_complete(self, pipeline: dict) -> dict:
        """Called when all initial load batches are complete.

        Strategy should transition to the appropriate state
        (in_sync, completed, etc.).

        Returns updated pipeline dict.
        """
        return None  # State machine handles default

    # ── Optional Hooks (implement for ongoing sync strategies) ──

    def detect_changes(self, pipeline: dict) -> bool:
        """Check if the source has changes that need syncing.

        Called when sync_status == 'in_sync'.
        Return True if changes detected (will trigger out_of_sync).

        Raise NotImplementedError for one-shot strategies (rewrite).
        """
        raise NotImplementedError("Strategy does not support ongoing sync")

    def execute_sync(self, pipeline: dict, batch_number: int) -> dict:
        """Execute an ongoing sync operation.

        Called when sync_status transitions from out_of_sync.
        Should create a batch record, submit work, update pipeline.

        Args:
            pipeline: Current pipeline dict
            batch_number: Suggested batch number for tracking

        Returns updated pipeline dict, or None if sync completed synchronously.
        """
        raise NotImplementedError("Strategy does not support ongoing sync")

    # ── Direct reconcile interface (strategies outside the state machine) ──

    def reconcile(self, pipeline: dict) -> dict:
        """Legacy reconcile method.

        Strategies that haven't been migrated to the state machine
        still use this directly. Once all strategies are migrated,
        this can be removed.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement reconcile() "
            "or be migrated to state machine hooks")
