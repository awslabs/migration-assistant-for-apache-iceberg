# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Engine — orchestrates strategy execution."""

import logging

from .models import MigrationType
from .state_machine import PipelineStateMachine
from ..strategies import (
    MigrationStrategy,
    RewriteStrategy,
    IncrementalStrategy,
    MorphStrategy,
    AddFilesStrategy,
    S3TablesManagedStrategy,
)
from ..strategies.iceberg_incremental import IcebergIncrementalStrategy

log = logging.getLogger(__name__)

STRATEGY_MAP: dict[MigrationType, type[MigrationStrategy]] = {
    MigrationType.REWRITE: RewriteStrategy,
    MigrationType.INCREMENTAL: IncrementalStrategy,
    MigrationType.MORPH: MorphStrategy,
    MigrationType.ADD_FILES: AddFilesStrategy,
    # Managed Migration is deliberately NOT in _STATE_MACHINE_STRATEGIES below:
    # it polls the managed API's own state machine via its reconcile().
    MigrationType.S3TABLES_MANAGED: S3TablesManagedStrategy,
}

# Strategies reconciled by the shared state machine. Others implement
# reconcile() themselves.
_STATE_MACHINE_STRATEGIES = {
    RewriteStrategy, MorphStrategy, IncrementalStrategy,
    IcebergIncrementalStrategy, AddFilesStrategy,
}


class MigrationEngine:
    """Dispatches pipeline operations to the appropriate strategy."""

    def __init__(self):
        self._strategies: dict[MigrationType, MigrationStrategy] = {
            k: v() for k, v in STRATEGY_MAP.items()
        }
        self._iceberg_incremental = IcebergIncrementalStrategy()
        self._state_machine = PipelineStateMachine()

    def get_strategy(self, migration_type: str,
                     source_format: str = "") -> MigrationStrategy:
        """Get the appropriate strategy based on type and source format.
        
        For incremental migrations from Iceberg sources, uses the
        snapshot-based IcebergIncrementalStrategy instead of the
        event-tracking-based IncrementalStrategy.
        """
        mt = MigrationType(migration_type)
        if mt == MigrationType.INCREMENTAL and source_format == "iceberg":
            return self._iceberg_incremental
        strategy = self._strategies.get(mt)
        if not strategy:
            raise ValueError(f"Unknown migration type: {migration_type}")
        return strategy

    def validate(self, pipeline: dict) -> tuple[bool, str]:
        strategy = self.get_strategy(pipeline["migration_type"],
                                     pipeline.get("source_format", ""))
        return strategy.validate(pipeline)

    def submit(self, pipeline: dict) -> dict:
        """Submit the pipeline's initial work."""
        strategy = self.get_strategy(pipeline["migration_type"],
                                     pipeline.get("source_format", ""))
        return strategy.submit(pipeline)

    def reconcile(self, pipeline: dict) -> dict:
        """Reconcile external state for a running pipeline."""
        strategy = self.get_strategy(pipeline["migration_type"],
                                     pipeline.get("source_format", ""))

        # State-machine strategies share one reconciler; others reconcile themselves
        if type(strategy) in _STATE_MACHINE_STRATEGIES:
            return self._state_machine.reconcile(pipeline, strategy)
        return strategy.reconcile(pipeline)

    def cancel(self, pipeline: dict) -> dict:
        """Cancel a running pipeline."""
        strategy = self.get_strategy(pipeline["migration_type"],
                                     pipeline.get("source_format", ""))
        return strategy.cancel(pipeline)

    def approve(self, pipeline: dict, username: str = "") -> dict:
        """Approve a plan-review pause (Managed Migration only).

        Dispatches to the strategy's approve(); raises NotImplementedError for
        strategies that don't support a review pause.
        """
        strategy = self.get_strategy(pipeline["migration_type"],
                                     pipeline.get("source_format", ""))
        approve = getattr(strategy, "approve", None)
        if not callable(approve):
            raise NotImplementedError(
                f"{pipeline['migration_type']} does not support approve")
        return approve(pipeline, username)

    def complete(self, pipeline: dict, username: str = "") -> dict:
        """Complete/cutover a migration (Managed Migration only).

        Dispatches to the strategy's complete(); raises NotImplementedError for
        strategies that don't implement an API-driven cutover.
        """
        strategy = self.get_strategy(pipeline["migration_type"],
                                     pipeline.get("source_format", ""))
        complete = getattr(strategy, "complete", None)
        if not callable(complete):
            raise NotImplementedError(
                f"{pipeline['migration_type']} does not support complete")
        return complete(pipeline, username)
