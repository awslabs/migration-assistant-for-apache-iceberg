# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Core domain logic — engine, models, state machine, spec parsing."""
from .models import MigrationType, MigrationStatus, TargetType
from .spec import MigrationSpec
from .state_machine import PipelineStateMachine

__all__ = [
    "MigrationType",
    "MigrationStatus",
    "TargetType",
    "MigrationEngine",
    "MigrationSpec",
    "PipelineStateMachine",
    "reconcile_loop",
]

# ``engine`` imports the strategies package, which in turn imports from this
# package. Resolve these two names lazily to avoid a circular import.
_LAZY = {
    "MigrationEngine": ".engine",
    "reconcile_loop": ".reconciler",
}


def __getattr__(name):
    if name in _LAZY:
        import importlib
        module = importlib.import_module(_LAZY[name], __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
