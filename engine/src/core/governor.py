# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Concurrency governor for pipeline execution (Phase 3.1 / T16).

Caps how many pipelines are actively running at once so a burst of submissions
can't exceed EMR Serverless / Athena workgroup concurrency quotas or run up
uncontrolled compute cost. Pipelines that can't get a slot are parked in the
`queued` status and promoted to running by the reconcile loop as slots free up.

Notes:
  * Morph initial-loads are ALSO bounded separately by MorphStrategy's in-process
    semaphore (Phase 0.1) because they consume engine-host threads; this governor
    is the remote-quota/cost cap for EMR/Athena work.
  * The ceiling is a soft, count-based gate (running pipelines vs. limit), not a
    hard reservation — good enough to smooth bursts without distributed locking,
    given the reconcile loop is single-instance.
"""

import logging
import os

log = logging.getLogger(__name__)

# Max pipelines allowed in the 'running' state at once. Keep below the account's
# EMR Serverless concurrent-job and Athena workgroup query limits. Default 40.
DEFAULT_MAX_RUNNING = 200


def max_running() -> int:
    """The concurrency cap, read on every call.

    The app pushes ENGINE_MAX_RUNNING_PIPELINES from its Settings page into
    the engine's environment at runtime, so the value must not be cached at
    import. Invalid or non-positive values fall back to the default.
    """
    raw = os.getenv("ENGINE_MAX_RUNNING_PIPELINES", "")
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_MAX_RUNNING
    return value if value > 0 else DEFAULT_MAX_RUNNING

# Statuses that count as "occupying a slot" for governor purposes.
_ACTIVE_STATUSES = ("running",)

# Status used to park a pipeline that is ready to start but over the ceiling.
QUEUED_STATUS = "queued"


def running_count(repo) -> int:
    """Number of pipelines currently occupying a concurrency slot."""
    total = 0
    for status in _ACTIVE_STATUSES:
        total += len(repo.list_all(status=status))
    return total


def has_capacity(repo) -> bool:
    """True if a new pipeline may start without exceeding the ceiling."""
    return running_count(repo) < max_running()


def available_slots(repo) -> int:
    """How many more pipelines may start right now (never negative)."""
    return max(0, max_running() - running_count(repo))
