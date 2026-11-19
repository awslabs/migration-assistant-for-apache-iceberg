# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-pipeline locks that keep a reconcile and a user action from interleaving.

The engine runs as a single task (the stack sets DesiredCount: 1), so an
in-process lock per pipeline id is enough. Running more than one engine task
would need a shared lease (for example a DynamoDB lock item) instead.

* The reconcile loop takes the lock without waiting. If a user action holds it,
  that pipeline is skipped for this sweep and picked up on the next one.
* User actions wait a short, bounded time (ENGINE_ACTION_LOCK_WAIT_SECONDS,
  default 5s, kept below the app's request timeouts). Some reconciles block for
  minutes (synchronous XTable invokes, inline Athena polls), so an action that
  cannot get the lock in time proceeds anyway rather than hanging. The status
  guards in db/status_guard.py still stop that reconcile from overwriting the
  action's status.
"""
import logging
import os
import threading
from contextlib import contextmanager

log = logging.getLogger(__name__)

ACTION_WAIT_SECONDS = float(os.getenv("ENGINE_ACTION_LOCK_WAIT_SECONDS", "5"))

_registry_lock = threading.Lock()
_locks: dict[str, threading.Lock] = {}


def _lock_for(pipeline_id: str) -> threading.Lock:
    with _registry_lock:
        lock = _locks.get(pipeline_id)
        if lock is None:
            lock = _locks[pipeline_id] = threading.Lock()
        return lock


@contextmanager
def try_pipeline_lock(pipeline_id: str):
    """Non-blocking lock for the reconcile loop. Yields True if acquired."""
    lock = _lock_for(pipeline_id)
    acquired = lock.acquire(blocking=False)
    try:
        yield acquired
    finally:
        if acquired:
            lock.release()


@contextmanager
def action_lock(pipeline_id: str, wait_seconds: float = None):
    """Bounded-wait lock for user actions. Always yields; True if acquired."""
    wait = ACTION_WAIT_SECONDS if wait_seconds is None else wait_seconds
    lock = _lock_for(pipeline_id)
    acquired = lock.acquire(timeout=max(wait, 0))
    if not acquired:
        log.warning("Pipeline %s: reconcile still running after %.0fs; proceeding "
                    "with the user action (status guards apply)",
                    pipeline_id[:8], wait)
    try:
        yield acquired
    finally:
        if acquired:
            lock.release()


def with_pipeline_lock(func):
    """Decorator for API routes whose first argument is `pipeline_id`.

    Preserves the signature (via functools.wraps) so FastAPI still sees the
    route's real parameters.
    """
    import functools

    @functools.wraps(func)
    def wrapper(pipeline_id: str, *args, **kwargs):
        with action_lock(pipeline_id):
            return func(pipeline_id, *args, **kwargs)

    return wrapper
