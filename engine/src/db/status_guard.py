# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Guards that keep concurrent pipeline status writes from clobbering each other.

Two writers change a pipeline's status at the same time: the background
reconcile loop (many threads, each working from a snapshot it read earlier) and
the API routes that handle user actions (complete, pause, cancel, ...). Without
guards, a reconcile that read the old state can overwrite a user's action.

Both repository backends apply these rules in `update_status`:

* Terminal statuses are sticky. Once a pipeline is `completed` or `cancelled`,
  a status write is refused unless the caller passes ``force=True``.
* Compare-and-set. If an expected status is given (explicitly, or through the
  reconcile context below), the write only happens when the stored status still
  matches it.

A refused write is not an error: `update_status` returns the pipeline as it is
now, so callers see the real state instead of the one they tried to write.
"""
import threading
from contextlib import contextmanager

TERMINAL_STATUSES = frozenset({"completed", "cancelled"})

# pipeline_id -> expected status, per thread. Set by the reconcile loop around a
# single pipeline's reconcile so every status write it makes is compare-and-set
# against the status the reconcile started from.
_local = threading.local()


def _expectations() -> dict:
    exp = getattr(_local, "expected", None)
    if exp is None:
        exp = _local.expected = {}
    return exp


@contextmanager
def expect_status(pipeline_id: str, status: str):
    """Make status writes for `pipeline_id` on this thread compare-and-set.

    Writes inside the block only succeed while the stored status equals the
    tracked expectation. After each successful write the expectation moves to
    the new status, so a reconcile can still make several transitions in a row.
    """
    exp = _expectations()
    previous = exp.get(pipeline_id)
    exp[pipeline_id] = status
    try:
        yield
    finally:
        if previous is None:
            exp.pop(pipeline_id, None)
        else:
            exp[pipeline_id] = previous


def expected_status(pipeline_id: str):
    """The status this thread expects for `pipeline_id`, or None."""
    return _expectations().get(pipeline_id)


def record_write(pipeline_id: str, status: str) -> None:
    """Advance this thread's expectation after a successful status write."""
    exp = _expectations()
    if pipeline_id in exp:
        exp[pipeline_id] = status
