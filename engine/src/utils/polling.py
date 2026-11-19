# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Polling and delay utilities.

Provides named helpers for async polling and propagation waits,
replacing bare time.sleep() calls throughout the codebase.
"""

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def poll_until_complete(
    check: Callable[[], T],
    is_done: Callable[[T], bool],
    interval: float = 1.0,
    max_attempts: int = 30,
) -> T:
    """Poll check() every `interval` seconds until is_done(result) is True.

    Returns the last result from check(), regardless of whether is_done
    was satisfied. Callers should inspect the result to determine outcome.
    """
    for _ in range(max_attempts):
        time.sleep(interval)  # nosemgrep: arbitrary-sleep
        result = check()
        if is_done(result):
            return result
    return result


def wait_for_propagation(seconds: float, reason: str = "") -> None:
    """Delay for eventual-consistency propagation.

    Use this instead of bare time.sleep() when waiting for AWS resource
    propagation (Lambda activation, S3 notification, IAM policies, etc.).
    The `reason` parameter documents intent for debugging.
    """
    time.sleep(seconds)  # nosemgrep: arbitrary-sleep

