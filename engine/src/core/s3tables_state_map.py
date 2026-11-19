# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pure projection of the S3 Tables Migration API state onto engine state.

The managed API runs its own state machine; the `s3tables_managed` strategy
polls `get_table_migration` and projects the returned `state` onto the engine's
existing `status` + `sync_status` (modeled like the incremental strategy so it
flows through the app's `_map_status`). See the integration plan §4.

No AWS, no DB — a single pure function, unit-testable in isolation.

Note: the terminal cutover (`complete_table_migration` → engine `completed`)
is driven by the strategy's finalize path, NOT by an API `state`, so it is not
represented here. The `succeeded` API state maps to `in_sync` (migrated, cutover
pending); cutover then flips status to `completed`.
"""

# API state → (engine status, engine sync_status).
# sync_status of None means "leave as-is / not applicable" (terminal statuses).
_PROJECTION: dict[str, tuple[str, str | None]] = {
    "new":           ("running", "initial_load"),
    "planning":      ("running", "initial_load"),
    "pendingReview": ("running", "awaiting_review"),
    "queued":        ("running", "initial_load"),
    "migrating":     ("running", "syncing"),
    "succeeded":     ("running", "in_sync"),
    "failed":        ("running", "failed"),
    "cancelling":    ("cancelled", None),
    "cancelled":     ("cancelled", None),
}

# Terminal-from-the-API-side states (no further transitions expected).
TERMINAL_STATES = frozenset({"succeeded", "failed", "cancelled"})


def project_state(api_state: str) -> tuple[str, str | None]:
    """Map an API `state` to (engine_status, engine_sync_status).

    Unknown states default to ("running", None) — stay running and keep polling
    rather than prematurely terminating on a state we don't recognize.
    """
    return _PROJECTION.get(api_state or "", ("running", None))


def is_migrated(api_state: str) -> bool:
    """True when data movement has succeeded (target exists, cutover pending)."""
    return api_state == "succeeded"


def is_failed(api_state: str) -> bool:
    return api_state == "failed"


def is_cancelled(api_state: str) -> bool:
    return api_state in ("cancelling", "cancelled")
