# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Notification service — emits pipeline status events to Amazon EventBridge.

When a table's pipeline status changes (detected during the sync poll in
pipeline_sync.py), an event is published to EventBridge with the list of
recipients who should be notified. Recipients are resolved from the plan's
owning groups, filtered by each user's notification preferences.

Recipient resolution chain:
    plan.owners (group names)
      -> group_members (users in those groups)
        -> users.email + notification_preferences[username]

A downstream consumer (e.g. an EventBridge rule -> Lambda -> SES) is
responsible for actually delivering the notification. This module only
publishes the domain event; it does not send email itself.

All emission is best-effort and fully exception-isolated — a notification
failure must never disrupt the pipeline sync loop.

Configuration (via settings / APP_NOTIFICATIONS_* env vars):
    APP_NOTIFICATIONS_ENABLED         "true"/"false" (default false)
    APP_NOTIFICATIONS_EVENTBRIDGE_BUS event bus name (default "default")
    APP_NOTIFICATIONS_EVENTS          comma-separated event types to emit
    APP_NOTIFICATIONS_REGION          AWS region (default us-east-1)
"""

import json
import logging
from datetime import datetime, timezone

from app.database import get_conn, _fetchall, _fetchone

log = logging.getLogger(__name__)

# Maps a table's new status to a notification event type. The event type
# also matches the corresponding column in notification_preferences.
# Only these transitions produce notifications.
_STATUS_EVENT_MAP = {
    "completed": "pipeline_completed",
    "failed": "pipeline_failed",
    "out_of_sync": "sync_out_of_sync",
}

# Plan-level event (emitted separately when the whole plan completes)
_PLAN_COMPLETED_EVENT = "plan_completed"

_EVENT_SOURCE = "IMA.migrations"
_DETAIL_TYPE = "Pipeline Status Change"
_PLAN_DETAIL_TYPE = "Plan Status Change"


def _get_setting(key: str, default: str = "") -> str:
    from app.services.settings_service import get_setting
    val = get_setting(key)
    return val if val else default


def _notifications_enabled() -> bool:
    return _get_setting("APP_NOTIFICATIONS_ENABLED", "false").lower() == "true"


def _enabled_events() -> set[str]:
    raw = _get_setting(
        "APP_NOTIFICATIONS_EVENTS",
        "pipeline_completed,pipeline_failed,sync_out_of_sync,plan_completed")
    return {e.strip() for e in raw.split(",") if e.strip()}


def _eventbridge_client():
    import boto3
    region = _get_setting("APP_NOTIFICATIONS_REGION", "us-east-1")
    return boto3.client("events", region_name=region)


def event_type_for_status(new_status: str) -> str | None:
    """Return the notification event type for a table status, or None."""
    return _STATUS_EVENT_MAP.get(new_status)


def get_plan_notification_recipients(owners: list[str],
                                     event_type: str) -> list[dict]:
    """Resolve which users should be notified for an event on a plan.

    Args:
        owners: list of group names that own the plan
        event_type: notification event type (matches notification_preferences column)

    Returns:
        list of {username, email, name} for users who:
          - belong to at least one owning group
          - are NOT system admins (admins are excluded by design)
          - have opted in to this event type in their preferences
          - have a non-empty email address
    """
    if not owners:
        return []

    conn = get_conn()
    try:
        # Collect unique usernames across all owning groups
        usernames: set[str] = set()
        for group in owners:
            rows = _fetchall(conn,
                "SELECT username FROM group_members WHERE group_name=?",
                (group,))
            for r in rows:
                usernames.add(r["username"])

        if not usernames:
            return []

        recipients = []
        for username in usernames:
            user = _fetchone(conn,
                "SELECT username, name, email, role FROM users WHERE username=?",
                (username,))
            if not user:
                continue
            # Exclude system admins by role
            if user.get("role") == "admin":
                continue
            email = (user.get("email") or "").strip()
            if not email:
                continue
            # Check notification preference for this event type
            prefs = _fetchone(conn,
                "SELECT * FROM notification_preferences WHERE username=?",
                (username,))
            if prefs is not None:
                # Explicit preference row — respect the stored value
                if not bool(prefs.get(event_type, 0)):
                    continue
            # No preference row — defaults are all enabled, so notify
            recipients.append({
                "username": username,
                "email": email,
                "name": user.get("name", ""),
            })
        return recipients
    finally:
        conn.close()


def _publish(detail_type: str, detail: dict) -> None:
    """Publish a single event to EventBridge. Raises on failure (caller isolates)."""
    bus = _get_setting("APP_NOTIFICATIONS_EVENTBRIDGE_BUS", "default")
    client = _eventbridge_client()
    resp = client.put_events(Entries=[{
        "Source": _EVENT_SOURCE,
        "DetailType": detail_type,
        "Detail": json.dumps(detail),
        "EventBusName": bus,
    }])
    failed = resp.get("FailedEntryCount", 0)
    if failed:
        raise RuntimeError(f"EventBridge rejected {failed} ent/entries: {resp.get('Entries')}")


def emit_pipeline_event(plan: dict, table: dict, old_status: str,
                        new_status: str, error_message: str = "") -> None:
    """Emit a pipeline status-change event. Best-effort, never raises.

    Args:
        plan: dict with at least id, name, owners (list of group names)
        table: dict with source_database, source_table, target_database,
               target_table, migration_type
        old_status, new_status: the transition
        error_message: optional, included for failure events
    """
    try:
        if not _notifications_enabled():
            return
        event_type = event_type_for_status(new_status)
        if not event_type or event_type not in _enabled_events():
            return

        owners = plan.get("owners", [])
        recipients = get_plan_notification_recipients(owners, event_type)
        if not recipients:
            # No one to notify — skip publishing to avoid noise
            log.debug("No recipients for %s on plan %s; skipping event",
                      event_type, plan.get("id", "")[:8])
            return

        detail = {
            "event_type": event_type,
            "plan_id": plan.get("id", ""),
            "plan_name": plan.get("name", ""),
            "source_database": table.get("source_database", ""),
            "source_table": table.get("source_table", ""),
            "target_database": table.get("target_database", ""),
            "target_table": table.get("target_table", ""),
            "migration_type": table.get("migration_type", ""),
            "old_status": old_status,
            "new_status": new_status,
            "error_message": error_message or "",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "recipients": recipients,
        }
        _publish(_DETAIL_TYPE, detail)
        log.info("Emitted %s event for %s.%s (%d recipients)",
                 event_type, table.get("source_database", ""),
                 table.get("source_table", ""), len(recipients))
    except Exception as e:
        log.warning("Failed to emit pipeline notification event: %s", e)


def emit_plan_completed_event(plan: dict) -> None:
    """Emit a plan-completed event. Best-effort, never raises."""
    try:
        if not _notifications_enabled():
            return
        if _PLAN_COMPLETED_EVENT not in _enabled_events():
            return

        owners = plan.get("owners", [])
        recipients = get_plan_notification_recipients(owners, _PLAN_COMPLETED_EVENT)
        if not recipients:
            return

        detail = {
            "event_type": _PLAN_COMPLETED_EVENT,
            "plan_id": plan.get("id", ""),
            "plan_name": plan.get("name", ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "recipients": recipients,
        }
        _publish(_PLAN_DETAIL_TYPE, detail)
        log.info("Emitted %s event for plan %s (%d recipients)",
                 _PLAN_COMPLETED_EVENT, plan.get("id", "")[:8], len(recipients))
    except Exception as e:
        log.warning("Failed to emit plan-completed notification event: %s", e)
