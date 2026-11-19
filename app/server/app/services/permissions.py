# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Permissions service — resolves user access to plans based on group membership.

Access rules:
- Admins can do anything regardless of group membership.
- A user with "manage" role on a group that owns a plan can perform all actions on that plan.
- A user with "view" role on a group that owns a plan can only read it.
- A user with no membership in any owning group cannot see the plan at all.
- Plans with NO owners are visible to all authenticated users (legacy/unowned plans).
"""

from app.database import get_conn, _fetchall, _fetchone


def get_user_role(username: str) -> str:
    """Get the system-level role for a user (admin, editor, viewer)."""
    conn = get_conn()
    row = _fetchone(conn, "SELECT role FROM users WHERE username=?", (username,))
    conn.close()
    return row["role"] if row else "viewer"


def get_user_group_memberships(username: str) -> dict[str, str]:
    """Return {group_name: role} for all groups the user belongs to.

    role is 'manage' or 'view'.
    """
    conn = get_conn()
    rows = _fetchall(conn,
        "SELECT group_name, role FROM group_members WHERE username=?",
        (username,))
    conn.close()
    return {r["group_name"]: r["role"] for r in rows}


def get_plan_access(username: str, plan_owners: list[str]) -> str | None:
    """Determine user's access level to a specific plan.

    Returns:
        "manage" — user can perform all actions
        "view" — user can only read
        None — user cannot see this plan at all
    """
    # Admin bypasses all checks
    user_role = get_user_role(username)
    if user_role == "admin":
        return "manage"

    # Plans with no owners are visible to everyone (read-only for non-admins)
    if not plan_owners:
        return "view"

    memberships = get_user_group_memberships(username)

    # Find the highest access level across all owning groups
    best_access = None
    for group in plan_owners:
        if group in memberships:
            role = memberships[group]
            if role == "manage":
                return "manage"  # Can't get better than this
            if role == "view":
                best_access = "view"

    return best_access


def get_manageable_groups(username: str) -> list[str]:
    """Return group names where user has 'manage' role.

    Admins get all groups.
    """
    user_role = get_user_role(username)
    conn = get_conn()
    if user_role == "admin":
        rows = _fetchall(conn, "SELECT name FROM groups ORDER BY name")
        conn.close()
        return [r["name"] for r in rows]
    rows = _fetchall(conn,
        "SELECT group_name FROM group_members WHERE username=? AND role='manage'",
        (username,))
    conn.close()
    return [r["group_name"] for r in rows]


def filter_plans_for_user(username: str, plans: list) -> list:
    """Filter a list of plans to only those the user can see.

    Each plan must have an `owners` attribute (list of group names).
    Admins see everything.
    """
    user_role = get_user_role(username)
    if user_role == "admin":
        return plans

    memberships = get_user_group_memberships(username)

    visible = []
    for plan in plans:
        owners = plan.owners if hasattr(plan, "owners") else []
        # Plans with no owners are visible to everyone
        if not owners:
            visible.append(plan)
            continue
        # User needs membership in at least one owning group
        if any(g in memberships for g in owners):
            visible.append(plan)

    return visible


def check_manage_access(username: str, plan_owners: list[str]) -> bool:
    """Check if user has manage access to a plan. Raises nothing, just returns bool."""
    return get_plan_access(username, plan_owners) == "manage"
