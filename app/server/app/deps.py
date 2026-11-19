# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared FastAPI dependencies for authentication and authorization.

Usage in routes:
    from app.deps import require_admin, get_current_user

    @router.post("/users", dependencies=[Depends(require_admin)])
    def create_user(...):
        ...

    @router.get("/profile")
    def get_profile(username: str = Depends(get_current_user)):
        ...
"""

import json

from fastapi import Cookie, HTTPException, Depends
from typing import Optional

from app.services.permissions import (
    get_user_role,
    get_plan_access,
)
from app.services import session_service
from app.database import get_conn, _fetchone, _fetchall

# Name of the HttpOnly cookie that carries the opaque session token.
SESSION_COOKIE_NAME = "ima_session"


def get_current_user(ima_session: Optional[str] = Cookie(None)) -> str:
    """Resolve the authenticated username from the session cookie.

    The client presents an opaque session token in the `ima_session` HttpOnly
    cookie. We validate it server-side (existence, absolute expiry, idle
    timeout) and return the associated username. Any missing, unknown, or
    expired token results in 401 — there is no header-based fallback.
    """
    username = session_service.get_session_user(ima_session)
    if not username:
        raise HTTPException(status_code=401, detail="Authentication required")
    return username


def _mfa_required_for_role(role: str) -> bool:
    """Whether MFA is mandatory for a given system role.

    Controlled by APP_REQUIRE_MFA (default off, so existing deployments are
    not locked out). When on, MFA is mandatory for privileged roles — admin and
    editor (editors are the plan-manage tier). Viewers are not
    forced. Off by default = opt-in, safe upgrade.
    """
    import os
    if os.getenv("APP_REQUIRE_MFA", "false").lower() != "true":
        return False
    return role in ("admin", "editor")


def require_usable_session(username: str = Depends(get_current_user)) -> str:
    """Authenticated AND not blocked by a mandatory onboarding step.

    A session is "usable" only when the account has cleared any forced
    onboarding gate:
      * must_change_password — the seeded/default admin or an admin-reset
        credential must set a new password first.
      * MFA enrollment — when APP_REQUIRE_MFA=true, privileged users
        (admin/editor) must enroll a second factor before using the app.

    Such an account is authenticated but may only reach the auth router
    (change-password, MFA enroll/confirm, me, logout). Returns 403 with a
    machine-readable marker the SPA can act on.
    """
    from app.services import user_service, mfa_service
    if user_service.must_change_password(username):
        raise HTTPException(status_code=403, detail="PASSWORD_CHANGE_REQUIRED")
    if _mfa_required_for_role(get_user_role(username)) and not mfa_service.is_mfa_enabled(username):
        raise HTTPException(status_code=403, detail="MFA_ENROLLMENT_REQUIRED")
    return username


def require_admin(username: str = Depends(require_usable_session)) -> str:
    """Dependency that ensures the current user has admin role.

    Also requires a usable session (not in forced password-change state),
    so a seeded/default admin cannot perform admin actions until they set a new
    password. Raises 403 if the user is not an admin.
    """
    role = get_user_role(username)
    if role != "admin":
        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )
    return username


def require_editor_or_admin(username: str = Depends(get_current_user)) -> str:
    """Dependency that ensures the current user has at least editor role.

    Raises 403 if the user is a viewer.
    """
    role = get_user_role(username)
    if role not in ("admin", "editor"):
        raise HTTPException(
            status_code=403,
            detail="Editor or admin access required"
        )
    return username


def require_self_or_admin(
    username: str,
    current_user: str = Depends(get_current_user),
) -> str:
    """Check that the current user is either the target user or an admin.

    Use this for endpoints where users can modify their own data,
    but admins can modify anyone's data.
    """
    if current_user == username:
        return current_user
    role = get_user_role(current_user)
    if role != "admin":
        raise HTTPException(
            status_code=403,
            detail="You can only modify your own data, or be an admin"
        )
    return current_user


# ═══════════════════════════════════════════════════════════════════════════
# PLAN-OWNERSHIP AUTHORIZATION
# ═══════════════════════════════════════════════════════════════════════════


def _get_plan_owners(plan_id: str) -> Optional[list[str]]:
    """Fetch a plan's owner group list by ID (DB-only, no service import to
    avoid circular dependencies). Returns None if the plan does not exist."""
    conn = get_conn()
    row = _fetchone(conn, "SELECT owners FROM migration_plans WHERE id=?", (plan_id,))
    conn.close()
    if row is None:
        return None
    try:
        return json.loads(row["owners"] or "[]")
    except (ValueError, TypeError):
        return []


def require_plan_view(plan_id: str, username: str = Depends(get_current_user)) -> str:
    """Dependency: caller must have at least VIEW access to the plan.

    404 if the plan does not exist (avoids leaking existence to non-owners).
    403 if the caller has no access. Returns the username on success.
    """
    owners = _get_plan_owners(plan_id)
    if owners is None:
        raise HTTPException(status_code=404, detail="Plan not found")
    access = get_plan_access(username, owners)
    if access is None:
        raise HTTPException(status_code=403, detail="You do not have access to this plan")
    return username


def require_plan_manage(plan_id: str, username: str = Depends(get_current_user)) -> str:
    """Dependency: caller must have MANAGE access to the plan.

    404 if the plan does not exist. 403 if the caller cannot manage it.
    Returns the username on success.
    """
    owners = _get_plan_owners(plan_id)
    if owners is None:
        raise HTTPException(status_code=404, detail="Plan not found")
    access = get_plan_access(username, owners)
    if access != "manage":
        raise HTTPException(
            status_code=403,
            detail="You need manage access to this plan"
        )
    return username


def _plan_ids_for_pipeline(pipeline_id: str) -> list[str]:
    """Return the plan id(s) that own a given pipeline via plan_tables.

    A pipeline is created per plan-table, so the pipeline_id maps back to the
    owning plan through the plan_tables.pipeline_id column. Returns all matching
    plan ids (normally one).
    """
    conn = get_conn()
    rows = _fetchall(
        conn,
        "SELECT DISTINCT plan_id FROM plan_tables WHERE pipeline_id=?",
        (pipeline_id,),
    )
    conn.close()
    return [r["plan_id"] for r in rows]


def require_pipeline_view(pipeline_id: str, username: str = Depends(get_current_user)) -> str:
    """Dependency: caller must have at least VIEW access to the plan that owns
    the given pipeline (pipeline routes are keyed by pipeline id, not plan
    id, so we resolve ownership through plan_tables).

    404 if the pipeline maps to no known plan (avoids leaking existence).
    403 if the caller cannot see any owning plan. Returns the username.
    """
    plan_ids = _plan_ids_for_pipeline(pipeline_id)
    if not plan_ids:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    for pid in plan_ids:
        owners = _get_plan_owners(pid)
        if owners is None:
            continue
        if get_plan_access(username, owners) is not None:
            return username
    raise HTTPException(status_code=403, detail="You do not have access to this pipeline")


def require_pipeline_manage(pipeline_id: str, username: str = Depends(get_current_user)) -> str:
    """Dependency: caller must have MANAGE access to the plan that owns the
    given pipeline (mutating pipeline routes). Mirrors require_pipeline_view but
    requires 'manage' rather than any access.

    404 if the pipeline maps to no known plan (avoids leaking existence).
    403 if the caller cannot manage any owning plan. Returns the username.
    """
    plan_ids = _plan_ids_for_pipeline(pipeline_id)
    if not plan_ids:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    for pid in plan_ids:
        owners = _get_plan_owners(pid)
        if owners is None:
            continue
        if get_plan_access(username, owners) == "manage":
            return username
    raise HTTPException(status_code=403, detail="You need manage access to this pipeline")
