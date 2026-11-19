# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Auth and user management endpoints.

Replace with real auth (Cognito, OAuth, etc.) when ready.
"""

import os

from fastapi import APIRouter, HTTPException, Depends, Request, Response
from pydantic import BaseModel
from typing import Optional

from app.models import CreateUserRequest, UserInfo
from app.services import user_service, session_service, mfa_service
from app.deps import get_current_user, require_admin, require_self_or_admin, SESSION_COOKIE_NAME
from app.rate_limit import (
    limiter, RATE_LIMIT_LOGIN,
    is_account_locked, get_lockout_remaining,
    record_failed_login, record_successful_login,
)

router = APIRouter(prefix="/auth", tags=["auth"])

# Cookie hardening. Secure defaults to on; set SESSION_COOKIE_SECURE=false only
# for local HTTP development. SameSite=strict blocks the session cookie from
# being sent on cross-site requests, which is our primary CSRF defense.
_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "true").lower() != "false"
_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "strict")


def _set_session_cookie(response: Response, raw_token: str) -> None:
    """Attach the opaque session token as a hardened HttpOnly cookie."""
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=raw_token,
        max_age=session_service.SESSION_ABSOLUTE_TTL,
        httponly=True,
        secure=_COOKIE_SECURE,
        samesite=_COOKIE_SAMESITE,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    """Remove the session cookie from the client."""
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        httponly=True,
        secure=_COOKIE_SECURE,
        samesite=_COOKIE_SAMESITE,
    )


class LoginRequest(BaseModel):
    username: str
    password: str
    mfa_code: Optional[str] = None


class LoginResponse(BaseModel):
    username: str
    name: str
    role: str
    email: str = ""
    theme_preference: str = "system"
    # True when the user must change their password before doing anything else
    # (default/seeded account or admin-reset credential).
    must_change_password: bool = False


class ChangePasswordRequest(BaseModel):
    username: str
    current_password: str
    new_password: str


class UpdateEmailRequest(BaseModel):
    email: str


class UpdateThemeRequest(BaseModel):
    theme: str  # "light" | "dark" | "system"


class NotificationPrefsRequest(BaseModel):
    pipeline_completed: bool = True
    pipeline_failed: bool = True
    sync_out_of_sync: bool = True
    plan_completed: bool = True


@router.post("/login", response_model=LoginResponse)
@limiter.limit(RATE_LIMIT_LOGIN)
def login(login_data: LoginRequest, request: Request, response: Response):
    """Authenticate a user with username/password and start a session.

    Rate limited to prevent brute-force attacks. Accounts are temporarily
    locked after repeated failed attempts. On success, an opaque server-side
    session is created and delivered as an HttpOnly cookie.
    """
    # Check account lockout
    if is_account_locked(login_data.username):
        remaining = get_lockout_remaining(login_data.username)
        raise HTTPException(
            status_code=423,
            detail=f"Account temporarily locked due to too many failed attempts. "
                   f"Try again in {remaining} seconds.",
        )

    result = user_service.authenticate(login_data.username, login_data.password)
    if not result:
        locked = record_failed_login(login_data.username)
        if locked:
            raise HTTPException(
                status_code=423,
                detail="Account temporarily locked due to too many failed attempts. "
                       "Try again in 15 minutes.",
            )
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Second factor: if the account has MFA enabled, a valid TOTP code is
    # mandatory. A missing code returns 401 with a machine-readable marker so the
    # SPA can prompt for the code; a wrong code counts as a failed attempt.
    if result.get("mfa_enabled"):
        if not login_data.mfa_code:
            raise HTTPException(status_code=401, detail="MFA_REQUIRED")
        if not mfa_service.verify_code(login_data.username, login_data.mfa_code):
            record_failed_login(login_data.username)
            raise HTTPException(status_code=401, detail="Invalid MFA code")

    record_successful_login(login_data.username)
    raw_token = session_service.create_session(result["username"])
    _set_session_cookie(response, raw_token)
    return LoginResponse(**result)


@router.post("/logout")
def logout(request: Request, response: Response):
    """End the current session and clear the session cookie.

    Idempotent: safe to call even without a valid session. Always clears the
    client cookie so the browser drops any stale token.
    """
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        session_service.delete_session(token)
    _clear_session_cookie(response)
    return {"detail": "Logged out"}


@router.get("/me", response_model=LoginResponse)
def get_me(current_user: str = Depends(get_current_user)):
    """Return the currently authenticated user (used by the SPA to restore
    session state on load). 401 if the session cookie is missing or invalid."""
    profile = user_service.get_user_profile(current_user)
    if not profile:
        raise HTTPException(status_code=401, detail="Authentication required")
    profile["must_change_password"] = user_service.must_change_password(current_user)
    return LoginResponse(**profile)


@router.post("/change-password")
def change_password(
    request: ChangePasswordRequest,
    response: Response,
    current_user: str = Depends(get_current_user),
):
    """Change the logged-in user's password.

    Users can only change their own password (verified via current_password).
    On success, ALL of the target user's existing sessions are revoked so that
    a compromised session cannot survive a password change. If the caller is
    changing their own password, a fresh session is issued for this request so
    the current client stays logged in while every other session is dropped.
    """
    if request.username != current_user:
        # Only allow admins to change other users' passwords via reset
        from app.services.permissions import get_user_role
        if get_user_role(current_user) != "admin":
            raise HTTPException(status_code=403, detail="You can only change your own password")
    success, msg = user_service.change_password(
        request.username, request.current_password, request.new_password
    )
    if not success:
        raise HTTPException(status_code=400, detail=msg)
    # Revoke every existing session for the account whose password changed.
    session_service.delete_user_sessions(request.username)
    # Keep the current caller logged in only when they changed their OWN password.
    if request.username == current_user:
        raw_token = session_service.create_session(current_user)
        _set_session_cookie(response, raw_token)
    return {"detail": "Password changed successfully"}


@router.post("/users/{username}/reset-password")
@limiter.limit("3/minute")
def reset_password(username: str, request: Request, _admin: str = Depends(require_admin)):
    """Reset a user's password (admin only). Returns the new generated password.

    Revokes all of the target user's sessions so the old credential cannot
    continue to be used after a reset.
    """
    new_pw = user_service.reset_password(username)
    if not new_pw:
        raise HTTPException(status_code=404, detail="User not found")
    session_service.delete_user_sessions(username)
    return {"detail": "Password reset successfully", "new_password": new_pw}


@router.put("/users/{username}/email")
def update_email(username: str, request: UpdateEmailRequest, current_user: str = Depends(get_current_user)):
    """Update user's email address. Users can update their own; admins can update anyone's."""
    require_self_or_admin(username, current_user)
    user_service.update_email(username, request.email)
    return {"detail": "Email updated"}


@router.put("/users/{username}/theme")
def update_theme(username: str, request: UpdateThemeRequest, current_user: str = Depends(get_current_user)):
    """Update user's theme preference. Users can update their own; admins can update anyone's."""
    require_self_or_admin(username, current_user)
    if not user_service.update_theme_preference(username, request.theme):
        raise HTTPException(status_code=400, detail="Invalid theme. Must be 'light', 'dark', or 'system'.")
    return {"detail": "Theme preference updated", "theme": request.theme}


@router.get("/users/{username}/notifications")
def get_notifications(username: str, current_user: str = Depends(get_current_user)):
    """Get notification preferences for a user. Users can view their own; admins can view anyone's."""
    require_self_or_admin(username, current_user)
    return user_service.get_notification_preferences(username)


@router.put("/users/{username}/notifications")
def update_notifications(username: str, request: NotificationPrefsRequest, current_user: str = Depends(get_current_user)):
    """Update notification preferences. Users can update their own; admins can update anyone's."""
    require_self_or_admin(username, current_user)
    return user_service.save_notification_preferences(username, request.model_dump())


class UpdateRoleRequest(BaseModel):
    role: str  # "admin" | "editor" | "viewer"


@router.put("/users/{username}/role")
def update_role(username: str, request: UpdateRoleRequest, _admin: str = Depends(require_admin)):
    """Update a user's system role (admin only).

    Revokes the target user's sessions so the privilege change forces a fresh
    login rather than lingering in an already-established session.
    """
    if request.role not in ("admin", "editor", "viewer"):
        raise HTTPException(status_code=400, detail="Role must be admin, editor, or viewer")
    if not user_service.update_role(username, request.role):
        raise HTTPException(status_code=404, detail="User not found")
    session_service.delete_user_sessions(username)
    return {"detail": "Role updated", "role": request.role}


@router.get("/users/{username}/groups")
def get_user_groups(username: str, current_user: str = Depends(get_current_user)):
    """Get all groups a user belongs to. Users can view their own; admins can view anyone's."""
    require_self_or_admin(username, current_user)
    return user_service.get_user_groups(username)


@router.get("/users/{username}/profile")
def get_user_profile(username: str, current_user: str = Depends(get_current_user)):
    """Get user profile data. Users can view their own; admins can view anyone's."""
    require_self_or_admin(username, current_user)
    from app.database import get_conn, _fetchone
    conn = get_conn()
    row = _fetchone(conn, "SELECT email, theme_preference FROM users WHERE username=?", (username,))
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return {"email": row.get("email", ""), "theme_preference": row.get("theme_preference", "system")}


@router.get("/users", response_model=list[UserInfo])
def list_users(_admin: str = Depends(require_admin)):
    """List all users (admin only)."""
    return user_service.list_users()


@router.post("/users", response_model=UserInfo, status_code=201)
def create_user(request: CreateUserRequest, _admin: str = Depends(require_admin)):
    """Create a new user (admin only)."""
    try:
        return user_service.create_user(request)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.delete("/users/{username}")
def delete_user(username: str, _admin: str = Depends(require_admin)):
    """Delete a user (admin only).

    `user_service.delete_user` also removes the user's session rows (required
    by the sessions->users foreign key), so no separate revocation is needed.
    """
    if not user_service.delete_user(username):
        raise HTTPException(status_code=404, detail="User not found")
    return {"detail": "User deleted"}


# ═══════════════════════════════════════════════════════════════════════════
# MFA (TOTP) ENROLLMENT & MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════


class MfaConfirmRequest(BaseModel):
    code: str


class MfaDisableRequest(BaseModel):
    # Re-authenticate with the current password before disabling MFA.
    password: str


@router.get("/mfa/status")
def mfa_status(current_user: str = Depends(get_current_user)):
    """Whether MFA is currently enabled for the calling user."""
    return {"mfa_enabled": mfa_service.is_mfa_enabled(current_user)}


@router.post("/mfa/enroll")
def mfa_enroll(current_user: str = Depends(get_current_user)):
    """Begin TOTP enrollment for the calling user.

    Returns the shared secret and an otpauth:// URI for the authenticator-app
    QR code. MFA is NOT active until confirmed via /mfa/confirm with a valid
    code. Re-enrolling regenerates the secret and (re)disables MFA until
    confirmation.
    """
    return mfa_service.begin_enrollment(current_user)


@router.post("/mfa/confirm")
def mfa_confirm(request: MfaConfirmRequest, current_user: str = Depends(get_current_user)):
    """Confirm TOTP enrollment by verifying a code; enables MFA on success."""
    if not mfa_service.confirm_enrollment(current_user, request.code):
        raise HTTPException(status_code=400, detail="Invalid or expired MFA code")
    return {"detail": "MFA enabled"}


@router.post("/mfa/disable")
def mfa_disable(request: MfaDisableRequest, current_user: str = Depends(get_current_user)):
    """Disable MFA for the calling user after re-verifying their password."""
    if not user_service.authenticate(current_user, request.password):
        raise HTTPException(status_code=401, detail="Password is incorrect")
    mfa_service.disable_mfa(current_user)
    return {"detail": "MFA disabled"}


@router.post("/users/{username}/mfa/reset")
def admin_reset_mfa(username: str, _admin: str = Depends(require_admin)):
    """Admin: clear a user's MFA (e.g. lost device). Revokes their sessions."""
    mfa_service.disable_mfa(username)
    session_service.delete_user_sessions(username)
    return {"detail": "MFA reset"}
