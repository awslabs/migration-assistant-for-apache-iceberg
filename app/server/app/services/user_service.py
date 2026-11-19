# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""User management service."""
import re
import secrets
import string
from typing import Optional

import bcrypt

from app.database import get_conn, _fetchall, _fetchone, _run_sql, _commit
from app.models import CreateUserRequest, UserInfo


# Password complexity: min 8 chars, at least 1 uppercase, 1 lowercase, 1 digit, 1 special
PASSWORD_PATTERN = re.compile(
    r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]).{8,}$'
)
CREDENTIAL_REQUIREMENTS = (
    "Must be at least 8 characters and include: "
    "1 uppercase letter, 1 lowercase letter, 1 digit, and 1 special character."
)


def _hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, hashed: str) -> bool:
    """Verify a password against a stored bcrypt hash.

    Anything that is not a bcrypt hash is rejected.
    """
    if not hashed.startswith(("$2a$", "$2b$", "$2y$")):
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except ValueError:
        return False


def validate_password(password: str) -> tuple[bool, str]:
    """Validate password meets complexity requirements."""
    if not PASSWORD_PATTERN.match(password):
        return False, CREDENTIAL_REQUIREMENTS
    return True, ""


def authenticate(username: str, password: str) -> Optional[dict]:
    conn = get_conn()
    row = _fetchone(
        conn,
        "SELECT username, name, password, role, email, theme_preference, "
        "must_change_password, mfa_enabled FROM users WHERE username=?",
        (username,),
    )
    conn.close()
    if not row:
        return None
    stored_hash = row["password"]
    if not _verify_password(password, stored_hash):
        return None
    return {
        "username": row["username"],
        "name": row["name"],
        "role": row["role"],
        "email": row.get("email", ""),
        "theme_preference": row.get("theme_preference", "system"),
        "must_change_password": bool(row.get("must_change_password", 0)),
        "mfa_enabled": bool(row.get("mfa_enabled", 0)),
    }


def list_users() -> list[UserInfo]:
    conn = get_conn()
    rows = _fetchall(conn, "SELECT username, name, role FROM users")
    conn.close()
    return [UserInfo(**r) for r in rows]


def get_user_profile(username: str) -> Optional[dict]:
    """Return the login-shaped profile for a user, or None if not found.

    Mirrors the dict returned by `authenticate` so it can populate the same
    LoginResponse (used by the /auth/me session-restore endpoint).
    """
    conn = get_conn()
    row = _fetchone(
        conn,
        "SELECT username, name, role, email, theme_preference FROM users WHERE username=?",
        (username,),
    )
    conn.close()
    if not row:
        return None
    return {
        "username": row["username"],
        "name": row["name"],
        "role": row["role"],
        "email": row.get("email", ""),
        "theme_preference": row.get("theme_preference", "system"),
    }


def create_user(request: CreateUserRequest) -> UserInfo:
    ok, msg = validate_password(request.password)
    if not ok:
        raise ValueError(msg)
    conn = get_conn()
    role_val = request.role.value if hasattr(request.role, "value") else request.role
    hashed = _hash_password(request.password)
    try:
        _run_sql(conn, "INSERT INTO users (username, name, password, role, email, theme_preference) VALUES (?,?,?,?,?,?)",
                 (request.username, request.name, hashed, role_val, "", "system"))
        _commit(conn)
    except Exception:
        conn.close()
        raise ValueError(f"User '{request.username}' already exists")
    conn.close()
    return UserInfo(username=request.username, name=request.name, role=request.role)


def change_password(username: str, current_password: str, new_password: str) -> tuple[bool, str]:
    """Change a user's password after verifying their current password."""
    ok, msg = validate_password(new_password)
    if not ok:
        return False, msg
    conn = get_conn()
    row = _fetchone(conn, "SELECT password FROM users WHERE username=?", (username,))
    if not row:
        conn.close()
        return False, "Current password is incorrect"
    if not _verify_password(current_password, row["password"]):
        conn.close()
        return False, "Current password is incorrect"
    # Reject reusing the current password as the "new" one.
    if _verify_password(new_password, row["password"]):
        conn.close()
        return False, "New password must be different from the current password"
    hashed = _hash_password(new_password)
    # Clear the forced-change flag: the user has now set a password of their own.
    _run_sql(conn, "UPDATE users SET password=?, must_change_password=0 WHERE username=?",
             (hashed, username))
    _commit(conn)
    conn.close()
    return True, ""


def must_change_password(username: str) -> bool:
    """Return True if the user is required to change their password before use."""
    conn = get_conn()
    row = _fetchone(conn, "SELECT must_change_password FROM users WHERE username=?", (username,))
    conn.close()
    return bool(row and row.get("must_change_password", 0))


def reset_password(username: str) -> Optional[str]:
    """Generate a random password for a user. Returns the new password or None if user not found."""
    conn = get_conn()
    row = _fetchone(conn, "SELECT username FROM users WHERE username=?", (username,))
    if not row:
        conn.close()
        return None
    # Generate a compliant password: 12 chars with upper, lower, digit, special
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    while True:
        new_pw = ''.join(secrets.choice(chars) for _ in range(12))
        ok, _ = validate_password(new_pw)
        if ok:
            break
    hashed = _hash_password(new_pw)
    # An admin-reset password is temporary: force the user to change it on next login.
    _run_sql(conn, "UPDATE users SET password=?, must_change_password=1 WHERE username=?",
             (hashed, username))
    _commit(conn)
    conn.close()
    return new_pw


def update_email(username: str, email: str) -> bool:
    """Update the user's email address."""
    conn = get_conn()
    _run_sql(conn, "UPDATE users SET email=? WHERE username=?", (email, username))
    _commit(conn)
    conn.close()
    return True


def update_theme_preference(username: str, theme: str) -> bool:
    """Update the user's theme preference."""
    if theme not in ("light", "dark", "system"):
        return False
    conn = get_conn()
    _run_sql(conn, "UPDATE users SET theme_preference=? WHERE username=?", (theme, username))
    _commit(conn)
    conn.close()
    return True


def get_notification_preferences(username: str) -> dict:
    """Get notification preferences for a user."""
    conn = get_conn()
    row = _fetchone(conn, "SELECT * FROM notification_preferences WHERE username=?", (username,))
    conn.close()
    if not row:
        return {
            "username": username,
            "pipeline_completed": True,
            "pipeline_failed": True,
            "sync_out_of_sync": True,
            "plan_completed": True,
        }
    return {
        "username": row["username"],
        "pipeline_completed": bool(row["pipeline_completed"]),
        "pipeline_failed": bool(row["pipeline_failed"]),
        "sync_out_of_sync": bool(row["sync_out_of_sync"]),
        "plan_completed": bool(row["plan_completed"]),
    }


def save_notification_preferences(username: str, prefs: dict) -> dict:
    """Upsert notification preferences for a user."""
    conn = get_conn()
    existing = _fetchone(conn, "SELECT username FROM notification_preferences WHERE username=?", (username,))
    if existing:
        _run_sql(conn, """UPDATE notification_preferences SET
            pipeline_completed=?, pipeline_failed=?,
            sync_out_of_sync=?, plan_completed=?
            WHERE username=?""", (
            int(prefs.get("pipeline_completed", True)),
            int(prefs.get("pipeline_failed", True)),
            int(prefs.get("sync_out_of_sync", True)),
            int(prefs.get("plan_completed", True)),
            username,
        ))
    else:
        _run_sql(conn, """INSERT INTO notification_preferences
            (username, pipeline_completed, pipeline_failed,
             sync_out_of_sync, plan_completed)
            VALUES (?,?,?,?,?)""", (
            username,
            int(prefs.get("pipeline_completed", True)),
            int(prefs.get("pipeline_failed", True)),
            int(prefs.get("sync_out_of_sync", True)),
            int(prefs.get("plan_completed", True)),
        ))
    _commit(conn)
    conn.close()
    return get_notification_preferences(username)


def update_role(username: str, role: str) -> bool:
    """Update a user's system role. Returns False if user not found."""
    conn = get_conn()
    cur = _run_sql(conn, "UPDATE users SET role=? WHERE username=?", (role, username))
    _commit(conn)
    updated = cur.rowcount > 0
    conn.close()
    return updated


def get_user_groups(username: str) -> list[dict]:
    """Get all groups a user belongs to."""
    conn = get_conn()
    rows = _fetchall(conn, """
        SELECT g.name, g.description, gm.role
        FROM group_members gm
        JOIN groups g ON gm.group_name = g.name
        WHERE gm.username=?
        ORDER BY g.name
    """, (username,))
    conn.close()
    return [{"name": r["name"], "description": r["description"], "role": r["role"]} for r in rows]


def delete_user(username: str) -> bool:
    conn = get_conn()
    # Remove dependent session rows first: sessions.username has a FK to
    # users.username, so the user row cannot be deleted while sessions exist.
    _run_sql(conn, "DELETE FROM sessions WHERE username=?", (username,))
    cur = _run_sql(conn, "DELETE FROM users WHERE username=?", (username,))
    _commit(conn)
    deleted = cur.rowcount > 0
    conn.close()
    return deleted
