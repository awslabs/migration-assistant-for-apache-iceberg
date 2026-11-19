# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Server-side session management.

Replaces the former `X-Username` header placeholder with real, server-side
sessions. A session is an opaque, cryptographically-random token issued at
login. The raw token is delivered to the client only once (in an HttpOnly
cookie) and is NEVER stored server-side — only its SHA-256 hash is persisted,
so a database disclosure cannot be used to forge live sessions.

Session lifetime is bounded two ways:
  * Absolute TTL  — a hard cap from creation (default 8h); a session cannot be
    renewed past this even with continuous activity.
  * Idle TTL      — a sliding window from last activity (default 30m); an idle
    session expires. Each validated request slides the window forward, capped
    by the absolute expiry.

Sessions are explicitly invalidated on logout, and all of a user's sessions
are revoked on password change or role change (see auth routes).
"""
import hashlib
import os
import secrets
import time

from app.database import get_conn, _fetchone, _run_sql, _commit


# ── Configuration (env-overridable) ──────────────────────────────────────────
# Absolute maximum session lifetime, regardless of activity.
SESSION_ABSOLUTE_TTL = int(os.getenv("SESSION_ABSOLUTE_TTL", str(8 * 60 * 60)))  # 8h
# Idle timeout — session expires this long after the last validated request.
SESSION_IDLE_TTL = int(os.getenv("SESSION_IDLE_TTL", str(30 * 60)))  # 30m

# Number of random bytes for the raw token (43+ url-safe chars).
_TOKEN_BYTES = 32


def _hash_token(raw_token: str) -> str:
    """Return the SHA-256 hex digest of a raw session token.

    SHA-256 (not bcrypt) is appropriate here: the token is already 256 bits of
    uniform randomness, so it is not brute-forceable and needs no salt/stretch.
    """
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def create_session(username: str) -> str:
    """Create a new session for `username` and return the RAW token.

    The raw token is what goes in the client cookie; only its hash is stored.
    """
    raw_token = secrets.token_urlsafe(_TOKEN_BYTES)
    token_hash = _hash_token(raw_token)
    now = time.time()
    expires_at = now + SESSION_ABSOLUTE_TTL

    conn = get_conn()
    _run_sql(
        conn,
        "INSERT INTO sessions (token_hash, username, created_at, expires_at, last_seen_at) "
        "VALUES (?,?,?,?,?)",
        (token_hash, username, now, expires_at, now),
    )
    _commit(conn)
    conn.close()
    return raw_token


def get_session_user(raw_token: str) -> str | None:
    """Validate a raw session token and return the username, or None.

    A session is valid when it exists, is within its absolute expiry, and has
    not been idle longer than the idle TTL. On success the idle window slides
    forward (bounded by the absolute expiry). Expired sessions are deleted.
    """
    if not raw_token:
        return None

    token_hash = _hash_token(raw_token)
    now = time.time()

    conn = get_conn()
    row = _fetchone(
        conn,
        "SELECT username, expires_at, last_seen_at FROM sessions WHERE token_hash=?",
        (token_hash,),
    )
    if row is None:
        conn.close()
        return None

    expires_at = float(row["expires_at"])
    last_seen_at = float(row["last_seen_at"])

    # Absolute expiry or idle timeout -> invalid; clean it up.
    if now >= expires_at or (now - last_seen_at) > SESSION_IDLE_TTL:
        _run_sql(conn, "DELETE FROM sessions WHERE token_hash=?", (token_hash,))
        _commit(conn)
        conn.close()
        return None

    # Slide the idle window forward (does not extend the absolute cap).
    _run_sql(
        conn,
        "UPDATE sessions SET last_seen_at=? WHERE token_hash=?",
        (now, token_hash),
    )
    _commit(conn)
    conn.close()
    return row["username"]


def delete_session(raw_token: str) -> None:
    """Invalidate a single session by its raw token (logout)."""
    if not raw_token:
        return
    token_hash = _hash_token(raw_token)
    conn = get_conn()
    _run_sql(conn, "DELETE FROM sessions WHERE token_hash=?", (token_hash,))
    _commit(conn)
    conn.close()


def delete_user_sessions(username: str) -> None:
    """Invalidate ALL sessions for a user.

    Called on password change and role change so that a credential/privilege
    change immediately logs out every existing session for that user.
    """
    conn = get_conn()
    _run_sql(conn, "DELETE FROM sessions WHERE username=?", (username,))
    _commit(conn)
    conn.close()


def purge_expired() -> int:
    """Delete all sessions past their absolute expiry. Returns rows removed.

    Idle-expired sessions are cleaned lazily on access; this sweeps the hard
    expiry so the table does not grow unbounded with abandoned sessions.
    """
    now = time.time()
    conn = get_conn()
    cur = _run_sql(conn, "DELETE FROM sessions WHERE expires_at < ?", (now,))
    _commit(conn)
    removed = cur.rowcount if cur.rowcount is not None else 0
    conn.close()
    return removed
