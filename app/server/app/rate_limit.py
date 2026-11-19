# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Rate limiting configuration using slowapi.

Provides:
- Global rate limiter instance for the FastAPI app
- Per-IP rate limiting on sensitive endpoints (login, password reset)
- Temporary account lockout after repeated failed login attempts

Configuration via environment variables:
- RATE_LIMIT_LOGIN: Rate limit for login attempts (default: "5/minute")
- RATE_LIMIT_DEFAULT: Default rate limit for API endpoints (default: "60/minute")
- ACCOUNT_LOCKOUT_ATTEMPTS: Failed attempts before lockout (default: 5)
- ACCOUNT_LOCKOUT_DURATION: Lockout duration in seconds (default: 900 = 15 min)
"""

import os
import time
from collections import defaultdict
from threading import Lock

from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
from fastapi.responses import JSONResponse

# ─── Rate Limiter ─────────────────────────────────────────────────────────────

RATE_LIMIT_LOGIN = os.getenv("RATE_LIMIT_LOGIN", "5/minute")
RATE_LIMIT_DEFAULT = os.getenv("RATE_LIMIT_DEFAULT", "60/minute")

limiter = Limiter(key_func=get_remote_address)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Custom handler for rate limit exceeded errors."""
    return JSONResponse(
        status_code=429,
        content={
            "detail": "Too many requests. Please try again later.",
            "retry_after": exc.detail,
        },
    )


# ─── Account Lockout ──────────────────────────────────────────────────────────

ACCOUNT_LOCKOUT_ATTEMPTS = int(os.getenv("ACCOUNT_LOCKOUT_ATTEMPTS", "5"))
ACCOUNT_LOCKOUT_DURATION = int(os.getenv("ACCOUNT_LOCKOUT_DURATION", "900"))  # 15 minutes

_failed_attempts: dict[str, list[float]] = defaultdict(list)
_lockouts: dict[str, float] = {}
_lock = Lock()


def is_account_locked(username: str) -> bool:
    """Check if an account is currently locked out."""
    with _lock:
        lockout_until = _lockouts.get(username)
        if lockout_until is None:
            return False
        if time.time() >= lockout_until:
            # Lockout expired — clear it
            del _lockouts[username]
            _failed_attempts.pop(username, None)
            return False
        return True


def get_lockout_remaining(username: str) -> int:
    """Get remaining lockout time in seconds. Returns 0 if not locked."""
    with _lock:
        lockout_until = _lockouts.get(username)
        if lockout_until is None:
            return 0
        remaining = int(lockout_until - time.time())
        return max(0, remaining)


def record_failed_login(username: str) -> bool:
    """Record a failed login attempt. Returns True if account is now locked."""
    now = time.time()
    with _lock:
        # Remove attempts older than the lockout window
        cutoff = now - ACCOUNT_LOCKOUT_DURATION
        _failed_attempts[username] = [
            t for t in _failed_attempts[username] if t > cutoff
        ]
        _failed_attempts[username].append(now)

        if len(_failed_attempts[username]) >= ACCOUNT_LOCKOUT_ATTEMPTS:
            _lockouts[username] = now + ACCOUNT_LOCKOUT_DURATION
            return True
        return False


def record_successful_login(username: str) -> None:
    """Clear failed attempt history on successful login."""
    with _lock:
        _failed_attempts.pop(username, None)
        _lockouts.pop(username, None)
