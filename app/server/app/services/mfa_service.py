# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""TOTP multi-factor authentication service.

MFA is TOTP-based (RFC 6238), compatible with authenticator apps (Google
Authenticator, Authy, 1Password, etc.). The per-user secret is generated at
enrollment, stored on the users row, and verified at login.

Enrollment flow:
  1. begin_enrollment(username) -> returns a fresh secret + otpauth:// URI for
     the QR code. The secret is stored but mfa_enabled stays 0 until confirmed.
  2. The user scans the QR and submits a current code.
  3. confirm_enrollment(username, code) verifies the code and flips mfa_enabled=1.

Login flow:
  * verify_code(username, code) is checked after the password succeeds, for any
    user whose mfa_enabled=1.

Security notes:
  * The TOTP secret is a shared secret; treat it like a credential. It is stored
    on the (KMS-encrypted at rest) users table and never returned after
    enrollment confirmation.
  * verify() uses a validation window of +/-1 step to tolerate clock skew.
"""

import pyotp

from app.database import get_conn, _fetchone, _run_sql, _commit

# Issuer label shown in the authenticator app.
_ISSUER = "Migration Assistant for Apache Iceberg"


def _get_secret(username: str) -> str | None:
    conn = get_conn()
    row = _fetchone(conn, "SELECT totp_secret FROM users WHERE username=?", (username,))
    conn.close()
    return row["totp_secret"] if row and row.get("totp_secret") else None


def is_mfa_enabled(username: str) -> bool:
    """Return True if the user has confirmed TOTP MFA."""
    conn = get_conn()
    row = _fetchone(conn, "SELECT mfa_enabled FROM users WHERE username=?", (username,))
    conn.close()
    return bool(row and row.get("mfa_enabled", 0))


def begin_enrollment(username: str) -> dict:
    """Generate (or regenerate) a TOTP secret for the user and return the
    provisioning details. Does NOT enable MFA — that requires confirmation.

    Returns {"secret": <base32>, "otpauth_uri": <otpauth://...>}.
    """
    secret = pyotp.random_base32()
    conn = get_conn()
    # Store the secret but keep MFA disabled until the user proves they can
    # generate a valid code (confirm_enrollment).
    _run_sql(conn, "UPDATE users SET totp_secret=?, mfa_enabled=0 WHERE username=?",
             (secret, username))
    _commit(conn)
    conn.close()
    uri = pyotp.totp.TOTP(secret).provisioning_uri(name=username, issuer_name=_ISSUER)
    return {"secret": secret, "otpauth_uri": uri}


def confirm_enrollment(username: str, code: str) -> bool:
    """Verify a code against the pending secret and enable MFA on success."""
    secret = _get_secret(username)
    if not secret:
        return False
    if not pyotp.TOTP(secret).verify(code, valid_window=1):
        return False
    conn = get_conn()
    _run_sql(conn, "UPDATE users SET mfa_enabled=1 WHERE username=?", (username,))
    _commit(conn)
    conn.close()
    return True


def verify_code(username: str, code: str) -> bool:
    """Verify a TOTP code for a user with MFA enabled. False if no secret/code."""
    if not code:
        return False
    secret = _get_secret(username)
    if not secret:
        return False
    return pyotp.TOTP(secret).verify(code, valid_window=1)


def disable_mfa(username: str) -> None:
    """Disable MFA and clear the stored secret for a user."""
    conn = get_conn()
    _run_sql(conn, "UPDATE users SET mfa_enabled=0, totp_secret=NULL WHERE username=?",
             (username,))
    _commit(conn)
    conn.close()
