# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for default-credential elimination and MFA.

Default credentials — the seeded admin account must not have a static, guessable password and
must be forced to change its password before it can use the application.

MFA — TOTP multi-factor authentication: enrollment, login enforcement, and
optional role-based mandatory enrollment (APP_REQUIRE_MFA).
"""

import pyotp

from tests.conftest import auth

# Policy-compliant fixture password for test users created in this module. This
# is throwaway test data, not a real credential.
TEST_USER_PASSWORD = "Str0ng!Pass1"  # nosec B105 - test fixture, not a secret


# ── Seeded admin is forced to change password ────────────────────────────

class TestForcedPasswordChange:
    """The seeded admin is flagged must_change_password and gated out of the app
    until it sets a new, policy-compliant password."""

    def _seed_admin_session(self, client):
        # The seeded admin exists from init_db; mint a session for it directly.
        return auth("admin")

    def test_seeded_admin_is_flagged_must_change(self, client, db_path):
        from app.services import user_service
        assert user_service.must_change_password("admin") is True

    def test_forced_change_admin_blocked_from_protected_router(self, client, db_path):
        # A usable-session gate must reject the seeded admin on protected routers.
        r = client.get("/api/plans", **self._seed_admin_session(client))
        assert r.status_code == 403
        assert r.json()["detail"] == "PASSWORD_CHANGE_REQUIRED"

    def test_forced_change_admin_blocked_from_admin_actions(self, client, db_path):
        r = client.get("/api/auth/users", **self._seed_admin_session(client))
        assert r.status_code == 403

    def test_forced_change_admin_can_change_password(self, client, db_path):
        self._seed_admin_session(client)
        # Look up the seeded admin's current password is unknown (random), so
        # change_password via the API requires current_password. Instead verify
        # the service path clears the flag with a known reset.
        from app.services import user_service
        # Simulate: admin knows their bootstrap pw by resetting through the DB.
        # Use change_password service directly with a fresh known password after
        # forcing a known current password.
        new_hash_pw = "NewStr0ng!Pass"  # nosec B105 - test fixture, not a secret
        # Set a known current password first (admin-reset sets must_change=1).
        temp_pw = user_service.reset_password("admin")
        ok, msg = user_service.change_password("admin", temp_pw, new_hash_pw)
        assert ok, msg
        assert user_service.must_change_password("admin") is False

    def test_change_password_rejects_reuse(self, client, db_path):
        from app.services import user_service
        temp_pw = user_service.reset_password("admin")
        ok, _ = user_service.change_password("admin", temp_pw, temp_pw)
        assert ok is False

    def test_change_password_enforces_policy(self, client, db_path):
        from app.services import user_service
        temp_pw = user_service.reset_password("admin")
        ok, _ = user_service.change_password("admin", temp_pw, "weak")
        assert ok is False


# ── TOTP MFA ──────────────────────────────────────────────────────────────

class TestMfaEnrollmentAndLogin:
    """Enroll a user in TOTP, then require the code at login."""

    def _make_active_user(self):
        """Create an editor with a known password and cleared change-flag."""
        from app.services import user_service
        from app.models import CreateUserRequest, UserRole
        user_service.create_user(CreateUserRequest(
            username="mfauser", name="MFA User", password=TEST_USER_PASSWORD, role=UserRole.EDITOR))
        return "mfauser", TEST_USER_PASSWORD

    def test_enroll_then_confirm_enables_mfa(self, client, db_path):
        user, _ = self._make_active_user()
        from app.services import mfa_service
        enroll = mfa_service.begin_enrollment(user)
        assert "secret" in enroll and enroll["otpauth_uri"].startswith("otpauth://")
        assert mfa_service.is_mfa_enabled(user) is False  # not until confirmed
        code = pyotp.TOTP(enroll["secret"]).now()
        assert mfa_service.confirm_enrollment(user, code) is True
        assert mfa_service.is_mfa_enabled(user) is True

    def test_login_requires_mfa_code_when_enabled(self, client, db_path):
        user, pw = self._make_active_user()
        from app.services import mfa_service
        enroll = mfa_service.begin_enrollment(user)
        mfa_service.confirm_enrollment(user, pyotp.TOTP(enroll["secret"]).now())

        # Password-only login now fails with the MFA_REQUIRED marker.
        r = client.post("/api/auth/login", json={"username": user, "password": pw})
        assert r.status_code == 401
        assert r.json()["detail"] == "MFA_REQUIRED"

        # Wrong code is rejected.
        r = client.post("/api/auth/login",
                        json={"username": user, "password": pw, "mfa_code": "000000"})
        assert r.status_code == 401

        # Correct code succeeds.
        good = pyotp.TOTP(enroll["secret"]).now()
        r = client.post("/api/auth/login",
                        json={"username": user, "password": pw, "mfa_code": good})
        assert r.status_code == 200

    def test_login_without_mfa_when_disabled(self, client, db_path):
        user, pw = self._make_active_user()
        r = client.post("/api/auth/login", json={"username": user, "password": pw})
        assert r.status_code == 200


class TestMandatoryMfaEnrollment:
    """When APP_REQUIRE_MFA=true, privileged users without MFA are gated."""

    def test_editor_without_mfa_gated_when_required(self, client, db_path, monkeypatch):
        from app.services import user_service
        from app.models import CreateUserRequest, UserRole
        user_service.create_user(CreateUserRequest(
            username="needmfa", name="Need MFA", password=TEST_USER_PASSWORD, role=UserRole.EDITOR))
        monkeypatch.setenv("APP_REQUIRE_MFA", "true")
        r = client.get("/api/plans", **auth("needmfa"))
        assert r.status_code == 403
        assert r.json()["detail"] == "MFA_ENROLLMENT_REQUIRED"

    def test_viewer_not_gated_when_required(self, client, db_path, monkeypatch):
        from app.services import user_service
        from app.models import CreateUserRequest, UserRole
        user_service.create_user(CreateUserRequest(
            username="viewmfa", name="View", password=TEST_USER_PASSWORD, role=UserRole.VIEWER))
        monkeypatch.setenv("APP_REQUIRE_MFA", "true")
        # Viewer is not forced into MFA; a protected read should not 403 on MFA.
        r = client.get("/api/plans", **auth("viewmfa"))
        assert r.status_code == 200
