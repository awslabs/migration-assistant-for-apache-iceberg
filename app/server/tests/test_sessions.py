# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Session-lifecycle tests.

These verify the new server-side session mechanism end to end:
  * login issues an HttpOnly session cookie and /me restores identity from it;
  * logout invalidates the session server-side (not just client-side);
  * expired / idle sessions are rejected;
  * password change, role change, and user deletion revoke sessions;
  * the session token is stored hashed, never in the clear.

Tests that need a "logged-in" client drive the real POST /api/auth/login flow
using the seeded admin account (default password "admin" from init_db). For
non-admin users we mint sessions directly via the service, which is the same
path login uses.
"""
import time


from tests.conftest import auth


# The admin password set by the `seed` fixture (see conftest.py). The real
# seeded admin ships with a random bootstrap password and must_change_password=1
#; the fixture normalizes it to this known, onboarded password.
ADMIN_PW = "AdminPassw0rd!"  # nosec B105 - test fixture, not a secret


class TestLoginIssuesSession:
    def test_login_sets_httponly_session_cookie(self, client, seed):
        r = client.post("/api/auth/login", json={"username": "admin", "password": ADMIN_PW})
        assert r.status_code == 200
        # A session cookie must have been set...
        set_cookie = r.headers.get("set-cookie", "")
        assert "ima_session=" in set_cookie
        # ...and it must be hardened.
        assert "httponly" in set_cookie.lower()
        assert "samesite=strict" in set_cookie.lower()

    def test_login_response_has_no_token_in_body(self, client, seed):
        # The raw token must live only in the cookie, never in the JSON body.
        r = client.post("/api/auth/login", json={"username": "admin", "password": ADMIN_PW})
        body = r.json()
        assert "token" not in body
        assert "session" not in body

    def test_me_returns_current_user_after_login(self, client, seed):
        client.post("/api/auth/login", json={"username": "admin", "password": ADMIN_PW})
        # TestClient keeps the cookie in its jar, so /me should now resolve.
        r = client.get("/api/auth/me")
        assert r.status_code == 200
        assert r.json()["username"] == "admin"
        assert r.json()["role"] == "admin"

    def test_me_requires_a_session(self, client, seed):
        assert client.get("/api/auth/me").status_code == 401


class TestLogoutInvalidates:
    def test_logout_invalidates_session_server_side(self, client, seed):
        client.post("/api/auth/login", json={"username": "admin", "password": ADMIN_PW})
        assert client.get("/api/auth/me").status_code == 200

        # Log out — this must revoke the session on the server, not just clear
        # the client cookie.
        assert client.post("/api/auth/logout").status_code == 200

        # The cookie jar was cleared by the Set-Cookie deletion; a follow-up is
        # unauthenticated.
        assert client.get("/api/auth/me").status_code == 401

    def test_logout_token_cannot_be_replayed(self, client, seed):
        # Capture the raw token, log out, then replay the captured token
        # directly. A replay must fail — proving server-side invalidation.
        from app.services import session_service

        raw = session_service.create_session("admin")
        assert client.get("/api/auth/me", cookies={"ima_session": raw}).status_code == 200

        client.post("/api/auth/logout", cookies={"ima_session": raw})
        # Replay the same token after logout.
        assert client.get("/api/auth/me", cookies={"ima_session": raw}).status_code == 401

    def test_logout_is_idempotent_without_session(self, client, seed):
        # Logging out with no session should not error.
        assert client.post("/api/auth/logout").status_code == 200


class TestSessionExpiry:
    def test_absolutely_expired_session_rejected(self, client, seed, monkeypatch):
        from app.services import session_service

        # Force a zero-length absolute TTL so the freshly minted session is
        # already past its hard expiry.
        monkeypatch.setattr(session_service, "SESSION_ABSOLUTE_TTL", 0)
        raw = session_service.create_session("admin")
        assert session_service.get_session_user(raw) is None
        assert client.get("/api/auth/me", cookies={"ima_session": raw}).status_code == 401

    def test_idle_expired_session_rejected(self, client, seed, monkeypatch):
        from app.services import session_service

        # Long absolute cap, but a 1-second idle window.
        monkeypatch.setattr(session_service, "SESSION_ABSOLUTE_TTL", 3600)
        monkeypatch.setattr(session_service, "SESSION_IDLE_TTL", 1)
        raw = session_service.create_session("admin")
        # Immediately valid.
        assert session_service.get_session_user(raw) == "admin"
        # After the idle window passes, it must be rejected.
        time.sleep(1.2)
        assert session_service.get_session_user(raw) is None

    def test_expired_session_row_is_cleaned_up(self, client, seed, monkeypatch):
        from app.services import session_service
        from app.database import get_conn, _fetchone

        monkeypatch.setattr(session_service, "SESSION_ABSOLUTE_TTL", 0)
        raw = session_service.create_session("admin")
        # Access triggers lazy cleanup of the expired row.
        assert session_service.get_session_user(raw) is None
        conn = get_conn()
        row = _fetchone(conn, "SELECT COUNT(*) AS c FROM sessions")
        conn.close()
        assert row["c"] == 0


class TestCredentialChangeRevokesSessions:
    def test_role_change_revokes_target_sessions(self, client, seed):
        from app.services import session_service

        # editor_a has a live session.
        victim = session_service.create_session("editor_a")
        assert session_service.get_session_user(victim) == "editor_a"

        # Admin changes editor_a's role.
        admin = auth("admin")
        r = client.put("/api/auth/users/editor_a/role", json={"role": "viewer"}, **admin)
        assert r.status_code == 200

        # editor_a's prior session is now invalid.
        assert session_service.get_session_user(victim) is None

    def test_password_reset_revokes_target_sessions(self, client, seed):
        from app.services import session_service

        victim = session_service.create_session("editor_a")
        assert session_service.get_session_user(victim) == "editor_a"

        admin = auth("admin")
        r = client.post("/api/auth/users/editor_a/reset-password", **admin)
        assert r.status_code == 200
        assert session_service.get_session_user(victim) is None

    def test_self_password_change_revokes_other_sessions_but_keeps_current(self, client, seed):
        from app.services import session_service

        # admin logs in on "device A" (the TestClient cookie jar).
        client.post("/api/auth/login", json={"username": "admin", "password": ADMIN_PW})
        # admin also has a session on "device B".
        device_b = session_service.create_session("admin")
        assert session_service.get_session_user(device_b) == "admin"

        # admin changes their own password on device A.
        strong_new = "NewPassw0rd!"  # nosec B105 - test fixture, not a secret
        r = client.post("/api/auth/change-password", json={
            "username": "admin",
            "current_password": ADMIN_PW,
            "new_password": strong_new,
        })
        assert r.status_code == 200

        # device B's session is revoked...
        assert session_service.get_session_user(device_b) is None
        # ...but device A stays logged in (a fresh cookie was issued).
        assert client.get("/api/auth/me").status_code == 200

    def test_delete_user_revokes_sessions(self, client, seed):
        from app.services import session_service

        # Give 'nobody' a session, then delete the user.
        victim = session_service.create_session("nobody")
        assert session_service.get_session_user(victim) == "nobody"

        admin = auth("admin")
        r = client.delete("/api/auth/users/nobody", **admin)
        assert r.status_code == 200
        assert session_service.get_session_user(victim) is None


class TestTokenStorageHardening:
    def test_raw_token_is_not_stored(self, client, seed):
        from app.services import session_service
        from app.database import get_conn, _fetchone

        raw = session_service.create_session("admin")
        conn = get_conn()
        # The raw token must never appear in the table; only its hash.
        row = _fetchone(conn, "SELECT token_hash FROM sessions WHERE username=?", ("admin",))
        conn.close()
        assert row is not None
        assert row["token_hash"] != raw
        assert len(row["token_hash"]) == 64  # sha256 hex digest
