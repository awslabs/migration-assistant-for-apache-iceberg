# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Configuration-hardening tests for session cookies.

These assert the *default* (unconfigured) posture is production-safe, so a
missing env var can never silently downgrade cookie security. The values are
read from the modules with a clean environment; the test client fixture that
disables Secure for HTTP transport is deliberately NOT used here.
"""
import importlib


def _reload_auth_with_env(monkeypatch, **env):
    """Reload the auth-routes module under a controlled environment and return
    the recomputed cookie-flag module globals."""
    for k in ("SESSION_COOKIE_SECURE", "SESSION_COOKIE_SAMESITE"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import app.routes.auth as auth_routes
    importlib.reload(auth_routes)
    return auth_routes


def test_cookie_secure_defaults_on(monkeypatch):
    """With no env override, the session cookie must be Secure (HTTPS-only)."""
    auth_routes = _reload_auth_with_env(monkeypatch)
    assert auth_routes._COOKIE_SECURE is True
    # Restore module to a clean default state for other tests.
    importlib.reload(auth_routes)


def test_cookie_samesite_defaults_strict(monkeypatch):
    auth_routes = _reload_auth_with_env(monkeypatch)
    assert auth_routes._COOKIE_SAMESITE == "strict"
    importlib.reload(auth_routes)


def test_cookie_secure_only_off_when_explicitly_false(monkeypatch):
    # Only the literal "false" disables Secure — any other value stays secure.
    auth_routes = _reload_auth_with_env(monkeypatch, SESSION_COOKIE_SECURE="false")
    assert auth_routes._COOKIE_SECURE is False
    auth_routes = _reload_auth_with_env(monkeypatch, SESSION_COOKIE_SECURE="0")
    assert auth_routes._COOKIE_SECURE is True  # not "false" -> still secure
    importlib.reload(auth_routes)


def test_session_ttl_defaults(monkeypatch):
    """Absolute and idle TTL defaults are sane (8h / 30m) when unset."""
    for k in ("SESSION_ABSOLUTE_TTL", "SESSION_IDLE_TTL"):
        monkeypatch.delenv(k, raising=False)
    import app.services.session_service as ss
    importlib.reload(ss)
    assert ss.SESSION_ABSOLUTE_TTL == 8 * 60 * 60
    assert ss.SESSION_IDLE_TTL == 30 * 60
    assert ss.SESSION_IDLE_TTL < ss.SESSION_ABSOLUTE_TTL
    importlib.reload(ss)
