# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pytest fixtures for App authorization tests.

These tests exercise the authorization model: deny-by-default authentication
plus group-based plan-ownership authorization. Requests authenticate with real
server-side sessions; the tests verify *authorization* — who is allowed to do
what.

Isolation strategy:
    The SQLite path lives in `app.database.SQLITE_PATH` and is read inside
    `get_conn()` at call time (not import time), so pointing it at a fresh temp
    file before `init_db()` gives every test module a clean, isolated database
    without touching the developer's real `app/data/app.db`.
"""
import json

import pytest


@pytest.fixture()
def db_path(tmp_path, monkeypatch):
    """Point the app's SQLite database at an isolated temp file and init it."""
    from app import database

    test_db = tmp_path / "authz_test.db"
    # Force SQLite (never Aurora) and redirect the path for this test.
    monkeypatch.setattr(database, "_is_pg", False)
    monkeypatch.setattr(database, "SQLITE_PATH", test_db)
    database.init_db()
    return test_db


@pytest.fixture()
def seed(db_path):
    """Seed a deterministic authorization fixture and return its handles.

    Layout:
        users:
            admin           (seeded by init_db, role=admin)
            editor_a        role=editor, member of group "team-a" (manage)
            viewer_a        role=viewer, member of group "team-a" (view)
            editor_b        role=editor, member of group "team-b" (manage)
            nobody          role=viewer, no group memberships
        groups:
            team-a, team-b
        plans:
            plan_a          owned by ["team-a"]
            plan_b          owned by ["team-b"]
            plan_unowned    owners=[]  (legacy: visible to all authenticated)
    """
    from app.database import get_conn, _run_sql, _commit
    import bcrypt

    conn = get_conn()
    pw = bcrypt.hashpw(b"pw", bcrypt.gensalt()).decode("utf-8")

    # The seeded admin ships flagged must_change_password=1 with a random
    # bootstrap password. For the authz/session matrix we want a normally
    # onboarded admin: give it a known, policy-compliant password and clear the
    # forced-change flag. The forced-change gate itself is covered separately in
    # test_auth_hardening.py.
    admin_pw = "AdminPassw0rd!"  # nosec B105 - test fixture, not a secret
    _run_sql(conn, "UPDATE users SET password=?, must_change_password=0 WHERE username=?",
             (bcrypt.hashpw(admin_pw.encode(), bcrypt.gensalt()).decode("utf-8"), "admin"))

    def add_user(username, role):
        _run_sql(conn, "INSERT INTO users (username,name,password,role) VALUES (?,?,?,?)",
                 (username, username.title(), pw, role))

    def add_group(name):
        _run_sql(conn, "INSERT INTO groups (name,description) VALUES (?,?)", (name, ""))

    def add_member(group, username, grole):
        _run_sql(conn, "INSERT INTO group_members (group_name,username,role) VALUES (?,?,?)",
                 (group, username, grole))

    def add_plan(plan_id, name, owners):
        _run_sql(conn,
                 "INSERT INTO migration_plans (id,name,owners,created_at,created_by) "
                 "VALUES (?,?,?,?,?)",
                 (plan_id, name, json.dumps(owners), "2026-01-01T00:00:00", "admin"))

    add_user("editor_a", "editor")
    add_user("viewer_a", "viewer")
    add_user("editor_b", "editor")
    add_user("nobody", "viewer")

    add_group("team-a")
    add_group("team-b")

    add_member("team-a", "editor_a", "manage")
    add_member("team-a", "viewer_a", "view")
    add_member("team-b", "editor_b", "manage")

    add_plan("plan_a", "Plan A", ["team-a"])
    add_plan("plan_b", "Plan B", ["team-b"])
    add_plan("plan_unowned", "Legacy Plan", [])

    def add_plan_table(plan_id, source_db, source_table, pipeline_id):
        _run_sql(conn,
                 "INSERT INTO plan_tables (plan_id, source_database, source_table, "
                 "target_database, target_table, pipeline_id) VALUES (?,?,?,?,?,?)",
                 (plan_id, source_db, source_table, "tgt_db", source_table, pipeline_id))

    # Link a pipeline to each owned plan so pipeline-scoped authz can be
    # exercised: pipe_a belongs to team-a's plan, pipe_b to team-b's.
    add_plan_table("plan_a", "srcdb", "tbl_a", "pipe_a")
    add_plan_table("plan_b", "srcdb", "tbl_b", "pipe_b")

    _commit(conn)
    conn.close()

    return {
        "admin": "admin",
        "admin_pw": admin_pw,
        "editor_a": "editor_a",
        "viewer_a": "viewer_a",
        "editor_b": "editor_b",
        "nobody": "nobody",
        "plan_a": "plan_a",
        "plan_b": "plan_b",
        "plan_unowned": "plan_unowned",
        "pipe_a": "pipe_a",
        "pipe_b": "pipe_b",
    }


@pytest.fixture()
def client(db_path):
    """A TestClient bound to the app with the isolated DB already active.

    `raise_server_exceptions=False` so that a 500 (unexpected server error)
    surfaces as a response the test can assert on, rather than propagating and
    masking an authorization regression as an error.
    """
    from starlette.testclient import TestClient
    from app.main import app
    from app.rate_limit import limiter
    from app.routes import auth as auth_routes

    # The TestClient speaks HTTP (http://testserver), so a Secure cookie set at
    # login would be held by the jar but never sent back, breaking every
    # session round-trip. Turn Secure off for the test transport — production
    # keeps SESSION_COOKIE_SECURE=true (verified separately in test_config).
    auth_routes._COOKIE_SECURE = False

    # Disable per-IP rate limiting for tests: all requests share the TestClient
    # source address, so the login limiter would otherwise trip across tests and
    # produce spurious 401/429s unrelated to what is under test. Rate limiting
    # itself is exercised by dedicated tests, not the authz/session matrix.
    limiter.enabled = False
    try:
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c
    finally:
        limiter.enabled = True
        auth_routes._COOKIE_SECURE = True


def auth(username: str) -> dict:
    """Build request headers/cookies authenticating as `username`.

    Mints a real server-side session for the user and returns it as a `cookies`
    kwarg dict so it can be splatted into any TestClient call:

        client.get("/api/plans", **auth("editor_a"))

    This exercises the same session-validation path that a browser would use
    (the HttpOnly `ima_session` cookie), so the authorization tests run on top
    of the real session mechanism rather than a bypass.
    """
    from app.services import session_service
    from app.deps import SESSION_COOKIE_NAME

    raw_token = session_service.create_session(username)
    return {"cookies": {SESSION_COOKIE_NAME: raw_token}}
