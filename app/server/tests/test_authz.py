# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Authorization tests — deny-by-default + group-based plan ownership.

Threats addressed:
    Cross-tenant access (IDOR) — a user in group A must not read or mutate a
        plan owned solely by group B.
    RBAC bypass — a non-admin must not be able to escalate their own (or
        anyone's) system role.

The suite is organized by concern:
    1. Deny-by-default authentication (no session -> 401) on protected routers.
    2. Public allowlist stays public (health, login).
    3. Cross-tenant plan access denial, read and write.
    4. View-vs-manage separation within an owning group.
    5. Unowned (legacy) plan visibility.
    6. Admin override.
    7. Role-change is admin-only (regression).
    8. Group / source / table mutations require the right role.
"""

from tests.conftest import auth


# ── 1. Deny-by-default: protected routers reject unauthenticated callers ──────

class TestDenyByDefault:
    """Every non-public router must reject a request with no session cookie (401)."""

    def test_list_plans_requires_auth(self, client, seed):
        assert client.get("/api/plans").status_code == 401

    def test_get_plan_requires_auth(self, client, seed):
        assert client.get("/api/plans/plan_a").status_code == 401

    def test_list_groups_requires_auth(self, client, seed):
        assert client.get("/api/groups").status_code == 401

    def test_list_sources_requires_auth(self, client, seed):
        assert client.get("/api/sources").status_code == 401

    def test_list_tables_requires_auth(self, client, seed):
        assert client.get("/api/tables").status_code == 401

    def test_list_users_requires_auth(self, client, seed):
        assert client.get("/api/auth/users").status_code == 401

    def test_bogus_session_cookie_rejected(self, client, seed):
        # A cookie that does not map to a stored session must be rejected.
        r = client.get("/api/plans", cookies={"ima_session": "not-a-real-token"})
        assert r.status_code == 401

    def test_stale_header_auth_no_longer_works(self, client, seed):
        # The old X-Username header must NOT authenticate anyone anymore.
        r = client.get("/api/plans", headers={"X-Username": "admin"})
        assert r.status_code == 401


# ── 2. Public allowlist stays reachable without auth ──────────────────────────

class TestPublicAllowlist:
    def test_health_is_public(self, client, seed):
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_login_endpoint_is_public(self, client, seed):
        # No X-Username. Bad credentials -> 401 from the login logic itself,
        # NOT from the deny-by-default auth layer. A 403/other would mean the
        # router-level guard wrongly blocked the public login route.
        r = client.post("/api/auth/login", json={"username": "admin", "password": "wrong"})
        assert r.status_code in (401, 423)  # invalid creds, or locked from prior test runs


# ── 3. Cross-tenant plan access denial ───────────────────────────────────

class TestCrossTenantDenial:
    """A user whose only membership is team-a must not touch team-b's plan."""

    def test_read_other_groups_plan_denied(self, client, seed):
        # editor_a manages team-a; plan_b is owned by team-b only.
        r = client.get("/api/plans/plan_b", **auth("editor_a"))
        assert r.status_code == 403

    def test_delete_other_groups_plan_denied(self, client, seed):
        r = client.delete("/api/plans/plan_b", **auth("editor_a"))
        assert r.status_code == 403

    def test_update_other_groups_plan_denied(self, client, seed):
        r = client.put(
            "/api/plans/plan_b",
            **auth("editor_a"),
            json={"name": "hijacked"},
        )
        assert r.status_code == 403

    def test_finalize_other_groups_plan_denied(self, client, seed):
        r = client.post("/api/plans/plan_b/finalize", **auth("editor_a"))
        assert r.status_code == 403

    def test_sync_other_groups_plan_denied(self, client, seed):
        # One of the 5 gaps closed in task #4 — sync must enforce view access.
        r = client.post("/api/plans/plan_b/sync", **auth("editor_a"))
        assert r.status_code == 403

    def test_get_table_spec_other_groups_plan_denied(self, client, seed):
        # Gap closed in task #4 — spec GET must enforce view access.
        r = client.get(
            "/api/plans/plan_b/tables/somedb/sometable/spec",
            **auth("editor_a"),
        )
        assert r.status_code == 403

    def test_update_table_analysis_other_groups_plan_denied(self, client, seed):
        # Gap closed in task #4 — analysis PUT must enforce manage access.
        r = client.put(
            "/api/plans/plan_b/tables/somedb/sometable/analysis",
            **auth("editor_a"),
            json={"source_format": "parquet"},
        )
        assert r.status_code == 403

    def test_resume_from_failure_other_groups_plan_denied(self, client, seed):
        # Gap closed in task #4 — resume-from-failure must enforce manage access.
        r = client.post(
            "/api/plans/plan_b/tables/somedb/sometable/resume-from-failure",
            **auth("editor_a"),
        )
        assert r.status_code == 403

    def test_list_plans_is_filtered_by_membership(self, client, seed):
        # editor_a should see plan_a and the unowned plan, but NOT plan_b.
        r = client.get("/api/plans", **auth("editor_a"))
        assert r.status_code == 200
        ids = {p["id"] for p in r.json()}
        assert "plan_a" in ids
        assert "plan_unowned" in ids
        assert "plan_b" not in ids

    def test_tables_in_plans_filtered_by_membership(self, client, seed):
        # /meta/tables-in-plans must not leak table->plan mappings for plan_b.
        r = client.get("/api/plans/meta/tables-in-plans", **auth("editor_a"))
        assert r.status_code == 200
        # Response is a dict of table-key -> JSON string containing plan id.
        for value in r.json().values():
            assert "plan_b" not in value


# ── 3b. Pipeline routes are plan-scoped (pipeline_id -> owning plan) ──────

class TestPipelineScopedAccess:
    """Pipeline routes are keyed by pipeline id; ownership is resolved through
    plan_tables so a user cannot touch another group's pipeline."""

    def test_get_other_groups_pipeline_denied(self, client, seed):
        # editor_a manages team-a; pipe_b belongs to team-b's plan.
        r = client.get("/api/pipeline/pipe_b", **auth("editor_a"))
        assert r.status_code == 403

    def test_pipeline_batches_other_group_denied(self, client, seed):
        r = client.get("/api/pipeline/pipe_b/batches", **auth("editor_a"))
        assert r.status_code == 403

    def test_pipeline_costs_other_group_denied(self, client, seed):
        r = client.get("/api/pipeline/pipe_b/costs", **auth("editor_a"))
        assert r.status_code == 403

    def test_pipeline_sync_now_other_group_denied(self, client, seed):
        r = client.post("/api/pipeline/pipe_b/sync-now", **auth("editor_a"))
        assert r.status_code == 403

    def test_unknown_pipeline_is_404_not_leaked(self, client, seed):
        r = client.get("/api/pipeline/does-not-exist", **auth("editor_a"))
        assert r.status_code == 404

    def test_owner_passes_authz_gate(self, client, seed):
        # editor_a owns pipe_a: the authz gate must pass. The engine isn't
        # running in tests, so the proxy call fails downstream — assert only
        # that we got PAST the authz layer (not 401/403/404).
        r = client.get("/api/pipeline/pipe_a", **auth("editor_a"))
        assert r.status_code not in (401, 403, 404)

    def test_pipeline_route_requires_auth(self, client, seed):
        assert client.get("/api/pipeline/pipe_a").status_code == 401

    def test_admin_passes_pipeline_authz_gate(self, client, seed):
        r = client.get("/api/pipeline/pipe_b", **auth("admin"))
        assert r.status_code not in (401, 403, 404)


# ── 4. View vs. manage separation inside an owning group ──────────────────────

class TestViewVsManage:
    """viewer_a has 'view' on team-a: can read plan_a, cannot mutate it."""

    def test_viewer_can_read_own_groups_plan(self, client, seed):
        r = client.get("/api/plans/plan_a", **auth("viewer_a"))
        assert r.status_code == 200
        assert r.json()["id"] == "plan_a"

    def test_viewer_cannot_delete_own_groups_plan(self, client, seed):
        r = client.delete("/api/plans/plan_a", **auth("viewer_a"))
        assert r.status_code == 403

    def test_viewer_cannot_finalize_own_groups_plan(self, client, seed):
        r = client.post("/api/plans/plan_a/finalize", **auth("viewer_a"))
        assert r.status_code == 403

    def test_manager_can_read_own_groups_plan(self, client, seed):
        r = client.get("/api/plans/plan_a", **auth("editor_a"))
        assert r.status_code == 200


# ── 5. Unowned (legacy) plans are visible to any authenticated user ───────────

class TestUnownedPlans:
    def test_unowned_plan_readable_by_any_authenticated_user(self, client, seed):
        r = client.get("/api/plans/plan_unowned", **auth("nobody"))
        assert r.status_code == 200

    def test_unowned_plan_read_still_requires_auth(self, client, seed):
        assert client.get("/api/plans/plan_unowned").status_code == 401


# ── 6. Admin override ─────────────────────────────────────────────────────────

class TestAdminOverride:
    def test_admin_can_read_any_plan(self, client, seed):
        assert client.get("/api/plans/plan_a", **auth("admin")).status_code == 200
        assert client.get("/api/plans/plan_b", **auth("admin")).status_code == 200

    def test_admin_sees_all_plans(self, client, seed):
        r = client.get("/api/plans", **auth("admin"))
        assert r.status_code == 200
        ids = {p["id"] for p in r.json()}
        assert {"plan_a", "plan_b", "plan_unowned"} <= ids


# ── 7. Role-change is admin-only (regression) ──────────────────────────────

class TestRoleChangeAdminOnly:
    def test_editor_cannot_escalate_own_role(self, client, seed):
        # editor_a tries to make themselves admin.
        r = client.put(
            "/api/auth/users/editor_a/role",
            **auth("editor_a"),
            json={"role": "admin"},
        )
        assert r.status_code == 403

    def test_viewer_cannot_change_any_role(self, client, seed):
        r = client.put(
            "/api/auth/users/nobody/role",
            **auth("viewer_a"),
            json={"role": "admin"},
        )
        assert r.status_code == 403

    def test_role_change_requires_auth(self, client, seed):
        r = client.put("/api/auth/users/nobody/role", json={"role": "admin"})
        assert r.status_code == 401

    def test_admin_can_change_role(self, client, seed):
        r = client.put(
            "/api/auth/users/nobody/role",
            **auth("admin"),
            json={"role": "editor"},
        )
        assert r.status_code == 200
        assert r.json()["role"] == "editor"


# ── 8. Group / source / table mutations require the right role ────────────────

class TestPrivilegedMutations:
    def test_create_group_requires_admin(self, client, seed):
        r = client.post("/api/groups", **auth("editor_a"), json={"name": "x"})
        assert r.status_code == 403

    def test_create_group_unauthenticated_denied(self, client, seed):
        assert client.post("/api/groups", json={"name": "x"}).status_code == 401

    def test_add_group_member_requires_admin(self, client, seed):
        r = client.post(
            "/api/groups/team-a/members",
            **auth("editor_a"),
            json={"username": "nobody", "role": "view"},
        )
        assert r.status_code == 403

    def test_admin_can_create_group(self, client, seed):
        r = client.post("/api/groups", **auth("admin"), json={"name": "team-c"})
        assert r.status_code == 201

    def test_create_user_requires_admin(self, client, seed):
        r = client.post(
            "/api/auth/users",
            **auth("editor_a"),
            json={"username": "mallory", "name": "M", "password": "pw", "role": "admin"},
        )
        assert r.status_code == 403

    def test_list_sources_requires_admin(self, client, seed):
        # sources router is admin-only (matches UI gating).
        r = client.get("/api/sources", **auth("editor_a"))
        assert r.status_code == 403

    def test_admin_can_list_sources(self, client, seed):
        r = client.get("/api/sources", **auth("admin"))
        assert r.status_code == 200


# ── 9. Non-admin plan creation ownership guard ────────────────────────────────

class TestPlanCreationOwnership:
    def test_editor_cannot_assign_unmanaged_group_as_owner(self, client, seed):
        # editor_a manages team-a only; assigning team-b as owner must fail.
        r = client.post(
            "/api/plans",
            **auth("editor_a"),
            json={"name": "sneaky", "owners": ["team-b"]},
        )
        assert r.status_code == 403

    def test_viewer_cannot_create_plan(self, client, seed):
        r = client.post(
            "/api/plans",
            **auth("viewer_a"),
            json={"name": "nope", "owners": []},
        )
        assert r.status_code == 403

    def test_editor_can_create_plan_for_managed_group(self, client, seed):
        r = client.post(
            "/api/plans",
            **auth("editor_a"),
            json={"name": "legit", "owners": ["team-a"]},
        )
        assert r.status_code == 201
        assert r.json()["owners"] == ["team-a"]
