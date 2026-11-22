# Migration Assistant User Guide

**Audience:** Operators and data engineers who will use the Migration Assistant day to day —
configuring access, registering sources, planning and running migrations, and
monitoring them to completion.

**Scope:** This guide picks up **after deployment is complete** (all three
stacks deployed and the app reachable — see the Deployment Guide) and covers
how to *operate* the platform. It does not re-explain the internal design of
each migration strategy (see `technical-design.md`) or how the components fit
together (see `solution-design.md`). It focuses on what you do in the UI and the
AWS access you must grant for migrations to actually move data.

---

## Contents

1. [Post-Deployment Checklist](#1-post-deployment-checklist)
2. [Granting Data Access (IAM roles per component)](#2-granting-data-access-iam-roles-per-component)
3. [Configuration Reference](#3-configuration-reference)
4. [User Management and Access Control](#4-user-management-and-access-control)
5. [Registering Sources](#5-registering-sources)
6. [Creating a Plan](#6-creating-a-plan)
7. [Using the AI Assistant](#7-using-the-ai-assistant)
8. [Promoting a Plan to a Migration](#8-promoting-a-plan-to-a-migration)
9. [Monitoring Migrations (statuses explained)](#9-monitoring-migrations-statuses-explained)
10. [Finalizing a Migration](#10-finalizing-a-migration)
11. [Post-Migration Steps](#11-post-migration-steps)
12. [Operational Playbook](#12-operational-playbook)

---

## 1. Post-Deployment Checklist

Do these once, in order, before running any migration. Each is expanded in a
later section.

1. **Log in and change the bootstrap admin password.** On first login you are
   prompted to set a new password. Do this before creating other users.
2. **Verify the engine is reachable.** Open the app. If you see a banner reading
   "Migration engine unavailable", the app cannot reach the engine — confirm the
   engine URL under Settings (Section 3) and that the engine task is running.
3. **Confirm the Migration Assistant configuration.** Under **Settings**, verify the Engine,
   Athena, EMR Serverless, and (if used) Morph/Notifications groups point at the
   right resources for your account (Section 3).
4. **Grant data access to the component roles.** Migrations will not move data
   until the relevant IAM roles have access to your source and target buckets and
   Glue catalog (Section 2). This is the most common reason a first migration
   fails.
5. **Create groups and users.** Set up the groups that will own plans and invite
   your team (Section 4).
6. **Register your first source** and refresh its inventory (Section 5).

---

## 2. Granting Data Access (IAM roles per component)

The Migration Assistant uses **separate IAM roles per component**, each scoped to the least
privilege it needs. After deployment these roles exist but may be
**placeholders** with no access to your data — you grant access explicitly. This
separation is deliberate: a compromise of one component does not expose the
others.

### 2.1 Which role does what

| Component | Role (logical id) | Needs access to | When it runs |
|---|---|---|---|
| AI assistant | `AgentExecutionRole` | Source bucket (read), Glue catalog (read) | During AI analysis |
| Engine task | `EngineTaskRole` | Source (read), target (read/write), Glue catalog (read/write) | Creates target tables in-process (incremental/morph setup); orchestration |
| EMR Serverless | `EmrExecutionRole` | Source (read), target (read/write), Glue catalog | Data-movement jobs (rewrite, incremental batches, add_files) |
| S3 tracker Lambda | `TrackingLambdaRole` | Source bucket (read) | Ongoing sync — records S3 object events |
| XTable Lambda | `XTableLambdaRole` | Source (read), target (read/write), Glue catalog | Morph on Hudi/Delta sources (only if XTable deployed) |
| S3 Tables Migration | `S3TablesMigrationRole` | Source (read), Glue source table (read), Lake Formation grants if governed — S3 Tables actions + reports write are granted in-stack | Managed Migration (assumed by the AWS S3 Tables Migration API) |
| App backend | `TaskRole` (`${ENV}-app-data-access`) | Own-account Glue catalog (read-only) | Source inventory refresh, plan building |

The exact minimal IAM policy for each role is in the Deployment Guide,
**Appendix A** — that appendix also lists which role each policy attaches to.
Use it as the source of truth for the JSON.

### 2.2 The rule of thumb for "which role do I grant?"

Match the role to the point in the lifecycle that is failing:

- **AI analysis fails with access denied** → grant `AgentExecutionRole`.
- **Table creation fails** → grant `EngineTaskRole` (it creates the target
  table in-process for incremental and morph).
- **Data load / EMR job fails on read or write** → grant `EmrExecutionRole`.
- **Managed Migration fails with `sourcePermissions`** → grant
  `S3TablesMigrationRole` read on the source: S3 data **and** the Glue source
  table (plus Lake Formation grants if the database is governed by it).
- **Ongoing sync never detects changes** → check `TrackingLambdaRole` and that
  the source bucket has the event notification (created automatically per
  pipeline for sync-capable strategies).

### 2.3 Lake Formation caveat (important)

If your Glue databases are governed by **AWS Lake Formation** (not plain IAM
access mode), an IAM policy alone is **not sufficient**. The role also needs
explicit Lake Formation grants:

- `DESCRIBE` on the database, `SELECT`/`DESCRIBE` on its tables (read paths).
- `CREATE_TABLE` on the target database (for the role that creates tables —
  `EngineTaskRole`, and `EmrExecutionRole` for data writes).
- `DATA_LOCATION_ACCESS` on the registered S3 location.

Being a Lake Formation **administrator does not grant data access** — admin lets
you manage grants, not read data. A **read-only** LF admin cannot even grant to
itself. Symptoms of a missing LF grant: only some databases/tables are visible,
or `CreateTable`/`SELECT` fails with `Insufficient Lake Formation permission`
even though the IAM policy looks correct. Grant the specific permissions above
to the affected role using a full LF admin identity.

### 2.4 Cross-account sources

If the source data lives in a different AWS account from the Migration Assistant, that account
must additionally grant access (bucket policy and/or Lake Formation) to the
Migration Assistant role ARNs. Register the source with its account id and, if used, the
cross-account IAM role ARN (Section 5).

---

## 3. Configuration Reference

All runtime configuration is managed under **Settings** in the app. Each setting
resolves in this precedence order: **database override (set in the UI) → environment
variable (from the stack) → built-in default.** Changing a value in the UI writes
a database override, which takes effect without redeploying.

Settings are grouped. The ones that matter operationally:

> **One region for everything.** Set the **AWS Region** (`APP_REGION`) once,
> in the **General** group at the top of Settings. It applies to Athena, EMR
> Serverless, S3 event tracking, source discovery (Glue), notifications, S3
> Tables, and XTable — and is pushed to the engine automatically whenever you
> change it. There are no separate per-service region settings. If you leave it
> unset it defaults to the app's own region. (A pipeline can still carry its own
> `source.region` in its spec if one source lives in a different region.)
>
> **One bucket for logs and reports.** Likewise set the **Logs Bucket**
> (`ENGINE_LOGS_BUCKET`, also in **General**) once, to a bucket name. The Migration Assistant
> writes everything operational there, split by prefix: Athena query results
> (`athena-query-results/`), EMR Serverless job logs (`emr-logs/`), and Managed
> Migration compatibility/migration reports (`migration-reports/`). There are no
> separate output-location, log-URI or reports-bucket settings. A deployed stack
> already provisions this bucket (the `LogsBucket` stack output) and is wired to
> it by default.

### 3.1 Engine

| Setting | Why it matters |
|---|---|
| **Engine URL** (`ENGINE_URL`) | Where the app reaches the engine. If wrong, you get the "engine unavailable" banner and no pipeline status/costs load. In a deployed environment this is the Service Connect address (e.g. `http://engine.local:8001`). |
| **AI Agent ARN/URL** (`AGENT_URL`) | The AgentCore runtime ARN (`arn:aws:bedrock-agentcore:...`) the app invokes for AI analysis. Must be set before the AI Assistant works. For local dev it is an HTTP URL. |
| **Target Table Prefix** (`APP_TARGET_PREFIX`) | Prefix applied to target Iceberg table names (default `ima__`). Determines the default target table name when you don't override it. |

### 3.2 Reconciler

Controls how responsive the platform is to state changes. Trade responsiveness
against API/cost load.

| Setting | Why it matters |
|---|---|
| **Reconcile Interval** (`ENGINE_RECONCILE_INTERVAL_SECONDS`, default 30) | How often the engine polls running pipelines. Also the floor on how quickly a status transition (and a job's observed duration) is reflected. |
| **Max Running Pipelines** (`ENGINE_MAX_RUNNING_PIPELINES`, default 200) | How many pipelines can run at once. Extra pipelines wait as `queued` and start as slots free up. Pipelines that are in sync still count as running until finalized. Keep it below your EMR Serverless and Athena concurrency quotas. Takes effect on the next reconcile sweep. |
| **Incremental Sync Interval** (`ENGINE_INCREMENTAL_INTERVAL_SECONDS`, default 3600) | How often an in-sync incremental pipeline checks for and applies source changes. |
| **Morph Sync Interval** (`ENGINE_MORPH_INTERVAL_SECONDS`, default 3600) | Same, for morph pipelines. |

### 3.3 Athena

Required when any plan uses the Athena engine.

| Setting | Why it matters |
|---|---|
| **Athena Workgroup** | The workgroup queries run in (region comes from the global AWS Region). |
| **Query results location** | Not a separate setting: results go to `s3://<Logs Bucket>/athena-query-results/`. Athena will not run without a Logs Bucket (or the stack's workgroup output location). |

### 3.4 EMR Serverless

Required when any plan uses the EMR engine.

| Setting | Why it matters |
|---|---|
| **EMR Application ID** / **Execution Role ARN** | The app and role the engine submits Spark jobs to. |
| **EMR job logs** | Not a separate setting: logs go to `s3://<Logs Bucket>/emr-logs/` — your first stop when a Spark job fails. |
| **EMR Scripts URI** (`ENGINE_EMR_SCRIPTS_URI`) | S3 location of the Spark scripts. If unset it falls back to the log URI; if the scripts are not actually there, jobs fail with `FileNotFound` on the script. |

### 3.5 Morph / Tracking / Notifications

| Setting | Why it matters |
|---|---|
| **XTable Lambda ARN** (`ENGINE_XTABLE_LAMBDA_ARN`) | Required for morph on Hudi/Delta sources. Without it, those morph pipelines cannot convert. |
| **Morph Worker Threads** (`ENGINE_MORPH_WORKERS`) | Parallelism for Parquet footer reads during morph. |
| **Tracking Lambda Role ARN / Table** | Backing resources for ongoing-sync S3 event tracking. |
| **Notifications Enabled** (`APP_NOTIFICATIONS_ENABLED`) + related keys | When on, pipeline/plan lifecycle events are published to EventBridge (`pipeline_completed`, `pipeline_failed`, `sync_out_of_sync`, `plan_completed`). Wire these to email/Slack/etc. downstream. |

> Tip: after any deployment, do a quick pass through Settings and confirm the
> Engine, Athena, and EMR groups reflect the resources the stack created. A
> mismatch here is the most common cause of "it deployed but nothing works".

---

## 4. User Management and Access Control

The Migration Assistant has **two layers** of access control. Understand both before inviting a
team.

### 4.1 System roles

Every user has one system role:

| Role | Can do |
|---|---|
| **admin** | Everything, on every plan, regardless of group membership. Manages users, groups, and settings. |
| **editor** | Create and operate plans, subject to group ownership (below). Cannot manage users/settings. |
| **viewer** | Read-only. |

Manage users under **Users** (admin only): create a user with a username, name,
initial password, and role. New users are prompted to change their password on
first login.

### 4.2 Groups and plan ownership

Beyond system roles, access to a *specific plan* is governed by **group
ownership**:

- A plan is **owned by one or more groups**.
- Within a group, a member has either **manage** or **view** access.
- A user with **manage** on any owning group can operate the plan (execute,
  pause, finalize, etc.). A user with **view** can only read it.
- A user in none of the owning groups **cannot see the plan at all**.
- **A plan with no owners is visible (read-only) to all authenticated users** —
  useful for legacy/shared plans, but assign owners for anything sensitive.
- **Admins bypass all group checks.**

Practical setup: create a group per team (e.g. `analytics-migrations`), give the
team's operators **manage** and stakeholders **view**, and set that group as the
owner when creating plans. This scopes who can run vs. merely watch a migration.

---

## 5. Registering Sources

A **source** is an AWS account + catalog whose tables the Migration Assistant can inventory and
migrate.

1. Go to **Sources → Create Source**.
2. Provide:
   - **Name** — a friendly label.
   - **AWS account id** — the account holding the source data/catalog.
   - **IAM role ARN** (optional) — for cross-account access; leave blank to use
     the app's own role for same-account catalogs.
   - **Default group** — the group that will own plans created from this source.
3. Save, then **Refresh** the source. Refresh scans the Glue catalog and
   populates the **inventory** (databases, tables, formats, sizes, partitioning).

**If refresh fails with access denied:** the app backend role (or the provided
cross-account role) lacks Glue read access — see Section 2. **If a table shows an
unexpected format**, the catalog registration is what the Migration Assistant trusts; the
inventory reflects the Glue metadata, not a deep file scan.

Re-run refresh whenever the source catalog changes (new tables/partitions) so the
inventory stays current.

---

## 6. Creating a Plan

A **plan** is a staging area: a set of source tables plus the migration settings
to apply to them. Nothing moves data while a plan is in draft.

1. **Plans → New Plan.** Set:
   - **Name / description.**
   - **Target** — `S3 Tables` (managed Iceberg) or `Iceberg on S3`
     (self-managed). For S3 Tables, provide the **table bucket ARN**.
   - **Allowed migration types** — which strategies this plan may use. The
     primary path is **managed migration** (`s3tables_managed`), for **Parquet**
     sources going to an **S3 Tables** target (see Section 8.1); the others are
     rewrite / incremental / morph / add_files for sources and targets outside
     that scope.
   - **Execution engine** — Athena or EMR Serverless (defaults from Settings).
   - **Swap tables** — whether, on finalize, the target replaces the original
     table name.
   - **Owners** — the group(s) that own this plan (Section 4).
2. **Add tables** from the source inventory. Each added table becomes a **plan
   table** with a chosen migration type.
3. **Choose a migration type per table.** The UI only offers types that are
   **compatible** with the table's format and the plan's allowed types. If a table
   shows "Incompatible", no allowed type fits it — adjust the plan's allowed types
   or remove the table.

You can edit a plan freely while it is in **draft**. Use the AI Assistant
(Section 7) to get per-table recommendations before committing.

---

## 7. Using the AI Assistant

The AI Assistant analyzes each table and recommends a migration strategy, batch
size, partition spec, sort order, and rough size/cost/time estimates. It is
advisory — you decide what to apply.

1. From the plan, start **AI Analysis**. A progress panel streams each table's
   analysis as it runs (the assistant calls tools to inspect Glue metadata, S3
   layout, Iceberg/Hudi manifests, and access patterns).
2. When complete you get, per table:
   - **Recommended type** — the strategy the assistant would choose based purely
     on the table's technical characteristics (format, size, partitioning, file
     sizes, active writers). It is not constrained to your plan's current type
     selection, so it may differ from what's set.
   - **Estimated size / cost / time**, the resolved **partition spec** and **sort
     order**, and a **reasoning summary** explaining *why*.
   - **Schema compatibility** — flags issues that would block or complicate
     conversion (e.g. decimal precision overflow, ambiguous timestamp timezone).
3. **Apply Recommendations** updates the plan tables whose recommended type
   differs from their current type. Review first — recommendations are based on
   table characteristics, not on any policy constraints you may have.
4. **Download Report** exports the full analysis (HTML or Markdown) — a formatted
   record of every table's recommendation, estimates, spec, and reasoning. Useful
   for review/sign-off before promoting the plan.

**Prerequisites:** the AI Assistant requires `AGENT_URL` set (Section 3) and the
`AgentExecutionRole` granted read access to the source and Glue catalog (Section
2). If analysis errors out, check both.

**Reading the estimates honestly:** cost/time/size are **estimates** from
metadata and sampling, not billed actuals. Treat them as relative guidance for
choosing a strategy, not as a bill.

---

## 8. Promoting a Plan to a Migration

When the plan looks right, promote it. This is where data starts moving.

1. **Stage** the plan. Staging validates the plan (each table has a compatible
   type, S3 Tables targets have a bucket ARN, etc.) and moves it from `draft`/`ready`
   to **`staged`** — pipelines are created but not yet executing.
2. **Execute.** From a staged plan, **Execute** kicks off the migration: each
   plan table becomes a running **pipeline** driven by the engine's reconcile
   loop. The plan moves to **`in_progress`**.

You need **manage** access on the plan (Section 4) to stage/execute.

Once executing, you monitor and operate per Section 9. You can **pause**,
**resume**, **cancel**, **retry** failed tables, or **remove** tables — either
for the whole plan or, using the row checkboxes + **Actions** menu, for a
selected subset of tables.

### 8.1 Managed Migration (S3 Tables) — the plan-review step

A table set to **Managed Migration** (`s3tables_managed`) is migrated by the
**AWS S3 Tables Migration API** rather than an Athena/EMR job — AWS reads the
source and writes the S3 Tables target. It applies only to **Parquet** sources
with an **S3 Tables** target (the plan must supply a valid S3 Tables table
bucket ARN).

Two settings under **Settings > Managed Migration** control how mismatches are
handled. Both default to `fail`, which stops the migration:
**Unmatched Source Fields Policy** (`fail` or `drop`) for source fields with no
matching target column, and **Incompatible Values Policy** (`fail` or `setNull`)
for values that cannot be represented in the target type. The values are read
when a table is submitted, so changes apply to migrations started afterward.

Unlike the other strategies, Managed Migration **pauses for a plan review**
before moving data:

1. On execute, AWS produces a **compatibility plan** and the table enters
   **`awaiting_review`**. A review card appears on the table's detail panel with
   the planning stats (objects scanned and planned, partitions), any **blocking
   findings** counted by category (access, metadata, data, internal), and links
   to the **table** and **object compatibility reports** in S3. The table report
   lists, per destination column, its type change and what your migration
   policies will do to it — open it before approving.
2. Review the findings, then click **Approve** on that card to resume. Data
   movement proceeds only after approval. Approval requires **manage** access.
3. After the initial load, the table reaches `initial_load_complete`; ongoing
   deltas sync unattended (`syncing` → `sync_complete`), and cutover happens on
   finalize.

**Partitioning and sorting.** A Managed Migration keeps the source table's
partitioning: a table cannot be repartitioned as part of the migration, and rows
are not sorted while it runs. The AI Assistant's partition-transform and sort-key
recommendations therefore do not apply to these tables (they are kept on the plan
but not sent to AWS). To change partitioning or ordering, use a different
strategy, or adjust the table after migration.

If a Managed Migration table never leaves `pending` or fails immediately with a
parameter error, the engine image is likely missing the S3 Tables Migration
model, or the migration role / reports bucket is not configured — see the
Deployment Guide (§5 and §14).

---

## 9. Monitoring Migrations (statuses explained)

There are three status dimensions. Read them together.

### 9.1 Plan status

| Status | Meaning |
|---|---|
| **draft** | Being assembled. No data movement. Freely editable. |
| **ready** | Assembled and validated enough to stage. |
| **staged** | Pipelines created; not yet executing. |
| **in_progress** | Executing — one or more pipelines are running. |
| **paused** | Execution paused; resumable. |
| **completed** | All tables finished (and finalized, where applicable). |
| **cancelled** | Execution cancelled. Can be moved back to planning. |
| **failed** | One or more tables failed and need attention. |

### 9.2 Table (pipeline) status

Each table in an active plan shows its own status:

| Status | Meaning |
|---|---|
| **staged** | Pipeline created, not started. |
| **pending** | Queued to start. |
| **translating** | Preparing/converting (e.g. schema/spec setup, morph conversion). |
| **initial_load** | Performing the first full copy of data (incremental/large loads). |
| **in_progress** / **executing** | Actively running a job (e.g. a Spark/Athena job or sync). |
| **in_sync** | Initial load complete and the target is up to date with the source. Steady state for sync-capable strategies. |
| **out_of_sync** | Source changes detected (or a sync interval elapsed) and the target needs a sync run. The pipeline will sync on its next cycle. |
| **awaiting_review** | *(Managed Migration only)* AWS produced a compatibility plan and paused. Review the plan-review card and **Approve** to proceed (Section 8.1). |
| **initial_load_complete** / **sync_complete** | *(Managed Migration only)* The AWS-managed initial load (or a later sync) finished; the table is up to date. |
| **syncing** / **sync_failed** | *(Managed Migration only)* An AWS-managed sync is in progress, or the last one failed and will be retried. |
| **paused** | Operator-paused. |
| **completed** | Done. For rewrite this is terminal; for sync strategies this typically means finalized. |
| **failed** | The pipeline errored. Inspect the error, then **Retry** (starts over) or **Resume from failure** (continues from the failed batch, non-rewrite only). |
| **cancelled** | Operator-cancelled. |

### 9.3 Sync status (sync-capable strategies)

For incremental and morph pipelines, a separate **sync status** tracks the
steady-state loop: `initial_load` → `in_sync` ⇄ `out_of_sync`/`syncing`. An
`out_of_sync` pipeline is not broken — it has detected drift and will reconcile
on its next interval (see the Reconciler settings). The detail panel shows when
the next sync is due.

### 9.4 What to watch

- **Pipeline detail panel** shows status, sync status, the active EMR job id
  (copyable), start time, and, for out-of-sync pipelines, time until next sync.
- **Batch progress** shows per-batch status and duration for batched strategies.
- **Costs** show accumulated estimated compute cost (Athena bytes scanned / EMR
  vCPU-memory hours) per pipeline.
- If the **engine-unavailable banner** appears, status/cost data will not load —
  fix the engine URL/connectivity (Section 3) before trusting what you see.

### 9.5 Acting on a subset of tables

Select table rows with the checkboxes, then use the **Actions** menu to Pause,
Resume, Sync Now, Retry, Resume-from-failure, Cancel, or Remove the selected
tables. Actions are only offered where valid for the selected tables' current
status.

---

## 10. Finalizing a Migration

Finalize performs the **cutover**. It becomes available only when every table in
the plan is in a finalizable state:

- **rewrite** tables at `staged` (their CTAS runs at finalize),
- **incremental / morph / add_files** tables at `in_sync`, and
- **managed migration** tables at `initial_load_complete` / `sync_complete`
  (cutover calls the S3 Tables `CompleteTableMigration` API),

with at least one table not already `completed`.

On **Finalize**:

- Rewrite tables execute their final CTAS.
- In-sync incremental/morph/add_files tables are marked complete.
- Managed Migration tables are cut over via the S3 Tables Migration API.
- If **swap tables** is enabled and the target name differs from the source, the
  original table is replaced by the new Iceberg table (the cutover). This is
  **not reversible** — confirm you have validated the target first (Section 11).

Finalize requires **manage** access. After finalize, the plan moves toward
**completed**.

> Sequencing tip: for sync strategies, let pipelines reach **in_sync** and
> validate the target *before* finalizing. Finalizing while a table is still
> loading is blocked by the eligibility rule above, by design.

---

## 11. Post-Migration Steps

After finalize, before you consider the migration done:

1. **Validate the target table.** Use the Migration Assistant's built-in **validation** feature
   (Section 11.1) to compare source and target automatically, and/or query the
   target yourself (Athena or your engine of choice) to confirm row counts and
   spot-check values. Rewrite pipelines already do a row-count check internally,
   but an independent validation is worth it for cutover confidence.
2. **Confirm the table format and location.** For S3 Tables targets, verify the
   table appears in the intended table bucket/namespace; for Iceberg-on-S3, verify
   the metadata/location.
3. **Repoint consumers.** Update downstream jobs, dashboards, and queries to the
   new Iceberg table (or the swapped name, if you enabled table swap).
4. **Decommission the source (deliberately).** Only after consumers are validated
   on the target. If you did **not** enable swap, the original and target coexist —
   decide when to retire the original.
5. **Clean up sync infrastructure.** For strategies that ran ongoing sync, the
   per-pipeline S3 event notification and tracker are no longer needed once the
   pipeline is completed/finalized. Confirm they are not still consuming events
   for a table you've cut over.
6. **Archive the plan.** Completed plans can be **archived** to move them out of
   the active list while remaining viewable for the record.
7. **Review costs.** Check the per-pipeline cost summary for an actual-usage
   sense of what the migration consumed.

### 11.1 Data Validation

The Migration Assistant can validate a migrated table against its source on demand, comparing
them with a tiered set of checks. Validation requires **manage** access and is
available once a plan has moved past the pre-execution stages (including after
it is completed/finalized).

**Running it:** from a migration, open the **Actions** menu and choose **Perform
Validations** (validates all eligible tables), or select specific table rows
first to validate just those. Pick a **tier** in the dialog:

- **Basic** — row-count parity, schema conformance, partition count, null-count
  parity on key columns, non-emptiness + bounds (plus a manifest-integrity check
  for morph tables).
- **Advanced** — everything in Basic plus per-column aggregates
  (sum/min/max/distinct), an order-independent row-level content hash (sampled
  on large tables), and per-partition reconciliation.

**Reading results:** each table's detail panel shows a **validation results
card** with the overall pass/fail and per-check source-vs-target values. You can
**download the full report** (HTML) for sign-off. Some checks may show as
**skipped** rather than failed — e.g. the per-column aggregate check is skipped
if the engine cannot resolve the table's columns from Glue; grant Glue read to
the engine role (Section 2) if you need them.

**Engine note:** Athena-engine tables are validated via Athena queries; EMR
tables are validated by a single Spark job. Either way the result is stored and
shown per table — see `solution-design.md` §4.10 for the mechanism.

---

## 12. Operational Playbook

Quick triage for the most common situations.

| Symptom | Likely cause | Where to look |
|---|---|---|
| "Migration engine unavailable" banner; no status/costs | App cannot reach the engine | Settings → Engine URL; engine task running; network path |
| AI analysis errors immediately or times out | `AGENT_URL` unset/wrong, or `AgentExecutionRole` lacks access | Settings → AI Agent ARN; Section 2 |
| Table creation fails (`CreateTable` / warehouse / access) | `EngineTaskRole` missing IAM or Lake Formation grant | Section 2.1 / 2.3; deployment guide Appendix A |
| Spark/EMR job fails | Missing EMR config, role access, or scripts not uploaded | Settings → EMR group; the `emr-logs/` prefix of the Logs Bucket for the job's logs |
| Pipeline stuck `out_of_sync` and never syncs | Sync interval not yet elapsed, or tracker/Lambda access | Reconciler intervals (Section 3.2); `TrackingLambdaRole` |
| Only some source tables visible | Lake Formation governs the catalog; role lacks LF grants | Section 2.3 |
| A table failed mid-load | Job error | Table detail → error message; **Retry** (from scratch) or **Resume from failure** (non-rewrite) |
| Finalize button not available | Not all tables in a finalizable state | Section 10 eligibility rules; bring stragglers to in_sync/staged |
| Managed Migration table stuck `awaiting_review` | Waiting for operator approval of the compatibility plan | Table detail → plan-review card → **Approve** (Section 8.1) |
| Managed Migration failed and the message says "no reports were written" | The service failed before planning produced any report, so there is nothing in S3 to open; the failure reason (e.g. `sourcePermissions`) is the whole story | Fix the reason shown, then Retry. If reports are expected but missing after a successful plan, confirm the engine stack is current: its role may still be scoped to an older reports prefix (`migration-reports/*`) |
| Managed Migration fails immediately with a parameter error | Engine image missing the S3 Tables Migration model, or migration role / reports bucket unset | Deployment Guide §5 and §14 |
| Validation checks show "skipped" | Engine role lacks Glue read (column resolution for V7) | Section 2; grant Glue read to the engine role |

For anything that isn't behavior you can resolve from the UI/Settings, the
engine logs and the EMR logs (`emr-logs/` in the Logs Bucket) are the authoritative next stop.
