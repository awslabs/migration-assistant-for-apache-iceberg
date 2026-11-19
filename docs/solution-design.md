# Solution Design: Migration Assistant for Apache Iceberg

**Audience:** Engineers understanding how the Migration Assistant's components fit together.
This document covers architecture, component interactions, data flow, and
infrastructure. For migration strategy internals (how each strategy converts
data), see `technical-design.md`.

---

## Contents

1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [App (Frontend + Backend)](#3-app-frontend--backend)
4. [Engine](#4-engine)
5. [AI Agent](#5-ai-agent)
6. [Infrastructure](#6-infrastructure)
7. [Data Flow](#7-data-flow)
8. [Security Model](#8-security-model)
9. [Observability](#9-observability)

---

## 1. System Overview

The Migration Assistant is an end-to-end migration platform that converts Hive/Parquet,
Hudi, Delta, CSV, ORC, Avro, and Iceberg tables into Apache Iceberg format
on AWS. It consists of four main components:

| Component | Role | Runtime |
|---|---|---|
| **App** | Web UI + REST API for plan management, user management, source discovery | ECS Fargate (Python/FastAPI + React) |
| **Engine** | Migration orchestration — pipeline execution, reconcile loop, state machine | ECS Fargate (Python/FastAPI) |
| **Agent** | AI-powered analysis — recommends migration types, batch sizes, sort orders | AWS Bedrock AgentCore |
| **Infrastructure** | Compute/storage — EMR Serverless, Athena, DynamoDB, S3, Lambda | CloudFormation stacks |

The App is the user-facing control plane. The Engine is the headless
execution plane. They communicate over HTTP (Service Connect). The Agent
is invoked on-demand for analysis tasks and communicates with the Engine
to inspect source tables.

The Engine's primary path is **Managed Migration** (`s3tables_managed`), which
migrates Parquet tables into S3 Tables through the AWS S3 Tables Migration API.
It also runs five self-driven strategies for other sources and targets —
**Rewrite**, **Incremental**, **Morph**, **Add Files**, and
**Iceberg-Incremental** — and provides an on-demand **data validation**
subsystem (tiered checks V1–V8) that compares each migrated table against its
source. For strategy internals see `technical-design.md`.

---

## 2. Architecture

```
                            User (Browser)
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │    App (React + API)   │
                    │   Port 8000 / Fargate  │
                    └────────────┬───────────┘
                                 │ HTTP (Service Connect)
                                 ▼
                    ┌────────────────────────┐
                    │   Engine               │
                    │   Port 8001 / Fargate  │
                    └──┬──────────┬──────────┘
                       │          │
            ┌──────────┘          └──────────────┐
            ▼                                    ▼
┌───────────────────┐              ┌──────────────────────┐
│ DynamoDB Tables   │              │ Execution Engines    │
│ (pipelines,       │              │ ┌─────────────────┐  │
│  batches, costs,  │              │ │ EMR Serverless  │  │
│  tracking,        │              │ │ (Spark jobs)    │  │
│  validation, etc.)│              │ ├─────────────────┤  │
└───────────────────┘              │ │ Athena          │  │
                                   │ │ (SQL queries)   │  │
┌───────────────────┐              │ ├─────────────────┤  │
│ S3 Buckets        │              │ │ In-process      │  │
│ - Scripts         │              │ │ (PyIceberg)     │  │
│ - Logs            │              │ ├─────────────────┤  │
│ - Source data     │              │ │ S3 Tables       │  │
└───────────────────┘              │ │ Migration API   │  │
                                   │ └─────────────────┘  │
                                   └──────────────────────┘
                                            │
┌───────────────────┐              ┌────────┴───────────┐
│ Lambda Functions  │              │ AWS Glue Catalog   │
│ - S3 Tracker      │              │ (source + target   │
│ - XTable          │              │  table metadata)   │
│ - Notifications   │              └────────────────────┘
│ - DB Bootstrap    │
└───────────────────┘
```

---

## 3. App (Frontend + Backend)

### 3.1 Frontend (React + TypeScript)

Single-page application built with React, Vite, TanStack Router, and
TanStack Query. Deployed as static assets served by the backend.

**Key pages:**
- **Dashboard** — migration progress overview, active plans, quick actions
- **Inventory** — browse all tables from sources, tree navigation by
  source/database, add tables to plans
- **Plans** — create/edit migration plans, assign tables, configure
  strategy/engine/target, run AI analysis
- **Migrations** — active migration monitoring, per-pipeline progress,
  batch tracking, sync status
- **Sources** — manage Glue Data Catalog connections, refresh inventory
- **Settings** — engine configuration (EMR, Athena, tracking, etc.)
- **Users** — role management (admin/editor/viewer), groups, permissions

### 3.2 Backend (FastAPI)

Python REST API handling:
- **Authentication** — session-based with bcrypt password hashing, role-based
  access control (admin/editor/viewer system roles + group-based plan ownership)
- **Plan lifecycle** — create → add tables → analyze → finalize → execute →
  monitor → complete/archive
- **Source management** — connect to Glue catalogs, refresh table inventory,
  format detection, partition discovery
- **Pipeline sync** — polls the Engine for pipeline status updates, maps
  engine states to app display states, derives plan-level status
- **Notifications** — resolves recipients per plan ownership, emits events
  to EventBridge for Lambda → SES delivery
- **AI assistant** — streams agent analysis results to the UI, applies
  recommendations (migration type, batch size, sort order)

### 3.3 Database

SQLite (local/dev) or Aurora PostgreSQL (production). Stores:
- Users, groups, permissions
- Sources, tables inventory
- Migration plans, plan tables (with per-table spec, status, analysis)
- Settings (key-value, persisted across restarts)

The Engine uses a separate persistence layer (DynamoDB) — the App never
writes to DynamoDB directly. Status synchronization is via HTTP polling.

---

## 4. Engine

The engine's flagship capability is **Managed Migration** (§4.9): it drives the
AWS S3 Tables Migration API to move Parquet tables into S3 Tables, with a
plan-review/approval gate and AWS-managed data movement. The remaining
strategies (Rewrite, Incremental, Morph, Add Files, Iceberg-Incremental) and the
validation subsystem (§4.10) support everything outside that path. The
subsections below describe the shared engine machinery first (API server,
reconcile loop, state machine, execution engines), then each component.

### 4.1 API Server

FastAPI application exposing pipeline CRUD, batch management, progress
tracking, and control operations (start, cancel, retry, sync-now, complete,
swap-tables).

### 4.2 Reconcile Loop

Background async task (`reconciler.py`) that runs every N seconds
(default 30, configurable via `ENGINE_RECONCILE_INTERVAL_SECONDS`). Each
sweep first promotes any `queued` pipelines that now fit under the concurrency
cap (`ENGINE_MAX_RUNNING_PIPELINES`, default 200), then for each pipeline in
`status=running`:

1. Loads the pipeline record from DynamoDB
2. Resolves the appropriate strategy (Rewrite/Incremental/Morph/AddFiles/
   IcebergIncremental/S3TablesManaged) based on `migration_type` +
   `source_format`
3. For state-machine strategies, delegates to
   `PipelineStateMachine.reconcile(pipeline, strategy)`; for Managed Migration
   (`s3tables_managed`), calls the strategy's own `reconcile()` (see §4.3)

Per-pipeline reconciles run concurrently on a thread pool
(`ENGINE_RECONCILE_WORKERS`, default 50), since the work is I/O-bound
(EMR/Athena/API/DB calls).

### 4.3 State Machine (`state_machine.py`)

Common lifecycle driver for the batch/job-based strategies — Rewrite,
Incremental, Morph, Iceberg-Incremental, and Add Files. **Managed Migration
(`s3tables_managed`) is deliberately NOT on the state-machine path**: it is a
polling adapter over the AWS S3 Tables Migration API with no internal
batches/jobs, so it implements its own `reconcile()` that re-derives state by
calling `GetTableMigration` and projecting the API state onto the Migration Assistant's
`sync_status` (see §4.9).

Phases (state-machine strategies):

| Phase | Condition | Action |
|---|---|---|
| 1. Poll Job | `emr_job_run_id` set | Poll EMR/Athena, handle success/failure |
| 2. Initial Load | `sync_status` in (pending, initial_load) | Advance batches sequentially |
| 3. Failed Sync | `sync_status` = failed | Wait interval, reset to out_of_sync |
| 4. Change Detection | `sync_status` = in_sync | Call `strategy.detect_changes()` |
| 5. Execute Sync | `sync_status` = out_of_sync | If interval elapsed, call `strategy.execute_sync()` |
| 6. Syncing | `sync_status` = syncing | Wait for job completion |

Strategies implement hooks: `submit()`, `validate()`, `submit_batch()`,
`on_job_success()`, `on_initial_load_complete()`, `detect_changes()`,
`execute_sync()`, `cancel()`.

### 4.4 Execution Engines

- **Athena** (`engines/athena.py`) — submits SQL queries, polls via
  `GetQueryExecution`, reads results via `GetQueryResults`
- **EMR Serverless** (`engines/emr.py`) — submits Spark job runs with
  script path + spark-submit parameters, polls via `GetJobRun`
- **In-process** — PyIceberg operations (Morph manifest building, table
  creation) run directly in the engine process
- **S3 Tables Migration API** (`engines/s3tables_migration.py`) — a thin boto3
  client used only by Managed Migration. Calls `StartTableMigration`,
  `SyncTableMigration`, `GetTableMigration`, `UpdateTableMigration` (approve/
  cancel), and `CompleteTableMigration` (cutover); AWS performs the data
  movement, the Migration Assistant only drives/polls it.


### 4.5 Spark Scripts (`src/spark/`)

Submitted to EMR Serverless as job entry points:
- `rewrite_ctas.py` — full-table CTAS for Rewrite strategy
- `incremental_rewrite.py` — batched INSERT/DELETE for Incremental
- `add_files.py` — Iceberg `system.add_files()` procedure for Add Files
- `iceberg_sync.py` — snapshot-diff sync for Iceberg-source Incremental
- `create_table.py` — empty target table creation
- `validate.py` — runs the validation checks for EMR-engine pipelines (one job
  validates all EMR pipelines in a run and writes results to DynamoDB itself)

Each script receives a `--spec` JSON argument containing the full pipeline
specification (source, target, strategy, iceberg properties). Scripts build SQL
through `spark_sql_builder.py` (shipped as a `--py-files` dependency), which
validates identifiers without requiring sqlglot on the EMR classpath.

### 4.6 Database Layer (`src/db/`)

DynamoDB-backed (production) or SQLite (local dev). Tables:
- **pipelines** — one record per migration pipeline (status, sync_status,
  spec, progress counters, job references)
- **batches** — per-pipeline batch records (batch_number, status,
  partitions, emr_job_run_id, timing)
- **progress** — per-partition progress tracking (for UI progress bars)
- **object_tracking** — S3 event records from tracker Lambdas (pending →
  processed lifecycle, 7-day TTL)
- **snapshots** — Iceberg snapshot cursors for IcebergIncremental
- **costs** — per-pipeline compute cost records (Athena bytes scanned,
  EMR vCPU/memory hours)
- **validation_results** (`ima-validation-results`) — per-pipeline
  validation-run history (tier, engine, overall status, per-check results)

### 4.7 S3 Event Tracking (`src/tracking/`)

For strategies with ongoing sync (Incremental, Morph, Add Files on
Parquet sources):

1. `TrackingManager` creates a dedicated Lambda per pipeline
2. Lambda code (`lambda_code.py`) is packaged as a zip and deployed
3. An S3 bucket notification is configured on the source prefix
4. S3 events (ObjectCreated/ObjectRemoved) → Lambda → DynamoDB row
5. The reconcile loop's `detect_changes()` queries for pending events

### 4.8 XTable Integration (`src/xtable/`)

For Morph strategy on Hudi/Delta sources:
- `XTableClient` invokes the XTable Lambda (synchronous)
- `commit_tracker` reads Hudi/Delta commit timelines to detect changes
  for ongoing sync (without S3 events)

### 4.9 Managed Migration (`strategies/s3tables_managed.py` + `engines/s3tables_migration.py`)

Migrates Hive-Parquet sources into S3 Tables using the AWS **S3 Tables Migration
API** rather than an Athena/EMR data-movement job. Applies only when the source
is `parquet` and the target is `s3_tables`. AWS performs the migration; the Migration Assistant
submits, polls, and drives the lifecycle:

- **submit()** calls `StartTableMigration` with `reviewMigrationPlan=true`. The
  service produces a compatibility plan and pauses; the Migration Assistant surfaces this as
  `sync_status = awaiting_review`.
- **approve()** (operator action) issues `UpdateTableMigration → queued` to
  resume once the plan is reviewed.
- **reconcile()** polls `GetTableMigration` and projects the API state onto
  engine status (own reconcile path, not the state machine).
- **execute_sync()** calls `SyncTableMigration` for ongoing deltas (unattended,
  no review pause).
- **complete()** calls `CompleteTableMigration` for cutover.

Configuration (resolved from engine env/Settings): the migration IAM role
(`ENGINE_S3TABLES_MIGRATION_ROLE_ARN`, assumed by the service) and a reports
reports location (the common logs bucket, `ENGINE_LOGS_BUCKET`, under the
`migration-reports/` prefix) where the service writes compatibility/migration
reports. For `start`/`sync` the
client region is pinned to the target table-bucket's region.

### 4.10 Validation (`core/validator.py`, `validation_checks.py`, `validation_sql.py`)

On-demand subsystem that compares a migrated table against its source. Two
tiers of checks:

- **Basic:** V1 row-count parity, V2 schema conformance, V3 partition count,
  V4 null-count parity (key columns), V5 non-emptiness + bounds, V6 morph
  manifest integrity (morph only).
- **Advanced:** the basic set plus V7 per-column aggregates (sum/min/max/
  distinct) and V8 per-partition reconciliation.

Each pipeline validates on its own `execution_engine`. **Athena** pipelines run
the check SQL via Athena, fanning out across a bounded pool
(`ENGINE_VALIDATION_WORKERS`, default 5); V7 resolves its column list
from Glue (`GetTable`) and are skipped if resolution fails. **EMR** pipelines
are validated by a single `validate.py` Spark job that writes results to the
`validation_results` DynamoDB table itself. Results (tier, overall status,
per-check source/target values) are persisted per run and surfaced in the UI.

---

## 5. AI Agent

### 5.1 Runtime

Deployed on AWS Bedrock AgentCore — a managed runtime that handles
scaling, authentication, and invocation routing. The agent is a Python
application with tools that the LLM calls to analyze source tables.

### 5.2 Tools

| Tool | Purpose |
|---|---|
| `glue_analyzer` | Fetch table schema, partitions, properties from Glue |
| `s3_analyzer` | Analyze S3 data layout, file sizes, partition skew |
| `hudi_analyzer` | Inspect Hudi timeline, compaction history, COW vs MOR |
| `iceberg_analyzer` | Read Iceberg metadata, snapshots, schema evolution |
| `schema_mapper` | Map source schema to Iceberg types, detect issues |
| `migration_estimator` | Estimate size, cost, duration for each strategy |
| `cloudtrail_analyzer` | Analyze table access patterns for sort order hints |

### 5.3 Workflow

The App's "AI Analysis" feature streams a multi-table analysis:
1. For each table in a plan, the agent calls tools to gather metadata
2. Recommends: migration type, batch size, sort order, partition spec
3. Detects schema compatibility issues (decimal overflow, timestamp
   ambiguity, unsupported types)
4. Results are streamed to the UI and persisted on plan_tables records

---

## 6. Infrastructure

### 6.1 CloudFormation Stacks

| Stack | Resources |
|---|---|
| `{env}-engine` | VPC, subnets, ECS cluster, Engine task definition/service, DynamoDB tables, S3 buckets, EMR Serverless app, Athena workgroup, Lambda roles, KMS keys |
| `{env}-app` | App task definition/service, Aurora PostgreSQL, Secrets Manager, DB bootstrap Lambda, Notification Lambda, EventBridge rule |
| `{env}-agent` | Agent IAM role, artifacts bucket, logs bucket |

### 6.2 Networking

- Engine and App run in private subnets (no public IPs)
- Communication via ECS Service Connect (HTTP, internal DNS)
- Internet access via NAT Gateway (for Glue/S3/EMR API calls)
- Access to the App via SSM port forwarding (no ALB by default)

### 6.3 Key AWS Services Used

| Service | Purpose |
|---|---|
| ECS Fargate | App + Engine containers |
| DynamoDB | Engine state persistence (pipelines, batches, tracking) |
| Aurora PostgreSQL | App persistence (users, plans, inventory) |
| EMR Serverless | Spark job execution for data movement |
| Athena | SQL query execution for data movement |
| S3 | Source/target data, scripts, logs |
| Lambda | S3 event tracking, XTable conversion, notifications, DB bootstrap |
| Glue Data Catalog | Source table metadata, target Iceberg table registration |
| KMS | Encryption for DynamoDB, Secrets Manager |
| EventBridge | Pipeline status notification routing |
| SES | Email delivery for notifications |
| Bedrock AgentCore | AI agent hosting |
| Secrets Manager | Database credentials |
| CloudWatch Logs | Container and Lambda logging |

---

## 7. Data Flow

### 7.1 Plan Execution Flow

```
User creates plan (App UI)
  → App stores plan + tables in Aurora
  → User clicks "Execute"
  → App calls Engine /api/pipeline (POST) for each table
  → Engine creates pipeline record in DynamoDB
  → Engine calls strategy.submit() → creates batches, starts first job
  → Reconcile loop polls job status every sweep (default 30s)
  → On job success → advance to next batch (or transition to in_sync)
  → App polls Engine via sync endpoint → updates plan_tables status
  → UI reflects real-time progress
```

### 7.2 Settings Flow

```
Admin configures settings in App UI (Manage > Settings)
  → App stores in SQLite/Aurora
  → On next Engine poll (or explicit push), App POSTs to Engine /api/settings
  → Engine writes values to os.environ
  → All engine modules read settings lazily at call time (not cached at import)
```

### 7.3 Source Refresh Flow

```
User clicks "Refresh" on a source
  → App calls Glue GetDatabases/GetTables for that account
  → Detects format (Parquet/Hudi/Delta/Iceberg/CSV/etc.) from InputFormat/SerDe
  → Upserts into tables_inventory (format, location, size, partitioned)
  → Detects removed tables (in inventory but not in Glue) → marks as removed
```

### 7.4 Ongoing Sync Flow (S3 Events)

```
Source file created/deleted in S3
  → S3 bucket notification → Tracker Lambda
  → Lambda writes event to DynamoDB object_tracking table
  → Reconcile loop: detect_changes() finds pending events → out_of_sync
  → After sync interval: execute_sync() groups events by partition
  → Submits EMR/Athena job to re-sync affected partitions
  → On completion: marks events processed → in_sync
```

### 7.5 Managed Migration Flow (S3 Tables)

```
User executes an s3tables_managed plan
  → Engine strategy.submit() → StartTableMigration (reviewMigrationPlan=true)
  → AWS produces a compatibility plan and PAUSES
  → Reconcile loop GetTableMigration → sync_status = awaiting_review
  → App surfaces the compatibility report; operator clicks Approve
  → Engine approve() → UpdateTableMigration(queued) → AWS runs the migration
  → Reconcile loop polls GetTableMigration, projects state to engine status
  → Ongoing deltas: execute_sync() → SyncTableMigration (unattended)
  → Cutover: complete() → CompleteTableMigration
```

### 7.6 Validation Flow

```
User requests validation for one or more pipelines (tier: basic|advanced)
  → Engine builds a per-pipeline check plan (V1–V8 for the tier)
  → Athena pipelines: run check SQL via Athena, fanned out across a worker pool
       (V7 resolves columns from Glue first; skipped if unresolved)
  → EMR pipelines: one validate.py Spark job validates them all
  → Each run's per-check source/target values + overall status are written to
    the validation_results table
  → App reads results and renders pass/fail per check in the UI
```

---

## 8. Security Model

### 8.1 Authentication & Authorization

- **App-level:** bcrypt-hashed passwords, session tokens, three system
  roles (admin/editor/viewer)
- **Plan access:** group-based ownership. Plans are owned by groups;
  users in a group with "manage" role can modify, "view" can only read.
  Admins bypass all checks.
- **IAM:** each component has least-privilege IAM roles scoped to
  same-account resources (S3 `aws:ResourceAccount` condition, Glue/Athena/
  EMR scoped to account+region ARNs)

### 8.2 Data Protection

- DynamoDB tables encrypted with customer-managed KMS CMK (key rotation enabled)
- S3 buckets: AES256 server-side encryption, versioning, public access blocked
- Aurora: encrypted at rest (KMS), IAM-only authentication (no passwords
  in connection strings)
- Secrets Manager: customer-managed CMK encryption

### 8.3 Network Security

- All services in private subnets (no public access)
- S3 bucket notifications scoped to specific prefixes per pipeline
- EMR Serverless jobs execute in the VPC with no internet egress
  (S3/Glue via VPC endpoints)
- **Service exposure & CORS.** The App backend is the only browser-facing
  service; it enforces an explicit CORS origin allowlist (`CORS_ALLOWED_ORIGINS`
  env / `CorsAllowedOrigins` stack parameter). The Engine API (port 8001) is
  internal and server-to-server only — reached by the App over Service Connect
  (`engine.local:8001`), its security group allows ingress on 8001 only from
  the VPC CIDR via a scoped SG-to-SG rule. The Engine API therefore runs with
  **no CORS middleware** (no browser calls it cross-origin), and a wildcard CORS
  policy must never be added to it.

### 8.4 Input Validation

- SQL construction uses `sqlglot` AST builder for Athena queries,
  eliminating string interpolation and injection vectors (`sql_builder.py`)
- Standalone Spark scripts (EMR) build SQL via `spark_sql_builder.py`, which
  validates identifiers against injection metacharacters before interpolation
  (sqlglot is not available on the EMR classpath)
- API inputs validated via Pydantic models
- File paths validated before subprocess execution

### 8.5 IAM Least Privilege & Data-Plane Separation

The Migration Assistant separates the **control plane** (solution-owned resources) from the
**data plane** (customer source/target data and Glue catalog):

- Each role's inline policies grant access only to resources the solution
  deploys — DynamoDB tables, the stack KMS CMK, the Athena workgroup, the EMR
  Serverless application, the scripts/logs buckets, and stack Lambdas — all
  scoped to specific ARNs, not wildcards.
- All source-data, target-data, Glue catalog, S3 Tables and Lake Formation
  access is externalized to per-role **placeholder managed policies** that ship
  empty (an inert `Deny`). Operators populate them post-deployment with grants
  scoped to their own data, per the deployment guide. This lets each customer
  design data access to their governance requirements without editing the
  templates.
- Several roles carry placeholders, each with a documented minimal permission
  set: App backend and AI agent are **read-only**; the Engine reads source and
  manages the target catalog; EMR and XTable perform data movement
  (read source, write target); the tracker Lambda is source read-only; and the
  **S3 Tables Migration role** (assumed by the migration service for Managed
  Migration) is source read-only, with its S3 Tables actions and reports-bucket
  write granted in-stack rather than via a placeholder.
- The EMR execution role carries an explicit `Deny` on writes to the scripts
  bucket it executes from, so a permissive customer data-access policy can never
  reopen a script-tampering / RCE path (Deny always wins).
- `iam:PassRole` is constrained to the specific stack-owned role ARNs.

---

## 9. Observability

### 9.1 Logging

- Engine + App containers → CloudWatch Logs (structured, per-container streams)
- Lambda functions → CloudWatch Logs (per-function log groups)
- EMR Serverless jobs → S3 log bucket (driver stdout/stderr)
- Log level configurable via `ENGINE_LOG_LEVEL` env var

### 9.2 Metrics

- Per-pipeline: partitions completed/total, bytes processed, batch
  durations, sync cycle timing
- Per-plan: aggregate progress across all tables
- Cost tracking: Athena bytes scanned, EMR vCPU-hours/memory-GB-hours
  (stored in DynamoDB costs table, surfaced in UI)

### 9.3 Alerting

- Pipeline failures surface immediately in the UI (per-table error messages)
- EventBridge notifications emit `pipeline_failed` events → email alerts
  to plan owners (if notifications enabled)
- Stuck pipelines: `out_of_sync` status with "Next Sync" countdown visible
  in the pipeline detail panel
