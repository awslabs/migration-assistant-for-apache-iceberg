# Migration Support Matrix

The Migration Assistant's primary path is **Managed Migration** (`s3tables_managed`), which
migrates Parquet tables into S3 Tables through the AWS **S3 Tables Migration
API**. Alongside it, the Migration Assistant supports four additional self-driven strategies for other
sources and targets: **Rewrite**, **Incremental**, **Morph** and **Add Files**.

The authoritative format→strategy eligibility rules live in
`app/server/app/config/migration_rules.yaml`. Target-type constraints that format rules cannot express (e.g. Managed Migration requiring an S3 Tables target) are enforced in the app's `plan_service` and in each strategy's `validate()`.

## Managed Migration (S3 Tables Migration API)

Managed Migration (`s3tables_managed`) is the primary path: it hands the
migration to the AWS **S3 Tables Migration API** instead of running an
Athena/EMR data-movement job. AWS reads the source, writes the S3 Tables
target, and produces compatibility and migration reports; the Migration Assistant drives and
polls the lifecycle.

**Eligibility (`validate()`):**

- **Source format:** Hive-style **Parquet** only.
- **Target:** **S3 Tables** only. `target_bucket_arn` must be a real S3 Tables
  *table bucket* ARN of the form `arn:aws:s3tables:<region>:<account>:bucket/<name>`
  (a plain `arn:aws:s3:::...` bucket ARN or bare name is rejected).
- **Migration role:** an IAM role the service assumes must be configured
  (`ENGINE_S3TABLES_MIGRATION_ROLE_ARN`; wired in-stack to `S3TablesMigrationRole`).
- **Reports location:** the common logs bucket must be configured
  (`ENGINE_LOGS_BUCKET`; reports land under its `migration-reports/` prefix).

**Lifecycle & API operations:**

| Phase | API call | Engine status |
|-------|----------|-----------------|
| Start (with plan review) | `StartTableMigration` (`reviewMigrationPlan=true`) | pauses → `awaiting_review` |
| Operator approves plan | `UpdateTableMigration` → `queued` | resumes |
| Poll progress | `GetTableMigration` | projected onto `sync_status` |
| Ongoing delta sync | `SyncTableMigration` (unattended) | — |
| Cutover | `CompleteTableMigration` | `completed` |
| Cancel | `UpdateTableMigration` → `cancelled` | `cancelled` |

**What it cannot do.** A migration cannot repartition a table, and it does not
sort rows (migration compaction is binpack-only). The target keeps the source's
partitioning, and any recommended partition spec or sort order is not applied.

**Operator interaction — plan review.** Unlike the other strategies, Managed
Migration **pauses for a plan review** after start: the service produces a
compatibility plan (table and object compatibility reports, written to the
reports bucket) and waits. The pipeline
surfaces as `awaiting_review`; an operator reviews and approves before data
movement proceeds. Ongoing syncs run unattended (no review pause).

The remaining strategies below cover sources and targets outside Managed
Migration's scope (non-Parquet sources, Iceberg-on-S3 targets, and
zero-data-movement conversions).

---

## Source Format → Target Type → Supported Strategies

| Source Format | Target: S3 Tables | Target: Iceberg on S3 |
|---------------|-------------------|----------------------|
| **Parquet** | **Managed Migration**, Rewrite, Incremental | Rewrite, Incremental, Morph, Add Files |
| **Hudi** | Rewrite, Incremental | Rewrite, Incremental, Morph |
| **Delta** | Rewrite, Incremental | Rewrite, Incremental, Morph |
| **Iceberg** | Rewrite, Incremental (snapshot-based) | N/A |
| **CSV** | Rewrite | Rewrite |
| **JSON** | Rewrite | Rewrite |
| **Avro** | Rewrite | Rewrite |
| **ORC** | Rewrite | Rewrite |

---

## Strategy × Engine Implementation Status

| Strategy | Athena Engine | EMR Serverless Engine | Other |
|----------|--------------|----------------------|-------|
| **Managed Migration** | N/A | N/A | ✅ AWS S3 Tables Migration API (no Athena/EMR) |
| **Rewrite** | ✅ Implemented | ✅ Implemented | — |
| **Incremental** | ✅ Implemented | ✅ Implemented | — |
| **Morph** | N/A | N/A | ✅ In-process (pyiceberg); Hudi/Delta via XTable Lambda |
| **Add Files** | ❌ Not supported | ✅ Implemented (Iceberg `add_files` procedure) | — |

> **Managed Migration uses neither Athena nor EMR.** AWS performs the migration
> server-side; the engine only submits, polls, approves, and completes it via
> the S3 Tables Migration API (`engines/s3tables_migration.py`).

---

## Strategy Constraints

| Strategy | Format Requirement | Target Requirement | Partitioning Requirement | Engine |
|----------|-------------------|-------------------|-------------------------|--------|
| **Managed Migration** | Parquet only | S3 Tables only | None | S3 Tables Migration API |
| **Rewrite** | Any format | Any target | None | Athena or EMR |
| **Incremental** | Parquet, Hudi, Delta | Any target | Must be partitioned | Athena or EMR |
| **Morph** | Parquet, Hudi, Delta | Iceberg on S3 only | None | In-process |
| **Add Files** | Hive-Parquet | Iceberg on S3 only | Must be partitioned | EMR Serverless only |
| **Iceberg-Incremental** | Iceberg | S3 Tables | None | Athena or EMR |

---

## Key Restrictions

1. **Morph + S3 Tables**: Not supported. S3 Tables requires data in the S3 Tables bucket — morph points manifests to existing file locations.

2. **Incremental + Unpartitioned**: Not supported. Incremental relies on partition-based batch processing.

3. **Iceberg source**: Rewrite, or snapshot-based Incremental. Covers Iceberg-to-S3-Tables upgrades.

4. **Non-columnar formats (CSV, JSON, Avro)**: Rewrite only — lack columnar metadata for morph and efficient partition reads for incremental.

5. **ORC**: Rewrite only in current rules config. Incremental could be enabled in `migration_rules.yaml`.

6. **Add Files**: Hive-Parquet sources only, **EMR Serverless only**, and target must be Iceberg on S3 (files stay in place — not valid for S3 Tables).

7. **Managed Migration**: Parquet sources only and **S3 Tables target only**; requires a valid S3 Tables table-bucket ARN, a migration IAM role, and a reports bucket (see the section above). Does not run on Athena or EMR.

---

## Execution Engine Availability

| Strategy | Athena | EMR Serverless | S3 Tables Migration API | In-process |
|----------|--------|----------------|------------------------|------------|
| **Managed Migration** | — | — | ✅ AWS-managed | — |
| **Rewrite** | ✅ CTAS query | ✅ Spark CTAS job | — | — |
| **Incremental** | ✅ Batch INSERT queries | ✅ Spark batch jobs | — | — |
| **Morph** | — | — | — | ✅ pyiceberg (+ XTable Lambda for Hudi/Delta) |
| **Add Files** | — | ✅ `add_files` Spark job | — | — |
| **Iceberg-Incremental** | ✅ Snapshot-diff queries | ✅ Spark jobs | — | — |

Morph runs entirely within the engine process using pyiceberg (the
Hudi/Delta path additionally invokes the XTable Lambda). Managed Migration runs
entirely within AWS via the S3 Tables Migration API — the Migration Assistant only orchestrates
it. Neither needs an Athena/EMR engine selection.
