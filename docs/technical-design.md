# Technical Design: Migration Strategies

**Audience:** Builders understanding the design approach of each migration
strategy. This document is scoped to *strategy design* — how each strategy
converts a source table to Iceberg and how it stays in sync afterward. It
deliberately excludes the Migration Assistant application/API/UI concerns (pipeline
CRUD, DynamoDB schema, cost tracking, etc.) except where they're load-bearing
for understanding a strategy's correctness or failure modes.

## Contents

1. [Overview](#1-overview)
2. [Rewrite Strategy](#2-rewrite-strategy)
3. [Incremental Strategy](#3-incremental-strategy)
4. [Morph Strategy](#4-morph-strategy)
5. [Add Files Strategy](#5-add-files-strategy)
6. [Managed Migration Strategy](#6-managed-migration-strategy)
7. [Cross-Strategy Comparison](#7-cross-strategy-comparison)

---

## 1. Overview

The Migration Assistant's primary strategy is **Managed Migration**, which moves Parquet
tables into S3 Tables through the AWS S3 Tables Migration API. Beyond that,
the Migration Assistant also implements several self-driven strategies for converting a source
table (Hive/Parquet, Hudi, Delta, CSV, JSON, ORC, Avro, or Iceberg) into an
Apache Iceberg table, each trading off differently between **data
movement**, **cost**, **latency to first query**, and **ongoing sync
capability**:

| Strategy | Data movement | Ongoing sync | Core mechanism |
|---|---|---|---|
| **Managed Migration** | AWS-managed (into S3 Tables) | Yes | Delegate to the AWS S3 Tables Migration API; poll/approve/sync/cutover |
| **Rewrite** | Full copy, once | None (one-shot) | Single CTAS (CREATE TABLE AS SELECT) |
| **Incremental** | Full copy, batched | Yes | Batched INSERT per partition/date-range, then periodic re-sync of updated partitions |
| **Morph** | None — metadata only | Yes | Point Iceberg manifests at existing files; re-point on new commits |
| **Add Files** | None — metadata only | Yes | Register existing Parquet files into Iceberg via `add_files` procedure; re-register on changed partitions |

**Start with Managed Migration (§6)** — it is the flagship path and the one
that does **not** run data movement itself: AWS performs the migration
server-side and the Migration Assistant orchestrates it (submit → plan review → approve →
sync → cutover). The remaining sections cover the self-driven strategies, which
are detailed first below only because they share the common machinery described
next.

Incremental has two implementations behind one `migration_type`: the
S3-event-driven `IncrementalStrategy` for Hive/Parquet/Hudi/Delta sources,
and the snapshot-diff `IcebergIncrementalStrategy` selected automatically
for Iceberg sources (§3.6).

The self-driven strategies are driven by a common **spec** (source, target,
strategy type, Iceberg properties — partition spec, sort order, table
properties) and a common **execution engine** choice (Athena, EMR
Serverless, or — for Morph — no external engine at all, since it runs
in-process). Managed Migration uses the spec too, but its "engine" is the
AWS S3 Tables Migration API rather than Athena/EMR.

---

## 2. Rewrite Strategy

### 2.1 Design intent

Rewrite is the simplest and most universal strategy: it performs a full
logical copy of the source table into a newly created Iceberg table via a
single `CREATE TABLE ... AS SELECT` (CTAS) statement. It is a **one-shot**
operation — `MigrationStrategy._one_shot = True` — meaning once the CTAS
succeeds, the pipeline transitions directly to `completed` with no ongoing
sync phase. This makes it the only strategy in the codebase that does not
implement `detect_changes()`/`execute_sync()`.

Rewrite is the fallback strategy for cases the other two can't handle:
non-columnar formats (CSV/JSON), unpartitioned tables, and
Iceberg-to-Iceberg conversion (e.g. upgrading an S3-Iceberg table to S3
Tables), since it makes no assumptions about source format, partitioning,
or column statistics availability — it only needs the source to be
queryable via SQL.

### 2.2 Data flow

```
Source table (any format)
        │
        │  CREATE TABLE <target> USING iceberg
        │  [PARTITIONED BY (...)] [TBLPROPERTIES (...)]
        │  AS SELECT * FROM <source>
        ▼
Target Iceberg table (new, fully populated)
        │
        │  ALTER TABLE <target> WRITE ORDERED BY ... (if sort_order specified)
        ▼
Optional: table swap — DROP <source>, RENAME <target> → <source's name>
```

The CTAS is a single atomic statement from the query engine's perspective:
either the whole table is created and populated, or nothing is (subject to
the specific engine's CTAS transactionality — see engine sections below).
Because the entire dataset is read and rewritten in one pass, this is the
strategy most likely to see straightforward, uniform data-layout and
Iceberg partitioning/sort-order applied consistently across the whole
table — there's no partial-migration skew possible.

### 2.3 Partition spec and sort order translation

The spec's `iceberg_properties.partition_spec` (list of
`{source_column, transform}`) is translated into the target engine's DDL
syntax:

- `identity` → the raw column name
- `year`/`month`/`day`/`hour` → the corresponding Iceberg hidden-partition
  function, e.g. `month(event_date)`
- `bucket` → `bucket(N, column)`
- `truncate` → `truncate(width, column)`

Sort order (`iceberg_properties.sort_order`) cannot be expressed in a CTAS
`WITH`/`TBLPROPERTIES` clause on either engine, so it's applied as a
**second, separate statement** after the CTAS succeeds:
`ALTER TABLE ... WRITE ORDERED BY col1 ASC NULLS LAST, ...` (EMR/Spark) or
by setting the `write.sort-order` table property directly (Athena, which
has no `WRITE ORDERED BY` DDL). This means sort order is a *write hint* for
future writes/compaction, not something the CTAS's initial data files are
necessarily physically sorted by — reviewers should treat "sort order
configured" and "existing files sorted" as separate guarantees for
Rewrite.

### 2.4 Table swap (optional)

If `strategy.swap_tables` is set and the target table name differs from
the source, Rewrite can perform an in-place swap after a successful copy:
`DROP TABLE <source>` followed by `ALTER TABLE <target> RENAME TO
<source's name>`. This lets the migration be transparent to downstream
consumers that reference the original table name — but it is a
**destructive** step (the original Hive table registration is dropped)
and is only executed after the row-count verification below passes.

### 2.5 Correctness check

After the CTAS, the EMR/Spark path (`rewrite_ctas.py`) explicitly compares
`SELECT COUNT(*)` on source vs. target and raises if they don't match,
failing the job rather than silently completing with missing rows. The
Athena CTAS path relies on the query engine's own atomicity guarantees
(see §2.6) rather than a separate count check.

### 2.6 Ongoing sync

**None by design.** Rewrite has no `detect_changes()` implementation — it
inherits `MigrationStrategy.detect_changes()`, which raises
`NotImplementedError("Strategy does not support ongoing sync")`. Any
source-table changes after the CTAS completes are invisible to the target
until a brand new Rewrite pipeline is run. This is the correctness
tradeoff reviewers should weigh most heavily: Rewrite is appropriate for
static/archival tables or one-time migrations, not for tables under active
write load, since there is no mechanism — automatic or manual — to catch
up afterward within this strategy. (Contrast with Incremental and Morph,
both of which are designed around a permanent sync loop.)

### 2.7 Engine: Athena

- **Mechanism:** a single `CREATE TABLE ... WITH (...) AS SELECT ... FROM
  ...` query submitted to Athena, polled via `GetQueryExecution` until
  `SUCCEEDED`/`FAILED`/`CANCELLED`.
- **Limitations:**
  - **No native `WRITE ORDERED BY`.** Sort order must be applied via a
    `TBLPROPERTIES('write.sort-order' = ...)` `ALTER TABLE`, which is a
    metadata hint only — Athena CTAS does not physically sort output rows
    to match it.
  - **Unsupported source column types** (`smallint`, `tinyint`) must be
    explicitly `CAST` in the `SELECT` list before Iceberg CTAS will accept
    them — handled by `AthenaClient._build_select_columns`, which fetches
    the Glue schema and injects casts only for the specific columns that
    need them.
  - **Query timeouts / result size.** Athena enforces per-query timeouts
    and workgroup-level data-scanned limits; a single CTAS over a very
    large source table can be throttled or time out with no partial
    checkpoint — the whole statement must be resubmitted from scratch.
  - **S3 Tables namespace creation is a separate, sequential step** before
    the CTAS can target an S3 Tables bucket, and is a soft-fail (a
    namespace-creation error is logged and the CTAS is attempted anyway,
    since "already exists" is a common benign case) — meaning a
    misconfigured target namespace surfaces as a CTAS failure, not a
    clearer namespace error.
- **Cost model:** billed per byte scanned by the CTAS query (plus, for
  data actually written, standard S3 storage) — no compute cluster to
  provision or tear down, so Athena has effectively zero startup latency
  for small-to-medium tables, at the cost of the per-query scan pricing
  model applying to the *entire* source table on every run.

### 2.8 Engine: EMR Serverless

- **Mechanism:** the Spark script `rewrite_ctas.py` is submitted as an EMR
  Serverless job run (`entryPoint` = script path, `entryPointArguments` =
  `["--spec", <json>]`). The script builds and executes the same logical
  CTAS via Spark SQL against the Iceberg Spark runtime, then verifies row
  counts and optionally performs the table swap.
- **Limitations:**
  - **Cold-start latency.** Every job run pays EMR Serverless's
    application pre-initialization and Spark session startup time (tens
    of seconds to a couple of minutes depending on configuration) even
    for tiny tables — there is no persistent warm cluster in this design.
  - **Spark property wiring is manual and per-job.** The Hive metastore
    (Glue) client factory, the `glue_catalog`/`s3tablescatalog` Iceberg
    catalog implementations, and warehouse locations are all injected as
    `--conf` values built fresh for every job submission
    (`_build_iceberg_spark_props`/`rewrite.py`'s `_submit_emr`); a
    misconfigured `target_location` or missing `execution_role_arn`
    surfaces only as a job failure, not a pre-flight validation error.
  - **No fine-grained progress.** EMR Serverless job status is polled as a
    single opaque state (`RUNNING`/`SUCCESS`/`FAILED`/`CANCELLED`) — the
    orchestrator has no visibility into how far through the CTAS a
    running job actually is, unlike Incremental's per-batch progress.
  - **Cost visibility** comes from `totalResourceUtilization`
    (vCPU-hours, memory-GB-hours) reported only after the job finishes —
    there's no mid-flight cost signal.
- **Cost model:** billed for vCPU/memory/storage consumed for the
  lifetime of the Spark job (including cold start), regardless of how
  much data is actually scanned — the inverse tradeoff from Athena's
  scan-based pricing.

### 2.9 Engine: none (not applicable)

Rewrite has no in-process/no-engine path. Every Rewrite pipeline must pick
Athena or EMR Serverless — there is no code path where a CTAS-equivalent
operation runs without submitting to one of these two query/compute
engines, because a full table copy inherently requires a distributed query
or compute engine to read all of the source and write the target. (Morph
is the strategy designed around avoiding an engine entirely; see §4.9.)

---

## 3. Incremental Strategy

### 3.1 Design intent

Incremental performs the same logical full-copy as Rewrite, but splits it
into a sequence of smaller **batches** — one per partition group or
date-range — so that (a) very large tables don't require a single
enormous CTAS, and (b) the pipeline can remain **permanently
synchronizing**: after the initial load finishes, Incremental transitions
into an ongoing loop that periodically detects and re-copies only the
partitions that changed in the source since the last sync.

Incremental requires the source table to be **partitioned**
(`validate()` explicitly rejects unpartitioned sources, directing the
caller to use Rewrite instead) — the entire batching and change-detection
design is built around partition boundaries as the unit of work.

### 3.2 Initial load: batch discovery and creation

Unlike Rewrite, Incremental must first determine *what the batches are*
before submitting any data-copying work. This differs by execution engine:

- **Athena path:** batch 0 is a `CREATE TABLE ... WHERE 1=0` (creates an
  empty target with the source's schema/partition spec/table properties —
  no data yet). Batch 1 onward depends on whether the source is
  **date-partitioned** (see §3.3): if so, a **discovery batch** query
  (`SELECT MIN(...), MAX(...) FROM source`) runs first to find the full
  date range, and that range is then sliced into `strategy.batch_size`
  (default 5) roughly-equal date-range batches. If the source is not
  date-partitioned (or has no partitions matching known date patterns), a
  single `__full_insert__` batch is created — meaning "date-partitioned"
  and "generic partitioned" tables take genuinely different code paths for
  the *initial* load, even though both converge to the same ongoing-sync
  design.
- **EMR Serverless path:** the target table is created directly via
  PyIceberg (`ManifestBuilder.ensure_table`) reading the source's schema
  from Glue — no Spark job needed for table creation, because pyiceberg
  can create an empty Iceberg table's metadata without touching data
  files. This is a meaningfully different mechanism from the Athena path's
  `CREATE TABLE ... WHERE 1=0`, though the end result (empty Iceberg
  table with correct schema/partition-spec) is equivalent.

Batches are always processed **strictly sequentially** — the state machine
(`PipelineStateMachine._advance_initial_load`) submits the next pending
batch only after the current running batch completes, and stops
immediately (failing the whole pipeline) on the first batch failure. There
is no parallel batch execution and no partial-completion state below the
batch level: a batch's INSERT either fully succeeds or the pipeline halts.

### 3.3 Date-partition detection

Because most large partitioned tables are partitioned by some form of
date/time, Incremental has a heuristic partition-classifier
(`common.detect_date_partitioning`) that inspects the source's partition
key names and types to classify the table into one of four modes, checked
in this priority order:

1. **`composite_ymd`** — separate `year`/`month`/`day` (or `yr`/`mon`/`dy`)
   integer partition columns. Range filters are built as compound
   `year > … OR (year = … AND month > …) OR …` boolean expressions since
   there's no single sortable column.
2. **`single_date`** — a single partition column typed `date` (including
   Iceberg's `year`/`month`/`day`/`hour` *transform* types, which imply
   the underlying source column is itself a date/timestamp).
3. **`single_timestamp`** — a single column typed `timestamp`.
4. **`single_string`** — a single column whose *name* matches a
   known date-like pattern (`date`, `dt`, `event_date`, `created_at`,
   etc.) or contains `date`/`_dt`, even though its declared type is
   `string`. This is the least reliable heuristic — a string column named
   `date` but not literally a date is possible.

If none of these match, the table is still a valid Incremental target
(partitioned, just not date-partitioned) and falls back to a single
`__full_insert__` batch for the initial load, with **no discovery step**
and, more importantly, **no ongoing per-partition sync capability** on
the Athena path — the date-detection design is directly load-bearing for
the sync mechanism described next.

### 3.4 Batch execution SQL

Each batch is materialized as a `partitions` marker string interpreted by
either the Athena SQL builder or the EMR `incremental_rewrite.py` script.
Marker formats:

- `__full_insert__` / `__unpartitioned__`: `INSERT INTO target SELECT *
  FROM source`
- `__date_range__{mode}__{cols}__{start}__{end}__`: `INSERT INTO target
  SELECT * FROM source WHERE <date filter for [start, end)>`
- `year=2024&month=06` (standard partition key=value string): the EMR
  path treats this as a **rewrite**, not a pure insert — it issues
  `DELETE FROM target WHERE year=2024 AND month=6` followed by `INSERT
  INTO target SELECT * FROM source WHERE year=2024 AND month=6`. This
  DELETE+INSERT pattern (rather than plain INSERT) is what makes
  re-running a batch idempotent and what makes the *ongoing sync* correct
  when a previously-migrated partition is later updated in place (common
  for late-arriving data or corrections) — a partition doesn't
  double-count rows on retry, and a partition whose source data changed
  gets fully replaced rather than appended-to.
- `__discover_date_range__{mode}__{cols}__`: not a data operation at all —
  runs `SELECT MIN(...), MAX(...)` and returns the result for the
  orchestrator to turn into concrete date-range batches (Athena path
  only).

### 3.5 Ongoing sync — Hive/Parquet-style sources

Once all initial-load batches complete, the pipeline's `sync_status`
becomes `in_sync` and enters the permanent sync loop
(`PipelineStateMachine.reconcile`, phases 4-6). The sync mechanism differs
substantially by whether the underlying strategy class is
`IncrementalStrategy` (Hive/Parquet/Hudi/Delta sources via S3 events) or
`IcebergIncrementalStrategy` (Iceberg source via snapshot diffs — §3.6).

For `IncrementalStrategy`, ongoing sync is driven by **S3 object
notifications**, not polling the source table's contents directly:

1. **Tracker creation** (`TrackingManager.create_tracker`, once, during
   `submit()`/setup): a dedicated Lambda function is created per pipeline
   and wired to the source S3 prefix via an S3 bucket notification
   (`s3:ObjectCreated:*` and `s3:ObjectRemoved:*`) scoped to that prefix.
   Deliberately created **before** the initial scan/load begins, so that
   any file that lands during the scan window is still captured as an
   event rather than silently missed.
2. **Event capture** (Lambda, always running): each S3 event is written
   as a row into a DynamoDB `object_tracking` table, keyed by pipeline ID,
   with the event type (`created`/`deleted`), the raw S3 key, and — if the
   key follows Hive-style `partition_col=value` path segments — the
   parsed partition values. Each row carries a 7-day TTL.
3. **Change detection** (`detect_changes`, polled while `in_sync`): simply
   checks whether *any* pending (unprocessed) event rows exist for the
   pipeline. This is a cheap DynamoDB query, not an S3 listing or source
   query — the design assumes the Lambda is the source of truth for "did
   anything change," not periodic re-scanning.
4. **Sync execution** (`execute_sync`, once `out_of_sync` and the sync
   interval has elapsed): pending events are read, classified into
   "new file" vs. "deleted file," and their partition values are grouped.
   For each affected partition, the same DELETE+INSERT pattern from §3.4
   is issued against the target. Events are marked processed only after
   the corresponding rewrite succeeds.
5. **Interval gating:** sync doesn't fire immediately on every detected
   change — `_interval_elapsed()` requires
   `ENGINE_INCREMENTAL_INTERVAL_SECONDS` (default 3600s/1hr) to have
   passed since the pipeline's `updated_at`. This is a deliberate
   batching/cost tradeoff: `out_of_sync` status is surfaced to the UI
   immediately for visibility, but the actual re-sync query work is
   throttled to at most once per interval, so a burst of many small file
   writes to the source collapses into a single sync operation rather
   than one sync per file.

**Design implication for reviewers:** correctness of ongoing sync is
entirely dependent on the Lambda + S3 notification pipeline staying alive
and correctly configured for the lifetime of the pipeline. If the Lambda
is deleted out-of-band, or the bucket notification is overwritten by
another process, changes to the source silently stop propagating with no
built-in self-healing — `detect_changes()` will simply always return
`False` (no pending events found) rather than surface an error.

### 3.6 Ongoing sync — Iceberg sources (`IcebergIncrementalStrategy`)

For migrating an Iceberg-on-S3 table to S3 Tables, sync uses a
fundamentally different (and more precise) mechanism: **Iceberg snapshot
diffing** via PyIceberg, since Iceberg's own metadata already tracks
exactly which data files were added/removed between any two snapshots —
no S3 event capture is needed at all.

1. **Snapshot cursor:** at submission, the source's current snapshot ID
   (`snapshot_before`) is recorded. This is the baseline the target was
   built from.
2. **Change detection** (`detect_changes`): compares the source's *latest*
   snapshot ID against the tracked cursor — a cheap PyIceberg metadata
   read, no data scan.
3. **Convergence loop** (`_reconcile_convergence`/multi-phase design —
   `catch_up` → `converging` → `in_sync`): this is the most involved part
   of the whole Incremental design. Because new snapshots can continue
   landing on the source *while* the target is catching up on earlier
   ones, a single diff-and-sync pass is not guaranteed to fully close the
   gap. The design instead **iterates**: diff source snapshots
   [cursor → latest], compute affected partitions
   (`IcebergMetadataReader.get_affected_partitions`, which compares the
   set of data files present at two snapshot IDs and maps file→partition
   value), issue `INSERT OVERWRITE` for exactly those partitions, advance
   the cursor, and repeat — capped at `_MAX_CONVERGENCE_ITERATIONS` (10)
   before forcing a transition to `in_sync` regardless of full
   convergence, to guarantee the loop terminates even under continuous
   heavy write load on the source.
4. **Delete safety check:** before assuming an append-only fast path,
   `has_deletes_between()` inspects snapshot summaries for
   `deleted-data-files`/`deleted-records` counts or `delete`/`overwrite`/
   `replace` operation types. If a source snapshot in the range being
   caught up on cannot be resolved (e.g. it was expired from the
   snapshot list, which Iceberg does asynchronously as a lifecycle
   policy), the check **fails safe by assuming deletes occurred** —
   preferring an unnecessary partition rewrite over silently missing a
   deletion.
5. **Atomicity:** partition catch-up uses `INSERT OVERWRITE`
   (replace-partition-contents), which is atomic per statement in Athena/
   Iceberg — a partition is either fully replaced or untouched, never
   half-written, even under concurrent readers.

This design is meaningfully more robust than the S3-event-based sync in
§3.5 because it has no separate infrastructure (Lambda, bucket
notifications, DynamoDB tracking table) that can drift out of sync with
reality — snapshot metadata *is* the source of truth and is read fresh on
every check. The tradeoff is that it only applies when the source is
itself already an Iceberg table.

### 3.7 Engine: Athena

- **Mechanism:** batch 0 (`CREATE TABLE ... WHERE 1=0`) and every
  subsequent batch (`INSERT INTO ... SELECT ... WHERE <filter>`, or
  `DELETE`+`INSERT` for partition-key-based rewrite batches) are submitted
  as individual Athena queries, polled sequentially to completion before
  the next is submitted.
- **Limitations:**
  - **Per-INSERT overhead compounds across many small batches.** Because
    batches are strictly sequential (§3.2), a table sliced into a large
    number of date-range batches pays Athena's per-query startup/planning
    latency once per batch rather than once total — unlike a single
    Rewrite CTAS.
  - **The `DELETE FROM target WHERE ...` used for partition-key rewrite
    batches (EMR path) has no Athena equivalent** in the batch-submission
    code shown here — Athena's batch path relies on `INSERT OVERWRITE`
    semantics for the Iceberg-incremental convergence loop (§3.6) instead
    of explicit DELETE, so the exact DELETE+INSERT idempotency guarantee
    described in §3.4 is an EMR/Spark-path-specific mechanism, not a
    universal one — reviewers verifying idempotency should check which
    engine a given pipeline uses.
  - **Discovery batches consume a full query slot** just to compute
    MIN/MAX — on a very large source table this can itself be an
    expensive full scan if the date column isn't well-suited to
    partition pruning.
  - Same S3 Tables namespace pre-creation caveat as Rewrite (§2.7).
- **Cost model:** same as Rewrite (§2.7) — pay-per-byte-scanned — but now
  paid **once per batch** for the initial load, and again **once per
  sync cycle** for whatever partitions are dirty. A table with many small,
  frequent partition-level changes can accumulate more total Athena scan
  cost over time than a single Rewrite would have, in exchange for never
  going stale.

### 3.8 Engine: EMR Serverless

- **Mechanism:** the initial empty-table creation goes through PyIceberg
  directly (no Spark job — §3.2). Each subsequent batch invokes
  `incremental_rewrite.py` as an EMR Serverless job run with the
  partition markers for that batch as a script argument.
- **Limitations:**
  - **Same cold-start-per-job cost as Rewrite (§2.8), multiplied by batch
    count.** Where Rewrite pays EMR startup latency once, Incremental
    pays it once per batch *and* once per sync cycle — this is the most
    significant EMR-specific limitation for Incremental specifically,
    since the whole point of batching is many small jobs rather than one
    large one.
  - **Discovery marker parsing happens Spark-side.** The
    `__discover_date_range__` marker's `DISCOVERY_RESULT:{json}` is
    written to stdout and must be scraped from the job's driver log by
    the orchestrator — there is no structured return channel from an EMR
    Serverless job run, only logs and exit state. This is more fragile
    than Athena's `GetQueryResults` API for the same discovery step.
  - **Ongoing sync via S3 events still applies identically** (§3.5) — the
    engine choice only affects how the batch's data-movement SQL/Spark
    runs, not the DynamoDB/Lambda-based change-detection design.
- **Cost model:** same vCPU/memory/storage-hours-for-job-lifetime model as
  Rewrite, incurred repeatedly — once per initial-load batch, once per
  sync cycle thereafter.

### 3.9 Engine: none (not applicable)

Like Rewrite, Incremental's actual data-copying step (the INSERT/DELETE
SQL) always requires Athena or EMR Serverless — there is no in-process
data-movement path. The one exception is the **target table creation**
step on the EMR path, which uses PyIceberg directly with no query engine
(§3.2) — but this creates only empty table metadata, not data, so it
doesn't constitute a no-engine data-migration path in the way Morph does.

---

## 4. Morph Strategy

### 4.1 Design intent

Morph is the only strategy that performs **zero data movement**. Instead
of copying rows, it builds Iceberg manifest metadata that points directly
at the source's *existing* Parquet files (for native Parquet sources) or
translates existing Hudi/Delta commit metadata into equivalent Iceberg
metadata (for Hudi/Delta sources, via Apache XTable). The underlying
Parquet files never move or get rewritten — only a layer of Iceberg
metadata is created/maintained on top of them.

This makes Morph dramatically cheaper and faster than Rewrite/Incremental
for large tables (no data scan, no data write — see cost comparison in
`working/xtable-lambda-integration.md`: roughly 1000× cheaper than
Rewrite for the Hudi/Delta path), at the cost of tighter constraints:
Morph only supports **columnar source formats** (`parquet`, `hudi`,
`delta` — explicitly enforced in `validate()`) and only an **Iceberg-on-S3
target**, never S3 Tables, because S3 Tables requires data to physically
reside in the managed S3 Tables bucket — pointing at files in an arbitrary
external location isn't possible for that target type.

Morph runs **entirely in-process** within the engine (via
PyIceberg) — there is no Athena query and no EMR job for the Parquet
path. The Hudi/Delta path is the one exception, delegating the actual
metadata translation to an external XTable Lambda invocation (§4.6),
though even that is not an Athena/EMR "engine" in the Migration Assistant's sense.

### 4.2 Native Parquet path — initial load

For `parquet` sources, the initial load is a two-phase, purely
metadata-driven process, executed as two sequential batches
(`__morph_setup__`, `__morph_initial_load__`) both run in-process (no
external job):

**Setup batch:**
1. **Create the S3 event tracker first** — same Lambda +
   bucket-notification mechanism as Incremental (§3.5), and deliberately
   created *before* scanning, for the same reason: to avoid a race where a
   file written during the scan is missed by both the scan and the
   tracker.
2. **Fetch schema from Glue** (`_fetch_glue_schema`) — columns and
   partition keys, translated into an Iceberg `Schema` via
   `schema_from_glue` (Hive/Glue type strings mapped to Iceberg types,
   including decimal precision/scale parsing and a documented distinction
   between Hive's timezone-naive `timestamp` and explicit
   `timestamptz`/`timestamp with time zone`).
3. **Create the (empty) target Iceberg table** via PyIceberg
   (`ManifestBuilder.ensure_table`) using that schema and an
   identity-transform partition spec matching the source's partition
   columns.

**Initial-load batch — the actual metadata-only "migration":**
1. **List every data file** under the source S3 prefix
   (`morph.scanner.list_data_files`), filtering out non-data markers
   (`_SUCCESS`, `_temporary`, hidden dot-files, and Iceberg/Delta metadata
   subdirectories that might exist under a nested location) and
   zero-byte objects.
2. For each file, **read the Parquet footer** in place
   (`read_parquet_footer`) to extract row count and per-column statistics
   (compressed size, value counts, null counts, and — for single-row-group
   files only — raw min/max bounds; INT96-encoded timestamp columns are
   explicitly skipped for bounds since their physical representation
   isn't Iceberg-compatible). This is the critical technique that makes
   Morph possible: Iceberg manifests need per-file statistics to support
   query planning (predicate pushdown, file pruning), and those
   statistics can be read directly from files Spark/Hive already wrote,
   without re-processing the data.
3. Build one Iceberg `DataFile` entry per source file — constructed
   manually (**not** via PyIceberg's built-in `add_files()`) because that
   built-in has two specific incompatibilities the manual path works
   around: it doesn't tolerate Hive-style partition columns that are
   absent from the Parquet file itself (encoded only in the S3 path, per
   Hive convention) without a schema-compatibility patch, and it doesn't
   pass through the downcast option needed for legacy nanosecond-precision
   timestamp columns written by older Spark/Hive versions.
4. **Commit files in chunks** (default 500 files per chunk, configurable
   via `ENGINE_MORPH_WORKERS` for the *parallelism* of footer reads,
   not chunk size) via `fast_append` transactions — one Iceberg snapshot
   per chunk, not one per file and not a single snapshot for the whole
   table. This is a deliberate **restartability** design: if the process
   fails partway through a very large file list, the next attempt
   compares already-committed file paths (via `table.scan().plan_files()`)
   against the full list and skips files already present in a prior
   snapshot, resuming rather than restarting from zero.
5. **Drain gap events**: after the scan completes, any S3 events the
   tracker Lambda captured *during* the scan window are read from
   DynamoDB and reconciled — files that turned out to already be in the
   initial scan are simply marked processed (no-op); genuinely new files
   that arrived after the scan snapshot was effectively taken are added
   via an incremental commit; deletions are removed. This closes the race
   window opened by creating the tracker before scanning (step 1 above).

### 4.3 Why manifest-only migration is correct

The core design assumption reviewers should verify is: **an Iceberg
manifest entry's per-file statistics, once written, are treated as
immutable facts about that file.** Since Morph never rewrites the
underlying Parquet files, this is safe *only if* the source files
themselves are never mutated in place after being scanned — which holds
for Hive/Spark's write-once file semantics (a "file" in these formats is
never edited, only added or superseded by directory-level partition
overwrite) but would be a correctness hazard for any write pattern that
does in-place file mutation. This assumption is implicit in the design
rather than actively enforced/validated by the Migration Assistant.

### 4.4 Ongoing sync — native Parquet path

Sync reuses the same S3-event-tracking infrastructure as Incremental
(§3.5) — same Lambda-per-pipeline, same DynamoDB `object_tracking` table,
same `detect_changes()`-checks-for-pending-events design — but the
**sync execution differs fundamentally** because there's no data to move:

1. **Change detection**: identical mechanism to Incremental — pending
   event rows exist ⇒ `out_of_sync`.
2. **Sync execution** (`_execute_parquet_sync`): pending events are
   classified into new-file and deleted-file lists (parsing partition
   values from the event or, if absent, re-deriving them from the S3 key
   path). New files are added via `add_files_incremental` — which is the
   *same* footer-read + manual-DataFile-construction + `fast_append`
   logic as the initial load (§4.2 step 2-4), just applied to a small
   incremental file list instead of the full table. Deleted files are
   removed via `commit_incremental`, which issues an Iceberg `overwrite`
   transaction that calls `delete_data_file()` for each removed file's
   manifest entry.
3. Because each sync cycle is itself a `fast_append`/`overwrite`
   transaction, **every sync produces a new, atomic, independently
   time-travel-queryable Iceberg snapshot** — this is a byproduct of the
   design (not something Rewrite or even most of Incremental's paths
   offer as cleanly) worth calling out to reviewers evaluating
   auditability.

### 4.5 Hudi/Delta path — why it's structurally different

Morph's `validate()` allows `hudi` and `delta` as source formats, but the
*mechanism* is entirely different from the Parquet path above, because
Hudi/Delta tables already have their own metadata layer (Hudi's commit
timeline / Delta's `_delta_log`) that describes which files are logically
part of the table at any point in time — scanning raw files directly (as
the Parquet path does) would not correctly account for that layer (e.g.
superseded/compacted files in Hudi, or files removed by a Delta `VACUUM`
that are still physically present but logically deleted).

Instead of the Migration Assistant's own scanner, this path delegates to **Apache
XTable** running as a separate AWS Lambda function
(`working/xtable-lambda-integration.md`), which understands Hudi/Delta
commit semantics natively and translates them directly into Iceberg
metadata (manifests, `metadata.json`) — still with no data file
movement, but via a different metadata-translation engine than the one
the engine implements itself for Parquet.

### 4.6 Hudi/Delta initial load

1. Single batch, marker `__xtable_convert__`, executed synchronously
   in-process (`_execute_xtable_batch`) — no polling loop, because the
   Lambda invocation itself is synchronous (`InvocationType:
   RequestResponse`) and XTable conversions complete in seconds for
   typical table sizes (see cost/duration table in the XTable design doc:
   ~5s for 10GB/80 files up to ~60s for 1TB/8000 files).
2. Before invoking XTable, any **stale metadata from a previous failed
   attempt** is cleaned up (`_cleanup_stale_metadata`) — otherwise a retry
   could hit Iceberg schema-evolution errors from a half-written prior
   metadata state.
3. XTable Lambda is invoked with `sync_mode: FULL` — translates the
   *entire* commit history into Iceberg metadata in one pass, not just
   the latest state.
4. The resulting Iceberg table is registered in the Glue Catalog
   (`_register_iceberg_table`) so it becomes queryable by any
   Iceberg-aware engine, exactly as if it had been created natively.
5. The current Hudi commit timestamp / Delta version is recorded as
   `last_synced_commit` metadata on the pipeline — this is the cursor the
   ongoing sync mechanism (next section) will compare against.

### 4.7 Ongoing sync — Hudi/Delta path

This is a **commit-cursor comparison**, not S3-event-driven — no Lambda
tracker is created for Hudi/Delta sources at all, because Hudi's
`.hoodie/` timeline and Delta's `_delta_log/` are themselves authoritative
records of every commit, made available to the Migration Assistant via native Rust
bindings (`hudi-rs`, `delta-rs`) with a **fallback to direct S3 listing**
of the respective metadata directory if those libraries are unavailable
in the runtime (`commit_tracker.py` — e.g. listing `.hoodie/*.commit` /
`*.deltacommit` files and taking the lexicographically-latest timestamp,
or listing `_delta_log/*.json` and taking the highest zero-padded version
number).

1. **Change detection** (`_detect_xtable_changes`/`has_new_commits`):
   compares the source's current latest commit marker against
   `last_synced_commit`. For Hudi this is a string timestamp comparison;
   for Delta, an integer version comparison.
2. **Sync execution**: invokes the same XTable Lambda, but with
   `sync_mode: INCREMENTAL` — XTable's own incremental-sync capability
   translates only the commits since the last sync, not the full timeline
   again (this is the mechanism responsible for the strategy remaining
   cheap on an ongoing basis, not just on initial load).
3. After a successful incremental sync, the Glue table's
   `metadata_location` pointer is explicitly updated
   (`_update_iceberg_metadata_pointer`) to the newest Iceberg
   `metadata.json` XTable produced, and `last_synced_commit` is advanced
   to the new cursor value.
4. Same interval-gating as Incremental (§3.5 step 5) applies once
   `out_of_sync` is detected, before the sync actually executes.

**Design implication for reviewers:** this path's correctness depends
entirely on an **externally deployed, separately maintained XTable Lambda
function being configured and reachable** (`ENGINE_XTABLE_LAMBDA_ARN`).
If unconfigured, `XTableClient.is_configured()` returns false and both
initial load and every subsequent sync attempt fail outright — there is
no fallback to the Migration Assistant's native Parquet-footer scanning for Hudi/Delta
sources, since (per §4.5) that scanning approach would not correctly
interpret Hudi/Delta's file-level semantics.

### 4.8 Engine: Athena / EMR Serverless

**Not applicable to Morph's core mechanism.** Morph is explicitly designed
to avoid needing a query/compute engine for the migration itself — the
`Strategy.execution_engine` field on the spec is not consulted anywhere in
`morph.py`'s logic. Reviewers should not expect Morph pipelines to have an
"Athena vs EMR" choice at all; the only external compute involved is the
XTable Lambda invocation for Hudi/Delta sources (§4.5-4.7), which is a
fixed part of the Morph design, not an engine option.

One indirect interaction worth noting: once a Morph target table exists,
it is a completely standard Iceberg table in Glue — any query engine
(Athena, EMR, Trino, Spark, etc.) can read it identically to a
Rewrite/Incremental-produced table. The "no engine" property applies only
to how Morph *builds* the table, not to how it's subsequently queried.

### 4.9 Engine: none (this is the default/only mode)

This is Morph's defining characteristic and the reason it's discussed
separately from Rewrite/Incremental's "no engine" sections: for Morph,
**no-engine is not a limited/fallback mode — it is the entire design**.
All in-process work (S3 listing, Parquet footer reads, PyIceberg manifest
construction and commits) runs inside the same Python process as the
engine itself, using a `ThreadPoolExecutor` for I/O-bound
parallelism (footer reads) rather than a distributed compute framework.

- **What this buys:** no cold-start latency, no per-job compute cost, and
  (per §4.2 step 4) restartable, chunked commits that don't require an
  external job orchestrator to track progress — progress is persisted
  directly to the pipeline/batch records after each chunk.
- **What this costs / limits:**
  - **Vertical, not horizontal, scaling.** Footer-read parallelism is
    bounded by `ENGINE_MORPH_WORKERS` (default 32) threads on a single
    process/host — there is no path to scaling this out across multiple
    workers for an extremely large file count, unlike Athena/EMR which
    can add more distributed compute.
  - **Long-running in-process work happens on a background thread**
    (`_run_initial_load_background`) within the same application process
    serving the rest of the Migration Assistant's API — a very large initial scan (many
    hundreds of thousands of files) ties up in-process resources for the
    duration, rather than being isolated in an external job.
  - **No engine-level retry/checkpoint semantics** — restartability is
    entirely the responsibility of the chunk-and-compare-existing-files
    logic in §4.2 step 4, not any built-in engine feature.

---

## 5. Add Files Strategy

### 5.1 Design intent

Add Files is a **metadata-only, one-shot** strategy for converting
partitioned Hive/Parquet tables to Iceberg without any data movement —
conceptually similar to Morph's Parquet path (§4.2), but using Iceberg's
native `add_files` procedure (`system.add_files()`) on EMR Serverless
rather than building manifests manually via PyIceberg in-process. This
makes it structurally simpler than Morph (delegates all metadata
construction to Iceberg's own runtime, which handles schema inference,
file statistics, and manifest writing natively) at the cost of requiring
an EMR Serverless job per batch.

Add Files only supports **partitioned Parquet sources** (explicitly
enforced in `validate()`) and only targets **Iceberg-on-S3** (not S3
Tables), since the `add_files` procedure registers existing data files by
location — data must remain accessible at its original S3 path, which S3
Tables does not allow. There is **no ongoing sync** — the strategy
registers the existing file set and transitions to `in_sync` permanently
(no Lambda tracker, no DynamoDB events, no change detection).

### 5.2 Data flow

```
Source Parquet table (partitioned)
        │
        │  Batch 0: CREATE TABLE <target> via Spark SQL
        │           (empty Iceberg table with source schema)
        ▼
Target Iceberg table (empty)
        │
        │  Batches 1..N: CALL system.add_files(
        │    table => '<target>',
        │    source_table => '`parquet`.`<source_location>/partition=value`',
        │    partition_filter => map('partition_col', 'value'))
        │  — one batch per chunk of partitions
        ▼
Target Iceberg table (files registered, metadata complete,
                      no data copied — files remain in original location)
```

### 5.3 Batching model

Add Files uses a partition-driven batching model (distinct from
Incremental's date-range batching):

1. **Partition discovery**: all partition values are fetched from the Glue
   catalog (`get_partitions` paginated call) at submit time.
2. **Batch sizing**: partitions are chunked into batches of configurable
   size (default derived from the agent's `recommend_add_files_batch_size`
   tool: roughly `total_partitions / 10`, clamped to `[10, 500]`). Each
   batch gets a subset of partition values.
3. **Batch 0**: creates the empty target Iceberg table via Spark SQL
   (`CREATE TABLE ... USING iceberg`), reading the source schema from the
   Parquet data files (not from Glue's declared schema — avoiding the
   Glue-vs-physical type mismatch that affects other strategies).
4. **Batches 1..N**: each invokes Iceberg's `system.add_files()` procedure
   once per partition value in the chunk. The procedure scans the
   specified source path's Parquet footers, constructs `DataFile` entries
   with full column statistics, and commits them atomically as a new
   Iceberg snapshot.

Batches are processed **strictly sequentially** by the state machine
(same as Incremental — no parallel execution).

### 5.4 Per-partition `add_files` calls

A key design detail: `system.add_files()` is called **once per partition**
rather than once for all partitions in a batch. This is a correctness
requirement — the Iceberg `add_files` procedure's `partition_filter`
argument only accepts a single partition value map per invocation. The
batch boundary (how many partitions are grouped into one EMR job run) is a
cost/parallelism knob, while the per-partition invocation within each job
is a fixed constraint imposed by the procedure's API contract.

The Spark script (`add_files.py`) iterates over the batch's partition list
and calls `add_files` once per entry, committing each as a separate
Iceberg snapshot. If any single partition fails, the batch fails —
previously-registered partitions within the same batch remain committed
(they were separate snapshots), and on retry the strategy re-submits the
entire batch (including already-committed partitions — `add_files` on an
already-registered partition is idempotent: Iceberg detects duplicate file
paths and skips them).

### 5.5 Source schema handling

Batch 0 (create table) reads the schema directly from the source Parquet
files rather than from the Glue catalog. This avoids the
`SchemaColumnConvertNotSupportedException` that other strategies can hit
when Glue's declared types disagree with the physical data (e.g. a column
declared as `int` in Glue that is physically stored as INT64/long). The
`add_files` procedure itself also reads from the physical files, so the
target table's schema and the registered files' actual types are
guaranteed consistent by construction.

### 5.6 Ongoing sync

Add Files uses the same **S3 object tracking** infrastructure as
Incremental (§3.5) and Morph's Parquet path (§4.4): a per-pipeline Lambda
+ S3 bucket notification captures all file-creation/deletion events under
the source prefix, writing them to a DynamoDB `object_tracking` table.

1. **Tracker creation** (during `submit()`, before initial load): a
   dedicated Lambda is deployed and wired to the source S3 prefix —
   deliberately created before the first batch runs, so any files added
   during the initial-load window are captured rather than silently missed.
2. **Change detection** (`detect_changes()`): checks for any pending
   (unprocessed) event rows in DynamoDB for this pipeline — a cheap query
   identical to Incremental/Morph.
3. **Sync execution** (`execute_sync()`): pending events are grouped by
   partition (parsing partition values from the S3 key paths). For each
   affected partition, a single EMR Serverless job is submitted with
   `--operation sync_partitions`, which runs Iceberg's `add_files`
   procedure with **overwrite** semantics — this deletes the existing
   Iceberg data file registrations for the affected partitions and
   re-registers the current file set from S3, effectively handling both
   new files and deleted files in one pass.
4. Events are marked processed optimistically after job submission — if
   the job fails, the next sync cycle re-detects the same partitions
   (events that arrived after the failed job will be new pending rows).
5. Same interval-gating (`ENGINE_ADD_FILES_INTERVAL_SECONDS`, default
   3600s) as Incremental — detected changes trigger `out_of_sync` status
   immediately for UI visibility, but the actual sync job is throttled to
   at most once per interval.

**Design implication:** same as Incremental §3.5 — correctness of ongoing
sync depends on the Lambda + S3 notification pipeline staying alive. If
the Lambda is deleted or the bucket notification overwritten,
`detect_changes()` stops finding new events and the target silently goes
stale.

### 5.7 Engine: EMR Serverless (only)

- **Mechanism:** each batch (including batch 0 — the create-table step)
  is submitted as an EMR Serverless Spark job (`add_files.py` script),
  with `--spec` and `--operation` / `--partitions` arguments.
- **Why EMR only (no Athena):** Athena does not support the
  `system.add_files()` procedure — it is a Spark-only Iceberg runtime
  feature that requires the Iceberg Spark extensions to be loaded. The
  strategy's `validate()` explicitly rejects `execution_engine=athena`.
- **Spark properties:** identical Iceberg catalog configuration to the
  Incremental EMR path — Hive metastore (Glue) for source resolution,
  `glue_catalog` for the Iceberg target, S3 Tables catalog if applicable.
  Additionally, `spark.sql.catalogImplementation=hive` is set to ensure
  the source Parquet table resolves correctly through the Glue metastore.
- **Cold-start cost:** same per-job EMR Serverless startup overhead as
  Rewrite/Incremental — paid once per batch. For a table with many
  partitions split across N batches, this is N cold starts, which is the
  primary EMR cost contributor (the actual `add_files` calls are very
  fast, as they read only Parquet footers, not row data).
- **Cost model:** vCPU/memory/storage-hours for the job lifetime, but
  since `add_files` only reads footer metadata (not row data), actual
  compute consumption per batch is minimal — the dominant cost is EMR's
  minimum billing granularity per job (typically 1 minute), multiplied by
  the number of batches.

### 5.8 Retry behavior

- **Batch 0 (create table) on retry:** drops and recreates the target
  table from scratch (safe — the table was empty or partially populated
  with file registrations that will be re-registered).
- **Batches 1..N on retry:** stale DynamoDB batch records from a previous
  failed run are cleared (`delete_from_number`), and the batch is
  resubmitted with a fresh file list. Already-committed partitions (from
  a partially successful prior run) are handled idempotently by
  `add_files` (duplicate file paths are skipped).

### 5.9 Comparison to Morph (Parquet path)

| Aspect | Add Files | Morph (Parquet path) |
|---|---|---|
| Data movement | None | None |
| Metadata construction | Iceberg's native `add_files` procedure (Spark runtime) | Manual PyIceberg `DataFile` construction in-process |
| Ongoing sync | Yes (S3 event-driven, delete + re-add_files per partition) | Yes (S3 event-driven, file-level add/remove) |
| Engine required | EMR Serverless | None (in-process) |
| Per-file statistics | Extracted by Iceberg runtime (native, complete) | Extracted manually from Parquet footers (skips INT96 bounds) |
| Partitioned source required | Yes | No |
| Source format constraint | Parquet only | Parquet, Hudi, Delta |
| Target constraint | Iceberg-on-S3 only | Iceberg-on-S3 only |
| Cost for initial registration | EMR cold-start x N batches (footer-only compute) | Near-zero (in-process Python, no external job) |
| Cost for ongoing sync | EMR cold-start per sync cycle (one job for all dirty partitions) | Near-zero (in-process PyIceberg commit) |
| Correctness of schema | Reads from physical files (guaranteed consistent) | Reads from Glue (may disagree with physical, reconciled) |
| Sync granularity | Partition-level (entire partition re-registered) | File-level (individual files added/removed) |

Add Files and Morph are complementary rather than competing: Add Files
delegates metadata construction entirely to Iceberg's own procedure (less
custom code, guaranteed-correct schema and statistics), while Morph does
it in-process (no external engine needed, finer file-level sync
granularity). Both have ongoing sync via S3 event tracking — the key
difference is that Add Files syncs at partition granularity (re-registers
an entire partition) while Morph syncs at file granularity (adds/removes
individual files). Add Files is also restricted to partitioned Parquet
sources, while Morph additionally supports unpartitioned tables and
Hudi/Delta formats (via XTable).

---

## 6. Managed Migration Strategy

### 6.1 Design intent

Managed Migration (`s3tables_managed`, `strategies/s3tables_managed.py`) is
structurally unlike the other strategies: it performs **no data movement of
its own and runs no Athena query or EMR job**. Instead it delegates the
entire migration to the AWS **S3 Tables Migration API** — AWS reads the
source, writes the target S3 Tables table, produces compatibility/migration
reports, and runs its own migration state machine. The Migration Assistant's role is to
*submit*, *poll*, *approve*, *sync*, and *cut over*, projecting the API's
migration state onto the pipeline's `sync_status`.

It applies only to **Hive-style Parquet sources** migrating to an **S3
Tables** target. `validate()` enforces: `source_format == parquet`,
`target_type == s3_tables`, a real S3 Tables *table bucket* ARN
(`arn:aws:s3tables:<region>:<account>:bucket/<name>` — a plain
`arn:aws:s3:::…` bucket ARN or bare name is rejected), a migration IAM role
(`ENGINE_S3TABLES_MIGRATION_ROLE_ARN`, assumed by the service), and a
reports location (the common logs bucket, `ENGINE_LOGS_BUCKET`, under
`migration-reports/` — the migration API requires a report output
configuration).

Because AWS owns the data movement, this strategy is **not on the
`PipelineStateMachine` path** (unlike Rewrite/Incremental/Morph/AddFiles). It
implements its own `reconcile()` that re-derives state from the API on every
sweep — the engine routes `s3tables_managed` pipelines to the strategy's
`reconcile()` directly (see solution-design §4.3).

### 6.2 Lifecycle and API operations

The thin client is `engines/s3tables_migration.py` (a boto3 wrapper, one
method per API operation). The strategy drives this lifecycle:

```
submit()   → StartTableMigration(reviewMigrationPlan=true)
             → AWS builds a compatibility plan and PAUSES
             → sync_status = awaiting_review
approve()  → UpdateTableMigration(requestedState=queued)   [operator action]
             → AWS runs the migration
reconcile()→ GetTableMigration (polled each sweep) → project state → sync_status
execute_sync() → SyncTableMigration(tableARN)     [ongoing deltas, unattended]
complete() → CompleteTableMigration            [cutover]
cancel()   → UpdateTableMigration(requestedState=cancelled)
```

The API state is projected onto the Migration Assistant's `sync_status` via
`core/s3tables_state_map.py` (e.g. the service's review-pause →
`awaiting_review`; terminal states → `failed`/completed). Post-load states
are `initial_load_complete` / `sync_complete` (never the Incremental-style
`out_of_sync`), reflecting that AWS, not the Migration Assistant, owns the migration clock.

### 6.3 Plan review and approval (the distinguishing phase)

The one operator interaction unique to this strategy is **plan review**.
`submit()` always starts the migration with `reviewMigrationPlan=true`, so
AWS first produces a **compatibility plan** (what it will migrate, with any
compatibility findings) and *pauses* rather than moving data. The Migration Assistant
surfaces this as `sync_status = awaiting_review` and shows the blocking
findings (per category) and links to the table and object compatibility
reports (written to the reports bucket) in the UI. An operator
reviews it and **approves** (`approve()` → `UpdateTableMigration(queued)`),
at which point AWS proceeds. `approve()` is guarded to only act when
`sync_status == awaiting_review`. Ongoing syncs run with
`reviewMigrationPlan=false` — unattended, no review pause.

### 6.4 Request assembly
Before building the request, `submit()` makes sure the target namespace exists
in the table bucket (`GetNamespace`, then `CreateNamespace` if it is missing),
because `StartTableMigration` requires it. The engine task role needs
`s3tables:GetNamespace` and `s3tables:CreateNamespace`; the stack grants both. A
failure here fails the pipeline with a "Could not create target namespace"
message.

`submit()` builds the `StartTableMigration` request from the spec:

- `roleARN` — the migration role the service assumes.
- `sourceType` — `glueTable` (a separate request member; it is no longer part of `source`).
- `destinationTable` — table bucket ARN + namespace + name, with
  `properties` (`writeFormat: parquet`, `icebergFormatVersion`,
  `compressionCodec`). Nothing else: see "What is deliberately not sent".
- `source` — a Glue table ref whose `catalogId` must be the **caller's**
  account.
- `dataMigration.migrationPolicies` — `unmatchedSourceFieldsPolicy` (default
  `fail`) and `incompatibleValuesPolicy` (default `fail`); both are set from the
  app's Managed Migration settings.
- `reviewMigrationPlan` — always `true` on start.
- `migrationOutput` — **required** on both Start and Sync. One general-purpose
  bucket (an S3 bucket ARN) plus one key prefix per report:
  `<prefix>/table-compatibility/`, `<prefix>/object-compatibility/` and
  `<prefix>/object-migration/`, all under `migration-reports/<pipeline-id>/` in
  the common logs bucket. The service reports each written report's `key`,
  checksum and (for the two compatibility reports) blocking `invalidFindings`
  back on `GetTableMigration`.

**What is deliberately not sent.** The destination is minimal on purpose: the
service infers the schema and the partitioning from the Glue source.

- *Partition spec* — a table cannot be repartitioned as part of a migration, and
  the API has no `partitionSpec` member. The partition transforms the AI
  assistant may recommend for other strategies do not apply here; the target
  keeps the source's partitioning.
- *Sort order* — sorting is not supported during migration, and migration
  compaction is binpack-only (the API rejects `sort`, `z-order` and `auto`; the
  compaction block carries no sort keys). A sort order recorded on the plan is
  kept on the pipeline but is **not** sent; the strategy logs that it was
  skipped.
- *Schema* — there is no `schema` member either, so the former before-call hook
  that spliced a schema into the request body is gone.

The Migration API ships in boto3/botocore 1.43.124 and later (§6.6).

**What the response gives back.** `GetTableMigration` echoes the request
(`roleARN`, `source`, `dataMigration`, `migrationOutput`) and adds
`planningStats` (`objectsScanned`, `objectsPlanned`, `partitionCount`),
`migrationStats` (`objectsMigrated`, `objectsFailedMigration`,
`objectsPendingMigration`) and, on failure, `failureReason {code, detail}`.
Blocking findings are counted per category — `access`, `metadata`, `data`,
`internal` — on each compatibility report; the engine sums the two reports into
one per-category count for the review card. Per-column type changes are no
longer returned inline: they exist only in the table compatibility report file
in S3, which the UI links to.

### 6.5 Region pinning

For `start`/`sync`, the client region is pinned to the **target table
bucket's region** (parsed from `target_bucket_arn`), because the S3 Tables
Migration API requires the caller's region to match the table bucket's
region. `ENGINE_S3TABLES_REGION` only influences other S3 Tables calls,
not this region-match requirement.

### 6.6 Runtime prerequisite — boto3 version
The S3 Tables Migration API is part of the official `s3tables` service
model in boto3/botocore **1.43.124** and later. botocore validates every
request against its bundled model client-side, so an older botocore
rejects `start`/`sync` with `ParamValidationError` ("Unknown parameter").
The engine pins `boto3>=1.43.124`.

### 6.7 Correctness notes for reviewers

- **AWS owns the data path.** Row-level correctness of the migration is the
  service's responsibility; the Migration Assistant surfaces AWS's compatibility findings
  (plan review) but does not independently verify row counts here. The
  separate on-demand **validation** subsystem (V1–V8, solution-design §4.10)
  is the mechanism for post-migration source/target parity checks.
- **No batches / no state machine.** Resume and resume-from-failure are
  handled by the strategy, not the batch machinery — a failed AWS migration
  cannot be resumed in place, so `resume_from_failure()` starts a fresh
  migration rather than continuing one.
- **Policy defaults are strict.** `fail` / `fail` stop the migration on a
  schema or value mismatch. Choose `drop` (unmatched source fields) or
  `setNull` (incompatible values) under Settings > Managed Migration to skip the
  offending field or value instead.

---

## 7. Cross-Strategy Comparison

| Aspect | Managed Migration | Rewrite | Incremental | Morph | Add Files |
|---|---|---|---|---|---|
| Data movement | AWS-managed (into S3 Tables) | Full copy | Full copy, batched | None | None |
| Source formats | Parquet only | Any | Parquet, Hudi, Delta, ORC, Avro, CSV, JSON | Parquet, Hudi, Delta only | Parquet only |
| Requires partitioned source | No | No | Yes | No | Yes |
| Target: S3 Tables | **S3 Tables only** | Yes | Yes | **No** (Iceberg-on-S3 only) | **No** (Iceberg-on-S3 only) |
| Ongoing sync | Yes — `SyncTableMigration` (AWS-managed) | **None** | Yes — S3 events (Hive-style) or snapshot diff (Iceberg source) | Yes — S3 events (Parquet) or XTable commit cursor (Hudi/Delta) | Yes — S3 events (delete + re-add_files per partition) |
| Requires external tracker infra | None (AWS-managed) | No | Lambda + DynamoDB (unless Iceberg source) | Lambda + DynamoDB (Parquet) or XTable Lambda (Hudi/Delta) | Lambda + DynamoDB |
| Athena engine | N/A | ✅ | ✅ | N/A | **No** (EMR only) |
| EMR Serverless engine | N/A | ✅ | ✅ | N/A | ✅ (required) |
| No-engine (in-process) | N/A — AWS S3 Tables Migration API | Not possible | Table-creation only (EMR path) | **Default and only mode** | Not possible |
| On the state machine | **No** (own `reconcile()`) | Yes | Yes | Yes | Yes |
| Operator approval step | **Yes** (plan review → approve) | No | No | No | No |
| Failure isolation unit | Whole migration (AWS-managed) | Whole table (single CTAS) | Single batch/partition | Single file-chunk commit | Single partition within a batch |
| New Iceberg snapshot per sync | AWS-managed | N/A (one-shot) | Per sync cycle (partition rewrite) | Per sync cycle (file add/remove) | Per sync cycle (partition re-registration) |

**Summary for reviewers:** these strategies are not interchangeable
implementations of the same idea — they represent different
correctness/cost tradeoffs that follow directly from the sync mechanism
each is built around. Rewrite's simplicity comes from explicitly
forfeiting ongoing sync. Incremental's ongoing-sync correctness rests on
either an external Lambda/DynamoDB event pipeline staying healthy
(Hive-style sources) or Iceberg's own snapshot metadata being available
and unexpired (Iceberg sources) — the latter is structurally more robust.
Morph's near-zero cost rests on the strong assumption that source files
are immutable once written, and — for Hudi/Delta — on a fully external
XTable Lambda being correctly deployed and kept available for the
lifetime of every such pipeline. Add Files offers the same zero-data-
movement property as Morph with ongoing sync (via the same S3 event
infrastructure as Incremental), but trades Morph's in-process execution
for an EMR-Serverless-per-sync-cycle cost model — in exchange, it
delegates all manifest construction to Iceberg's own native `add_files`
procedure, resulting in less custom code and guaranteed-correct schema
and statistics handling. Managed Migration is the outlier: it offloads the
entire data path to the AWS S3 Tables Migration API, so its "correctness"
is AWS's to own — the Migration Assistant's design responsibility narrows to request
assembly, state projection, and the plan-review/approval gate, with
post-migration parity left to the validation subsystem.

---

## Concurrent status updates

Two kinds of writers change a pipeline's status at the same time: the reconcile
loop (up to `ENGINE_RECONCILE_WORKERS` threads, each working from a snapshot)
and the API routes that handle user actions. Three rules keep a stale reconcile
from undoing a user action:

- **Sticky terminal states.** `completed` and `cancelled` cannot be overwritten
  by a status write unless the caller passes `force=True`. Nothing in the
  engine currently needs to leave those states (retry only applies to
  `failed`).
- **Compare-and-set during reconcile.** The reconcile loop runs each pipeline's
  reconcile inside `expect_status(...)` (`db/status_guard.py`). Every status
  write in it only lands if the stored status still matches what the reconcile
  last saw or wrote. DynamoDB uses a `ConditionExpression`; SQL adds the check
  to the `WHERE` clause. A refused write returns the pipeline's current state.
- **Per-pipeline lock.** `core/pipeline_locks.py` holds one in-process lock per
  pipeline. Reconcile takes it without waiting and skips the pipeline for that
  sweep if a user action holds it; it then re-reads the pipeline (consistent
  read) and skips it if the status moved on since the sweep listed it. User
  actions wait up to `ENGINE_ACTION_LOCK_WAIT_SECONDS` and then proceed
  anyway, because some reconciles block for minutes (synchronous XTable
  invokes, inline Athena polls); the first two rules still protect the result.
  The lock is in-process, which matches the engine's single task
  (`DesiredCount: 1`). Running several engine tasks would need a shared lease.

On the app side, plan and table actions only record what the engine confirms.
After each call the app reads the status the engine reports (re-reading it when
the engine refuses, so "already paused" counts as paused):

| Action | Counts as done when the engine reports |
|---|---|
| Finalize (complete) | `completed` |
| Pause | anything but `running` |
| Resume | anything but `paused` |
| Cancel | `cancelled`, `completed`, `failed`, or the pipeline is gone |
| Delete plan / remove table | the pipeline is gone, or `completed` |

If any table is not confirmed, the plan keeps its previous status, nothing is
deleted, and the request returns HTTP 409 listing the tables and the reason.
Tables that were confirmed keep their new state, so retrying the action only
has to deal with the rest.
