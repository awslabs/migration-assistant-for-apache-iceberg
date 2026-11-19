# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Migration cost and time estimation tools."""

import os
import math
from strands import tool

REGION = os.getenv("AWS_REGION", "us-east-1")

# EMR Serverless pricing (approximate, us-east-1)
EMR_VCPU_HOUR = 0.052624  # per vCPU-hour
EMR_MEMORY_GB_HOUR = 0.0057785  # per GB-hour
EMR_STORAGE_GB_HOUR = 0.000111  # per GB-hour

# Athena pricing (approximate, us-east-1): billed per TB of data SCANNED, with
# a 10 MB per-query minimum. A rewrite CTAS scans the full source table once, so
# the estimate is driven by table size rather than compute-hours.
ATHENA_PER_TB_SCANNED = 5.0  # USD per TB scanned
ATHENA_MIN_SCAN_BYTES = 10 * 1024 * 1024  # 10 MB per-query minimum


@tool
def estimate_migration_cost(
    total_size_gb: float,
    file_format: str,
    migration_type: str,
    partition_count: int = 0,
    file_size_skew: dict = None,
    execution_engine: str = "emr_serverless",
) -> dict:
    """Estimate migration time and cost.

    Cost model depends on the execution engine:
      - emr_serverless: compute-hours (vCPU/memory/storage-hour pricing).
      - athena: data SCANNED (per-TB), driven by estimated table size — a
        rewrite CTAS scans the source once, so cost scales with size, not
        compute time.

    Managed Migration (s3tables_managed) is handled by the AWS S3 Tables
    Migration service, which does not expose a pre-migration price; its cost
    estimate is reported as N/A (null).

    Args:
        total_size_gb: Total data size in GB
        file_format: Source format (csv, parquet, delta, hudi, etc.)
        migration_type: rewrite, incremental, morph, add_files, inplace, shadow, or s3tables_managed
        partition_count: Number of partitions (0 if unpartitioned)
        file_size_skew: File size distribution metrics from estimate_table_size (optional). Used to adjust throughput estimates when many small files increase per-file overhead.
        execution_engine: Execution engine the plan uses — "athena" or "emr_serverless". Determines the cost model. Ignored for s3tables_managed.
    """
    # Managed Migration has no pre-migration price or duration estimate, so
    # report N/A (null) instead of falling through to the EMR compute model.
    if migration_type == "s3tables_managed":
        return {
            "migration_type": "s3tables_managed",
            "total_size_gb": round(total_size_gb, 2),
            "source_format": file_format,
            "estimated_time_minutes": None,
            "estimated_time_hours": None,
            "estimated_cost_usd": None,
            "note": "Managed Migration is run by the AWS S3 Tables Migration "
                    "service; a pre-migration cost/time estimate is not "
                    "available (N/A).",
        }

    if migration_type == "inplace":
        return {
            "migration_type": "inplace",
            "estimated_time_minutes": 5,
            "estimated_cost_usd": 0.0,
            "note": "In-place migration only updates metadata. No data movement.",
        }

    if migration_type == "shadow":
        return {
            "migration_type": "shadow",
            "estimated_time_minutes": 2,
            "estimated_cost_usd": 0.0,
            "note": "Shadow creates a view layer. No data movement or compute cost.",
        }

    throughput_gb_per_min = {
        "parquet": 2.0,
        "orc": 1.8,
        "delta": 1.5,
        "hudi": 1.2,
        "avro": 1.0,
        "json": 0.5,
        "csv": 0.4,
    }
    rate = throughput_gb_per_min.get(file_format.lower(), 0.5)

    # Apply throughput penalty when file size skew indicates many small files.
    # Small files increase per-file overhead (S3 GET latency, task scheduling)
    # which reduces effective throughput.
    throughput_penalty = 1.0
    penalty_reason = None
    if file_size_skew:
        pct_under_16mb = file_size_skew.get("pct_under_16mb", 0)
        if pct_under_16mb > 50:
            # Majority small files: 40% throughput reduction
            throughput_penalty = 0.6
            penalty_reason = f"{pct_under_16mb:.0f}% of files under 16 MB — significant per-file overhead"
        elif pct_under_16mb > 20:
            # Moderate small-file presence: 20% throughput reduction
            throughput_penalty = 0.8
            penalty_reason = f"{pct_under_16mb:.0f}% of files under 16 MB — moderate per-file overhead"

    effective_rate = rate * throughput_penalty

    base_time_min = total_size_gb / effective_rate
    # Add overhead for partitioning
    partition_overhead = max(partition_count * 0.1, 0)
    total_time_min = base_time_min + partition_overhead + 5  # 5 min startup

    # Athena engine: cost is driven by DATA SCANNED, not compute-hours. A
    # rewrite CTAS scans the source table once, so cost scales with estimated
    # table size (per-TB pricing, 10 MB per-query minimum). Time still uses the
    # throughput model above. We do NOT size EMR executors here.
    if execution_engine == "athena":
        scanned_bytes = max(int(total_size_gb * (1024 ** 3)), ATHENA_MIN_SCAN_BYTES)
        scanned_tb = scanned_bytes / (1024 ** 4)
        cost = scanned_tb * ATHENA_PER_TB_SCANNED
        return {
            "migration_type": "rewrite",
            "execution_engine": "athena",
            "total_size_gb": round(total_size_gb, 2),
            "source_format": file_format,
            "estimated_time_minutes": round(total_time_min, 1),
            "estimated_time_hours": round(total_time_min / 60, 2),
            "estimated_cost_usd": round(cost, 2),
            "athena_config": {
                "data_scanned_gb": round(scanned_bytes / (1024 ** 3), 3),
                "price_per_tb_scanned_usd": ATHENA_PER_TB_SCANNED,
            },
            "throughput_adjustment": {
                "base_rate_gb_per_min": rate,
                "effective_rate_gb_per_min": round(effective_rate, 2),
                "penalty_factor": throughput_penalty,
                "reason": penalty_reason,
            } if throughput_penalty < 1.0 else None,
            "assumptions": [
                "Athena cost = data scanned x per-TB price; a rewrite CTAS "
                "scans the source once, so cost scales with table size",
                f"~${ATHENA_PER_TB_SCANNED:.0f}/TB scanned (us-east-1), "
                "10 MB per-query minimum",
                f"Throughput: ~{effective_rate:.1f} GB/min for {file_format}"
                + (f" (adjusted from {rate} due to small files)" if throughput_penalty < 1.0 else ""),
                "Excludes columnar/partition pruning — actual scan may be lower",
            ],
        }

    # Estimate EMR Serverless resources
    # Rule of thumb: 4 vCPUs, 16GB memory per executor
    parallelism = min(max(math.ceil(total_size_gb / 10), 2), 100)
    vcpus = parallelism * 4
    memory_gb = parallelism * 16
    storage_gb = total_size_gb * 1.5  # temp storage

    hours = total_time_min / 60
    cost = (
        vcpus * hours * EMR_VCPU_HOUR
        + memory_gb * hours * EMR_MEMORY_GB_HOUR
        + storage_gb * hours * EMR_STORAGE_GB_HOUR
    )

    return {
        "migration_type": "rewrite",
        "execution_engine": "emr_serverless",
        "total_size_gb": round(total_size_gb, 2),
        "source_format": file_format,
        "estimated_time_minutes": round(total_time_min, 1),
        "estimated_time_hours": round(total_time_min / 60, 2),
        "estimated_cost_usd": round(cost, 2),
        "emr_config": {
            "executors": parallelism,
            "vcpus_per_executor": 4,
            "memory_gb_per_executor": 16,
            "total_vcpus": vcpus,
            "total_memory_gb": memory_gb,
        },
        "throughput_adjustment": {
            "base_rate_gb_per_min": rate,
            "effective_rate_gb_per_min": round(effective_rate, 2),
            "penalty_factor": throughput_penalty,
            "reason": penalty_reason,
        } if throughput_penalty < 1.0 else None,
        "assumptions": [
            f"Throughput: ~{effective_rate:.1f} GB/min for {file_format}"
            + (f" (adjusted from {rate} due to small files)" if throughput_penalty < 1.0 else ""),
            f"Parallelism: {parallelism} executors",
            "EMR Serverless pricing (us-east-1)",
            "Includes 5 min startup overhead",
        ],
    }


@tool
def recommend_migration_type(
    file_format: str,
    total_size_gb: float,
    is_partitioned: bool,
    has_active_writers: bool = False,
    avg_file_size_mb: float = None,
    last_modified_days_ago: int = None,
    compression: str = None,
    file_size_skew: dict = None,
    hudi_table_type: str = None,
    hudi_has_pending_compactions: bool = False,
) -> dict:
    """Recommend the optimal migration type based on table characteristics.

    Args:
        file_format: Source format (csv, parquet, delta, hudi, etc.)
        total_size_gb: Estimated total size in GB
        is_partitioned: Whether the table is partitioned
        has_active_writers: Whether the table has active write workloads. If not explicitly set, inferred from last_modified_days_ago.
        avg_file_size_mb: Average file size in MB (from estimate_table_size details). If provided, used to detect small-file problems that favor rewrite.
        last_modified_days_ago: Days since the most recent data file was modified (from estimate_table_size details). Used to infer active writers when has_active_writers is not explicitly set.
        compression: Compression codec used by source files (e.g. none, gzip, snappy, zstd, lz4, bzip2). If 'none' or uncompressed, rewrite gains additional benefit from applying compression during migration.
        file_size_skew: File size distribution metrics from estimate_table_size (optional). Contains is_skewed, coefficient_of_variation, pct_under_16mb, p10_mb, p50_mb, p90_mb, etc.
        hudi_table_type: Hudi table type from get_hudi_table_metadata — "COPY_ON_WRITE" or "MERGE_ON_READ". Only set for Hudi tables.
        hudi_has_pending_compactions: Whether Hudi MoR table has pending compactions (from get_hudi_table_metadata or get_hudi_compaction_status).
    """
    fmt = file_format.lower()
    recommendations = []

    # Infer has_active_writers from data freshness if not explicitly set
    if not has_active_writers and last_modified_days_ago is not None:
        if last_modified_days_ago < 60:
            has_active_writers = True

    # Format-based eligibility
    supports_morph = fmt in ("parquet", "delta", "hudi")

    # Hudi MoR tables cannot be morphed — log files must be rewritten
    if fmt == "hudi" and hudi_table_type == "MERGE_ON_READ":
        supports_morph = False

    # Small-file detection: files under 16 MB are suboptimal for Iceberg.
    # Morph preserves existing layout, so small files remain small files.
    # Use file_size_skew for nuanced detection when available.
    has_small_files = avg_file_size_mb is not None and avg_file_size_mb < 16

    # Enhanced small-file detection using distribution metrics.
    # Even if the average is healthy, a bimodal distribution with many
    # small files still warrants rewrite over morph.
    has_skewed_file_sizes = False
    file_skew_detail = None
    if file_size_skew:
        has_skewed_file_sizes = file_size_skew.get("is_skewed", False)
        pct_under_16mb = file_size_skew.get("pct_under_16mb", 0)
        # If >20% of files are under 16 MB, treat as a small-file problem
        # even when the average looks healthy (bimodal distribution)
        if pct_under_16mb > 20 and not has_small_files:
            has_small_files = True
            file_skew_detail = (
                f"File sizes are skewed: {pct_under_16mb:.0f}% of files are under 16 MB "
                f"(p10={file_size_skew.get('p10_mb', 0):.1f} MB, "
                f"p50={file_size_skew.get('p50_mb', 0):.1f} MB, "
                f"p90={file_size_skew.get('p90_mb', 0):.1f} MB, "
                f"CV={file_size_skew.get('coefficient_of_variation', 0):.2f})"
            )
        elif has_skewed_file_sizes:
            file_skew_detail = (
                f"File size distribution is uneven "
                f"(p10={file_size_skew.get('p10_mb', 0):.1f} MB, "
                f"p50={file_size_skew.get('p50_mb', 0):.1f} MB, "
                f"p90={file_size_skew.get('p90_mb', 0):.1f} MB, "
                f"CV={file_size_skew.get('coefficient_of_variation', 0):.2f})"
            )

    # Compression detection: uncompressed data benefits significantly from
    # rewrite because compression can be applied during migration, reducing
    # storage costs and improving read performance.
    uncompressed_codecs = ("none", "uncompressed", "raw", "")
    is_uncompressed = (
        compression is not None
        and compression.lower().strip() in uncompressed_codecs
    )

    # Estimate storage savings from compression during rewrite.
    # Typical compression ratios for columnar formats with zstd/snappy:
    #   Parquet uncompressed → compressed: ~2-4x reduction
    #   CSV uncompressed → Parquet+compressed: ~5-10x reduction
    if is_uncompressed:
        if fmt in ("parquet", "orc"):
            compression_benefit_ratio = 0.35  # ~65% storage savings
        elif fmt in ("csv", "json", "avro"):
            compression_benefit_ratio = 0.15  # ~85% savings (format change + compression)
        else:
            compression_benefit_ratio = 0.50  # conservative estimate
        estimated_compressed_gb = total_size_gb * compression_benefit_ratio
    else:
        compression_benefit_ratio = None
        estimated_compressed_gb = None

    if avg_file_size_mb is not None:
        if avg_file_size_mb < 1:
            avg_file_size_display = f"{avg_file_size_mb * 1024:.0f} KB"
        else:
            avg_file_size_display = f"{avg_file_size_mb:.1f} MB"
    else:
        avg_file_size_display = None

    time_est = total_size_gb / 1.0  # rough estimate: ~1 GB per minute

    if has_active_writers:
        # Active writers: strategy depends on file health and table size
        if supports_morph and not has_small_files and is_partitioned and total_size_gb > 50:
            # Active writers + healthy files + large partitioned table → morph (primary), incremental (fallback)
            recommendations.append({
                "type": "morph",
                "confidence": "high",
                "reason": (
                    f"Table is {total_size_gb:.0f} GB in {fmt} format with healthy file sizes"
                    f"{f' (avg {avg_file_size_display})' if avg_file_size_display else ''} "
                    f"and active writers (last modified {last_modified_days_ago or 0} days ago). "
                    "Morph converts to Iceberg by generating metadata only — no data movement, "
                    "completing in minutes at zero compute cost. Writers can continue uninterrupted."
                ),
            })
            recommendations.append({
                "type": "incremental",
                "confidence": "medium",
                "reason": (
                    f"Incremental migration is an alternative if morph is not feasible. "
                    f"Processes {total_size_gb:.0f} GB in batches over time, minimizing downtime "
                    "while writers continue on the source."
                ),
            })
        elif has_small_files and is_partitioned and total_size_gb > 50:
            # Active writers + small files + large table → incremental (primary), rewrite (fallback)
            recommendations.append({
                "type": "incremental",
                "confidence": "high",
                "reason": (
                    f"Table has active writers (last modified {last_modified_days_ago or 0} days ago) "
                    f"and is {total_size_gb:.0f} GB across partitions with small files "
                    f"(avg {avg_file_size_display}). "
                    "Incremental migration processes batches over time, compacting small files "
                    "while minimizing downtime."
                ),
            })
            recommendations.append({
                "type": "rewrite",
                "confidence": "medium",
                "reason": (
                    f"Full rewrite is an alternative that compacts all small files in one pass. "
                    f"Estimated ~{time_est:.0f} min for {total_size_gb:.0f} GB. "
                    "Requires a cutover window where writers are paused."
                ),
            })
        else:
            # Active writers + small/unpartitioned table → rewrite (primary)
            recommendations.append({
                "type": "rewrite",
                "confidence": "high",
                "reason": (
                    f"Table has active writers (last modified {last_modified_days_ago or 0} days ago). "
                    f"Rewrite creates a clean Iceberg copy. "
                    f"Estimated ~{time_est:.0f} min for {total_size_gb:.0f} GB. "
                    "Requires a cutover window where writers are paused."
                ),
            })
    elif supports_morph and not has_small_files:
        # No active writers + columnar format + healthy file sizes: morph
        # But if data is uncompressed, rewrite gains significant benefit
        if is_uncompressed:
            # Uncompressed columnar data: rewrite is preferred to apply compression
            recommendations.append({
                "type": "rewrite",
                "confidence": "high",
                "reason": (
                    f"Table is {total_size_gb:.0f} GB in {fmt} format with no active writers"
                    f"{f' (last modified {last_modified_days_ago} days ago)' if last_modified_days_ago else ''}, "
                    f"but source data is uncompressed. "
                    f"Rewrite applies compression (e.g. zstd), reducing storage from "
                    f"~{total_size_gb:.0f} GB to ~{estimated_compressed_gb:.0f} GB "
                    f"(~{(1 - compression_benefit_ratio) * 100:.0f}% savings). "
                    "This improves query performance and reduces S3 costs."
                ),
            })
            # Morph as lower-confidence alternative
            recommendations.append({
                "type": "morph",
                "confidence": "medium",
                "reason": (
                    "Morph is possible (zero compute cost, minutes to complete) "
                    "but would preserve uncompressed files. Post-migration compaction "
                    "with compression would be needed to realize storage savings."
                ),
            })
        elif has_skewed_file_sizes and file_skew_detail:
            # File sizes are skewed but average is above threshold — morph is
            # still viable but rewrite would normalize the layout
            recommendations.append({
                "type": "morph",
                "confidence": "medium",
                "reason": (
                    f"Table is {total_size_gb:.0f} GB in {fmt} format with no active writers"
                    f"{f' (last modified {last_modified_days_ago} days ago)' if last_modified_days_ago else ''}. "
                    "Morph converts to Iceberg by generating metadata only — no data movement. "
                    f"However, {file_skew_detail}. "
                    "Post-migration compaction recommended to normalize file sizes."
                ),
            })
            recommendations.append({
                "type": "rewrite",
                "confidence": "medium",
                "reason": (
                    f"Rewrite normalizes the skewed file layout into optimally-sized Iceberg files "
                    f"in a single pass. {file_skew_detail}. "
                    f"Estimated ~{time_est:.0f} min for {total_size_gb:.0f} GB."
                ),
            })
        else:
            recommendations.append({
                "type": "morph",
                "confidence": "high",
                "reason": (
                    f"Table is {total_size_gb:.0f} GB in {fmt} format with no active writers"
                    f"{f' (last modified {last_modified_days_ago} days ago)' if last_modified_days_ago else ''}. "
                    "Morph converts to Iceberg by generating metadata only — no data movement, "
                    "completing in minutes at zero compute cost."
                ),
            })
            # Rewrite as fallback
            recommendations.append({
                "type": "rewrite",
                "confidence": "low",
                "reason": (
                    f"Full table rewrite is an alternative if you want to re-optimize "
                    f"file sizes or apply new partition transforms. "
                    f"Estimated ~{time_est:.0f} min for {total_size_gb:.0f} GB."
                ),
            })
    elif supports_morph and has_small_files:
        # No active writers but small files: rewrite to compact
        compression_note = ""
        if is_uncompressed:
            compression_note = (
                f" Additionally, source data is uncompressed — rewrite applies compression, "
                f"reducing storage from ~{total_size_gb:.0f} GB to ~{estimated_compressed_gb:.0f} GB "
                f"(~{(1 - compression_benefit_ratio) * 100:.0f}% savings)."
            )
        skew_note = ""
        if file_skew_detail:
            skew_note = f" {file_skew_detail}."
        recommendations.append({
            "type": "rewrite",
            "confidence": "high",
            "reason": (
                f"Table is {total_size_gb:.0f} GB in {fmt} format with no active writers, "
                f"but avg file size is {avg_file_size_display} (under 16 MB threshold). "
                "Rewrite compacts into optimally-sized Iceberg files, improving query performance. "
                f"Morph would preserve the small-file problem.{skew_note}{compression_note}"
            ),
        })
        # Morph as lower-confidence alternative
        recommendations.append({
            "type": "morph",
            "confidence": "low",
            "reason": (
                f"Morph is possible but would preserve the small-file layout "
                f"(avg {avg_file_size_display}). Post-migration compaction would be needed."
            ),
        })
    else:
        # Non-columnar format: rewrite is the only option
        compression_note = ""
        if is_uncompressed:
            compression_note = (
                f" Source data is uncompressed — rewrite to Iceberg with compression "
                f"reduces storage from ~{total_size_gb:.0f} GB to ~{estimated_compressed_gb:.0f} GB "
                f"(~{(1 - compression_benefit_ratio) * 100:.0f}% savings)."
            )
        recommendations.append({
            "type": "rewrite",
            "confidence": "high",
            "reason": (
                f"Full table migration creates a clean Iceberg copy. "
                f"Estimated ~{time_est:.0f} min for {total_size_gb:.0f} GB. "
                f"Rewrite is the only option for {fmt} format.{compression_note}"
            ),
        })

    # Managed Migration candidate (Parquet only). Valid only for an S3 Tables
    # target, which this tool does not know; the agent decides based on the plan.
    managed_candidate = None
    if fmt == "parquet":
        if has_small_files:
            managed_candidate = {
                "type": "s3tables_managed",
                "confidence": "high",
                "reason": (
                    f"Parquet table ({total_size_gb:.2f} GB"
                    f"{f', avg {avg_file_size_display}' if avg_file_size_display else ''}). "
                    "When targeting Amazon S3 Tables, Managed Migration is preferred over a "
                    "manual rewrite. "
                    "Only applicable when the target is S3 Tables."
                ),
            }
        else:
            managed_candidate = {
                "type": "s3tables_managed",
                "confidence": "high",
                "reason": (
                    f"Parquet table ({total_size_gb:.2f} GB). When targeting Amazon S3 Tables, "
                    "Managed Migration is preferred. "
                    "Only applicable when the target is S3 Tables."
                ),
            }
    # The primary is chosen from table characteristics before the managed
    # candidate is added; the agent elevates it when the target is S3 Tables.
    primary = recommendations[0]

    if managed_candidate is not None:
        # Offer as an additional candidate the agent can select for S3 Tables.
        recommendations.append(managed_candidate)

    result = {
        "file_format": fmt,
        "total_size_gb": round(total_size_gb, 2),
        "is_partitioned": is_partitioned,
        "has_active_writers": has_active_writers,
        "avg_file_size_mb": avg_file_size_mb,
        "avg_file_size_display": avg_file_size_display,
        "has_small_files": has_small_files,
        "has_skewed_file_sizes": has_skewed_file_sizes,
        "file_size_skew": file_size_skew,
        "last_modified_days_ago": last_modified_days_ago,
        "compression": compression,
        "is_uncompressed": is_uncompressed,
        "hudi_table_type": hudi_table_type,
        "hudi_has_pending_compactions": hudi_has_pending_compactions,
        "recommended_type": primary["type"],
        "confidence": primary["confidence"],
        "all_recommendations": recommendations,
    }

    # Include compression benefit details when applicable
    if is_uncompressed and estimated_compressed_gb is not None:
        result["compression_benefit"] = {
            "current_size_gb": round(total_size_gb, 2),
            "estimated_compressed_gb": round(estimated_compressed_gb, 2),
            "estimated_savings_pct": round((1 - compression_benefit_ratio) * 100, 0),
            "note": (
                "Rewrite applies compression (recommended: zstd for Iceberg). "
                "Actual savings depend on data cardinality and distribution."
            ),
        }

    return result


@tool
def recommend_partition_transform(
    partition_keys: list,
    column_types: dict,
    total_size_gb: float,
    partition_skew: dict = None,
) -> dict:
    """Recommend Iceberg partition transforms based on existing partitions.

    For partitioned tables: recommends transforms for existing partition
    columns, using skew data to decide between identity and bucket.

    For unpartitioned tables: analyzes the full column schema and table
    size to propose partition candidates. Only recommends partitioning
    when the table is large enough to benefit (>100 GB). Prefers
    timestamp/date columns, then high-cardinality ID columns.

    Args:
        partition_keys: List of partition column names (empty if unpartitioned)
        column_types: Dict mapping column name to data type (full schema)
        total_size_gb: Total table size in GB
        partition_skew: Partition skew metrics from estimate_table_size (optional). Contains is_skewed, coefficient_of_variation, max_to_median_ratio, etc.
    """
    transforms = []
    is_skewed = partition_skew.get("is_skewed", False) if partition_skew else False
    cv = partition_skew.get("coefficient_of_variation", 0) if partition_skew else 0
    suggest_partitioning = False

    if partition_keys:
        # --- Partitioned table: recommend transforms for existing keys ---
        for key in partition_keys:
            col_type = column_types.get(key, "string").lower()

            if "timestamp" in col_type or "date" in col_type:
                # Time-based partitioning
                if total_size_gb > 1000:
                    transforms.append({"column": key, "transform": "day", "reason": "Large table — daily partitions for optimal file sizes"})
                elif total_size_gb > 100:
                    transforms.append({"column": key, "transform": "month", "reason": "Medium table — monthly partitions balance file count and size"})
                else:
                    transforms.append({"column": key, "transform": "year", "reason": "Smaller table — yearly partitions avoid too many small files"})
            elif "int" in col_type or "long" in col_type:
                if is_skewed:
                    # Skewed integer partition — bucket transform will redistribute
                    # Choose bucket count based on severity
                    if cv > 1.5:
                        bucket_count = 64
                    elif cv > 1.0:
                        bucket_count = 32
                    else:
                        bucket_count = 16
                    max_median = partition_skew.get("max_to_median_ratio", 0)
                    transforms.append({
                        "column": key,
                        "transform": f"bucket({bucket_count})",
                        "reason": (
                            f"Partition skew detected (CV={cv:.2f}, largest partition is "
                            f"{max_median:.1f}x the median). Bucket transform redistributes "
                            f"data evenly across {bucket_count} buckets."
                        ),
                    })
                else:
                    # Even distribution — identity is fine
                    transforms.append({
                        "column": key,
                        "transform": "identity",
                        "reason": (
                            "Numeric partition with even distribution "
                            f"(CV={cv:.2f}) — current layout is balanced, identity transform preserves it."
                        ),
                    })
            else:
                if is_skewed:
                    transforms.append({
                        "column": key,
                        "transform": "bucket(16)",
                        "reason": (
                            f"String partition with skewed distribution (CV={cv:.2f}). "
                            "Bucket transform redistributes hot partitions."
                        ),
                    })
                else:
                    transforms.append({"column": key, "transform": "identity", "reason": "String/categorical with even distribution — keep as-is (identity transform)"})
    else:
        # --- Unpartitioned table: propose partition candidates from schema ---
        # Only suggest partitioning for tables large enough to benefit.
        # Below 100 GB, Iceberg metadata alone provides sufficient pruning
        # and adding partitions risks creating too many small files.
        if total_size_gb >= 100:
            suggest_partitioning = True
            candidates = _find_partition_candidates(column_types, total_size_gb)
            transforms = candidates

    return {
        "partition_keys": partition_keys,
        "total_size_gb": round(total_size_gb, 2),
        "partition_skew_detected": is_skewed,
        "suggest_partitioning": suggest_partitioning,
        "recommended_transforms": transforms,
        "note": (
            "Table is unpartitioned. Partition candidates suggested based on "
            "column types and table size. Adding partitioning during migration "
            "avoids a costly second rewrite later."
            if suggest_partitioning
            else "Recommendations based on measured partition size distribution. Review against your query patterns."
        ),
    }


def _find_partition_candidates(column_types: dict, total_size_gb: float) -> list:
    """Identify partition candidates from a full column schema.

    Priority order:
    1. Timestamp/date columns → time-based transform (best for most workloads)
    2. Columns with 'date' or 'time' in the name (even if typed as string)
    3. Integer/long columns with 'id' in the name → bucket transform

    Returns at most 2 candidates to avoid over-partitioning.
    """
    time_candidates = []
    date_named_candidates = []
    id_candidates = []

    for col_name, col_type in column_types.items():
        col_type_lower = col_type.lower()
        col_name_lower = col_name.lower()

        # Priority 1: Typed as timestamp or date
        if "timestamp" in col_type_lower or "date" in col_type_lower:
            if total_size_gb > 1000:
                transform = "day"
                reason = f"Large unpartitioned table ({total_size_gb:.0f} GB) — daily partitions on '{col_name}' enable time-range pruning"
            elif total_size_gb > 500:
                transform = "month"
                reason = f"Unpartitioned table ({total_size_gb:.0f} GB) — monthly partitions on '{col_name}' balance file count and pruning"
            else:
                transform = "month"
                reason = f"Unpartitioned table ({total_size_gb:.0f} GB) — monthly partitions on '{col_name}' avoid too many small files while enabling pruning"
            time_candidates.append({
                "column": col_name,
                "transform": transform,
                "reason": reason,
                "candidate_priority": 1,
            })

        # Priority 2: Name suggests time even if type is string
        elif any(hint in col_name_lower for hint in ("date", "time", "day", "month", "year", "created", "updated")):
            date_named_candidates.append({
                "column": col_name,
                "transform": "identity",
                "reason": (
                    f"Column '{col_name}' (type: {col_type}) appears time-related by name. "
                    "If values are date-like strings (e.g. '2024-01-15'), identity transform "
                    "provides partition pruning. Verify column values before applying."
                ),
                "candidate_priority": 2,
            })

        # Priority 3: Integer ID columns → bucket
        elif ("int" in col_type_lower or "long" in col_type_lower) and "id" in col_name_lower:
            bucket_count = 32 if total_size_gb > 500 else 16
            id_candidates.append({
                "column": col_name,
                "transform": f"bucket({bucket_count})",
                "reason": (
                    f"Unpartitioned table ({total_size_gb:.0f} GB) with integer column '{col_name}'. "
                    f"Bucket({bucket_count}) distributes data evenly for parallel reads."
                ),
                "candidate_priority": 3,
            })

    # Combine and limit to top 2 candidates
    all_candidates = time_candidates + date_named_candidates + id_candidates
    all_candidates.sort(key=lambda c: c["candidate_priority"])

    return all_candidates[:2]


@tool
def recommend_batch_size(
    total_size_gb: float,
    partition_count: int,
    file_format: str,
    partition_skew: dict = None,
) -> dict:
    """Recommend batch_size for incremental strategy initial load.

    The batch_size controls how many partitions are processed per
    EMR Serverless job during the initial load. Balances job duration,
    cost, and failure blast radius.

    When partition skew data is available, uses the largest partition size
    (rather than the average) to ensure no single batch exceeds the time
    target.

    Args:
        total_size_gb: Total table size in GB
        partition_count: Number of source partitions
        file_format: Source format (parquet, hudi, delta, etc.)
        partition_skew: Partition skew metrics from estimate_table_size (optional). Contains max_partition_gb, median_partition_gb, is_skewed, etc.
    """
    if partition_count == 0:
        return {
            "recommended_batch_size": 1,
            "partition_count": 0,
            "total_size_gb": round(total_size_gb, 2),
            "reason": "Unpartitioned table — single batch covers the full table.",
        }

    avg_partition_gb = total_size_gb / max(partition_count, 1)

    # Adjust target based on format throughput
    throughput = {"parquet": 2.0, "orc": 1.8, "delta": 1.5,
                  "hudi": 1.2, "avro": 1.0, "json": 0.5, "csv": 0.4}
    rate = throughput.get(file_format.lower(), 0.5)

    # Target ~30 min per batch job (excluding startup)
    target_batch_gb = rate * 30

    # If skew data is available, use the max partition size to ensure
    # the largest partition fits within a single batch's time budget.
    is_skewed = partition_skew.get("is_skewed", False) if partition_skew else False
    max_partition_gb = partition_skew.get("max_partition_gb", avg_partition_gb) if partition_skew else avg_partition_gb

    if is_skewed and max_partition_gb > avg_partition_gb * 2:
        # Skewed: size batches so even the largest partition fits
        # Use max_partition_gb as the per-partition budget
        batch_size = max(1, round(target_batch_gb / max(max_partition_gb, 0.01)))
    else:
        batch_size = max(1, round(target_batch_gb / max(avg_partition_gb, 0.01)))

    # Clamp: at least 1, at most all partitions, cap at 100
    batch_size = max(1, min(batch_size, partition_count, 100))

    total_batches = math.ceil(partition_count / batch_size)

    # Estimate worst-case batch time using max partition size when skewed
    if is_skewed:
        est_batch_time_min = (max_partition_gb * batch_size) / rate + 5
    else:
        est_batch_time_min = (avg_partition_gb * batch_size) / rate + 5

    reason_parts = [
        f"{partition_count} partitions, ~{avg_partition_gb:.1f} GB avg.",
        f"Batch size {batch_size} → {total_batches} batch(es).",
        f"~{est_batch_time_min:.0f} min per batch at {rate} GB/min ({file_format}).",
    ]

    if is_skewed:
        reason_parts.append(
            f"Skew detected: largest partition is {max_partition_gb:.1f} GB "
            f"(vs {avg_partition_gb:.1f} GB avg). Batch sized for worst case."
        )

    if total_size_gb > 500:
        reason_parts.append(
            "Large table — smaller batches limit failure blast radius.")
    if partition_count > 1000:
        reason_parts.append(
            "High partition count — batching avoids EMR job timeouts.")

    result = {
        "recommended_batch_size": batch_size,
        "partition_count": partition_count,
        "total_size_gb": round(total_size_gb, 2),
        "avg_partition_gb": round(avg_partition_gb, 2),
        "total_batches": total_batches,
        "estimated_batch_time_min": round(est_batch_time_min, 1),
        "reason": " ".join(reason_parts),
    }

    if is_skewed:
        result["max_partition_gb"] = round(max_partition_gb, 2)
        result["skew_adjusted"] = True

    return result


@tool
def recommend_add_files_batch_size(
    partition_count: int,
    total_size_gb: float = 0,
) -> dict:
    """Recommend batch_size for the Add Files strategy initial load.

    Unlike the incremental strategy (whose cost scales with data volume/GB
    scanned), the Add Files strategy registers existing files into Iceberg
    metadata-only, one partition at a time, with one Iceberg snapshot per
    partition. There is NO data movement or Spark read of file contents —
    the work per partition is a lightweight catalog operation.

    Because cost scales with the NUMBER OF PARTITIONS (not GB), batch_size is
    chosen to control:
      - the number of EMR Serverless jobs (one job per batch),
      - restartability granularity (a failed batch re-runs only its partitions),
      - and the failure blast radius for very large partition counts.

    Sizing heuristic (partition-count driven):
      - Unpartitioned table: single batch (batch_size = 1).
      - Otherwise aim for roughly 10 batches total so progress is visible and
        restarts are cheap, while keeping each batch's partition list a
        reasonable size for a single EMR job.
      - Clamp batch_size to [10, 500] and never exceed the partition count.

    Args:
        partition_count: Number of source partitions (from estimate_table_size).
        total_size_gb: Total table size in GB (informational only — does not
            drive sizing for add_files, included for context in the reason).
    """
    if partition_count <= 1:
        return {
            "recommended_batch_size": 1,
            "partition_count": partition_count,
            "total_batches": 1,
            "reason": (
                "Unpartitioned (or single-partition) table — a single add_files "
                "batch covers the whole table."
            ),
        }

    # Target ~10 batches so progress/restartability is granular without
    # creating an excessive number of EMR jobs.
    TARGET_BATCHES = 10
    MIN_BATCH = 10
    MAX_BATCH = 500

    batch_size = max(MIN_BATCH, round(partition_count / TARGET_BATCHES))
    batch_size = min(batch_size, MAX_BATCH, partition_count)

    total_batches = math.ceil(partition_count / batch_size)

    reason_parts = [
        f"{partition_count} partitions.",
        (
            "Add Files registers files per partition (metadata-only, "
            + "one snapshot per partition) — sizing is driven by partition "
            + "count, not data volume."
        ),
        f"Batch size {batch_size} -> {total_batches} batch(es).",
    ]
    if partition_count > 5000:
        reason_parts.append(
            "Very high partition count — batching limits EMR job runtime and "
            "keeps failure/restart granularity manageable.")

    return {
        "recommended_batch_size": batch_size,
        "partition_count": partition_count,
        "total_batches": total_batches,
        "total_size_gb": round(total_size_gb, 2),
        "reason": " ".join(reason_parts),
    }


@tool
def recommend_sort_order(
    columns: list,
    partition_keys: list,
    table_parameters: dict = None,
    total_size_gb: float = 0,
    column_statistics: dict = None,
) -> dict:
    """Recommend Iceberg sort order based on column schema and table characteristics.

    Analyzes the table's column schema to suggest 1-3 sort columns that
    maximize data clustering for common query patterns. Sort order in
    Iceberg improves min/max metadata pruning and reduces bytes scanned.

    Strategy:
    1. Exclude partition key columns (already used for file-level pruning)
    2. Use Glue column statistics (from get_glue_column_statistics) for
       NDV, null counts, and value ranges — this is the primary signal
    3. Fall back to table parameters for Spark-style column stats
    4. Prioritize by column type and naming conventions:
       - Timestamp/date columns (range scans) → ascending
       - High-cardinality ID columns (point lookups) → ascending
       - Categorical/enum columns (group-by queries) → ascending
    5. Limit to 2 sort columns (more adds write-amplification overhead)
    6. Skip sort recommendation for very small tables (< 1 GB)

    Args:
        columns: List of column dicts with 'name' and 'type' from get_glue_table
        partition_keys: List of partition column names to exclude from sort
        table_parameters: Glue table parameters dict (may contain spark.sql.statistics.colStats). Used as fallback when column_statistics is not available.
        total_size_gb: Total table size in GB (sort benefits scale with size)
        column_statistics: Dict of column statistics from get_glue_column_statistics tool (the 'statistics' field). Each entry contains ndv, null_count, min_value, max_value, etc.
    """
    if total_size_gb < 1:
        return {
            "recommended_sort_order": [],
            "total_size_gb": round(total_size_gb, 2),
            "reason": (
                "Table is under 1 GB — sort order provides minimal benefit "
                "at this scale. Iceberg metadata alone is sufficient for pruning."
            ),
        }

    # Normalize partition keys for exclusion
    partition_key_names = {k.lower() for k in partition_keys}

    # Build unified column stats from Glue column statistics (primary)
    # or Spark table parameters (fallback)
    col_stats = {}
    stats_source = None

    if column_statistics:
        # Primary: Glue Data Catalog column statistics
        col_stats = _parse_glue_column_statistics(column_statistics)
        stats_source = "glue_column_statistics"
    elif table_parameters:
        # Fallback: Spark-style stats in table parameters
        col_stats = _parse_column_stats(table_parameters)
        if col_stats:
            stats_source = "spark_table_parameters"

    # Categorize columns by sort priority
    timestamp_candidates = []
    id_candidates = []
    categorical_candidates = []

    for col in columns:
        col_name = col["name"]
        col_type = col.get("type", "string").lower()

        # Skip partition keys — they already provide file-level pruning
        if col_name.lower() in partition_key_names:
            continue

        stats = col_stats.get(col_name, {})
        ndv = stats.get("ndv")  # number of distinct values

        # Priority 1: Timestamp/date columns — ideal for range scan pruning
        if _is_time_column(col_name, col_type):
            score = _score_time_column(col_name, col_type, ndv, total_size_gb)
            timestamp_candidates.append({
                "column": col_name,
                "type": col_type,
                "direction": "asc",
                "priority": 1,
                "score": score,
                "reason": _time_column_reason(col_name, col_type, ndv),
            })

        # Priority 2: High-cardinality ID columns — point lookup pruning
        elif _is_id_column(col_name, col_type):
            score = _score_id_column(col_name, col_type, ndv, total_size_gb)
            id_candidates.append({
                "column": col_name,
                "type": col_type,
                "direction": "asc",
                "priority": 2,
                "score": score,
                "reason": _id_column_reason(col_name, col_type, ndv),
            })

        # Priority 3: Low-to-medium cardinality categorical columns
        elif _is_categorical_column(col_name, col_type, ndv):
            score = _score_categorical_column(col_name, col_type, ndv)
            categorical_candidates.append({
                "column": col_name,
                "type": col_type,
                "direction": "asc",
                "priority": 3,
                "score": score,
                "reason": _categorical_column_reason(col_name, col_type, ndv),
            })

    # Sort each category by score (higher is better) and combine
    timestamp_candidates.sort(key=lambda c: c["score"], reverse=True)
    id_candidates.sort(key=lambda c: c["score"], reverse=True)
    categorical_candidates.sort(key=lambda c: c["score"], reverse=True)

    # Build final recommendation: at most 2 sort columns
    # Prefer: 1 timestamp + 1 ID, or 1 timestamp + 1 categorical,
    # or 2 IDs if no timestamp available
    recommended = []

    if timestamp_candidates:
        recommended.append(timestamp_candidates[0])

    if id_candidates and len(recommended) < 2:
        recommended.append(id_candidates[0])

    if categorical_candidates and len(recommended) < 2:
        recommended.append(categorical_candidates[0])

    # If we still have nothing, try the best from any category
    if not recommended:
        all_candidates = timestamp_candidates + id_candidates + categorical_candidates
        if all_candidates:
            all_candidates.sort(key=lambda c: (c["priority"], -c["score"]))
            recommended = all_candidates[:2]

    sort_order = [
        {
            "column": r["column"],
            "direction": r["direction"],
            "reason": r["reason"],
        }
        for r in recommended
    ]

    if sort_order:
        col_summary = ", ".join(f"{s['column']} ({s['direction']})" for s in sort_order)
        note = (
            f"Recommended sort order: {col_summary}. "
            "Sort order improves Iceberg min/max metadata pruning, reducing bytes "
            "scanned for filtered queries. Columns chosen to complement partition "
            "pruning (partition keys excluded from sort)."
        )
    else:
        note = (
            "No strong sort order candidates identified. "
            "Consider sorting by the most common filter/join column in your queries."
        )

    return {
        "recommended_sort_order": sort_order,
        "total_size_gb": round(total_size_gb, 2),
        "partition_keys_excluded": list(partition_key_names),
        "columns_analyzed": len(columns),
        "column_stats_available": bool(col_stats),
        "column_stats_source": stats_source,
        "note": note,
    }


def _parse_column_stats(parameters: dict) -> dict:
    """Extract column-level statistics from Glue table parameters.

    Spark writes stats like:
      spark.sql.statistics.colStats.col_name.distinctCount = 12345
      spark.sql.statistics.colStats.col_name.nullCount = 0
      spark.sql.statistics.colStats.col_name.avgLen = 8
      spark.sql.statistics.colStats.col_name.maxLen = 32

    Returns dict of col_name → {ndv, null_count, avg_len, max_len}.
    """
    stats = {}
    prefix = "spark.sql.statistics.colStats."

    for key, value in parameters.items():
        if not key.startswith(prefix):
            continue
        # Format: spark.sql.statistics.colStats.<col_name>.<stat_name>
        remainder = key[len(prefix):]
        parts = remainder.rsplit(".", 1)
        if len(parts) != 2:
            continue
        col_name, stat_name = parts

        if col_name not in stats:
            stats[col_name] = {}

        try:
            if stat_name == "distinctCount":
                stats[col_name]["ndv"] = int(value)
            elif stat_name == "nullCount":
                stats[col_name]["null_count"] = int(value)
            elif stat_name == "avgLen":
                stats[col_name]["avg_len"] = int(value)
            elif stat_name == "maxLen":
                stats[col_name]["max_len"] = int(value)
        except (ValueError, TypeError):
            pass

    return stats


def _parse_glue_column_statistics(column_statistics: dict) -> dict:
    """Convert Glue column statistics (from get_glue_column_statistics) to
    the internal format used by sort order scoring.

    The input is the 'statistics' dict from get_glue_column_statistics,
    where each key is a column name and the value contains ndv, null_count,
    min_value, max_value, stat_type, etc.

    Returns dict of col_name → {ndv, null_count, min_value, max_value, ...}.
    """
    stats = {}
    for col_name, col_data in column_statistics.items():
        if not isinstance(col_data, dict):
            continue
        entry = {}

        ndv = col_data.get("ndv")
        if ndv is not None:
            entry["ndv"] = int(ndv)

        null_count = col_data.get("null_count")
        if null_count is not None:
            entry["null_count"] = int(null_count)

        # Min/max values for range assessment
        if col_data.get("min_value") is not None:
            entry["min_value"] = col_data["min_value"]
        if col_data.get("max_value") is not None:
            entry["max_value"] = col_data["max_value"]

        # String-specific stats
        if col_data.get("avg_length") is not None:
            entry["avg_len"] = col_data["avg_length"]
        if col_data.get("max_length") is not None:
            entry["max_len"] = col_data["max_length"]

        # Stat type for downstream logic
        if col_data.get("stat_type"):
            entry["stat_type"] = col_data["stat_type"]

        if entry:
            stats[col_name] = entry

    return stats


def _is_time_column(col_name: str, col_type: str) -> bool:
    """Check if a column is time-related by type or name."""
    col_type_lower = col_type.lower()
    col_name_lower = col_name.lower()

    # Type-based detection
    if any(t in col_type_lower for t in ("timestamp", "date", "datetime")):
        return True

    # Name-based detection (common naming patterns)
    time_hints = (
        "timestamp", "created_at", "updated_at", "modified_at",
        "event_time", "event_date", "log_time", "log_date",
        "ingestion_time", "processed_at", "occurred_at",
    )
    if col_name_lower in time_hints:
        return True

    # Partial name match for common suffixes/prefixes
    if col_name_lower.endswith(("_at", "_time", "_date", "_ts", "_datetime")):
        return True
    if col_name_lower.startswith(("dt_", "ts_", "date_", "time_")):
        return True

    return False


def _is_id_column(col_name: str, col_type: str) -> bool:
    """Check if a column is a high-cardinality ID column."""
    col_type_lower = col_type.lower()
    col_name_lower = col_name.lower()

    # Must be a numeric or string type (not boolean, array, struct, etc.)
    valid_types = ("int", "long", "bigint", "string", "varchar", "char", "uuid")
    if not any(t in col_type_lower for t in valid_types):
        return False

    # Name-based detection
    id_hints = (
        "user_id", "account_id", "customer_id", "session_id",
        "request_id", "transaction_id", "order_id", "item_id",
        "device_id", "trace_id", "correlation_id", "entity_id",
    )
    if col_name_lower in id_hints:
        return True

    # Ends with _id or _key (common patterns)
    if col_name_lower.endswith(("_id", "_key", "_uuid")):
        return True

    # Starts with id_ or is exactly "id"
    if col_name_lower == "id" or col_name_lower.startswith("id_"):
        return True

    return False


def _is_categorical_column(col_name: str, col_type: str, ndv: int = None) -> bool:
    """Check if a column is a low-to-medium cardinality categorical column.

    Good sort candidates when they appear in GROUP BY or WHERE clauses.
    """
    col_type_lower = col_type.lower()
    col_name_lower = col_name.lower()

    # Must be string or enum-like type
    if not any(t in col_type_lower for t in ("string", "varchar", "char")):
        return False

    # If we have NDV stats, use them: categorical = 2-1000 distinct values
    if ndv is not None:
        return 2 <= ndv <= 1000

    # Name-based heuristics for categorical columns
    categorical_hints = (
        "status", "state", "type", "category", "region", "country",
        "source", "channel", "platform", "tier", "level", "priority",
        "environment", "env", "action", "event_type", "method",
    )
    if col_name_lower in categorical_hints:
        return True

    if col_name_lower.endswith(("_type", "_status", "_state", "_category", "_code")):
        return True

    return False


def _score_time_column(col_name: str, col_type: str, ndv: int = None, total_size_gb: float = 0) -> float:
    """Score a time column for sort order suitability. Higher = better."""
    score = 10.0  # Base score for time columns

    # Typed timestamp/date is more reliable than name-inferred
    if any(t in col_type.lower() for t in ("timestamp", "date")):
        score += 5.0

    # Larger tables benefit more from time-based sort
    if total_size_gb > 100:
        score += 3.0
    elif total_size_gb > 10:
        score += 1.0

    # High NDV means fine-grained time values (better for range pruning)
    if ndv and ndv > 10000:
        score += 2.0

    # Prefer common event-time column names
    preferred_names = ("event_time", "created_at", "timestamp", "event_date")
    if col_name.lower() in preferred_names:
        score += 2.0

    return score


def _score_id_column(col_name: str, col_type: str, ndv: int = None, total_size_gb: float = 0) -> float:
    """Score an ID column for sort order suitability."""
    score = 5.0  # Base score for ID columns

    # High NDV is good for point-lookup pruning
    if ndv and ndv > 100000:
        score += 3.0
    elif ndv and ndv > 10000:
        score += 1.5

    # Prefer user/account IDs (common join/filter keys)
    preferred = ("user_id", "account_id", "customer_id")
    if col_name.lower() in preferred:
        score += 3.0

    # Numeric IDs sort more efficiently than string UUIDs
    if any(t in col_type.lower() for t in ("int", "long", "bigint")):
        score += 1.0

    return score


def _score_categorical_column(col_name: str, col_type: str, ndv: int = None) -> float:
    """Score a categorical column for sort order suitability."""
    score = 3.0  # Base score for categorical columns

    # Sweet spot: 10-100 distinct values clusters data well
    if ndv:
        if 10 <= ndv <= 100:
            score += 3.0
        elif 2 <= ndv <= 10:
            score += 1.5
        elif ndv > 100:
            score += 1.0

    # Prefer common filter columns
    preferred = ("status", "type", "region", "event_type", "category")
    if col_name.lower() in preferred:
        score += 2.0

    return score


def _time_column_reason(col_name: str, col_type: str, ndv: int = None) -> str:
    """Generate explanation for why a time column was chosen for sort."""
    parts = [f"'{col_name}'"]

    if any(t in col_type.lower() for t in ("timestamp", "date")):
        parts.append(f"(type: {col_type})")
    else:
        parts.append("(time-related by naming convention)")

    parts.append(
        "— sorting by time clusters recent data together, enabling "
        "efficient range-scan pruning via Iceberg min/max statistics"
    )

    if ndv and ndv > 10000:
        parts.append(f"(~{ndv:,} distinct values provide fine-grained clustering)")

    return " ".join(parts)


def _id_column_reason(col_name: str, col_type: str, ndv: int = None) -> str:
    """Generate explanation for why an ID column was chosen for sort."""
    parts = [f"'{col_name}'"]

    if ndv:
        parts.append(f"(~{ndv:,} distinct values)")

    parts.append(
        "— sorting by ID clusters related records together, improving "
        "point-lookup and join performance via min/max pruning"
    )

    return " ".join(parts)


def _categorical_column_reason(col_name: str, col_type: str, ndv: int = None) -> str:
    """Generate explanation for why a categorical column was chosen for sort."""
    parts = [f"'{col_name}'"]

    if ndv:
        parts.append(f"({ndv} distinct values)")

    parts.append(
        "— sorting by category clusters same-type records into contiguous "
        "file ranges, enabling predicate pushdown for filtered queries"
    )

    return " ".join(parts)
