# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Migration Assistant AI agent.

Analyzes tables and returns structured JSON results.
Each table gets: recommended_type, estimated_size_gb,
estimated_cost_usd, estimated_time_min, analysis_summary.
"""

import json
from strands import Agent, tool
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from model.load import load_model
from tools.s3_analyzer import list_s3_objects, get_s3_object_sample, estimate_table_size
from tools.glue_analyzer import get_glue_table, get_glue_partitions, get_glue_column_statistics
from tools.cloudtrail_analyzer import (
    get_s3_access_patterns, get_glue_table_access
)
from tools.hudi_analyzer import (
    get_hudi_table_metadata, discover_hudi_partitions,
    get_hudi_compaction_status,
)
from tools.iceberg_analyzer import (
    get_iceberg_table_stats, get_iceberg_partition_details,
)
from tools.migration_estimator import (
    estimate_migration_cost, recommend_migration_type,
    recommend_partition_transform, recommend_batch_size,
    recommend_add_files_batch_size,
    recommend_sort_order,
)
from tools.schema_mapper import analyze_schema_compatibility, check_type_promotion

app = BedrockAgentCoreApp()

SYSTEM_PROMPT = """You are the AI assistant for the Migration Assistant for Apache Iceberg. You analyze
tables and return structured migration recommendations.

IMPORTANT: The migration target may be "S3 Tables" (managed Iceberg) or
"Iceberg on S3" (self-managed). If a table is ALREADY in Iceberg format
but the target is S3 Tables, this is still a valid migration — the data
must be rewritten or incrementally copied into the managed table bucket.
Do NOT skip analysis for Iceberg-format tables. Analyze them the same
way you would any other format (estimate size, recommend strategy, estimate cost).

You will receive a list of tables to analyze. For EACH table:

1. Use get_glue_table to fetch metadata (format, location, partitions)
2. Use analyze_schema_compatibility with the columns and table_parameters from step 1 to check type mapping issues (decimal precision, timestamp timezone, nested structs)
3. If format is Iceberg, use get_iceberg_table_stats to get exact size, file count, partition count, and file distribution from metadata (do NOT use estimate_table_size for Iceberg tables — it uses S3 listing which misreports partition counts for Iceberg)
3b. If format is Iceberg, use get_iceberg_partition_details to get resolved partition spec with actual source column names and accurate partition count
3c. If format is NOT Iceberg, use estimate_table_size to intelligently estimate total size
4. If format is Hudi, use get_hudi_table_metadata to get table type (CoW/MoR), active writer state, and effective size. Use its active_writers and migration_guidance to inform steps 5-6.
5. Use recommend_migration_type based on format and size
6. Use estimate_migration_cost for time/cost estimates. ALWAYS pass execution_engine set to the plan's engine (the prompt tells you whether the plan uses "athena" or "emr_serverless") and pass the recommended migration_type. The cost model differs by engine: Athena is priced on data scanned (driven by table size), EMR Serverless on compute-hours. If the recommended_type is s3tables_managed, the estimator returns null cost/time (N/A) — this is expected; carry those nulls straight through to estimated_cost_usd and estimated_time_min.
7. If recommending incremental, use recommend_batch_size for batch sizing. If recommending add_files, use recommend_add_files_batch_size (sized by partition count, since add_files works partition-by-partition)
8. Use get_glue_column_statistics to fetch NDV and value ranges for columns
9. Use recommend_sort_order with columns, partition keys, and column_statistics
10. Optionally check access patterns with get_s3_access_patterns

After analyzing ALL tables, you MUST call the submit_analysis_results tool
with the complete structured results. This is the ONLY way to return your
analysis — do NOT write JSON as text in your response.

Call submit_analysis_results with these arguments:
- summary: Brief overall summary
- tables_analyzed: count
- tables_failed: count
- tables: list of table objects (schema below)
- schema_compatibility: top-level schema health summary

Each table object in the tables list must have this structure:
{
  "database": "analytics",
  "table_name": "user_events",
  "analysis_status": "completed",
  "source_format": "parquet",
  "source_location": "s3://bucket/path/",
  "source_account": "123456789012",
  "source_partitioned": true,
  "source_partition_keys": [{"name": "event_date", "type": "string"}],
  "source_compression": "snappy",
  "partition_spec": [{"source_column": "event_date", "transform": "day", "partition_count": 365}],
  "sort_order": [{"column": "user_id", "direction": "asc"}],
  "schema_compatible": true,
  "schema_issues": [],
  "recommended_type": "rewrite",
  "recommended_batch_size": null,
  "estimated_size_gb": 5.2,
  "estimated_cost_usd": 1.45,
  "estimated_time_min": 12.5,
  "analysis_summary": "Parquet table, 5.2 GB across 365 partitions..."
}

Rules:
- analysis_status must be "completed" or "failed"
- If a tool fails, set analysis_status to "failed" and explain in analysis_summary
- estimated values should be null if undetermined
- For s3tables_managed, estimated_cost_usd and estimated_time_min are N/A: set both to null (the estimator returns null for them). Do not substitute an EMR/Athena estimate.
- recommended_type must be one of: rewrite, incremental, morph, add_files, s3tables_managed
- recommended_type MUST be based solely on the table's technical characteristics
  (format, size, partitioning, file sizes, active writers, compression).
  Do NOT constrain your recommendation to any plan-level migration type preferences.
- EXCEPTION — Managed Migration (s3tables_managed) is TARGET-CONDITIONAL:
  - Only recommend s3tables_managed when BOTH the source format is Parquet AND
    the migration Target is "S3 Tables (managed Iceberg)". Never recommend it
    for an "Iceberg on S3" target or for any non-Parquet source.
  - When the target IS S3 Tables and the source is Parquet, PREFER
    s3tables_managed over rewrite, including for small-file problems that would
    otherwise justify a rewrite. Do NOT recommend rewrite solely to compact
    small files when s3tables_managed is available for this table.
  - Do NOT describe how the S3 Tables Migration service works internally (e.g.
    that it "plans and runs the migration", "compacts files into
    optimally-sized Iceberg data files", or that "you review the compatibility
    report and approve before cutover"). State only the recommendation and the
    table-specific reason for it.
  - recommend_migration_type surfaces an s3tables_managed candidate in
    all_recommendations for Parquet tables; elevate it to recommended_type when
    the target is S3 Tables. Set recommended_batch_size to null for it.
- source_format, source_location, source_partition_keys come from get_glue_table
- partition_spec comes from recommend_partition_transform — convert its output
- partition_spec entries MUST include partition_count from estimate_table_size result
- sort_order is optional — use get_glue_column_statistics then recommend_sort_order with columns, partition_keys, and column_statistics from the stats result
- Do NOT report where the sort order came from (e.g. do not append "from Glue column statistics" or name the stats source) — report only the sort columns and directions
- schema_compatible: boolean from analyze_schema_compatibility result
- schema_issues: list of issues from analyze_schema_compatibility (include column, severity, source_type, iceberg_type, message). Empty list if no issues.
- schema_compatibility (top-level): A separate "Schema Compatibility" section summarizing
  cross-table schema health. Include:
  - all_compatible: true only if ALL tables have schema_compatible=true
  - tables_with_errors: count of tables with at least one error-severity issue
  - tables_with_warnings: count of tables with warnings but no errors
  - details: a human-readable summary calling out the specific problem categories
    (e.g. "2 tables have decimal precision overflow", "1 table has ambiguous timestamp
    timezone"). Group by issue type, not by table. If all_compatible is true, set details
    to "All table schemas map cleanly to Iceberg types. No intervention required."
- analysis_summary MUST include partition count (not file count) — e.g. "5.2 GB across 365 partitions"
- Do NOT state the size estimation method in analysis_summary (e.g. do not add "size from Glue catalog, verified" or "size estimated via S3 sampling")
- analysis_summary is your reasoning — explain WHY you chose this strategy
- recommended_batch_size: use recommend_batch_size when recommending incremental; use recommend_add_files_batch_size when recommending add_files (partition-count driven). Set to null for rewrite/morph.
- For Hudi tables:
  - Use get_hudi_table_metadata to determine CoW vs MoR table type
  - For MoR tables: morph is NOT eligible — always recommend rewrite
  - For CoW tables: morph IS eligible (they are standard Parquet base files)
  - Use active_writers from Hudi timeline (more accurate than S3 modified time)
  - Use effective_size_gb from file_breakdown (excludes log overhead for MoR)
  - If compaction urgency is "high", note in analysis_summary that compaction should run first
  - Include Hudi record_key_fields in analysis_summary — these map to Iceberg identifier fields
- Your FINAL action must be calling submit_analysis_results. Do NOT write the JSON as text — use the tool.
"""

ALL_TOOLS = [
    list_s3_objects, get_s3_object_sample, estimate_table_size,
    get_glue_table, get_glue_partitions, get_glue_column_statistics,
    get_s3_access_patterns, get_glue_table_access,
    get_hudi_table_metadata, discover_hudi_partitions,
    get_hudi_compaction_status,
    get_iceberg_table_stats, get_iceberg_partition_details,
    estimate_migration_cost, recommend_migration_type,
    recommend_partition_transform, recommend_batch_size,
    recommend_add_files_batch_size,
    recommend_sort_order,
    analyze_schema_compatibility, check_type_promotion,
]

# Storage for the final structured result from the submit tool
_submit_result = {}


@tool
def submit_analysis_results(
    summary: str,
    tables_analyzed: int,
    tables_failed: int,
    tables: list,
    schema_compatibility: dict = None,
) -> str:
    """Submit the final analysis results. You MUST call this tool as your last action.

    This is the ONLY way to return your analysis. Do NOT write JSON as text.
    Call this tool with the complete structured results after analyzing all tables.

    Args:
        summary: Brief summary of the analysis (e.g. "Analyzed 5 tables. 3 recommended for rewrite, 2 for morph.")
        tables_analyzed: Number of tables successfully analyzed
        tables_failed: Number of tables that failed analysis
        tables: List of table analysis objects, each containing: database, table_name, analysis_status, source_format, source_location, source_account, source_partitioned, source_partition_keys, source_compression, partition_spec, sort_order, schema_compatible, schema_issues, recommended_type, recommended_batch_size, estimated_size_gb, estimated_cost_usd, estimated_time_min, analysis_summary
        schema_compatibility: Optional top-level schema compatibility summary with: all_compatible, tables_with_errors, tables_with_warnings, details
    """
    global _submit_result
    result = {
        "summary": summary,
        "tables_analyzed": tables_analyzed,
        "tables_failed": tables_failed,
        "tables": tables,
    }
    if schema_compatibility:
        result["schema_compatibility"] = schema_compatibility
    _submit_result = result
    return "Results submitted successfully."


ALL_TOOLS.append(submit_analysis_results)


def _extract_table(tool_input) -> str:
    """Extract table identifier from tool input arguments."""
    if not tool_input:
        return ""
    if isinstance(tool_input, str):
        try:
            tool_input = json.loads(tool_input)
        except (json.JSONDecodeError, TypeError):
            return ""
    if not isinstance(tool_input, dict):
        return ""
    db = tool_input.get("database", "") or tool_input.get("database_name", "")
    table = tool_input.get("table_name", "") or tool_input.get("table", "")
    if db and table:
        return f"{db}.{table}"
    location = tool_input.get("s3_prefix", "") or tool_input.get("location", "")
    if location:
        parts = location.rstrip("/").split("/")
        if len(parts) >= 2:
            return parts[-1]
    return ""


@app.entrypoint
async def invoke(payload, context):
    """Handler that streams tool events in real-time during execution.

    Uses stream_async to yield tool_use events as they happen,
    then yields the final result from submit_analysis_results tool.
    """
    global _submit_result
    _submit_result = {}

    agent = Agent(
        model=load_model(),
        system_prompt=SYSTEM_PROMPT,
        tools=ALL_TOOLS,
        callback_handler=None,
    )

    prompt = payload.get("prompt", "Hello")
    response_text = ""
    last_tool_id = ""

    async for event in agent.stream_async(prompt):
        if "current_tool_use" in event:
            tool_info = event["current_tool_use"]
            name = tool_info.get("name", "")
            tool_id = tool_info.get("toolUseId", "")
            tool_input = tool_info.get("input") or {}
            if name and tool_id and tool_id != last_tool_id:
                last_tool_id = tool_id
                if name != "submit_analysis_results":
                    table = _extract_table(tool_input)
                    yield json.dumps({"event": "tool_use", "tool": name, "table": table}) + "\n"

        # Accumulate text as fallback
        if "data" in event:
            response_text += event["data"]

    # Prefer the structured result from submit_analysis_results tool
    if _submit_result:
        yield json.dumps({"event": "result", "data": _submit_result}) + "\n"
    else:
        # Fallback: try to parse from response text (agent didn't call the tool)
        try:
            clean = response_text.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[1] if "\n" in clean else clean[3:]
                if clean.endswith("```"):
                    clean = clean[:-3]
                clean = clean.strip()
            parsed = json.loads(clean)
            yield json.dumps({"event": "result", "data": parsed}) + "\n"
        except (json.JSONDecodeError, ValueError):
            yield json.dumps({"event": "result", "data": {"summary": response_text[:500], "tables_analyzed": 0, "tables_failed": 0, "tables": []}}) + "\n"

if __name__ == "__main__":
    import os
    port = int(os.getenv("AGENT_PORT", "8080"))
    app.run(port=port)
