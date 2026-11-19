# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI Assistant — calls agent, stores results, returns structured data."""

import json
import logging
import os
import queue as sync_queue
import re
import asyncio
import threading
import httpx
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.deps import get_current_user
from app.services import plan_service

log = logging.getLogger(__name__)

router = APIRouter(prefix="/assistant", tags=["assistant"])


def _first_non_none(d: dict, *keys) -> str | None:
    """Get first non-None string value from dict by trying multiple keys."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            return str(v)
    return None


def _first_num(d: dict, *keys) -> float | None:
    """Get first numeric value from dict by trying multiple keys. Handles 0 correctly."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return None

def _get_agent_url():
    from app.services.settings_service import get_setting
    return get_setting("AGENT_URL") or "http://localhost:8080/invocations"


def _is_agentcore_arn(url_or_arn: str) -> bool:
    """Check if the AGENT_URL is an AgentCore runtime ARN (not a plain URL)."""
    return url_or_arn.startswith("arn:aws:bedrock-agentcore:")


def _extract_runtime_id(arn: str) -> str:
    """Extract runtime ID from ARN like arn:aws:bedrock-agentcore:us-east-1:123456:runtime/AgentName-xxx."""
    # ARN format: arn:aws:bedrock-agentcore:<region>:<account>:runtime/<id>
    return arn.split("/", 1)[1] if "/" in arn else arn


def _invoke_agentcore_sdk_raw(prompt: str):
    """Invoke AgentCore agent via boto3 SDK and return the response dict.
    Returns the full response dict from invoke_agent_runtime.
    """
    import boto3
    from botocore.config import Config
    agent_arn = _get_agent_url()
    region = agent_arn.split(":")[3] if ":" in agent_arn else os.environ.get("AWS_REGION", "us-east-1")

    # A full table-analysis run streams for well over botocore's default 60s
    # read timeout — a multi-table analysis routinely takes several minutes.
    # Without a raised read_timeout the invoke_agent_runtime read aborts mid-run
    # and surfaces in the UI as: Read timeout on endpoint URL: "None".
    # Tunable via AGENT_INVOKE_READ_TIMEOUT (seconds); default 900 (15m).
    read_timeout = int(os.environ.get("AGENT_INVOKE_READ_TIMEOUT", "900"))
    connect_timeout = int(os.environ.get("AGENT_INVOKE_CONNECT_TIMEOUT", "10"))
    cfg = Config(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        retries={"mode": "standard", "max_attempts": 1},
    )

    client = boto3.client("bedrock-agentcore", region_name=region, config=cfg)
    payload = json.dumps({"prompt": prompt}).encode()

    response = client.invoke_agent_runtime(
        agentRuntimeArn=agent_arn,
        payload=payload,
        contentType="application/json",
        accept="application/json",
    )

    return response


def _invoke_agentcore_sdk_sync(prompt: str) -> str:
    """Blocking implementation of the AgentCore SDK invocation + full body read.

    Runs on a worker thread (via asyncio.to_thread) — boto3 and iter_lines()/read()
    are synchronous and would otherwise block the single-threaded asyncio event loop
    for the entire duration of the agent call.
    """
    response = _invoke_agentcore_sdk_raw(prompt)
    content_type = response.get("contentType", "")
    body = response.get("response", b"")

    if "text/event-stream" in content_type:
        # Streaming SSE response — collect all data lines
        lines = []
        if hasattr(body, "iter_lines"):
            for line in body.iter_lines(chunk_size=10):
                if line:
                    decoded = line.decode("utf-8") if isinstance(line, bytes) else line
                    lines.append(decoded)
            return "\n".join(lines)
        elif hasattr(body, "read"):
            raw = body.read()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return raw
        return ""
    else:
        if hasattr(body, "read"):
            body = body.read()
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        return body


async def _invoke_agentcore_sdk(prompt: str) -> str:
    """Invoke AgentCore agent via boto3 SDK (for production).
    Returns the full response body as text (non-streaming convenience wrapper).

    The actual work runs on a worker thread via asyncio.to_thread so the blocking
    boto3 call doesn't stall the event loop for other coroutines (e.g. SSE writers).
    """
    return await asyncio.to_thread(_invoke_agentcore_sdk_sync, prompt)


def _iter_agentcore_lines_sync(prompt: str, out_queue: "sync_queue.Queue"):
    """Blocking producer that runs on a worker thread.

    Invokes the AgentCore SDK and pushes each decoded line/chunk onto a
    thread-safe queue as it arrives. Puts None as a sentinel when done, or an
    Exception instance if the call failed, so the async consumer can re-raise it.
    """
    try:
        response = _invoke_agentcore_sdk_raw(prompt)
        body = response.get("response", b"")

        if hasattr(body, "iter_lines"):
            for line in body.iter_lines(chunk_size=10):
                if line:
                    decoded = line.decode("utf-8") if isinstance(line, bytes) else line
                    out_queue.put(decoded + "\n")
        elif hasattr(body, "iter_chunks"):
            for chunk in body.iter_chunks(chunk_size=1024):
                if isinstance(chunk, bytes):
                    chunk = chunk.decode("utf-8")
                out_queue.put(chunk)
        elif hasattr(body, "read"):
            raw = body.read()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            out_queue.put(raw)
        else:
            if isinstance(body, bytes):
                body = body.decode("utf-8")
            out_queue.put(body)
    except Exception as e:  # noqa: BLE001 — re-raised on the consumer side
        out_queue.put(e)
    finally:
        out_queue.put(None)


async def _iter_agentcore_chunks(prompt: str):
    """Invoke AgentCore agent and yield response lines for streaming parsing.

    Uses iter_lines() per the AWS docs to get proper SSE line-by-line streaming.
    boto3 and iter_lines() are blocking, so the actual read loop runs on a worker
    thread (_iter_agentcore_lines_sync) that pushes lines onto a thread-safe queue.
    This async generator drains that queue one item at a time via asyncio.to_thread,
    which yields control back to the event loop between items — matching the
    natural interleaving behavior of the local httpx streaming path, so SSE events
    queued for the browser (e.g. tool_call) can be flushed progressively instead
    of only after the entire agent response has been read.
    """
    out_queue: sync_queue.Queue = sync_queue.Queue()
    thread = threading.Thread(target=_iter_agentcore_lines_sync, args=(prompt, out_queue), daemon=True)
    thread.start()

    while True:
        item = await asyncio.to_thread(out_queue.get)
        if item is None:
            break
        if isinstance(item, Exception):
            raise item
        yield item


def _local_recommend_type(fmt: str, size_gb: float, partitioned: bool, eligible: list) -> str | None:
    """Recommend migration type based purely on technical characteristics.

    Independent of plan-level migration type preferences.
    """
    if not eligible:
        return None
    fmt_lower = fmt.lower() if fmt else ""
    supports_morph = fmt_lower in ("parquet", "delta", "hudi")

    # Simple heuristic mirroring the agent's recommend_migration_type logic:
    # - Columnar formats with healthy characteristics → morph
    # - Non-columnar or small files → rewrite
    # - Large partitioned tables with active writers → incremental
    if supports_morph and "morph" in eligible:
        return "morph"
    elif "rewrite" in eligible:
        return "rewrite"
    elif "incremental" in eligible and partitioned and size_gb > 50:
        return "incremental"
    elif eligible:
        return eligible[0]
    return None


class AssistantRequest(BaseModel):
    plan_id: str


# How many tables to analyze per agent call. A single agent invocation emits ALL
# its results in one final tool call, which overflows the model's output-token
# budget for large plans (symptom: a long run that ends with an empty result and
# a silent "Analysis complete"). Batching keeps each call's output small enough
# to complete, and persists results incrementally so a later batch failing does
# not lose earlier batches. Tunable via env.
def _batch_size() -> int:
    try:
        return max(1, int(os.environ.get("APP_ASSISTANT_BATCH_SIZE", "15")))
    except ValueError:
        return 15


def _build_prompt(plan, batch_tables: list) -> str:
    """Build the analysis prompt for one batch of tables."""
    table_lines = [f"- {t.source_database}.{t.source_table}" for t in batch_tables]
    target_desc = ("S3 Tables (managed Iceberg)" if plan.target == "s3_tables"
                   else "Iceberg on S3 (self-managed)")
    # The execution engine drives the cost model (Athena = data scanned;
    # EMR Serverless = compute-hours), so the agent must know it to call
    # estimate_migration_cost with the right execution_engine.
    engine = getattr(plan, "execution_engine", None) or "athena"
    return (
        f"Analyze the following {len(batch_tables)} tables for migration.\n\n"
        f"Plan: {plan.name}\n"
        f"Target: {target_desc}\n"
        f"Execution engine: {engine}\n"
        "When estimating cost, pass this execution engine to "
        "estimate_migration_cost (Athena is priced on data scanned; EMR "
        "Serverless on compute-hours).\n\n"
        "NOTE: Even if a table is already in Iceberg format, it still needs migration "
        "when the target is S3 Tables — data must be rewritten or incrementally copied "
        "into the managed table bucket. Analyze ALL tables regardless of current format.\n\n"
        f"Tables:\n" + "\n".join(table_lines) + "\n\n"
        "IMPORTANT: Your recommended_type must be based ONLY on the technical characteristics "
        "of each table (format, size, partitioning, file sizes, active writers, compression). "
        "Do NOT constrain your recommendation to any plan-level migration type preferences.\n\n"
        "CRITICAL: Respond with ONLY a single JSON object. No preamble, no explanation, no markdown fences, no text before or after the JSON. "
        "The response must start with {{ and end with }}. "
        "Required JSON schema: {{\"summary\": \"...\", \"tables_analyzed\": N, \"tables_failed\": 0, \"tables\": ["
        "{{\"database\": \"...\", \"table_name\": \"...\", \"analysis_status\": \"completed\", "
        "\"recommended_type\": \"...\", \"estimated_size_gb\": N, \"estimated_cost_usd\": N, "
        "\"estimated_time_min\": N, \"analysis_summary\": \"...\"}}]}}"
    )


@router.post("/stream")
async def stream_assistant(request: AssistantRequest, username: str = Depends(get_current_user)):
    """Stream agent analysis progress via Server-Sent Events.

    The actual analysis runs in a background task. The SSE stream is just a
    progress viewer — if the client disconnects, analysis continues to completion.

    Emits events:
      - {"type": "start", "tables_count": N}
      - {"type": "tool_call", "tool": "...", "table": "...", "status": "calling"}
      - {"type": "tool_result", "tool": "...", "table": "...", "status": "done"}
      - {"type": "table_done", "table": "...", "index": N}
      - {"type": "complete", "summary": "...", "tables_analyzed": N}
      - {"type": "error", "message": "..."}
    """
    from app.services.permissions import check_manage_access
    plan = plan_service.get_plan(request.plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")

    # AI analysis is a mutation — requires manage access
    if not check_manage_access(username, plan.owners):
        raise HTTPException(status_code=403, detail="You need manage access to run AI analysis")

    if not plan.tables:
        async def empty_gen():
            yield f"data: {json.dumps({'type': 'complete', 'summary': 'No tables in this plan yet.', 'tables_analyzed': 0})}\n\n"
        return StreamingResponse(empty_gen(), media_type="text/event-stream")

    tables_to_analyze = [
        t for t in plan.tables
        if t.analysis_status in ("pending", "failed", "analyzing")
    ]

    if not tables_to_analyze:
        async def done_gen():
            yield f"data: {json.dumps({'type': 'complete', 'summary': 'All tables have already been analyzed.', 'tables_analyzed': 0})}\n\n"
        return StreamingResponse(done_gen(), media_type="text/event-stream")

    # Check if analysis is already in progress (all eligible tables are 'analyzing')
    all_analyzing = all(t.analysis_status == "analyzing" for t in tables_to_analyze)
    if all_analyzing:
        async def in_progress_gen():
            yield f"data: {json.dumps({'type': 'error', 'message': 'Analysis already in progress. Please wait for it to complete.'})}\n\n"
        return StreamingResponse(in_progress_gen(), media_type="text/event-stream")

    # ── Mark tables as analyzing ──
    from app.database import get_conn, _run_sql, _commit
    conn = get_conn()
    for t in tables_to_analyze:
        _run_sql(conn,
            "UPDATE plan_tables SET analysis_status='analyzing' WHERE plan_id=? AND source_database=? AND source_table=?",
            (request.plan_id, t.source_database, t.source_table),
        )
    _commit(conn)
    conn.close()

    # ── Launch background task ──
    # Events queue allows the SSE stream to read progress; if it disconnects, the task still completes.
    # The background task chunks the tables into batches (see _run_analysis_background)
    # so large plans don't overflow a single agent call's output-token budget.
    events_queue: asyncio.Queue = asyncio.Queue()
    asyncio.create_task(_run_analysis_background(
        request.plan_id, plan, tables_to_analyze, events_queue))

    # ── SSE stream reads from the queue ──
    async def event_stream():
        yield f"data: {json.dumps({'type': 'start', 'tables_count': len(tables_to_analyze)})}\n\n"
        while True:
            try:
                event = await asyncio.wait_for(events_queue.get(), timeout=300.0)
            except asyncio.TimeoutError:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Analysis timed out'})}\n\n"
                break
            yield f"data: {json.dumps(event)}\n\n"
            if event.get("type") in ("complete", "error"):
                break

    return StreamingResponse(event_stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


async def _run_analysis_background(
    plan_id: str,
    plan,
    tables_to_analyze: list,
    events_queue: asyncio.Queue,
):
    """Run the agent analysis in the background, ONE BATCH AT A TIME.

    Large plans are split into batches of _batch_size() tables. Each batch is a
    self-contained agent call whose results are parsed and persisted immediately,
    so a failure in a later batch never discards earlier batches' work. After the
    final batch we emit a single truthful terminal event: 'complete' only if at
    least one table was actually analyzed; otherwise 'error'.

    Results are stored regardless of whether the SSE client stays connected.
    """
    size = _batch_size()
    batches = [tables_to_analyze[i:i + size]
               for i in range(0, len(tables_to_analyze), size)]
    total = len(tables_to_analyze)
    total_analyzed = 0
    failed_tables: list = []
    batch_errors: list = []

    try:
        for bi, batch in enumerate(batches):
            await events_queue.put({
                "type": "batch_progress",
                "batch": bi + 1,
                "batches_total": len(batches),
                "batch_size": len(batch),
            })
            analyzed_keys, summary = await _analyze_one_batch(
                plan_id, plan, batch, events_queue)
            batch_analyzed = len(analyzed_keys)
            total_analyzed += batch_analyzed

            # ── Per-batch truthfulness ──
            # Any table in this batch the agent did NOT return a result for is a
            # failure for that table. Reset it to 'pending' (retryable) rather
            # than marking it 'completed' with a generic summary (the old silent
            # path). This is per batch, so partial success is preserved.
            for t in batch:
                key = f"{t.source_database}.{t.source_table}"
                if key not in analyzed_keys:
                    failed_tables.append(t)
                    _reset_table_pending(plan_id, t)
            if batch_analyzed == 0:
                if summary and summary.startswith("Batch failed:"):
                    batch_errors.append(summary)
                await events_queue.put({
                    "type": "batch_progress",
                    "batch": bi + 1,
                    "batches_total": len(batches),
                    "batch_size": len(batch),
                    "batch_analyzed": 0,
                    "warning": (summary if summary and summary.startswith("Batch failed:")
                                else "batch produced no recommendations"),
                })

        # ── Terminal event (truthful) ──
        failed_count = len(failed_tables)
        if total_analyzed == 0:
            message = (f"Analysis produced no recommendations for any of the "
                       f"{total} table(s). They remain pending — please retry.")
            if batch_errors:
                message += f" Cause: {batch_errors[0]}"
            await events_queue.put({"type": "error", "message": message})
        else:
            parts = [f"Analyzed {total_analyzed} of {total} table(s)"]
            if len(batches) > 1:
                parts.append(f"in {len(batches)} batches")
            if failed_count:
                parts.append(f"— {failed_count} could not be analyzed and remain pending")
            await events_queue.put({
                "type": "complete",
                "summary": ". ".join(parts) + ".",
                "tables_analyzed": total_analyzed,
                "tables_failed": failed_count,
            })
    except Exception as e:
        # Reset any still-'analyzing' tables back to 'pending' on unhandled errors.
        from app.database import get_conn, _run_sql, _commit
        conn = get_conn()
        for t in tables_to_analyze:
            _run_sql(conn,
                "UPDATE plan_tables SET analysis_status='pending' WHERE plan_id=? AND source_database=? AND source_table=? AND analysis_status='analyzing'",
                (plan_id, t.source_database, t.source_table))
        _commit(conn)
        conn.close()
        await events_queue.put({
            "type": "error",
            "message": str(e)[:200],
        })


def _reset_table_pending(plan_id: str, t) -> None:
    """Reset a single table's analysis_status to 'pending' so it can be retried."""
    from app.database import get_conn, _run_sql, _commit
    conn = get_conn()
    _run_sql(conn,
        "UPDATE plan_tables SET analysis_status='pending' WHERE plan_id=? AND source_database=? AND source_table=? AND analysis_status='analyzing'",
        (plan_id, t.source_database, t.source_table))
    _commit(conn)
    conn.close()


async def _analyze_one_batch(
    plan_id: str,
    plan,
    batch_tables: list,
    events_queue: asyncio.Queue,
) -> tuple[set, str]:
    """Analyze ONE batch of tables with a single agent call.

    Builds the batch prompt, invokes the agent (AgentCore or local httpx),
    streams tool/table progress to the queue, parses the result, and persists
    each returned table's analysis immediately. Returns (analyzed_keys, summary)
    where analyzed_keys is the set of "db.table" that got a stored result. On an
    HTTP error, falls back to local heuristic analysis for this batch. Never
    emits a terminal 'complete'/'error' — the caller owns the terminal event.
    """
    prompt = _build_prompt(plan, batch_tables)
    tables_to_analyze = batch_tables
    try:
        full_body = ""
        last_emitted_table = -1

        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=30.0)) as client:
            agent_url = _get_agent_url()
            payload_dict = {"prompt": prompt}

            async def _process_chunks(chunk_iter):
                nonlocal full_body, last_emitted_table
                async for chunk in chunk_iter:
                    full_body += chunk

                    for line in chunk.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith("data: "):
                            line = line[6:]
                        if line.startswith('"') and line.endswith('"'):
                            try:
                                line = json.loads(line)
                            except (json.JSONDecodeError, TypeError):
                                pass
                        if not isinstance(line, str):
                            continue
                        try:
                            evt = json.loads(line)
                            if evt.get("event") == "tool_use":
                                await events_queue.put({
                                    "type": "tool_call",
                                    "tool": evt.get("tool", ""),
                                    "table": evt.get("table", ""),
                                    "status": "calling",
                                })
                        except (json.JSONDecodeError, TypeError):
                            pass

                    for idx, t in enumerate(tables_to_analyze):
                        marker = f"{t.source_database}.{t.source_table}"
                        if marker in chunk and idx > last_emitted_table:
                            if last_emitted_table >= 0:
                                prev = tables_to_analyze[last_emitted_table]
                                await events_queue.put({
                                    "type": "table_done",
                                    "table": f"{prev.source_database}.{prev.source_table}",
                                    "index": last_emitted_table,
                                })
                            last_emitted_table = idx
                            await events_queue.put({
                                "type": "analyzing_table",
                                "table": marker,
                                "index": idx,
                                "total": len(tables_to_analyze),
                            })

            if _is_agentcore_arn(agent_url):
                await _process_chunks(_iter_agentcore_chunks(prompt))
            else:
                async def _http_chunks():
                    async with client.stream(
                        "POST",
                        agent_url,
                        json=payload_dict,
                        headers={"Content-Type": "application/json"},
                    ) as resp:
                        async for chunk in resp.aiter_text():
                            yield chunk

                await _process_chunks(_http_chunks())

        # ── Process the full response ──
        body = full_body.strip()
        found_result_event = False
        agent_error = ""
        for line in body.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("data: "):
                line = line[6:]
            if line.startswith('"') and line.endswith('"'):
                try:
                    line = json.loads(line)
                except (json.JSONDecodeError, TypeError):
                    pass
            if not isinstance(line, str):
                continue
            try:
                evt = json.loads(line)
                if evt.get("event") == "result" and "data" in evt:
                    body = json.dumps(evt["data"])
                    found_result_event = True
                    break
                # The AgentCore runtime reports failures (e.g. model access
                # denied) as a streamed {"error": ..., "error_type": ...} event.
                if evt.get("error") and not agent_error:
                    agent_error = str(evt["error"])
            except (json.JSONDecodeError, TypeError):
                pass

        if not found_result_event and agent_error:
            # Handled by the generic except below: logged, batch reset to
            # pending, and the message returned so the UI can show it.
            raise RuntimeError(f"Agent error: {agent_error}")

        if not found_result_event:
            body = full_body.strip()

        result = _parse_agent_response(body, tables_to_analyze)

        # ── Store results (only tables the agent actually returned) ──
        # Tables NOT returned are left 'analyzing' here; the caller marks them
        # pending (retryable) as part of per-batch truthfulness — we do NOT
        # write a generic "completed" row with no recommended_type.
        analyzed_keys: set = set()
        for table_result in result.get("tables", []):
            db = table_result.get("database", "")
            tbl = table_result.get("table_name", "")
            if not db or not tbl:
                continue
            plan_service.store_table_analysis(plan_id, db, tbl, table_result)
            analyzed_keys.add(f"{db}.{tbl}")

        summary = result.get("summary", "Analysis complete.")
        return analyzed_keys, summary

    except httpx.HTTPError:
        # Fallback to local heuristic analysis for THIS batch.
        from app.services import validation_service, table_service
        local_tables = []
        analyzed_keys = set()
        for idx, t in enumerate(tables_to_analyze):
            info = table_service.get_table(t.source_database, t.source_table)
            if not info:
                continue
            fmt_val = info.format.value if hasattr(info.format, 'value') else info.format
            eligible = validation_service.get_eligible_types(fmt_val)
            size_gb = round(info.size_bytes / (1024**3), 2)
            rec = _local_recommend_type(fmt_val, size_gb, info.partitioned, eligible)
            local_tables.append({
                "database": t.source_database,
                "table_name": t.source_table,
                "recommended_type": rec,
                "estimated_size_gb": size_gb,
                "analysis_status": "completed",
                "analysis_summary": f"Format: {fmt_val}. Size: {size_gb} GB. {'Partitioned. ' if info.partitioned else ''}Eligible types: {', '.join(eligible) if eligible else 'none'}.",
            })
            await events_queue.put({
                "type": "table_done",
                "table": f"{t.source_database}.{t.source_table}",
                "index": idx,
            })
            plan_service.store_table_analysis(plan_id, t.source_database, t.source_table, local_tables[-1])
            analyzed_keys.add(f"{t.source_database}.{t.source_table}")

        return analyzed_keys, (
            f"AI agent unavailable. Used local analysis for {len(local_tables)} table(s).")

    except Exception as e:
        # Unexpected failure for THIS batch only. Reset the batch's tables to
        # 'pending' (retryable) and return no analyzed keys so the caller counts
        # them as failed — other batches are unaffected.
        log.warning(f"[Assistant] Batch analysis failed: {str(e)[:1000]}")
        for t in batch_tables:
            _reset_table_pending(plan_id, t)
        return set(), f"Batch failed: {str(e)[:500]}"


def _parse_agent_response(body: str, tables_to_analyze: list) -> dict:
    """Parse the agent response body into a structured result dict."""
    # Handle SSE format: concatenate all data: lines
    result = None
    if "data: " in body or "data:" in body:
        parts = []
        for line in body.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            line = line.strip()
            if line.startswith("data:"):
                text = line[5:].strip() if line.startswith("data: ") else line[5:]
                if text == "[DONE]" or not text:
                    continue
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, str):
                        parts.append(parsed)
                    elif isinstance(parsed, dict):
                        result = parsed
                        parts = None
                        break
                    else:
                        parts.append(text)
                except (json.JSONDecodeError, TypeError):
                    parts.append(text)
        if parts is not None:
            body = "".join(parts)
        else:
            body = None

    if body is not None and result is None:
        try:
            clean = body.strip()
            extracted = None

            # Strategy 1: Find JSON inside ```json ... ``` blocks
            fence_open = re.search(r'```(?:json)?[\s\n]*\{', clean, re.DOTALL)
            if fence_open:
                inner_start = clean.index('{', fence_open.start())
                depth = 0
                for i in range(inner_start, len(clean)):
                    if clean[i] == '{': depth += 1
                    elif clean[i] == '}':
                        depth -= 1
                        if depth == 0:
                            extracted = clean[inner_start:i+1]
                            break

            # Strategy 2: Find the LARGEST valid JSON object
            if not extracted:
                best = None
                best_len = 0
                idx = 0
                while idx < len(clean):
                    start = clean.find("{", idx)
                    if start < 0:
                        break
                    depth = 0
                    end = -1
                    for i in range(start, len(clean)):
                        if clean[i] == "{": depth += 1
                        elif clean[i] == "}":
                            depth -= 1
                            if depth == 0:
                                end = i
                                break
                    if end > start:
                        candidate = clean[start:end+1]
                        if len(candidate) > best_len:
                            try:
                                test = json.loads(candidate)
                                if isinstance(test, dict) and ("tables" in test or "summary" in test or "table_name" in test):
                                    best = candidate
                                    best_len = len(candidate)
                            except (json.JSONDecodeError, ValueError):
                                pass
                    idx = start + 1
                if best:
                    extracted = best

            if not extracted:
                raise ValueError("No valid JSON found in response")

            result = json.loads(extracted)

            # Normalize tables
            tables_data = []
            if isinstance(result, dict):
                for key in ("tables", "table_analysis", "analysis", "results", "table_results", "migration_analysis"):
                    val = result.get(key)
                    if isinstance(val, list):
                        tables_data = val
                        break
                    elif isinstance(val, str):
                        try:
                            parsed_val = json.loads(val)
                            if isinstance(parsed_val, list):
                                tables_data = parsed_val
                                break
                        except (json.JSONDecodeError, TypeError):
                            # Value isn't JSON — not the tables payload; skip it.
                            continue
                if not tables_data:
                    for key, val in result.items():
                        if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                            sample = val[0]
                            if any(k in sample for k in ("database", "table_name", "source_table", "name")):
                                tables_data = val
                                break
            elif isinstance(result, list):
                tables_data = result
                result = {"tables": tables_data, "summary": "Analysis complete."}

            # Normalize table keys
            normalized_tables = []
            for t in tables_data:
                normalized = {}
                normalized["database"] = t.get("database") or t.get("source_database") or t.get("db") or ""
                normalized["table_name"] = t.get("table_name") or t.get("source_table") or t.get("name") or t.get("table") or ""
                normalized["source_format"] = t.get("source_format") or t.get("format") or None
                normalized["source_location"] = t.get("source_location") or t.get("location") or None
                normalized["source_account"] = t.get("source_account") or t.get("aws_account_id") or None
                normalized["source_partitioned"] = t.get("source_partitioned", False)
                normalized["source_partition_keys"] = t.get("source_partition_keys") or t.get("partition_keys") or None
                normalized["source_compression"] = t.get("source_compression") or t.get("compression") or None
                normalized["partition_spec"] = t.get("partition_spec") or None
                normalized["sort_order"] = t.get("sort_order") or None
                normalized["recommended_type"] = _first_non_none(t, "recommended_type", "recommendation", "migration_type", "type")
                normalized["recommended_batch_size"] = _first_num(t, "recommended_batch_size", "batch_size")
                normalized["estimated_size_gb"] = _first_num(t, "estimated_size_gb", "size_gb", "size")
                normalized["estimated_cost_usd"] = _first_num(t, "estimated_cost_usd", "cost", "cost_usd")
                normalized["estimated_time_min"] = _first_num(t, "estimated_time_min", "time_min", "time", "duration_min")
                normalized["analysis_summary"] = t.get("analysis_summary") or t.get("summary") or t.get("analysis") or t.get("notes") or t.get("reason") or None
                normalized["analysis_status"] = t.get("analysis_status", "completed")
                if normalized["database"] and normalized["table_name"]:
                    normalized_tables.append(normalized)

            if isinstance(result, dict):
                result["tables"] = normalized_tables
            else:
                result = {"tables": normalized_tables}

            if "summary" not in result:
                result["summary"] = result.get("overall_summary") or result.get("analysis_summary") or result.get("message") or "Analysis complete."
            result["tables_analyzed"] = len(normalized_tables)
            result["tables_failed"] = 0

        except (json.JSONDecodeError, ValueError) as parse_err:
            log.warning(f"[Assistant] JSON extraction failed: {parse_err}")
            log.debug(f"[Assistant] Body preview: {(body or '')[:300]}")
            result = {
                "summary": body[:1000] if body else "No response from agent.",
                "tables_analyzed": 0,
                "tables_failed": len(tables_to_analyze),
                "tables": [],
            }

    if result is None:
        result = {"summary": "No response.", "tables_analyzed": 0, "tables_failed": 0, "tables": []}

    return result


@router.post("")
async def invoke_assistant(request: AssistantRequest):
    """Invoke the AI agent to analyze tables in a plan."""
    plan = plan_service.get_plan(request.plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")

    if not plan.tables:
        return {
            "summary": "No tables in this plan yet. Add tables from the Inventory first.",
            "tables_analyzed": 0,
            "tables_failed": 0,
            "plan": plan.model_dump(),
        }

    # Only analyze tables that haven't been analyzed or failed before
    tables_to_analyze = [
        t for t in plan.tables
        if t.analysis_status in ("pending", "failed", "analyzing")
    ]

    if not tables_to_analyze:
        return {
            "summary": "All tables have already been analyzed. No new analysis needed.",
            "tables_analyzed": 0,
            "tables_failed": 0,
            "plan": plan.model_dump(),
        }

    # Build the prompt with only unanalyzed tables
    table_lines = []
    for t in tables_to_analyze:
        table_lines.append(f"- {t.source_database}.{t.source_table}")

    target_desc = "S3 Tables (managed Iceberg)" if plan.target == "s3_tables" else "Iceberg on S3 (self-managed)"
    prompt = (
        f"Analyze the following {len(tables_to_analyze)} tables for migration.\n\n"
        f"Plan: {plan.name}\n"
        f"Target: {target_desc}\n\n"
        "NOTE: Even if a table is already in Iceberg format, it still needs migration "
        "when the target is S3 Tables — data must be rewritten or incrementally copied "
        "into the managed table bucket. Analyze ALL tables regardless of current format.\n\n"
        f"Tables:\n" + "\n".join(table_lines) + "\n\n"
        "IMPORTANT: Your recommended_type must be based ONLY on the technical characteristics "
        "of each table (format, size, partitioning, file sizes, active writers, compression). "
        "Do NOT constrain your recommendation to any plan-level migration type preferences.\n\n"
        "CRITICAL: Respond with ONLY a single JSON object. No preamble, no explanation, no markdown fences, no text before or after the JSON. "
        "The response must start with {{ and end with }}. "
        "Required JSON schema: {{\"summary\": \"...\", \"tables_analyzed\": N, \"tables_failed\": 0, \"tables\": ["
        "{{\"database\": \"...\", \"table_name\": \"...\", \"analysis_status\": \"completed\", "
        "\"recommended_type\": \"...\", \"estimated_size_gb\": N, \"estimated_cost_usd\": N, "
        "\"estimated_time_min\": N, \"analysis_summary\": \"...\"}}]}}"
    )

    # Mark only these tables as analyzing
    from app.database import get_conn, _run_sql, _commit
    conn = get_conn()
    for t in tables_to_analyze:
        _run_sql(conn,
            "UPDATE plan_tables SET analysis_status='analyzing' WHERE plan_id=? AND source_database=? AND source_table=?",
            (request.plan_id, t.source_database, t.source_table),
        )
    _commit(conn)
    conn.close()

    # Call the agent (non-streaming)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=30.0)) as client:
            # Collect the full streamed response
            full_body = ""
            agent_url = _get_agent_url()

            if _is_agentcore_arn(agent_url):
                # Production: invoke via boto3 SDK
                full_body = await _invoke_agentcore_sdk(prompt)
            else:
                # Local dev: invoke via HTTP
                async with client.stream(
                    "POST",
                    agent_url,
                    json={"prompt": prompt},
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    async for chunk in resp.aiter_text():
                        full_body += chunk

            body = full_body.strip()
            log.debug(f"[Assistant] Raw response length: {len(body)}")

            # Handle SSE format: concatenate all data: lines
            # Only treat as SSE if the body starts with SSE-style lines
            # (not just because "data:" appears somewhere in JSON content)
            is_sse = False
            first_lines = body.split("\n", 5)[:5]
            for fl in first_lines:
                fl_stripped = fl.strip()
                if fl_stripped.startswith("data:") or fl_stripped.startswith("event:") or fl_stripped == "":
                    is_sse = True
                    break

            if is_sse:
                parts = []
                for line in body.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
                    line = line.strip()
                    if line.startswith("data:"):
                        text = line[5:].strip() if line.startswith("data: ") else line[5:]
                        if text == "[DONE]" or not text:
                            continue
                        try:
                            parsed = json.loads(text)
                            if isinstance(parsed, str):
                                parts.append(parsed)
                            elif isinstance(parsed, dict) and ("tables" in parsed or "summary" in parsed):
                                # Agent returned full JSON directly in a single SSE event
                                result = parsed
                                parts = None
                                break
                            else:
                                parts.append(text)
                        except (json.JSONDecodeError, TypeError):
                            parts.append(text)
                if parts is not None:
                    body = "".join(parts)
                else:
                    body = None  # already parsed

            # Parse the final JSON if not already parsed
            if body is not None:
                try:
                    clean = body.strip()
                    extracted = None

                    # Strategy 0: Try parsing the entire body as JSON directly
                    try:
                        direct = json.loads(clean)
                        if isinstance(direct, dict) and ("tables" in direct or "summary" in direct):
                            extracted = clean
                            log.debug(f"[Assistant] Extracted directly as JSON: {len(extracted)} chars")
                    except (json.JSONDecodeError, ValueError):
                        pass
                    
                    # Strategy 1: Find JSON inside ```json ... ``` blocks
                    if not extracted:
                        fence_open = re.search(r'```(?:json)?\s*\{', clean, re.DOTALL)
                        if fence_open:
                            inner_start = clean.index('{', fence_open.start())
                            # Brace-match from the { to find complete JSON
                            depth = 0
                            for i in range(inner_start, len(clean)):
                                if clean[i] == '{': depth += 1
                                elif clean[i] == '}':
                                    depth -= 1
                                    if depth == 0:
                                        candidate = clean[inner_start:i+1]
                                        try:
                                            test = json.loads(candidate)
                                            if isinstance(test, dict) and ("tables" in test or "summary" in test):
                                                extracted = candidate
                                                log.debug(f"[Assistant] Extracted from fence: {len(extracted)} chars")
                                        except (json.JSONDecodeError, ValueError):
                                            pass
                                        break
                    
                    # Strategy 2: Find the LARGEST valid JSON object by trying all { positions
                    if not extracted:
                        best = None
                        best_len = 0
                        idx = 0
                        while idx < len(clean):
                            start = clean.find("{", idx)
                            if start < 0:
                                break
                            # Brace-match from this position
                            depth = 0
                            end = -1
                            for i in range(start, len(clean)):
                                if clean[i] == "{":
                                    depth += 1
                                elif clean[i] == "}":
                                    depth -= 1
                                    if depth == 0:
                                        end = i
                                        break
                            if end > start:
                                candidate = clean[start:end+1]
                                if len(candidate) > best_len:
                                    # Verify it's valid JSON with a "tables" or "summary" key
                                    try:
                                        test = json.loads(candidate)
                                        if isinstance(test, dict) and ("tables" in test or "summary" in test or "table_name" in test):
                                            best = candidate
                                            best_len = len(candidate)
                                    except (json.JSONDecodeError, ValueError):
                                        pass
                            idx = start + 1
                        if best:
                            extracted = best
                            log.debug(f"[Assistant] Extracted by brace-match: {len(extracted)} chars")
                    
                    if not extracted:
                        raise ValueError("No valid JSON found in response")
                    
                    log.debug(f"[Assistant] Final extracted length: {len(extracted)}")
                    result = json.loads(extracted)
                    log.debug(f"[Assistant] Parsed JSON keys: {list(result.keys()) if isinstance(result, dict) else type(result)}")
                    
                    # Try to find tables under various possible keys
                    tables_data = []
                    if isinstance(result, dict):
                        for key in ("tables", "table_analysis", "analysis", "results", "table_results", "migration_analysis"):
                            val = result.get(key)
                            if isinstance(val, list):
                                tables_data = val
                                log.debug(f"[Assistant] Found {len(tables_data)} tables under key '{key}'")
                                break
                            elif isinstance(val, str):
                                log.debug(f"[Assistant] Key '{key}' is a string (len={len(val)}), not a list — trying to parse")
                                try:
                                    parsed_val = json.loads(val)
                                    if isinstance(parsed_val, list):
                                        tables_data = parsed_val
                                        log.debug(f"[Assistant] Parsed string into {len(tables_data)} tables")
                                        break
                                except (json.JSONDecodeError, TypeError):
                                    # Value isn't JSON — skip this key.
                                    continue
                        # If still empty, check if the result itself is a list of table analyses
                        if not tables_data:
                            # Look for nested structures
                            for key, val in result.items():
                                if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                                    # Check if items look like table analysis (have database or table_name keys)
                                    sample = val[0]
                                    if any(k in sample for k in ("database", "table_name", "source_table", "name")):
                                        tables_data = val
                                        log.debug(f"[Assistant] Found {len(tables_data)} tables under key '{key}' (auto-detected)")
                                        break
                    elif isinstance(result, list):
                        tables_data = result
                        result = {"tables": tables_data, "summary": "Analysis complete."}
                    
                    # Normalize table keys
                    normalized_tables = []
                    for t in tables_data:
                        normalized = {}
                        normalized["database"] = t.get("database") or t.get("source_database") or t.get("db") or ""
                        normalized["table_name"] = t.get("table_name") or t.get("source_table") or t.get("name") or t.get("table") or ""
                        # Spec fields from agent discovery
                        normalized["source_format"] = t.get("source_format") or t.get("format") or None
                        normalized["source_location"] = t.get("source_location") or t.get("location") or None
                        normalized["source_account"] = t.get("source_account") or t.get("aws_account_id") or None
                        normalized["source_partitioned"] = t.get("source_partitioned", False)
                        normalized["source_partition_keys"] = t.get("source_partition_keys") or t.get("partition_keys") or None
                        normalized["source_compression"] = t.get("source_compression") or t.get("compression") or None
                        normalized["partition_spec"] = t.get("partition_spec") or None
                        normalized["sort_order"] = t.get("sort_order") or None
                        # Agent reasoning
                        normalized["recommended_type"] = _first_non_none(t, "recommended_type", "recommendation", "migration_type", "type")
                        normalized["recommended_batch_size"] = _first_num(t, "recommended_batch_size", "batch_size")
                        normalized["estimated_size_gb"] = _first_num(t, "estimated_size_gb", "size_gb", "size")
                        normalized["estimated_cost_usd"] = _first_num(t, "estimated_cost_usd", "cost", "cost_usd")
                        normalized["estimated_time_min"] = _first_num(t, "estimated_time_min", "time_min", "time", "duration_min")
                        normalized["analysis_summary"] = t.get("analysis_summary") or t.get("summary") or t.get("analysis") or t.get("notes") or t.get("reason") or None
                        normalized["analysis_status"] = t.get("analysis_status", "completed")
                        # Schema evolution fields
                        normalized["schema_compatible"] = t.get("schema_compatible")
                        normalized["schema_issues"] = t.get("schema_issues") or []
                        if normalized["database"] and normalized["table_name"]:
                            normalized_tables.append(normalized)
                    
                    if isinstance(result, dict):
                        result["tables"] = normalized_tables
                    else:
                        result = {"tables": normalized_tables}
                    
                    # Extract summary
                    if "summary" not in result:
                        result["summary"] = result.get("overall_summary") or result.get("analysis_summary") or result.get("message") or "Analysis complete."
                    result["tables_analyzed"] = len(normalized_tables)
                    result["tables_failed"] = 0
                    
                    log.debug(f"[Assistant] Final: {len(normalized_tables)} normalized tables")
                except (json.JSONDecodeError, ValueError) as e:
                    log.warning(f"[Assistant] JSON parse failed: {e}")
                    log.debug(f"[Assistant] Body preview: {body[:300]}")
                    # Use the raw text as the summary
                    result = {
                        "summary": body[:1000] if body else "No response from agent.",
                        "tables_analyzed": 0,
                        "tables_failed": len(plan.tables),
                        "tables": [],
                    }

    except httpx.HTTPError as e:
        # Agent not reachable — do a local rule-based analysis instead
        # Recommendation is independent of plan migration types
        from app.services import validation_service, table_service
        local_tables = []
        for t in tables_to_analyze:
            info = table_service.get_table(t.source_database, t.source_table)
            if not info:
                continue
            fmt_val = info.format.value if hasattr(info.format, 'value') else info.format
            eligible = validation_service.get_eligible_types(fmt_val)
            size_gb = round(info.size_bytes / (1024**3), 2)
            # Recommend based on technical characteristics, not plan preferences
            rec = _local_recommend_type(fmt_val, size_gb, info.partitioned, eligible)
            local_tables.append({
                "database": t.source_database,
                "table_name": t.source_table,
                "recommended_type": rec,
                "estimated_size_gb": size_gb,
                "analysis_status": "completed",
                "analysis_summary": f"Format: {fmt_val}. Size: {size_gb} GB. {'Partitioned. ' if info.partitioned else ''}Eligible types: {', '.join(eligible) if eligible else 'none'}.",
            })
        result = {
            "summary": f"AI agent unavailable ({str(e)}). Used local rule-based analysis for {len(local_tables)} table(s).",
            "tables_analyzed": len(local_tables),
            "tables_failed": 0,
            "tables": local_tables,
        }
    except Exception as e:
        # Fallback to local analysis on any error
        # Recommendation is independent of plan migration types
        from app.services import validation_service, table_service
        local_tables = []
        for t in tables_to_analyze:
            info = table_service.get_table(t.source_database, t.source_table)
            if not info:
                continue
            fmt_val = info.format.value if hasattr(info.format, 'value') else info.format
            eligible = validation_service.get_eligible_types(fmt_val)
            size_gb = round(info.size_bytes / (1024**3), 2)
            # Recommend based on technical characteristics, not plan preferences
            rec = _local_recommend_type(fmt_val, size_gb, info.partitioned, eligible)
            local_tables.append({
                "database": t.source_database,
                "table_name": t.source_table,
                "recommended_type": rec,
                "estimated_size_gb": size_gb,
                "analysis_status": "completed",
                "analysis_summary": f"Format: {fmt_val}. Size: {size_gb} GB. Compatible: {', '.join(eligible) if eligible else 'none'}.",
            })
        result = {
            "summary": f"Used local analysis ({len(local_tables)} tables). Agent error: {str(e)[:200]}",
            "tables_analyzed": len(local_tables),
            "tables_failed": 0,
            "tables": local_tables,
        }

    # Store results per table
    for table_result in result.get("tables", []):
        plan_service.store_table_analysis(
            request.plan_id,
            table_result.get("database", ""),
            table_result.get("table_name", ""),
            table_result,
        )

    # Mark any unanalyzed tables not in results
    analyzed_keys = {
        f"{t['database']}.{t['table_name']}" for t in result.get("tables", [])
    }
    overall_summary = result.get("summary", "Analysis complete.")
    for t in tables_to_analyze:
        key = f"{t.source_database}.{t.source_table}"
        if key not in analyzed_keys:
            plan_service.store_table_analysis(
                request.plan_id,
                t.source_database,
                t.source_table,
                {
                    "analysis_status": "completed" if result.get("tables_analyzed", 0) > 0 else "pending",
                    "analysis_summary": overall_summary,
                },
            )

    # Return the updated plan
    updated_plan = plan_service.get_plan(request.plan_id)
    return {
        "summary": result.get("summary", "Analysis complete."),
        "schema_compatibility": result.get("schema_compatibility"),
        "tables_analyzed": result.get("tables_analyzed", 0),
        "tables_failed": result.get("tables_failed", 0),
        "plan": updated_plan.model_dump() if updated_plan else None,
    }
