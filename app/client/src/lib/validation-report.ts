// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import type { ValidationResult } from '@/types'

/** Optional table/pipeline context to enrich the report header. */
export interface ValidationReportContext {
  pipelineId: string
  sourceDatabase?: string
  sourceTable?: string
  targetDatabase?: string
  targetTable?: string
  migrationType?: string
}

// Render any value to a display string BEFORE escaping. Validation
// source/target values are not always scalars: null-count (V4) and aggregate
// (V7) checks return a per-column map, and per-partition checks return a list
// of rows. Passing those objects/arrays straight to String() yields the
// useless "[object Object]". Serialize structured values to compact JSON so
// their contents actually show in the report.
function toDisplay(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (typeof v === 'object') {
    try {
      return JSON.stringify(v)
    } catch {
      return String(v)
    }
  }
  return String(v)
}

// Escape a value for safe interpolation into HTML text/attributes. All
// validation values (source/target/detail) are treated as untrusted text.
function esc(v: unknown): string {
  if (v === null || v === undefined) return ''
  return toDisplay(v)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

const STATUS_COLOR: Record<string, string> = {
  passed: '#15803d',
  failed: '#b91c1c',
  error: '#b91c1c',
  skipped: '#6b7280',
  running: '#b45309',
}

function statusPill(status: string): string {
  const color = STATUS_COLOR[status] ?? '#6b7280'
  return `<span class="pill" style="background:${color}">${esc(status)}</span>`
}

/**
 * Build a self-contained, styled HTML report for a single validation run.
 * No external assets — safe to open offline or attach to a ticket.
 */
export function buildValidationReportHtml(
  run: ValidationResult,
  ctx: ValidationReportContext,
): string {
  const ranAt = run.completed_at || run.run_at
  const ranAtStr = ranAt ? new Date(ranAt).toLocaleString() : '—'
  const startedStr = run.run_at ? new Date(run.run_at).toLocaleString() : '—'

  const counts = (run.results || []).reduce<Record<string, number>>((acc, c) => {
    acc[c.status] = (acc[c.status] || 0) + 1
    return acc
  }, {})
  const countSummary = ['passed', 'failed', 'error', 'skipped']
    .filter((s) => counts[s])
    .map((s) => `${counts[s]} ${s}`)
    .join(' · ')

  const metaRows: [string, string][] = [
    ['Overall status', run.overall_status],
    ['Tier', run.tier],
    ['Engine', run.engine || '—'],
    ['Pipeline', ctx.pipelineId],
    ['Source', ctx.sourceTable ? `${ctx.sourceTable} (${ctx.sourceDatabase ?? ''})` : '—'],
    ['Target', ctx.targetTable ? `${ctx.targetTable} (${ctx.targetDatabase ?? ''})` : '—'],
    ['Migration type', ctx.migrationType || '—'],
    ['Run ID', run.run_id],
    ['Started', startedStr],
    ['Completed', ranAtStr],
    ['Triggered by', run.triggered_by || '—'],
  ]

  const metaHtml = metaRows
    .map(
      ([k, v]) =>
        `<tr><th>${esc(k)}</th><td>${
          k === 'Overall status' ? statusPill(String(v)) : esc(v)
        }</td></tr>`,
    )
    .join('')

  const checkRows = (run.results || [])
    .map(
      (c) => `
      <tr>
        <td class="mono">${esc(c.check_id)}</td>
        <td>${esc(c.name)}</td>
        <td>${statusPill(c.status)}</td>
        <td class="mono wrap">${esc(c.source_value)}</td>
        <td class="mono wrap">${esc(c.target_value)}</td>
        <td class="wrap">${esc(c.detail)}</td>
      </tr>`,
    )
    .join('')

  const title = `Validation report — ${
    ctx.sourceTable ? `${ctx.sourceDatabase ?? ''}.${ctx.sourceTable}` : ctx.pipelineId
  }`

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>${esc(title)}</title>
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 2rem; background: #f8fafc; color: #0f172a; }
  h1 { font-size: 1.25rem; margin: 0 0 0.25rem; }
  .sub { color: #64748b; font-size: 0.85rem; margin-bottom: 1.5rem; }
  .card { background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 1.25rem 1.5rem; margin-bottom: 1.5rem; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
  h2 { font-size: 0.95rem; text-transform: uppercase; letter-spacing: 0.05em; color: #475569; margin: 0 0 0.75rem; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th, td { text-align: left; padding: 0.5rem 0.6rem; vertical-align: top; border-bottom: 1px solid #f1f5f9; }
  .meta th { width: 180px; color: #64748b; font-weight: 500; }
  .checks th { background: #f1f5f9; color: #475569; font-weight: 600; position: sticky; top: 0; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.8rem; }
  .wrap { word-break: break-word; white-space: pre-wrap; max-width: 320px; }
  .pill { display: inline-block; padding: 0.1rem 0.5rem; border-radius: 999px; color: #fff; font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
  footer { color: #94a3b8; font-size: 0.75rem; margin-top: 2rem; }
  @media (prefers-color-scheme: dark) {
    body { background: #0b1220; color: #e2e8f0; }
    .card { background: #111a2e; border-color: #1e293b; box-shadow: none; }
    th, td { border-color: #1e293b; }
    .checks th { background: #1e293b; color: #cbd5e1; }
    .meta th, .sub, footer { color: #94a3b8; }
  }
</style>
</head>
<body>
  <h1>${esc(title)}</h1>
  <div class="sub">Migration validation report · ${countSummary || 'no checks'} · generated ${esc(new Date().toLocaleString())}</div>

  <div class="card">
    <h2>Run summary</h2>
    <table class="meta"><tbody>${metaHtml}</tbody></table>
  </div>

  <div class="card">
    <h2>Checks (${(run.results || []).length})</h2>
    <table class="checks">
      <thead><tr>
        <th>ID</th><th>Check</th><th>Status</th><th>Source</th><th>Target</th><th>Detail</th>
      </tr></thead>
      <tbody>${checkRows || '<tr><td colspan="6">No check results recorded.</td></tr>'}</tbody>
    </table>
  </div>

  <footer>Generated by Migration Assistant for Apache Iceberg. Report reflects validation run ${esc(run.run_id)}.</footer>
</body>
</html>`
}

/** Trigger a browser download of the report as a standalone .html file. */
export function downloadValidationReport(
  run: ValidationResult,
  ctx: ValidationReportContext,
): void {
  const html = buildValidationReportHtml(run, ctx)
  const blob = new Blob([html], { type: 'text/html' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  const namePart = ctx.sourceTable
    ? `${ctx.sourceDatabase ?? 'db'}.${ctx.sourceTable}`
    : ctx.pipelineId
  const stamp = (run.completed_at || run.run_at || '').replace(/[:.]/g, '-').slice(0, 19)
  a.href = url
  a.download = `validation-${namePart}-${stamp || run.run_id.slice(0, 8)}.html`
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}
