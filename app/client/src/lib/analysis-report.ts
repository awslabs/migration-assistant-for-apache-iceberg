// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Analysis report generation — builds a downloadable, self-contained report of
// the AI migration analysis for a plan. Produces both HTML (styled, inline CSS,
// no external deps) and Markdown. Everything is derived client-side from the
// plan + its analyzed tables; no backend call required.

import type { MigrationPlan, PlanTable } from '@/types'

const MIGRATION_TYPE_LABELS: Record<string, string> = {
  rewrite: 'Rewrite',
  incremental: 'Incremental',
  morph: 'Morph',
  add_files: 'Add Files',
}

const TARGET_LABELS: Record<string, string> = {
  s3_tables: 'S3 Tables (managed Iceberg)',
  s3_iceberg: 'Iceberg on S3 (self-managed)',
}

function mt(v: string | null | undefined): string {
  return (v && MIGRATION_TYPE_LABELS[v]) || v || '—'
}

function num(v: number | null | undefined, digits = 2, suffix = ''): string {
  if (v === null || v === undefined) return '—'
  return `${v.toFixed(digits)}${suffix}`
}

function esc(s: unknown): string {
  const str = s === null || s === undefined ? '' : String(s)
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

/** Aggregate summary numbers across the analyzed tables. */
export interface ReportTotals {
  total: number
  completed: number
  failed: number
  totalSizeGb: number
  totalCostUsd: number
  totalTimeMin: number
  byRecommendedType: Record<string, number>
  bySourceFormat: Record<string, number>
}

export function computeTotals(tables: PlanTable[]): ReportTotals {
  const totals: ReportTotals = {
    total: tables.length,
    completed: 0,
    failed: 0,
    totalSizeGb: 0,
    totalCostUsd: 0,
    totalTimeMin: 0,
    byRecommendedType: {},
    bySourceFormat: {},
  }
  for (const t of tables) {
    if (t.analysis_status === 'completed') totals.completed += 1
    if (t.analysis_status === 'failed') totals.failed += 1
    totals.totalSizeGb += t.estimated_size_gb ?? 0
    totals.totalCostUsd += t.estimated_cost_usd ?? 0
    totals.totalTimeMin += t.estimated_time_min ?? 0
    const rec = t.recommended_type || t.migration_type || 'unknown'
    totals.byRecommendedType[rec] = (totals.byRecommendedType[rec] ?? 0) + 1
    const fmt = t.source_format || 'unknown'
    totals.bySourceFormat[fmt] = (totals.bySourceFormat[fmt] ?? 0) + 1
  }
  return totals
}

function partitionSpecText(t: PlanTable): string {
  if (!t.partition_spec || t.partition_spec.length === 0) return 'None'
  return t.partition_spec
    .map(p => {
      if (p.transform === 'identity') return p.source_column
      if (p.transform === 'bucket') return `bucket(${p.num_buckets ?? '?'}, ${p.source_column})`
      if (p.transform === 'truncate') return `truncate(${p.width ?? '?'}, ${p.source_column})`
      return `${p.transform}(${p.source_column})`
    })
    .join(', ')
}

function sortOrderText(t: PlanTable): string {
  if (!t.sort_order || t.sort_order.length === 0) return 'None'
  return t.sort_order
    .map(s => `${s.column} ${(s.direction || 'asc').toUpperCase()}${s.null_order ? ` ${s.null_order.replace(/_/g, ' ')}` : ''}`)
    .join(', ')
}

function partitionKeysText(t: PlanTable): string {
  if (!t.source_partition_keys || t.source_partition_keys.length === 0) return 'Not partitioned'
  return t.source_partition_keys.map(k => `${k.name}: ${k.type}`).join(', ')
}

// ─── Markdown ────────────────────────────────────────────────────────────

export function buildMarkdownReport(
  plan: MigrationPlan,
  tables: PlanTable[],
  summary: string,
): string {
  const totals = computeTotals(tables)
  const now = new Date().toISOString()
  const lines: string[] = []

  lines.push(`# Migration Analysis Report — ${plan.name}`)
  lines.push('')
  lines.push(`Generated: ${now}`)
  lines.push('')

  // Plan overview
  lines.push('## Plan Overview')
  lines.push('')
  lines.push(`- **Plan name:** ${plan.name}`)
  if (plan.description) lines.push(`- **Description:** ${plan.description}`)
  lines.push(`- **Plan ID:** ${plan.id}`)
  lines.push(`- **Target:** ${TARGET_LABELS[plan.target] ?? plan.target}`)
  lines.push(`- **Execution engine:** ${plan.execution_engine}`)
  lines.push(`- **Swap tables:** ${plan.swap_tables ? 'Yes' : 'No'}`)
  if (plan.table_bucket_arn) lines.push(`- **Table bucket ARN:** ${plan.table_bucket_arn}`)
  lines.push(`- **Status:** ${plan.status}`)
  lines.push('')

  // Analysis summary
  lines.push('## Analysis Summary')
  lines.push('')
  if (summary) {
    lines.push(summary)
    lines.push('')
  }
  lines.push(`- **Tables analyzed:** ${totals.total}`)
  lines.push(`- **Completed:** ${totals.completed}`)
  lines.push(`- **Failed:** ${totals.failed}`)
  lines.push(`- **Total estimated size:** ${num(totals.totalSizeGb)} GB`)
  lines.push(`- **Total estimated cost:** $${num(totals.totalCostUsd)}`)
  lines.push(`- **Total estimated time:** ${num(totals.totalTimeMin, 1)} min`)
  lines.push('')

  // Breakdown by recommended type
  lines.push('### Recommended migration strategy')
  lines.push('')
  lines.push('| Strategy | Tables |')
  lines.push('| --- | --- |')
  for (const [k, v] of Object.entries(totals.byRecommendedType)) {
    lines.push(`| ${mt(k)} | ${v} |`)
  }
  lines.push('')

  // Breakdown by source format
  lines.push('### Source formats')
  lines.push('')
  lines.push('| Format | Tables |')
  lines.push('| --- | --- |')
  for (const [k, v] of Object.entries(totals.bySourceFormat)) {
    lines.push(`| ${k} | ${v} |`)
  }
  lines.push('')

  // Per-table detail
  lines.push('## Per-Table Analysis')
  lines.push('')
  for (const t of tables) {
    lines.push(`### ${t.source_database}.${t.source_table}`)
    lines.push('')
    lines.push(`- **Analysis status:** ${t.analysis_status}`)
    if (t.error_message) lines.push(`- **Error:** ${t.error_message}`)
    lines.push(`- **Source format:** ${t.source_format ?? '—'}`)
    lines.push(`- **Source location:** ${t.source_location ?? '—'}`)
    if (t.source_account) lines.push(`- **Source account:** ${t.source_account}`)
    lines.push(`- **Source compression:** ${t.source_compression || '—'}`)
    lines.push(`- **Source partitioning:** ${partitionKeysText(t)}`)
    lines.push(`- **Target:** ${t.target_database}.${t.target_table} (${TARGET_LABELS[t.target_type ?? ''] ?? t.target_type ?? plan.target})`)
    if (t.target_location) lines.push(`- **Target location:** ${t.target_location}`)
    if (t.target_bucket_arn) lines.push(`- **Target bucket ARN:** ${t.target_bucket_arn}`)
    lines.push(`- **Recommended strategy:** ${mt(t.recommended_type)}${t.recommended_type && t.recommended_type !== t.migration_type ? ` (currently: ${mt(t.migration_type)})` : ''}`)
    if (t.recommended_batch_size !== null && t.recommended_batch_size !== undefined) {
      lines.push(`- **Recommended batch size:** ${t.recommended_batch_size}`)
    }
    lines.push(`- **Estimated size:** ${num(t.estimated_size_gb)} GB`)
    lines.push(`- **Estimated cost:** $${num(t.estimated_cost_usd)}`)
    lines.push(`- **Estimated time:** ${num(t.estimated_time_min, 1)} min`)
    lines.push(`- **Iceberg partition spec:** ${partitionSpecText(t)}`)
    lines.push(`- **Sort order:** ${sortOrderText(t)}`)
    if (t.table_properties && Object.keys(t.table_properties).length > 0) {
      const props = Object.entries(t.table_properties).map(([k, v]) => `${k}=${v}`).join(', ')
      lines.push(`- **Table properties:** ${props}`)
    }
    if (t.analysis_summary) {
      lines.push('')
      lines.push('**Analysis reasoning:**')
      lines.push('')
      lines.push(`> ${t.analysis_summary.replace(/\n/g, '\n> ')}`)
    }
    lines.push('')
  }

  return lines.join('\n')
}

// ─── HTML ────────────────────────────────────────────────────────────────

function tableRow(label: string, value: string): string {
  return `<tr><th>${esc(label)}</th><td>${value}</td></tr>`
}

export function buildHtmlReport(
  plan: MigrationPlan,
  tables: PlanTable[],
  summary: string,
): string {
  const totals = computeTotals(tables)
  const now = new Date()

  const recRows = Object.entries(totals.byRecommendedType)
    .map(([k, v]) => `<tr><td>${esc(mt(k))}</td><td>${v}</td></tr>`)
    .join('')
  const fmtRows = Object.entries(totals.bySourceFormat)
    .map(([k, v]) => `<tr><td>${esc(k)}</td><td>${v}</td></tr>`)
    .join('')

  const tableSections = tables
    .map(t => {
      const recChanged = t.recommended_type && t.recommended_type !== t.migration_type
      const statusClass = t.analysis_status === 'completed' ? 'ok' : t.analysis_status === 'failed' ? 'fail' : 'warn'
      const rows = [
        tableRow('Analysis status', `<span class="badge ${statusClass}">${esc(t.analysis_status)}</span>`),
        t.error_message ? tableRow('Error', `<span class="err">${esc(t.error_message)}</span>`) : '',
        tableRow('Source format', esc(t.source_format ?? '—')),
        tableRow('Source location', `<code>${esc(t.source_location ?? '—')}</code>`),
        t.source_account ? tableRow('Source account', esc(t.source_account)) : '',
        tableRow('Source compression', esc(t.source_compression || '—')),
        tableRow('Source partitioning', esc(partitionKeysText(t))),
        tableRow('Target', `${esc(t.target_database)}.${esc(t.target_table)} <span class="muted">(${esc(TARGET_LABELS[t.target_type ?? ''] ?? t.target_type ?? plan.target)})</span>`),
        t.target_location ? tableRow('Target location', `<code>${esc(t.target_location)}</code>`) : '',
        t.target_bucket_arn ? tableRow('Target bucket ARN', `<code>${esc(t.target_bucket_arn)}</code>`) : '',
        tableRow('Recommended strategy', `<strong>${esc(mt(t.recommended_type))}</strong>${recChanged ? ` <span class="muted">(currently: ${esc(mt(t.migration_type))})</span>` : ''}`),
        (t.recommended_batch_size !== null && t.recommended_batch_size !== undefined) ? tableRow('Recommended batch size', esc(t.recommended_batch_size)) : '',
        tableRow('Estimated size', `${num(t.estimated_size_gb)} GB`),
        tableRow('Estimated cost', `$${num(t.estimated_cost_usd)}`),
        tableRow('Estimated time', `${num(t.estimated_time_min, 1)} min`),
        tableRow('Iceberg partition spec', esc(partitionSpecText(t))),
        tableRow('Sort order', esc(sortOrderText(t))),
        (t.table_properties && Object.keys(t.table_properties).length > 0)
          ? tableRow('Table properties', esc(Object.entries(t.table_properties).map(([k, v]) => `${k}=${v}`).join(', ')))
          : '',
      ].filter(Boolean).join('')

      const reasoning = t.analysis_summary
        ? `<div class="reasoning"><div class="reasoning-label">Analysis reasoning</div><p>${esc(t.analysis_summary)}</p></div>`
        : ''

      return `<section class="table-card">
        <h3>${esc(t.source_database)}.${esc(t.source_table)}</h3>
        <table class="kv">${rows}</table>
        ${reasoning}
      </section>`
    })
    .join('')

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Migration Analysis Report — ${esc(plan.name)}</title>
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    line-height: 1.5; color: #1a1a1a; background: #fff; margin: 0; padding: 2rem; max-width: 1100px; margin-inline: auto; }
  h1 { font-size: 1.6rem; margin: 0 0 .25rem; }
  h2 { font-size: 1.25rem; margin: 2rem 0 .75rem; border-bottom: 2px solid #e5e7eb; padding-bottom: .35rem; }
  h3 { font-size: 1.05rem; margin: 0 0 .75rem; }
  .meta { color: #6b7280; font-size: .85rem; margin-bottom: 1.5rem; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .75rem; margin: 1rem 0; }
  .stat { border: 1px solid #e5e7eb; border-radius: 8px; padding: .75rem 1rem; }
  .stat .label { font-size: .75rem; color: #6b7280; text-transform: uppercase; letter-spacing: .03em; }
  .stat .value { font-size: 1.4rem; font-weight: 600; margin-top: .15rem; }
  table { border-collapse: collapse; width: 100%; font-size: .9rem; }
  table.summary th, table.summary td { text-align: left; padding: .4rem .6rem; border-bottom: 1px solid #eee; }
  table.summary { max-width: 420px; }
  .table-card { border: 1px solid #e5e7eb; border-radius: 10px; padding: 1rem 1.25rem; margin: 1rem 0; }
  table.kv th { text-align: left; width: 210px; vertical-align: top; color: #374151; font-weight: 500; padding: .3rem .6rem .3rem 0; }
  table.kv td { padding: .3rem 0; vertical-align: top; }
  code { background: #f3f4f6; padding: .1rem .35rem; border-radius: 4px; font-size: .82rem; word-break: break-all; }
  .muted { color: #6b7280; }
  .err { color: #b91c1c; }
  .badge { display: inline-block; padding: .1rem .5rem; border-radius: 999px; font-size: .75rem; font-weight: 600; }
  .badge.ok { background: #dcfce7; color: #166534; }
  .badge.fail { background: #fee2e2; color: #991b1b; }
  .badge.warn { background: #fef9c3; color: #854d0e; }
  .reasoning { margin-top: .75rem; background: #f9fafb; border-left: 3px solid #6366f1; padding: .6rem .9rem; border-radius: 0 6px 6px 0; }
  .reasoning-label { font-size: .72rem; text-transform: uppercase; letter-spacing: .03em; color: #6b7280; margin-bottom: .25rem; }
  .reasoning p { margin: 0; white-space: pre-wrap; }
  @media (prefers-color-scheme: dark) {
    body { color: #e5e7eb; background: #0f1115; }
    h2 { border-color: #2a2f3a; }
    .stat, .table-card { border-color: #2a2f3a; }
    table.summary th, table.summary td { border-color: #2a2f3a; }
    code { background: #1a1f28; }
    .reasoning { background: #161a22; }
    .muted, .meta, .stat .label, .reasoning-label { color: #9ca3af; }
  }
</style>
</head>
<body>
  <h1>Migration Analysis Report</h1>
  <div class="meta">${esc(plan.name)} &middot; generated ${esc(now.toLocaleString())}</div>

  <h2>Plan Overview</h2>
  <table class="summary">
    ${tableRow('Plan name', esc(plan.name))}
    ${plan.description ? tableRow('Description', esc(plan.description)) : ''}
    ${tableRow('Plan ID', `<code>${esc(plan.id)}</code>`)}
    ${tableRow('Target', esc(TARGET_LABELS[plan.target] ?? plan.target))}
    ${tableRow('Execution engine', esc(plan.execution_engine))}
    ${tableRow('Swap tables', plan.swap_tables ? 'Yes' : 'No')}
    ${plan.table_bucket_arn ? tableRow('Table bucket ARN', `<code>${esc(plan.table_bucket_arn)}</code>`) : ''}
    ${tableRow('Status', esc(plan.status))}
  </table>

  <h2>Analysis Summary</h2>
  ${summary ? `<p>${esc(summary)}</p>` : ''}
  <div class="cards">
    <div class="stat"><div class="label">Tables</div><div class="value">${totals.total}</div></div>
    <div class="stat"><div class="label">Completed</div><div class="value">${totals.completed}</div></div>
    <div class="stat"><div class="label">Failed</div><div class="value">${totals.failed}</div></div>
    <div class="stat"><div class="label">Total size</div><div class="value">${num(totals.totalSizeGb)} GB</div></div>
    <div class="stat"><div class="label">Est. cost</div><div class="value">$${num(totals.totalCostUsd)}</div></div>
    <div class="stat"><div class="label">Est. time</div><div class="value">${num(totals.totalTimeMin, 1)} min</div></div>
  </div>

  <h3>Recommended migration strategy</h3>
  <table class="summary"><tr><th>Strategy</th><th>Tables</th></tr>${recRows}</table>

  <h3>Source formats</h3>
  <table class="summary"><tr><th>Format</th><th>Tables</th></tr>${fmtRows}</table>

  <h2>Per-Table Analysis</h2>
  ${tableSections}
</body>
</html>`
}

// ─── Download trigger ──────────────────────────────────────────────────────

function slugify(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'plan'
}

function triggerDownload(content: string, filename: string, mime: string): void {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

export function downloadAnalysisReport(
  plan: MigrationPlan,
  summary: string,
  format: 'html' | 'md',
): void {
  const tables = plan.tables ?? []
  const stamp = new Date().toISOString().slice(0, 10)
  const base = `IMA-analysis-${slugify(plan.name)}-${stamp}`
  if (format === 'html') {
    triggerDownload(buildHtmlReport(plan, tables, summary), `${base}.html`, 'text/html;charset=utf-8')
  } else {
    triggerDownload(buildMarkdownReport(plan, tables, summary), `${base}.md`, 'text/markdown;charset=utf-8')
  }
}
