// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useTranslation } from 'react-i18next'
import { Loader2, ShieldCheck, AlertTriangle } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/copy-button'
import { useToast } from '@/components/ui/toast'
import { usePipelineDetail, useApproveMigration } from '@/hooks/use-pipeline'

function parseMeta(specJson: string | undefined): Record<string, any> {
  if (!specJson) return {}
  try {
    const spec = JSON.parse(specJson)
    return spec?._metadata ?? {}
  } catch {
    return {}
  }
}

function parseJsonField<T>(v: unknown, fallback: T): T {
  if (v == null) return fallback
  if (typeof v === 'string') {
    try {
      return JSON.parse(v) as T
    } catch {
      return fallback
    }
  }
  return v as T
}

/**
 * Managed Migration plan-review card. Renders only while the pipeline is a
 * Managed Migration paused at awaiting_review — showing the planning stats, the
 * blocking compatibility findings (counted per category) and links to the two
 * compatibility reports in S3 (per-column type changes live in the table
 * compatibility report), plus an Approve button that resumes the migration.
 * Reads everything from the pipeline's persisted spec_json._metadata (captured
 * by the engine on each reconcile).
 */
export function ManagedMigrationReviewCard({ pipelineId }: { pipelineId: string }) {
  const { t } = useTranslation()
  const { data: pipeline } = usePipelineDetail(pipelineId)
  const approve = useApproveMigration()
  const { showToast } = useToast()

  if (!pipeline) return null
  if (pipeline.migration_type !== 's3tables_managed') return null
  if (pipeline.sync_status !== 'awaiting_review') return null

  const meta = parseMeta(pipeline.spec_json)
  const planning = parseJsonField<Record<string, any>>(meta.planning_stats, {})
  // Blocking findings per category (access | metadata | data | internal).
  const invalidFindings: Record<string, number> = planning.invalid_findings || {}
  // The two reports written during planning, before any data is copied.
  const reports = [
    { label: t('managed_migration.labels.table_compatibility_report'), uri: (meta.table_compat_report_uri || '') as string },
    { label: t('managed_migration.labels.object_compatibility_report'), uri: (meta.object_compat_report_uri || '') as string },
  ].filter(r => r.uri)

  const onApprove = () => {
    approve.mutate(pipelineId, {
      onSuccess: () => showToast(`Migration for ${pipeline.source_database}.${pipeline.source_table} approved — starting data movement`, 'success'),
      onError: (err: any) => showToast(err?.message || 'Failed to approve migration', 'error'),
    })
  }

  return (
    <div className="rounded-md border border-amber-200 bg-amber-50 dark:border-amber-800 dark:bg-amber-950/30 p-3 text-xs space-y-3">
      <div className="flex items-center justify-between">
        <span className="flex items-center gap-1.5 font-medium">
          <ShieldCheck className="h-3.5 w-3.5 text-amber-600" />
          {t('managed_migration.labels.plan_review')}
        </span>
        <Badge variant="warning">{t('managed_migration.labels.awaiting_review')}</Badge>
      </div>

      <p className="text-muted-foreground">
        {t('managed_migration.descriptions.review_intro')}
      </p>

      {/* Planning summary */}
      {(planning.objects_scanned != null || planning.objects_planned != null || planning.total_objects != null || planning.partition_count != null) && (
        <div className="grid grid-cols-3 gap-2">
          {planning.objects_scanned != null && (
            <div><span className="text-muted-foreground">{t('managed_migration.labels.objects_scanned')}</span> {planning.objects_scanned}</div>
          )}
          {(planning.objects_planned ?? planning.total_objects) != null && (
            <div><span className="text-muted-foreground">{t('managed_migration.labels.objects_planned')}</span> {planning.objects_planned ?? planning.total_objects}</div>
          )}
          {planning.partition_count != null && (
            <div><span className="text-muted-foreground">{t('managed_migration.labels.partitions')}</span> {planning.partition_count}</div>
          )}
        </div>
      )}

      {/* Invalid findings (compatibility issues) */}
      {Object.keys(invalidFindings).length > 0 && (
        <div className="space-y-1">
          <span className="flex items-center gap-1.5 font-medium text-amber-700">
            <AlertTriangle className="h-3.5 w-3.5" />
            {t('managed_migration.labels.compatibility_findings')}
          </span>
          {Object.entries(invalidFindings).map(([k, v]) => (
            <div key={k} className="flex justify-between">
              <span className="text-muted-foreground">{k}</span>
              <span>{v}</span>
            </div>
          ))}
        </div>
      )}

      {/* Compatibility report locations (S3). The table report lists each blocking
          finding and, per destination column, its type change and what the chosen
          migration policies do to it. */}
      {reports.map(r => (
        <div key={r.label} className="flex items-center justify-between gap-2">
          <span className="text-muted-foreground shrink-0">{r.label}</span>
          <span className="flex items-center gap-1 min-w-0">
            <span className="font-mono truncate" title={r.uri}>{r.uri}</span>
            <CopyButton value={r.uri} label="report location" />
          </span>
        </div>
      ))}

      <div className="flex justify-end pt-1 border-t">
        <Button size="sm" onClick={onApprove} disabled={approve.isPending}>
          {approve.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
          {t('managed_migration.labels.approve_migration')}
        </Button>
      </div>
    </div>
  )
}
