// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useTranslation } from 'react-i18next'
import { Activity } from 'lucide-react'

import { usePipelineDetail } from '@/hooks/use-pipeline'

/** Read the pipeline's persisted metadata bag (spec_json._metadata). */
function parseMeta(specJson: string | undefined): Record<string, any> {
  if (!specJson) return {}
  try {
    const spec = JSON.parse(specJson)
    return spec?._metadata ?? {}
  } catch {
    return {}
  }
}

/** Each _metadata value is itself a JSON string; parse it back to an object. */
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

function fmtNum(n: number | null | undefined): string {
  return n == null ? '—' : n.toLocaleString()
}

interface MigrationStats {
  objects_migrated?: number | null
  objects_failed?: number | null
  objects_pending?: number | null
}

interface PlanningStats {
  // objects_planned is the authoritative denominator (API: objectsPlanned);
  // total_objects is kept as a stable alias of it for backward compatibility.
  objects_scanned?: number | null
  objects_planned?: number | null
  total_objects?: number | null
  partition_count?: number | null
}

/**
 * Reports the Managed Migration data-movement stats gathered on the reconcile
 * loop (S3 Tables get_table_migration -> migrationStats / planningStats),
 * persisted under spec_json._metadata. Renders for s3tables_managed pipelines
 * once stats have been captured (i.e. after the first reconcile), covering the
 * queued/running phases and beyond.
 */
export function ManagedMigrationProgressCard({ pipelineId }: { pipelineId: string }) {
  const { t } = useTranslation()
  const { data: pipeline } = usePipelineDetail(pipelineId)

  if (!pipeline) return null
  if (pipeline.migration_type !== 's3tables_managed') return null

  const meta = parseMeta(pipeline.spec_json)
  const ms = parseJsonField<MigrationStats | null>(meta.migration_stats, null)
  const planning = parseJsonField<PlanningStats>(meta.planning_stats, {})

  // Nothing captured yet (no reconcile has run).
  if (!ms) return null

  const migrated = ms.objects_migrated ?? 0
  const pending = ms.objects_pending ?? 0
  const failed = ms.objects_failed ?? 0
  const totalObjects =
    planning.objects_planned ?? planning.total_objects ?? ((migrated + pending + failed) || 0)
  const pct = totalObjects > 0 ? Math.round((migrated / totalObjects) * 100) : null

  return (
    <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-2">
      <div className="flex items-center justify-between">
        <span className="flex items-center gap-1.5 font-semibold uppercase tracking-wide text-muted-foreground">
          <Activity className="h-3.5 w-3.5" />
          {t('managed_migration.labels.migration_progress')}{['syncing', 'sync_complete'].includes(pipeline.sync_status) ? ` (${t('managed_migration.labels.last_sync')})` : ''}
        </span>
        {pct != null && <span className="font-medium">{pct}%</span>}
      </div>

      {pct != null && (
        <div className="h-1.5 rounded-full overflow-hidden bg-muted">
          <div className="h-full bg-emerald-500" style={{ width: `${pct}%` }} />
        </div>
      )}

      <div className="grid grid-cols-3 gap-x-3 gap-y-1.5">
        <div><div className="text-muted-foreground">{t('managed_migration.labels.migrated')}</div>{fmtNum(ms.objects_migrated)} obj</div>
        <div><div className="text-muted-foreground">{t('managed_migration.labels.pending')}</div>{fmtNum(ms.objects_pending)} obj</div>
        <div className={failed > 0 ? 'text-destructive' : ''}><div className="text-muted-foreground">{t('managed_migration.labels.failed')}</div>{fmtNum(ms.objects_failed)} obj</div>
      </div>

      {(planning.partition_count != null || planning.objects_scanned != null || planning.objects_planned != null || planning.total_objects != null) && (
        <div className="flex flex-wrap gap-x-4 gap-y-1 border-t pt-2 text-muted-foreground">
          {planning.objects_scanned != null && <span>{t('managed_migration.labels.objects_scanned')} <span className="text-foreground">{fmtNum(planning.objects_scanned)}</span></span>}
          {(planning.objects_planned ?? planning.total_objects) != null && <span>{t('managed_migration.labels.objects_planned')} <span className="text-foreground">{fmtNum(planning.objects_planned ?? planning.total_objects)}</span></span>}
          {planning.partition_count != null && <span>{t('managed_migration.labels.partitions_colon')} <span className="text-foreground">{fmtNum(planning.partition_count)}</span></span>}
        </div>
      )}
    </div>
  )
}
