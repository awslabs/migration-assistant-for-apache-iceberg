// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { usePipelineDetail, usePipelineBatches, usePipelineCosts, usePlanCosts, useApproveMigration } from '@/hooks/use-pipeline'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/toast'
import { CopyButton } from '@/components/ui/copy-button'
import { usePlan, useExecutePlan, useCancelPlan, usePausePlan, useResumePlan, useDeletePlan, usePauseTable, useResumeTable, useCancelTable, useRemoveTableFromPlan, useSyncPlan, useFinalizeMigration, useReplan, useRetryTable, useArchivePlan, useResumeTableFromFailure, useSyncNow } from '@/hooks/use-plans'
import { useTables } from '@/hooks/use-migrations'
import { usePlanAccess } from '@/hooks/use-permissions'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { ValidationDialog } from '@/components/validation-dialog'
import { ValidationResultsCard } from '@/components/validation-results-card'
import { ManagedMigrationReviewCard } from '@/components/managed-migration-review-card'
import { ManagedMigrationRequestCard } from '@/components/managed-migration-request-card'
import { ManagedMigrationProgressCard } from '@/components/managed-migration-progress-card'
import { useRunPlanValidation, useRunTablesValidation, usePlanValidationStatus } from '@/hooks/use-data-validation'
import { ValidationStatusIcon } from '@/components/validation-status-icon'
import type { ValidationTier, ValidationSkipped } from '@/types'
import { Loader2, Play, Pause, XCircle, Trash2, RotateCcw, Search, X, ChevronLeft, ChevronRight, ChevronDown, ArrowUpDown, ArrowUp, ArrowDown, CheckCircle, FastForward, MoreHorizontal, ShieldCheck } from 'lucide-react'
import { useMemo, useState, useEffect } from 'react'
import { useParams, useNavigate } from '@tanstack/react-router'
import { useTranslation } from 'react-i18next'
import type { PlanStatus, TableInfo, MigrationPlan } from '@/types'

const statusV: Record<PlanStatus, 'default'|'secondary'|'success'|'destructive'|'warning'> = {
  draft:'secondary',ready:'default',staged:'default',in_progress:'warning',paused:'secondary',cancelled:'secondary',completed:'success',failed:'destructive'}
const mtL: Record<string,string> = {rewrite:'Rewrite',incremental:'Incremental',morph:'Morph',add_files:'Add Files',s3tables_managed:'Managed Migration'}
const targetL: Record<string,string> = {s3_tables:'S3 Tables',s3_iceberg:'Iceberg on S3'}
const fmtBadge: Record<string, 'default'|'secondary'|'outline'|'success'|'warning'> = {
  csv:'outline', avro:'secondary', parquet:'default', json:'outline', orc:'outline', hudi:'warning', delta:'secondary', iceberg:'success'}
const tblStatusV: Record<string, 'default'|'secondary'|'success'|'destructive'|'warning'|'outline'> = {
  staged:'outline', pending:'secondary', in_progress:'warning', completed:'success', cancelled:'outline', error:'destructive', failed:'destructive', in_sync:'success', out_of_sync:'warning', executing:'warning', initial_load:'default', translating:'warning', paused:'secondary', awaiting_review:'warning',
  // Managed Migration phase states.
  initial_load_complete:'success', sync_complete:'success', syncing:'warning', sync_failed:'warning'}

// Human-readable table-status label. Managed Migration phases get bespoke
// wording ("Initial load completed" / "Last sync completed"); everything else
// falls back to the underscore-to-space default.
const tblStatusLabel = (status: string): string => {
  switch (status) {
    case 'initial_load_complete': return 'Initial load completed'
    case 'sync_complete': return 'Last sync successful'
    case 'sync_failed': return 'Last sync failed'
    case 'syncing': return 'Syncing'
    case 'initial_load': return 'initial load'
    case 'executing': return 'syncing'
    default: return status.replace(/_/g, ' ')
  }
}
type SortDir = 'asc'|'desc'|null

function MigrationActions({ plan, canValidate = false, onValidate, validating = false }: { plan: MigrationPlan; canValidate?: boolean; onValidate?: () => void; validating?: boolean }) {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const { showToast } = useToast()
  const access = usePlanAccess(plan)
  const canManage = access === 'manage'
  const executePlan = useExecutePlan(); const cancelPlan = useCancelPlan()
  const pausePlan = usePausePlan(); const resumePlan = useResumePlan(); const deletePlan = useDeletePlan()
  const finalizeMigration = useFinalizeMigration(); const replan = useReplan(); const archivePlan = useArchivePlan()
  const [confirm, setConfirm] = useState<'cancel'|'delete'|'pause'|'resume'|'finalize'|'archive'|'replan'|null>(null)
  const [menuOpen, setMenuOpen] = useState(false)
  const canFinalize = plan.tables.length > 0 &&
    plan.tables.every(t =>
      (t.migration_type === 'rewrite' && t.status === 'staged') ||
      (['incremental','morph','add_files'].includes(t.migration_type) && t.status === 'in_sync') ||
      (t.migration_type === 's3tables_managed' && ['initial_load_complete','sync_complete'].includes(t.status)) ||
      t.status === 'completed'
    ) && plan.tables.some(t => t.status !== 'completed')
  const s = plan.status
  if (!canManage) return null
  return (<>
    {/* Full-screen overlay while executing */}
    {executePlan.isPending && (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
        <div className="flex flex-col items-center gap-3 rounded-lg border bg-background p-8 shadow-lg">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
          <span className="text-sm font-medium">{t('common.loading.preparing_execution')}</span>
          <span className="text-xs text-muted-foreground">{t('common.loading.submitting_pipelines')}</span>
          <span className="text-xs text-muted-foreground">{t('common.loading.might_take_minutes')}</span>
        </div>
      </div>
    )}
    {/* Full-screen overlay while deleting */}
    {deletePlan.isPending && (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
        <div className="flex flex-col items-center gap-3 rounded-lg border bg-background p-8 shadow-lg">
          <Loader2 className="h-8 w-8 animate-spin text-destructive" />
          <span className="text-sm font-medium">{t('common.loading.deleting_migration')}</span>
          <span className="text-xs text-muted-foreground">{t('common.loading.cleaning_up')}</span>
          <span className="text-xs text-muted-foreground">{t('common.loading.might_take_minutes')}</span>
        </div>
      </div>
    )}
    {finalizeMigration.isPending && (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
        <div className="flex flex-col items-center gap-3 rounded-lg border bg-background p-8 shadow-lg">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
          <span className="text-sm font-medium">{t('common.loading.finalizing_migration')}</span>
          <span className="text-xs text-muted-foreground">{t('common.loading.completing_cutover')}</span>
          <span className="text-xs text-muted-foreground">{t('common.loading.might_take_minutes')}</span>
        </div>
      </div>
    )}
    {(() => {
      // All migration-level actions folded into one dropdown. Build the item
      // list from the plan status (+ validation), then render.
      const items: { key: string; label: string; icon: React.ReactNode; onClick: () => void; variant?: 'default' | 'destructive'; disabled?: boolean }[] = []
      if (canValidate && onValidate) {
        items.push({ key: 'validate', label: 'Perform Validations', icon: <ShieldCheck className="h-3.5 w-3.5" />, onClick: onValidate, disabled: validating })
      }
      if (s === 'staged') {
        items.push({ key: 'execute', label: t('actions.actions.execute'), icon: <Play className="h-3.5 w-3.5" />, disabled: executePlan.isPending,
          onClick: () => executePlan.mutate(plan.id, { onSuccess: () => showToast(`Migration "${plan.name}" execution started`, 'success'), onError: (err: any) => showToast(err?.message || 'Failed to execute', 'error') }) })
        items.push({ key: 'cancel', label: t('common.actions.cancel'), icon: <XCircle className="h-3.5 w-3.5" />, onClick: () => setConfirm('cancel') })
      } else if (s === 'in_progress') {
        if (canFinalize) items.push({ key: 'finalize', label: t('actions.actions.finalize_migration'), icon: <CheckCircle className="h-3.5 w-3.5" />, disabled: finalizeMigration.isPending, onClick: () => setConfirm('finalize') })
        items.push({ key: 'pause', label: t('actions.actions.pause_all'), icon: <Pause className="h-3.5 w-3.5" />, onClick: () => setConfirm('pause') })
        items.push({ key: 'cancel', label: t('common.actions.cancel'), icon: <XCircle className="h-3.5 w-3.5" />, onClick: () => setConfirm('cancel') })
      } else if (s === 'paused') {
        items.push({ key: 'resume', label: t('actions.actions.resume_all'), icon: <Play className="h-3.5 w-3.5" />, onClick: () => setConfirm('resume') })
        items.push({ key: 'cancel', label: t('common.actions.cancel'), icon: <XCircle className="h-3.5 w-3.5" />, onClick: () => setConfirm('cancel') })
        items.push({ key: 'delete', label: t('common.actions.delete'), icon: <Trash2 className="h-3.5 w-3.5" />, variant: 'destructive', onClick: () => setConfirm('delete') })
      } else if (s === 'completed') {
        items.push({ key: 'archive', label: t('actions.actions.archive'), icon: <CheckCircle className="h-3.5 w-3.5" />, disabled: archivePlan.isPending, onClick: () => setConfirm('archive') })
      } else if (s === 'cancelled') {
        items.push({ key: 'replan', label: t('actions.actions.back_to_planning'), icon: <RotateCcw className="h-3.5 w-3.5" />, disabled: replan.isPending, onClick: () => setConfirm('replan') })
        items.push({ key: 'delete', label: t('common.actions.delete'), icon: <Trash2 className="h-3.5 w-3.5" />, variant: 'destructive', onClick: () => setConfirm('delete') })
      }
      if (items.length === 0) return null
      return (
        <div className="relative">
          <Button variant="outline" size="sm" className="gap-1" onClick={() => setMenuOpen(o => !o)}>
            <MoreHorizontal className="h-3.5 w-3.5" />Actions
            <ChevronDown className={`h-3 w-3 transition-transform ${menuOpen ? 'rotate-180' : ''}`} />
          </Button>
          {menuOpen && <>
            <div className="fixed inset-0 z-30" onClick={() => setMenuOpen(false)} />
            <div className="absolute right-0 top-full mt-1 z-40 w-52 rounded-md border bg-popover shadow-md py-1 text-sm">
              {items.map(it => (
                <button key={it.key} disabled={it.disabled}
                  className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${it.disabled ? 'opacity-40 cursor-not-allowed' : 'hover:bg-accent cursor-pointer'} ${it.variant === 'destructive' ? 'text-destructive' : ''}`}
                  onClick={() => { if (it.disabled) return; setMenuOpen(false); it.onClick() }}>
                  {it.icon}{it.label}
                </button>
              ))}
            </div>
          </>}
        </div>
      )
    })()}
    <ConfirmDialog open={confirm==='finalize'} title="Finalize Migration"
      message={`This will perform the cutover: rewrite tables will execute their CTAS, and in-sync incremental/morph/add_files tables will be marked complete. The original tables will be replaced. This action cannot be undone.`}
      confirmLabel="Finalize" onConfirm={()=>{setConfirm(null);finalizeMigration.mutate(plan.id,{onSuccess:()=>showToast(`Migration "${plan.name}" finalized`,'success'),onError:(err:any)=>showToast(err?.message||'Failed to finalize','error')})}} onCancel={()=>setConfirm(null)}/>
    <ConfirmDialog open={confirm==='pause'} title="Pause All Migrations"
      message={`This action will pause all migrations in the plan "${plan.name}" for all ${plan.tables.length} table(s). Do you wish to proceed?`}
      confirmLabel="Pause All" onConfirm={()=>{setConfirm(null);pausePlan.mutate(plan.id,{onSuccess:()=>showToast(`All migrations paused for "${plan.name}"`,'success'),onError:(err:any)=>showToast(err?.message||'Failed to pause','error')})}} onCancel={()=>setConfirm(null)}/>
    <ConfirmDialog open={confirm==='resume'} title="Resume All Migrations"
      message={`This action will resume all migrations in the plan "${plan.name}" for all ${plan.tables.length} table(s). Do you wish to proceed?`}
      confirmLabel="Resume All" onConfirm={()=>{setConfirm(null);resumePlan.mutate(plan.id,{onSuccess:()=>showToast(`All migrations resumed for "${plan.name}"`,'success'),onError:(err:any)=>showToast(err?.message||'Failed to resume','error')})}} onCancel={()=>setConfirm(null)}/>
    <ConfirmDialog open={confirm==='cancel'} title="Cancel Migration"
      message={`Cancel "${plan.name}"? This will stop all in-progress table migrations.`}
      confirmLabel="Cancel Migration" variant="destructive" onConfirm={()=>{setConfirm(null);cancelPlan.mutate(plan.id,{onSuccess:()=>showToast(`Migration "${plan.name}" cancelled`,'success'),onError:(err:any)=>showToast(err?.message||'Failed to cancel','error')})}} onCancel={()=>setConfirm(null)}/>
    <ConfirmDialog open={confirm==='delete'} title="Delete Migration"
      message={`Delete "${plan.name}"? All ${plan.tables.length} table(s) will be released back to the inventory. This cannot be undone.`}
      confirmLabel="Delete" variant="destructive" onConfirm={()=>{setConfirm(null);deletePlan.mutate(plan.id,{onSuccess:()=>{showToast(`Migration "${plan.name}" deleted`,'success');navigate({to:'/migrations'})},onError:(err:any)=>showToast(err?.message||'Failed to delete','error')})}} onCancel={()=>setConfirm(null)}/>
    <ConfirmDialog open={confirm==='archive'} title="Archive Migration"
      message={`Archive "${plan.name}"? It will be moved out of the active migrations list. You can still view it later.`}
      confirmLabel="Archive" onConfirm={()=>{setConfirm(null);archivePlan.mutate(plan.id,{onSuccess:()=>{showToast(`Migration "${plan.name}" archived`,'success');navigate({to:'/migrations'})},onError:(err:any)=>showToast(err?.message||'Failed to archive','error')})}} onCancel={()=>setConfirm(null)}/>
    <ConfirmDialog open={confirm==='replan'} title="Back to Planning"
      message={`Move "${plan.name}" back to planning? The migration will return to the draft/planning stage so it can be edited and re-run.`}
      confirmLabel="Back to Planning" onConfirm={()=>{setConfirm(null);replan.mutate(plan.id,{onSuccess:()=>{showToast(`Moved "${plan.name}" back to planning`,'success');navigate({to:'/plans/$planId',params:{planId:plan.id}})},onError:(err:any)=>showToast(err?.message||'Failed to replan','error')})}} onCancel={()=>setConfirm(null)}/>
  </>)
}

function CollapsibleSection({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: React.ReactNode }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className="space-y-2">
      <button className="flex items-center gap-1.5 w-full text-left cursor-pointer" onClick={() => setOpen(!open)}>
        <ChevronDown className={`h-3.5 w-3.5 text-muted-foreground transition-transform ${open ? '' : '-rotate-90'}`} />
        <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{title}</h4>
      </button>
      {open && children}
    </div>
  )
}

const batchStatusColor: Record<string, string> = {
  completed: 'bg-emerald-500', running: 'bg-blue-500 animate-pulse', pending: 'bg-zinc-300 dark:bg-zinc-600', failed: 'bg-red-500',
}
const batchStatusBadge: Record<string, 'success'|'default'|'secondary'|'destructive'|'warning'> = {
  completed: 'success', running: 'default', pending: 'secondary', failed: 'destructive',
}

function PipelineProgressPane({ pipelineId, migrationType }: { pipelineId: string; migrationType?: string }) {
  const { t } = useTranslation()
  const { data: pipeline } = usePipelineDetail(pipelineId)
  const { data: batchData } = usePipelineBatches(pipelineId)
  const { data: costs } = usePipelineCosts(pipelineId)

  if (!pipeline && !batchData) return null

  const batches = batchData?.batches ?? []
  const summary = batchData?.summary ?? {}
  const completedCount = summary.completed?.count ?? 0
  const totalCount = batches.length
  const runningCount = summary.running?.count ?? 0
  const failedCount = summary.failed?.count ?? 0
  const pendingCount = summary.pending?.count ?? 0

  // Parse batch partition labels for display
  const formatBatchLabel = (partitions: string, batchNum: number): string => {
    try {
      const parsed = JSON.parse(partitions)
      if (!Array.isArray(parsed) || parsed.length === 0) return '—'
      const first = parsed[0]
      if (first === '__create_table__') return 'Create target table'
      if (first === '__rewrite_ctas__') return 'Rewrite (CTAS)'
      if (first === '__mm_initial_load__') return 'Initial load'
      if (first === '__mm_sync__') return 'Sync'
      if (first.startsWith('__add_files_batch_')) {
        const match = first.match(/__add_files_batch_(\d+)__(\d+)_partitions__/)
        return match ? `Add files - batch ${match[1]} (${match[2]} partitions)` : `Add files - batch ${batchNum}`
      }
      if (first === '__morph_setup__') return 'Setup (tracker + schema + table)'
      if (first === '__morph_initial_load__') return 'Initial load (scan + manifest)'
      if (first === '__xtable_convert__') return 'XTable conversion (Hudi/Delta → Iceberg)'
      if (first === '__full_insert__') return 'Initial load - full table'
      if (first === '__initial_ctas__') return 'Create target table'
      if (first === '__full_table__') return 'Initial load - full table'
      if (first.startsWith('__discover_date_range__')) return 'Discover partition range'
      if (first.startsWith('__date_range__')) {
        const parts = first.replace('__date_range__', '').replace(/^_+|_+$/g, '').split('__')
        if (parts.length >= 4) return `Initial load - ${parts[2]} → ${parts[3]}`
        return `Initial load - batch ${batchNum}`
      }
      if (first.startsWith('__sync__append__')) return 'Sync - append only'
      if (first.startsWith('__sync__rewrite__')) {
        const match = first.match(/__sync__rewrite__(\d+)_partitions__/)
        return match ? `Sync - partition rewrite (${match[1]})` : 'Sync - partition rewrite'
      }
      if (first.startsWith('__sync__full_rewrite__')) return 'Sync - partition rewrite'
      if (first.startsWith('__sync__full_table__')) return 'Sync - full table'
      if (first.startsWith('__sync__full_fallback__')) return 'Sync - full rewrite'
      if (first.startsWith('__sync__xtable__')) return 'Sync - XTable incremental'
      if (first.startsWith('__sync__morph__')) {
        const match = first.match(/__sync__morph__(\d+)_events__/)
        return match ? `Sync - manifest update (${match[1]} events)` : 'Sync - manifest update'
      }
      if (first.startsWith('__sync__add_files__')) {
        const match = first.match(/__sync__add_files__(\d+)_partitions__/)
        return match ? `Sync - add_files (${match[1]} partitions)` : 'Sync - add_files'
      }
      if (first.startsWith('__sync__partition_rewrite__')) {
        const match = first.match(/__sync__partition_rewrite__(\d+)_events__/)
        return match ? `Sync - partition rewrite (${match[1]} events)` : 'Sync - partition rewrite'
      }
      if (first.startsWith('__sync__')) return 'Sync'
      return first.substring(0, 40)
    } catch { return '—' }
  }

  const formatTime = (ts: string): string => {
    if (!ts) return ''
    try {
      const d = new Date(ts)
      return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })
    } catch { return '' }
  }

  return (
    <div className="space-y-3">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{t('pipeline.labels.pipeline_progress')}</h4>

      {/* Managed Migration plan-review card — renders only for s3tables_managed
          at awaiting_review (self-gated inside the component). */}
      <ManagedMigrationReviewCard pipelineId={pipelineId} />

      {/* Managed Migration data-movement stats from the reconcile loop —
          renders for s3tables_managed once stats are captured (self-gated). */}
      <ManagedMigrationProgressCard pipelineId={pipelineId} />

      {/* Pipeline status summary */}
      {pipeline && (
        <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-2">
          <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('pipeline.labels.pipeline_id', 'Pipeline ID')}</span>
            <span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{pipelineId}</span><CopyButton value={pipelineId} label="pipeline ID" /></span></div>
          <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.status')}</span><Badge variant={batchStatusBadge[pipeline.status] ?? 'secondary'}>{pipeline.status}</Badge></div>
          {migrationType !== 'rewrite' && (
            <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.sync_status')}</span>
              <span className="flex items-center gap-1.5">
                {(pipeline.sync_status === 'initial_load' || pipeline.sync_status === 'syncing') && <Loader2 className="h-3 w-3 animate-spin text-amber-500" />}
                <Badge variant={
                  pipeline.sync_status === 'in_sync' || pipeline.sync_status === 'initial_load_complete' || pipeline.sync_status === 'sync_complete' ? 'success'
                  : pipeline.sync_status === 'out_of_sync' || pipeline.sync_status === 'initial_load' || pipeline.sync_status === 'syncing' || pipeline.sync_status === 'sync_failed' ? 'warning'
                  : 'secondary'}>{(() => {
                    if (pipeline.sync_status === 'initial_load_complete') return 'initial load completed'
                    if (pipeline.sync_status === 'sync_failed') return 'last sync failed'
                    if (pipeline.sync_status === 'sync_complete') {
                      // Append the last sync completion time to the badge.
                      const syncTimes = batches
                        .filter((b: any) => b.status === 'completed' && b.partitions?.includes('__mm_sync__') && b.completed_at)
                        .map((b: any) => b.completed_at)
                        .sort()
                      const lastSync = syncTimes.length > 0 ? syncTimes[syncTimes.length - 1] : null
                      return lastSync
                        ? `last sync completed ${new Date(lastSync).toLocaleString()}`
                        : 'last sync completed'
                    }
                    return (pipeline.sync_status || '—').replace(/_/g, ' ')
                  })()}</Badge>
              </span>
            </div>
          )}
          {pipeline.emr_job_run_id && (() => {
            // Managed Migration stores its migration id as "s3tm:<id>"; show the bare id.
            const jobId = pipeline.emr_job_run_id.replace(/^s3tm:/, '')
            return <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('pipeline.labels.active_job')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{jobId.substring(0, 30)}</span><CopyButton value={jobId} label="job ID" /></span></div>
          })()}
          {pipeline.started_at && <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.started')}</span><span>{new Date(pipeline.started_at).toLocaleString()}</span></div>}
          {pipeline.sync_status === 'out_of_sync' && (() => {
            const spec = typeof pipeline.spec_json === 'string' ? JSON.parse(pipeline.spec_json) : pipeline.spec_json
            const interval = spec?.strategy?.sync_interval ?? 3600
            // Use last completed sync batch's completed_at as the reference
            // (matches the engine's _interval_elapsed logic). Falls back to
            // started_at if no sync has ever run.
            const syncBatches = batches
              .filter((b: any) => b.status === 'completed' && b.partitions?.includes('__sync__'))
              .map((b: any) => b.completed_at)
              .filter(Boolean)
              .sort()
            const lastSyncTime = syncBatches.length > 0
              ? syncBatches[syncBatches.length - 1]
              : (pipeline.started_at || pipeline.created_at)
            if (!lastSyncTime) return null
            const lastSyncMs = new Date(lastSyncTime).getTime()
            const nextSync = new Date(lastSyncMs + interval * 1000)
            const now = Date.now()
            const remaining = Math.max(0, Math.round((nextSync.getTime() - now) / 60000))
            return (
              <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.next_sync')}</span>
                <span className={remaining === 0 ? 'text-amber-600 font-medium' : ''}>
                  {remaining === 0 ? 'imminent' : `~${remaining} min`}
                  <span className="text-muted-foreground ml-1 text-[10px]">({nextSync.toLocaleTimeString()})</span>
                </span>
              </div>
            )
          })()}
          {/* Managed Migration auto-syncs on the configured interval once the
              initial load (or a prior sync) has completed — show the countdown
              to the next automatic sync (a manual "Sync Now" runs it sooner). */}
          {migrationType === 's3tables_managed'
            && ['initial_load_complete', 'sync_complete'].includes(pipeline.sync_status) && (() => {
            const spec = typeof pipeline.spec_json === 'string' ? JSON.parse(pipeline.spec_json) : pipeline.spec_json
            const interval = spec?.strategy?.sync_interval ?? 3600
            // Reference = last completed batch (initial load or sync), matching
            // the engine's _auto_sync_due logic (NOT updated_at, which the
            // reconcile loop re-touches every poll).
            const completed = batches
              .filter((b: any) => b.status === 'completed' && b.completed_at)
              .map((b: any) => b.completed_at)
              .sort()
            const lastTime = completed.length > 0 ? completed[completed.length - 1] : (pipeline.started_at || pipeline.created_at)
            if (!lastTime) return null
            const nextSync = new Date(new Date(lastTime).getTime() + interval * 1000)
            const remaining = Math.max(0, Math.round((nextSync.getTime() - Date.now()) / 60000))
            return (
              <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.next_sync')}</span>
                <span className={remaining === 0 ? 'text-amber-600 font-medium' : ''}>
                  {remaining === 0 ? 'imminent' : `~${remaining} min`}
                  <span className="text-muted-foreground ml-1 text-[10px]">({nextSync.toLocaleTimeString()})</span>
                </span>
              </div>
            )
          })()}
        </div>
      )}

      {/* Cost summary */}
      {costs && costs.total_cost_usd > 0 && (
        <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1">
          <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.compute_cost')}</span><span className="font-medium">${costs.total_cost_usd.toFixed(4)}</span></div>
          {costs.athena_cost_usd > 0 && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.descriptions.athena_queries', { count: costs.athena_query_count })}</span><span>${costs.athena_cost_usd.toFixed(4)} ({(costs.athena_bytes_scanned / (1024**3)).toFixed(2)} GB scanned)</span></div>}
          {costs.emr_cost_usd > 0 && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.descriptions.emr_vcpu_hrs', { hours: costs.emr_vcpu_hours.toFixed(1) })}</span><span>${costs.emr_cost_usd.toFixed(4)}</span></div>}
        </div>
      )}

      {/* Batch progress bar */}
      {totalCount > 0 && (
        <div className="space-y-2">
          <div className="flex items-center justify-between text-xs">
            <span className="text-muted-foreground">{t('pipeline.labels.batches')}</span>
            <span className="font-medium">{completedCount}/{totalCount} {t('common.labels.completed')}</span>
          </div>
          <div className="flex gap-0.5 h-2 rounded-full overflow-hidden bg-muted">
            {batches.map((b, i) => (
              <div key={i} className={`flex-1 ${batchStatusColor[b.status] ?? 'bg-zinc-300'}`} title={`Batch ${b.batch_number}: ${b.status}`} />
            ))}
          </div>
          <div className="flex gap-3 text-xs text-muted-foreground">
            {completedCount > 0 && <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-emerald-500" />{completedCount} done</span>}
            {runningCount > 0 && <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-blue-500" />{runningCount} running</span>}
            {pendingCount > 0 && <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-zinc-300 dark:bg-zinc-600" />{pendingCount} pending</span>}
            {failedCount > 0 && <span className="flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-full bg-red-500" />{failedCount} failed</span>}
          </div>
        </div>
      )}

      {/* Batch list */}
      {batches.length > 0 && (
        <div className="rounded-md border overflow-hidden">
          <table className="w-full text-xs">
            <thead><tr className="border-b bg-muted/50">
              <th className="px-2 py-1.5 text-left font-medium w-8">#</th>
              <th className="px-2 py-1.5 text-left font-medium">{t('pipeline.labels.batch')}</th>
              <th className="px-2 py-1.5 text-left font-medium">{t('common.labels.status')}</th>
              <th className="px-2 py-1.5 text-left font-medium">{t('common.labels.duration')}</th>
              <th className="px-2 py-1.5 text-left font-medium">{t('common.labels.completed')}</th>
            </tr></thead>
            <tbody>{[...batches].reverse().map(b => {
              const duration = b.started_at && b.completed_at
                ? `${Math.round((new Date(b.completed_at).getTime() - new Date(b.started_at).getTime()) / 1000)}s`
                : b.started_at ? 'running...' : '—'
              // Show partition progress for running batches
              const progressInfo = b.status === 'running' && pipeline
                ? (() => {
                    const done = pipeline.partitions_completed ?? 0
                    const total = pipeline.partitions_total ?? 0
                    if (total > 0) {
                      const pct = Math.round((done / total) * 100)
                      return `${done.toLocaleString()}/${total.toLocaleString()} (${pct}%)`
                    }
                    if (done > 0) return `${done.toLocaleString()} files`
                    return null
                  })()
                : null
              return (
                <tr key={b.batch_number} className="border-b last:border-0">
                  <td className="px-2 py-1.5 text-muted-foreground">{b.batch_number}</td>
                  <td className="px-2 py-1.5 truncate max-w-[200px]" title={b.partitions}>{formatBatchLabel(b.partitions, b.batch_number)}</td>
                  <td className="px-2 py-1.5"><Badge variant={batchStatusBadge[b.status] ?? 'secondary'} className="text-[10px] px-1.5 py-0">{b.status}</Badge></td>
                  <td className="px-2 py-1.5 text-muted-foreground">{duration}</td>
                  <td className="px-2 py-1.5 text-muted-foreground whitespace-nowrap">
                    {progressInfo
                      ? <span className="text-amber-600 dark:text-amber-400 font-medium">{progressInfo}</span>
                      : formatTime(b.completed_at)}
                  </td>
                </tr>
              )
            })}</tbody>
          </table>
        </div>
      )}

      {batches.length === 0 && pipeline && (
        <div className="text-xs text-muted-foreground italic">{t('pipeline.messages.no_batch_data')}</div>
      )}
    </div>
  )
}

export function MigrationDetailPage() {
  const { t } = useTranslation()
  const { migrationId } = useParams({ strict: false }) as { migrationId: string }
  const { data: plan, isLoading, refetch: refetchPlan } = usePlan(migrationId)
  const { data: tables } = useTables()
  const { data: planCosts } = usePlanCosts(migrationId)
  const { showToast } = useToast()
  const pauseTable = usePauseTable(); const resumeTable = useResumeTable(); const cancelTable = useCancelTable()
  const removeTable = useRemoveTableFromPlan(); const retryTable = useRetryTable()
  const resumeFromFailure = useResumeTableFromFailure()
  const syncNow = useSyncNow()
  const syncPlan = useSyncPlan()
  const access = usePlanAccess(plan)
  const [retryOverlay, setRetryOverlay] = useState<string|null>(null)
  const [tblSearch, setTblSearch] = useState(''); const [tblPage, setTblPage] = useState(0)
  const [sortCol, setSortCol] = useState<string|null>(null); const [sortDir, setSortDir] = useState<SortDir>(null)
  const [detailPanel, setDetailPanel] = useState<any>(null)
  const [tableConfirm, setTableConfirm] = useState<{ action: 'delete'|'cancel'|'retry'; tables: any[] } | null>(null)
  const [panelWidth, setPanelWidth] = useState(560)
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set())
  const [actionsOpen, setActionsOpen] = useState(false)
  const runPlanValidation = useRunPlanValidation()
  const approveMigration = useApproveMigration()
  // Run a per-table action for several tables and report what the engine
  // confirmed. Failures carry the server's reason (e.g. engine refused).
  const runBulk = async (verb: string, items: { source_table: string }[], fn: (t: any) => Promise<unknown>) => {
    const results = await Promise.allSettled(items.map(fn))
    const failed = results
      .map((r, i) => (r.status === 'rejected' ? `${items[i].source_table}: ${(r.reason as Error)?.message ?? 'failed'}` : null))
      .filter(Boolean) as string[]
    const ok = items.length - failed.length
    if (!failed.length) showToast(`${verb} ${ok} table(s)`, 'success')
    else showToast(`${verb} ${ok} of ${items.length} table(s). ${failed.slice(0, 3).join('; ')}`, 'error')
  }
  const { data: validationStatus } = usePlanValidationStatus(migrationId)
  const runTablesValidation = useRunTablesValidation()
  // Validation dialog: 'plan' validates all eligible tables; a pipeline-id list
  // validates a selected subset. null = closed.
  const [validateDialog, setValidateDialog] = useState<{ scope: 'plan' | 'tables'; pipelineIds: string[]; label: string; skipped: ValidationSkipped[] } | null>(null)
  const tblPS = 10
  const tblMap = useMemo(() => { const m: Record<string, TableInfo> = {}; tables?.forEach(t => { m[`${t.database}.${t.table_name}`] = t }); return m }, [tables])

  // Auto-poll the engine for pipeline status when plan is active
  useEffect(() => {
    if (!plan || !['in_progress'].includes(plan.status)) return
    const interval = setInterval(() => {
      syncPlan.mutate(plan.id)
    }, 15000)
    // Sync immediately on mount
    syncPlan.mutate(plan.id)
    return () => clearInterval(interval)
  }, [plan?.id, plan?.status])

  if (isLoading) return <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
  if (!plan) return <div className="py-12 text-center text-muted-foreground">{t('migrations.labels.migration_not_found')}</div>

  const retryOverlayEl = retryOverlay && (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
      <div className="flex flex-col items-center gap-3 rounded-lg border bg-background p-8 shadow-lg">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
        <span className="text-sm font-medium">{t('common.labels.resubmitting')} {retryOverlay}...</span>
        <span className="text-xs text-muted-foreground">{t('common.loading.creating_pipeline')}</span>
        <span className="text-xs text-muted-foreground">{t('common.loading.might_take_seconds')}</span>
      </div>
    </div>
  )

  const isActive = access === 'manage' && ['in_progress','paused','staged','failed'].includes(plan.status)
  // Validation is a separate capability from the migration bulk actions: it can
  // run once a plan has moved past the pre-execution stages, including after it
  // is completed/finalized. Kept independent of isActive so existing logic is
  // untouched.
  const canValidate = access === 'manage' && !['draft','ready','staged'].includes(plan.status)
  // Statuses whose target is present and current — the only ones worth
  // validating. Managed Migration's post-load phases (initial_load_complete /
  // sync_complete) also have a live target, so they are validatable too.
  const validationEligible = (st: string) => ['completed','in_sync','initial_load_complete','sync_complete'].includes(st)
  const runTierValidation = (tier: ValidationTier) => {
    if (!validateDialog) return
    const done = () => { showToast(`Validation started for "${plan.name}"`,'success'); setValidateDialog(null); setSelectedRows(new Set()) }
    const fail = (err: any) => showToast(err?.message || 'Failed to start validation','error')
    if (validateDialog.scope === 'plan') {
      runPlanValidation.mutate({ planId: plan.id, tier }, { onSuccess: done, onError: fail })
    } else {
      runTablesValidation.mutate({ planId: plan.id, pipelineIds: validateDialog.pipelineIds, tier }, { onSuccess: done, onError: fail })
    }
  }
  const openPlanValidation = () => {
    const skipped: ValidationSkipped[] = plan.tables
      .filter(t => t.pipeline_id && !validationEligible(t.status))
      .map(t => ({ pipeline_id: t.pipeline_id as string, source_database: t.source_database, source_table: t.source_table, status: t.status, reason: `not eligible in status '${t.status}'` }))
    const eligibleCount = plan.tables.filter(t => t.pipeline_id && validationEligible(t.status)).length
    setValidateDialog({ scope: 'plan', pipelineIds: [], label: `${eligibleCount} eligible table(s) in this plan`, skipped })
  }
  const hSort = (c: string) => { if (sortCol === c) { setSortDir(sortDir === 'asc' ? 'desc' : sortDir === 'desc' ? null : 'asc'); if (sortDir === 'desc') setSortCol(null) } else { setSortCol(c); setSortDir('asc') } }
  const SI = ({ c }: { c: string }) => { if (sortCol !== c) return <ArrowUpDown className="h-3 w-3 ml-1 opacity-40" />; return sortDir === 'asc' ? <ArrowUp className="h-3 w-3 ml-1" /> : <ArrowDown className="h-3 w-3 ml-1" /> }
  let fl = plan.tables.filter(t => t.source_table.toLowerCase().includes(tblSearch.toLowerCase()) || t.source_database.toLowerCase().includes(tblSearch.toLowerCase()))
  if (sortCol && sortDir) {
    fl = [...fl].sort((a, b) => { const ai = tblMap[`${a.source_database}.${a.source_table}`]; const bi = tblMap[`${b.source_database}.${b.source_table}`]
      let av: any, bv: any
      if (sortCol === 'source_table') { av = a.source_table; bv = b.source_table } else if (sortCol === 'source_database') { av = a.source_database; bv = b.source_database }
      else if (sortCol === 'format') { av = ai?.format ?? ''; bv = bi?.format ?? '' }
      else if (sortCol === 'migration_type') { av = a.migration_type; bv = b.migration_type }
      else if (sortCol === 'status') { av = a.status; bv = b.status } else { av = ''; bv = '' }
      const c = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv)); return sortDir === 'desc' ? -c : c })
  }
  const tp = Math.max(1, Math.ceil(fl.length / tblPS)); const pg = fl.slice(tblPage * tblPS, (tblPage + 1) * tblPS)

  return (
    <div className={detailPanel
      ? // Break out of the layout's max-w-[1600px] container to use (almost)
        // the full window width when the details pane is open, so the table and
        // pane have maximum room. Full-viewport-width trick + small side padding.
        'space-y-6 relative left-1/2 right-1/2 -mx-[50vw] w-screen px-4'
      : 'space-y-6'}>
      {retryOverlayEl}
      <ValidationDialog
        open={!!validateDialog}
        targetLabel={validateDialog?.label ?? ''}
        skipped={validateDialog?.skipped ?? []}
        isPending={runPlanValidation.isPending || runTablesValidation.isPending}
        onRun={runTierValidation}
        onClose={() => setValidateDialog(null)}
      />
      <div className="flex items-start gap-4">
      <Card className="flex-1 min-w-0">
        <CardHeader>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3"><CardTitle>{plan.name}</CardTitle><Badge variant={statusV[plan.status]}>{plan.status.replace('_',' ')}</Badge></div>
            <div className="flex items-center gap-2">
              <MigrationActions plan={plan} canValidate={canValidate} onValidate={openPlanValidation} validating={runPlanValidation.isPending} />
            </div>
          </div>
          {plan.description && <CardDescription>{plan.description}</CardDescription>}
          <div className="flex flex-wrap gap-1.5 pt-1">
            <Badge variant="outline" className="border-blue-300 text-blue-700 dark:border-blue-700 dark:text-blue-400">{targetL[plan.target] ?? plan.target}</Badge>
            <Badge variant="outline" className="border-emerald-300 text-emerald-700 dark:border-emerald-700 dark:text-emerald-400">{plan.execution_engine === 'emr_serverless' ? 'EMR Serverless' : plan.execution_engine === 'athena' ? 'Athena' : plan.execution_engine}</Badge>
            {plan.migration_types.map(mt => <Badge key={mt} variant="outline" className="border-purple-300 text-purple-700 dark:border-purple-700 dark:text-purple-400">{mtL[mt] ?? mt}</Badge>)}
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-3 text-sm">
            <div><span className="text-muted-foreground">{t('common.labels.created_label')}</span> {new Date(plan.created_at).toLocaleDateString()} by {plan.created_by}</div>
            {plan.status === 'completed' && <div><span className="text-muted-foreground">{t('common.labels.completed_on')}</span> {new Date().toLocaleDateString()}</div>}
            {plan.owners.length > 0 && <div><span className="text-muted-foreground">{t('common.labels.owners')}</span> {plan.owners.join(', ')}</div>}
          </div>
          {planCosts && planCosts.total_cost_usd > 0 && (
            <div className="rounded-md border bg-muted/30 p-3 text-sm">
              <div className="flex items-center gap-4">
                <div><span className="text-muted-foreground">{t('pipeline.labels.total_compute_cost')}</span> <span className="font-semibold">${planCosts.total_cost_usd.toFixed(4)}</span></div>
                {planCosts.athena_cost_usd > 0 && <div><span className="text-muted-foreground">{t('pipeline.labels.athena_cost')}</span> ${planCosts.athena_cost_usd.toFixed(4)} <span className="text-xs text-muted-foreground">({planCosts.athena_query_count} queries)</span></div>}
                {planCosts.emr_cost_usd > 0 && <div><span className="text-muted-foreground">{t('pipeline.labels.emr_cost')}</span> ${planCosts.emr_cost_usd.toFixed(4)} <span className="text-xs text-muted-foreground">({planCosts.emr_vcpu_hours.toFixed(1)} vCPU-hrs)</span></div>}
              </div>
            </div>
          )}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span className="text-sm font-medium">{plan.tables.length} table(s)</span>
                <Button variant="ghost" size="icon" className="h-6 w-6" title="Refresh status" onClick={() => refetchPlan()}><RotateCcw className="h-3 w-3" /></Button>
              </div>
              <div className="flex items-center gap-2">
                {/* Validate selected — kept fixed in place whenever validation
                    is allowed (incl. completed plans). Disabled until rows are
                    selected; count is only shown when there's a selection. */}
                {canValidate && (() => {
                  const hasSel = selectedRows.size > 0
                  const selected = plan.tables.filter(t => selectedRows.has(`${t.source_database}.${t.source_table}`))
                  const eligible = selected.filter(t => t.pipeline_id && validationEligible(t.status))
                  const skipped: ValidationSkipped[] = selected
                    .filter(t => !(t.pipeline_id && validationEligible(t.status)))
                    .map(t => ({ pipeline_id: (t.pipeline_id as string) || `${t.source_database}.${t.source_table}`, source_database: t.source_database, source_table: t.source_table, status: t.status, reason: t.pipeline_id ? `not eligible in status '${t.status}'` : 'not migrated yet' }))
                  return (
                    <Button variant="outline" size="sm" className="h-8 text-xs gap-1" disabled={!hasSel}
                      onClick={() => { if (!hasSel) return; setValidateDialog({ scope: 'tables', pipelineIds: eligible.map(t => t.pipeline_id as string), label: `${eligible.length} selected table(s)`, skipped }) }}>
                      <ShieldCheck className="h-3.5 w-3.5" />Validate{hasSel ? ` (${selectedRows.size})` : ''}
                    </Button>
                  )
                })()}
                {/* Actions dropdown — kept fixed in place when the plan is
                    active. Disabled until rows are selected; count only shown
                    when there's a selection. */}
                {isActive && (() => {
                  const hasSel = selectedRows.size > 0
                  const selected = plan.tables.filter(t => selectedRows.has(`${t.source_database}.${t.source_table}`))
                  // Pausable statuses. For Managed Migration, pausing stops the
                  // auto-sync — its post-load states (initial_load_complete /
                  // sync_complete) and an in-flight sync are all pausable.
                  const pausableStatuses = ['pending','in_progress','translating','initial_load','executing','in_sync','out_of_sync','initial_load_complete','sync_complete','syncing']
                  const canPauseAny = selected.some(t => pausableStatuses.includes(t.status))
                  const canResumeAny = selected.some(t => t.status === 'paused')
                  const canCancelAny = selected.some(t => ['in_progress','translating','paused','pending','initial_load','executing','in_sync','out_of_sync'].includes(t.status))
                  const canDeleteAny = selected.some(t => ['staged','cancelled', 'failed', 'in_sync', 'out_of_sync'].includes(t.status))
                  const canRetryAny = selected.some(t => t.status === 'failed')
                  const canResumeFromFailure = selected.some(t => t.status === 'failed' && t.migration_type !== 'rewrite')
                  // Sync availability: incremental/morph sync when out_of_sync;
                  // Managed Migration (s3tables_managed) syncs after the initial
                  // load or a prior sync has completed.
                  const isSyncable = (t: any) => !!t.pipeline_id && (
                    t.status === 'out_of_sync' ||
                    (t.migration_type === 's3tables_managed' && ['initial_load_complete', 'sync_complete', 'failed'].includes(t.status)))
                  const canSyncAny = selected.some(isSyncable)
                  const isAwaitingReview = (t: any) => !!t.pipeline_id && t.status === 'awaiting_review'
                  const canApproveAny = selected.some(isAwaitingReview)
                  return (
                    <div className="relative">
                      <Button variant="outline" size="sm" className="h-8 text-xs gap-1" disabled={!hasSel} onClick={() => { if (!hasSel) return; setActionsOpen(!actionsOpen) }}>
                        <MoreHorizontal className="h-3.5 w-3.5" />Actions{hasSel ? ` (${selectedRows.size})` : ''}
                        <ChevronDown className={`h-3 w-3 transition-transform ${actionsOpen ? 'rotate-180' : ''}`} />
                      </Button>
                      {hasSel && actionsOpen && <>
                        <div className="fixed inset-0 z-30" onClick={() => setActionsOpen(false)} />
                        <div className="absolute right-0 top-full mt-1 z-40 w-48 rounded-md border bg-popover shadow-md py-1 text-sm">
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canPauseAny ? 'hover:bg-accent cursor-pointer' : 'opacity-40 cursor-not-allowed'}`}
                            disabled={!canPauseAny}
                            onClick={() => { if (!canPauseAny) return; setActionsOpen(false); const toPause = selected.filter(t => pausableStatuses.includes(t.status)); runBulk('Paused', toPause, t => pauseTable.mutateAsync({ planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table })); setSelectedRows(new Set()) }}>
                            <Pause className="h-3.5 w-3.5" />Pause
                          </button>
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canResumeAny ? 'hover:bg-accent cursor-pointer' : 'opacity-40 cursor-not-allowed'}`}
                            disabled={!canResumeAny}
                            onClick={() => { if (!canResumeAny) return; setActionsOpen(false); const toResume = selected.filter(t => t.status === 'paused'); runBulk('Resumed', toResume, t => resumeTable.mutateAsync({ planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table })); setSelectedRows(new Set()) }}>
                            <Play className="h-3.5 w-3.5" />Resume
                          </button>
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canSyncAny ? 'hover:bg-accent cursor-pointer' : 'opacity-40 cursor-not-allowed'}`}
                            disabled={!canSyncAny}
                            onClick={() => { if (!canSyncAny) return; setActionsOpen(false); const toSync = selected.filter(isSyncable); toSync.forEach(t => syncNow.mutate(t.pipeline_id!)); showToast(`Sync triggered for ${toSync.length} table(s)`, 'success'); setSelectedRows(new Set()) }}>
                            <FastForward className="h-3.5 w-3.5" />Sync Now
                          </button>
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canApproveAny ? 'hover:bg-accent cursor-pointer' : 'opacity-40 cursor-not-allowed'}`}
                            disabled={!canApproveAny}
                            onClick={async () => {
                              if (!canApproveAny) return
                              setActionsOpen(false)
                              const toApprove = selected.filter(isAwaitingReview)
                              const results = await Promise.allSettled(toApprove.map(t => approveMigration.mutateAsync(t.pipeline_id!)))
                              const failed = results.filter(r => r.status === 'rejected').length
                              const ok = toApprove.length - failed
                              if (ok > 0) showToast(`Approved ${ok} table(s)${failed ? `, ${failed} failed` : ''}`, failed ? 'error' : 'success')
                              else showToast(`Failed to approve ${failed} table(s)`, 'error')
                              setSelectedRows(new Set())
                            }}>
                            <CheckCircle className="h-3.5 w-3.5" />Approve review
                          </button>
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canRetryAny ? 'hover:bg-accent cursor-pointer' : 'opacity-40 cursor-not-allowed'}`}
                            disabled={!canRetryAny}
                            onClick={() => { if (!canRetryAny) return; setActionsOpen(false); setTableConfirm({ action: 'retry', tables: selected.filter(t => t.status === 'failed') }) }}>
                            <RotateCcw className="h-3.5 w-3.5" />Retry
                          </button>
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canResumeFromFailure ? 'hover:bg-accent cursor-pointer' : 'opacity-40 cursor-not-allowed'}`}
                            disabled={!canResumeFromFailure}
                            onClick={() => { if (!canResumeFromFailure) return; setActionsOpen(false); const failedNonRewrite = selected.filter(t => t.status === 'failed' && t.migration_type !== 'rewrite'); failedNonRewrite.forEach(t => resumeFromFailure.mutate({ planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table })); showToast(`Resuming ${failedNonRewrite.length} table(s) from failed batch`, 'success'); setSelectedRows(new Set()) }}>
                            <Play className="h-3.5 w-3.5" />Resume from failure
                          </button>
                          <div className="border-t my-1" />
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canCancelAny ? 'hover:bg-accent cursor-pointer text-destructive' : 'opacity-40 cursor-not-allowed text-destructive/50'}`}
                            disabled={!canCancelAny}
                            onClick={() => { if (!canCancelAny) return; setActionsOpen(false); setTableConfirm({ action: 'cancel', tables: selected.filter(t => ['in_progress','translating','paused','pending','initial_load','executing','in_sync','out_of_sync'].includes(t.status)) }) }}>
                            <XCircle className="h-3.5 w-3.5" />Cancel
                          </button>
                          <button className={`w-full px-3 py-1.5 text-left flex items-center gap-2 ${canDeleteAny ? 'hover:bg-accent cursor-pointer text-destructive' : 'opacity-40 cursor-not-allowed text-destructive/50'}`}
                            disabled={!canDeleteAny}
                            onClick={() => { if (!canDeleteAny) return; setActionsOpen(false); setTableConfirm({ action: 'delete', tables: selected.filter(t => ['staged','cancelled', 'failed', 'in_sync', 'out_of_sync'].includes(t.status)) }) }}>
                            <Trash2 className="h-3.5 w-3.5" />Remove
                          </button>
                        </div>
                      </>}
                    </div>
                  )
                })()}
                {plan.tables.length > 5 && <div className="relative max-w-xs"><Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input placeholder="Search tables..." value={tblSearch} onChange={e => { setTblSearch(e.target.value); setTblPage(0) }} className="pl-9 pr-8 h-8 text-xs" />
                  {tblSearch && <button onClick={() => setTblSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"><X className="h-3 w-3" /></button>}</div>}
              </div>
            </div>
            {fl.length > 0 && <div className="rounded-md border overflow-x-auto"><table className="w-full min-w-[640px] text-sm">
              <thead><tr className="border-b bg-muted/50">
                {(isActive || canValidate) && <th className="px-3 py-2 w-8"><input type="checkbox" className="rounded border-muted-foreground/50 cursor-pointer"
                  checked={pg.length > 0 && pg.every(t => selectedRows.has(`${t.source_database}.${t.source_table}`))}
                  onChange={e => {
                    const next = new Set(selectedRows)
                    pg.forEach(t => { const k = `${t.source_database}.${t.source_table}`; e.target.checked ? next.add(k) : next.delete(k) })
                    setSelectedRows(next)
                  }} /></th>}
                <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={() => hSort('source_table')}><span className="inline-flex items-center">{t('common.labels.table')}<SI c="source_table" /></span></th>
                <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={() => hSort('source_database')}><span className="inline-flex items-center">{t('common.labels.database')}<SI c="source_database" /></span></th>
                <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={() => hSort('status')}><span className="inline-flex items-center">{t('common.labels.status')}<SI c="status" /></span></th>
                <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={() => hSort('format')}><span className="inline-flex items-center">{t('common.labels.format')}<SI c="format" /></span></th>
                <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={() => hSort('migration_type')}><span className="inline-flex items-center">{t('common.labels.type')}<SI c="migration_type" /></span></th>
              </tr></thead>
              <tbody>{pg.map((t, i) => {
                const info = tblMap[`${t.source_database}.${t.source_table}`]
                const rowKey = `${t.source_database}.${t.source_table}`
                return (<tr key={i} className="border-b last:border-0 hover:bg-muted/30">
                  {(isActive || canValidate) && <td className="px-3 py-2"><input type="checkbox" className="rounded border-muted-foreground/50 cursor-pointer"
                    checked={selectedRows.has(rowKey)}
                    onChange={e => { const next = new Set(selectedRows); e.target.checked ? next.add(rowKey) : next.delete(rowKey); setSelectedRows(next) }} /></td>}
                  <td className="px-3 py-2 font-medium"><span className="inline-flex items-center gap-1.5">{t.pipeline_id && <ValidationStatusIcon state={validationStatus?.[t.pipeline_id as string]?.state} />}<button className="text-left underline text-primary hover:text-primary/80 cursor-pointer" onClick={() => setDetailPanel(t)}>{t.source_table}</button></span></td>
                  <td className="px-3 py-2 text-muted-foreground">{t.source_database}</td>
                  <td className="px-3 py-2"><span className="inline-flex items-center gap-1.5">{(t.status === 'initial_load' || t.status === 'executing') && <Loader2 className="h-3 w-3 animate-spin text-amber-500" />}<Badge variant={tblStatusV[t.status] ?? 'secondary'}>{tblStatusLabel(t.status)}</Badge></span></td>
                  <td className="px-3 py-2">{info ? <Badge variant={fmtBadge[info.format] ?? 'outline'}>{info.format}</Badge> : '—'}</td>
                  <td className="px-3 py-2"><Badge variant="outline">{mtL[t.migration_type] ?? t.migration_type}</Badge></td>
                </tr>)
              })}</tbody></table></div>}
            {fl.length > tblPS && <div className="flex items-center justify-end gap-2 text-xs text-muted-foreground">
              {tblPage * tblPS + 1}–{Math.min((tblPage + 1) * tblPS, fl.length)} of {fl.length}
              <Button variant="outline" size="icon" className="h-6 w-6" onClick={() => setTblPage(p => p - 1)} disabled={tblPage === 0}><ChevronLeft className="h-3 w-3" /></Button>
              <Button variant="outline" size="icon" className="h-6 w-6" onClick={() => setTblPage(p => p + 1)} disabled={tblPage >= tp - 1}><ChevronRight className="h-3 w-3" /></Button>
            </div>}
          </div>
        </CardContent>
      </Card>
      {detailPanel && (
        <div className="relative shrink-0 rounded-lg border bg-background shadow-sm flex flex-col sticky top-14 max-h-[calc(100vh-4.5rem)]" style={{ width: `${panelWidth}px` }}>
          {/* Resize handle */}
          <div className="absolute inset-y-0 left-0 w-1.5 cursor-col-resize hover:bg-primary/20 active:bg-primary/30 z-10"
            onMouseDown={(e) => {
              e.preventDefault()
              const startX = e.clientX
              const startWidth = panelWidth
              const onMove = (ev: MouseEvent) => {
                const newWidth = Math.max(360, Math.min(900, startWidth - (ev.clientX - startX)))
                setPanelWidth(newWidth)
              }
              const onUp = () => { document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }
              document.addEventListener('mousemove', onMove)
              document.addEventListener('mouseup', onUp)
            }}
          />
          <div className="flex items-center justify-between p-4 border-b">
            <div>
              <h3 className="text-sm font-semibold">{detailPanel.source_database}.{detailPanel.source_table}</h3>
              <p className="text-xs text-muted-foreground">{t('pipeline.labels.pipeline_details')}</p>
            </div>
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setDetailPanel(null)}><X className="h-4 w-4" /></Button>
          </div>
          <div className="flex-1 overflow-y-auto p-4 space-y-6 text-sm">
            {/* Status & Estimates */}
            <div className="space-y-3">
              <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{t('common.labels.status')}</h4>
              <div className="space-y-2">
                <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.table_status')}</span><Badge variant={tblStatusV[detailPanel.status] ?? 'secondary'}>{tblStatusLabel(detailPanel.status)}</Badge></div>
                <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.type')}</span><Badge variant="outline">{mtL[detailPanel.migration_type] ?? detailPanel.migration_type}</Badge></div>
                {detailPanel.pipeline_id && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('pipeline.labels.pipeline_id')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono text-xs truncate">{detailPanel.pipeline_id}</span><CopyButton value={detailPanel.pipeline_id} label="pipeline ID" /></span></div>}
                {detailPanel.error_message && detailPanel.status === 'failed' && (
                  <div className="space-y-1">
                    <span className="text-muted-foreground text-xs font-medium">{t('common.labels.error')}</span>
                    <div className="rounded-md border border-red-200 bg-red-50 dark:border-red-800 dark:bg-red-950 p-3 text-xs text-red-800 dark:text-red-200 whitespace-pre-wrap">{detailPanel.error_message}</div>
                  </div>
                )}
                {detailPanel.recommended_type && <div className="flex justify-between"><span className="text-muted-foreground">{t('form.labels.recommended')}</span><Badge variant="outline" className={detailPanel.recommended_type === detailPanel.migration_type ? 'border-emerald-500 text-emerald-600' : 'border-amber-500 text-amber-600'}>{mtL[detailPanel.recommended_type] ?? detailPanel.recommended_type}</Badge></div>}
                {detailPanel.recommended_batch_size != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.batch_size')}</span><span>{detailPanel.recommended_batch_size} partitions/batch</span></div>}
                {detailPanel.estimated_size_gb != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.estimated_size')}</span><span>~{detailPanel.estimated_size_gb} GB</span></div>}
                {detailPanel.estimated_cost_usd != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.estimated_cost')}</span><span>&lt;${detailPanel.estimated_cost_usd.toFixed(2)}</span></div>}
                {detailPanel.estimated_time_min != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.estimated_time')}</span><span>&lt;{Math.round(detailPanel.estimated_time_min)} min</span></div>}
              </div>
              {detailPanel.analysis_summary && (
                <div className="space-y-1">
                  <span className="text-muted-foreground text-xs font-medium">{t('common.labels.analysis')}</span>
                  <div className="rounded-md border bg-muted/30 p-3 text-xs whitespace-pre-wrap">
                    {detailPanel.analysis_summary}
                  </div>
                </div>
              )}
            </div>

            {/* Pipeline Progress — right after status */}
            {detailPanel.pipeline_id && <PipelineProgressPane pipelineId={detailPanel.pipeline_id} migrationType={detailPanel.migration_type} />}

            {/* Validation results — latest on-demand validation run, below the
                batch/progress info and above the spec. */}
            {detailPanel.pipeline_id && <ValidationResultsCard
              pipelineId={detailPanel.pipeline_id}
              sourceDatabase={detailPanel.source_database}
              sourceTable={detailPanel.source_table}
              targetDatabase={detailPanel.target_database}
              targetTable={detailPanel.target_table}
              migrationType={detailPanel.migration_type} />}

            {/* Spec — collapsible */}
            <CollapsibleSection title="Spec" defaultOpen={false}>
              <div className="space-y-2">
                <div className="space-y-1">
                  <span className="text-xs font-medium text-muted-foreground">{t('detail.labels.source')}</span>
                  <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1">
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.database')}</span><span className="font-mono">{detailPanel.source_database}</span></div>
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.table')}</span><span className="font-mono">{detailPanel.source_table}</span></div>
                    {detailPanel.source_format && <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.format')}</span><Badge variant={fmtBadge[detailPanel.source_format] ?? 'outline'}>{detailPanel.source_format}</Badge></div>}
                    {detailPanel.source_location && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('common.labels.location')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{detailPanel.source_location}</span><CopyButton value={detailPanel.source_location} label="location" /></span></div>}
                    {detailPanel.source_account && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('aws.labels.aws_account')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{detailPanel.source_account}</span><CopyButton value={detailPanel.source_account} label="account ID" /></span></div>}
                    {detailPanel.source_compression && <div className="flex justify-between"><span className="text-muted-foreground">{t('detail.labels.compression')}</span><span>{detailPanel.source_compression}</span></div>}
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('detail.labels.partitioned')}</span><span>{detailPanel.source_partitioned ? 'Yes' : 'No'}</span></div>
                    {detailPanel.source_partition_keys?.length > 0 && <div className="pt-1"><span className="text-muted-foreground">{t('detail.labels.partition_keys')}</span><div className="mt-1 flex flex-wrap gap-1">{detailPanel.source_partition_keys.map((pk: any, i: number) => <Badge key={i} variant="secondary" className="text-xs">{pk.name} ({pk.type})</Badge>)}</div></div>}
                  </div>
                </div>
                <div className="space-y-1">
                  <span className="text-xs font-medium text-muted-foreground">{t('form.labels.target')}</span>
                  <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1">
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.database')}</span><span className="font-mono">{detailPanel.target_database}</span></div>
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.table')}</span><span className="font-mono">{detailPanel.target_table}</span></div>
                    {detailPanel.target_type && <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.type')}</span><Badge variant="secondary">{targetL[detailPanel.target_type] ?? detailPanel.target_type}</Badge></div>}
                    {detailPanel.target_location && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('common.labels.location')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{detailPanel.target_location}</span><CopyButton value={detailPanel.target_location} label="location" /></span></div>}
                    {detailPanel.target_bucket_arn && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('form.labels.bucket_arn')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{detailPanel.target_bucket_arn}</span><CopyButton value={detailPanel.target_bucket_arn} label="bucket ARN" /></span></div>}
                  </div>
                </div>
                {(detailPanel.partition_spec?.length > 0 || detailPanel.sort_order?.length > 0) && (
                  <div className="space-y-1">
                    <span className="text-xs font-medium text-muted-foreground">{t('detail.labels.iceberg_properties')}</span>
                    <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-2">
                      {detailPanel.partition_spec?.length > 0 && <div><span className="text-muted-foreground">{t('detail.labels.partition_spec')}</span><div className="mt-1 flex flex-wrap gap-1">{detailPanel.partition_spec.map((p: any, i: number) => <Badge key={i} variant="secondary" className="text-xs">{p.transform}({p.source_column})</Badge>)}</div></div>}
                      {detailPanel.sort_order?.length > 0 && <div><span className="text-muted-foreground">{t('detail.labels.sort_order')}</span><div className="mt-1 flex flex-wrap gap-1">{detailPanel.sort_order.map((s: any, i: number) => <Badge key={i} variant="secondary" className="text-xs">{s.column} {s.direction ?? 'asc'}</Badge>)}</div></div>}
                    </div>
                  </div>
                )}
              </div>
            </CollapsibleSection>

            {/* Raw Spec JSON — collapsible */}
            <CollapsibleSection title="Spec JSON" defaultOpen={false}>
              <pre className="rounded-md border bg-muted/30 p-3 text-xs font-mono whitespace-pre-wrap overflow-x-auto">{JSON.stringify({
                version: "1.0",
                source: {
                  database: detailPanel.source_database,
                  table_name: detailPanel.source_table,
                  ...(detailPanel.source_format && { format: detailPanel.source_format }),
                  ...(detailPanel.source_location && { location: detailPanel.source_location }),
                  ...(detailPanel.source_account && { aws_account_id: detailPanel.source_account }),
                  partitioned: detailPanel.source_partitioned ?? false,
                  ...(detailPanel.source_partition_keys?.length > 0 && { partition_keys: detailPanel.source_partition_keys }),
                  ...(detailPanel.source_compression && { compression: detailPanel.source_compression }),
                },
                target: {
                  type: detailPanel.target_type ?? plan.target,
                  database: detailPanel.target_database,
                  table_name: detailPanel.target_table,
                  ...(detailPanel.target_location && { location: detailPanel.target_location }),
                  ...((detailPanel.target_bucket_arn || plan.table_bucket_arn) && { table_bucket_arn: detailPanel.target_bucket_arn || plan.table_bucket_arn }),
                },
                strategy: {
                  type: detailPanel.migration_type,
                  execution_engine: plan.execution_engine || "athena",
                  batch_size: detailPanel.recommended_batch_size ?? 5,
                  sync_interval: 3600,
                },
                iceberg_properties: {
                  ...(detailPanel.partition_spec?.length > 0 && { partition_spec: detailPanel.partition_spec }),
                  ...(detailPanel.sort_order?.length > 0 && { sort_order: detailPanel.sort_order }),
                  table_properties: { "format-version": "2", "write.parquet.compression-codec": "zstd" },
                },
              }, null, 2)}</pre>
            </CollapsibleSection>

            {/* Managed Migration request payload — below the Spec JSON section.
                Self-gated to s3tables_managed pipelines that have a captured
                StartTableMigration payload. Read-only (view + copy). */}
            {detailPanel.pipeline_id && (
              <ManagedMigrationRequestCard
                pipelineId={detailPanel.pipeline_id} />
            )}

          </div>
        </div>
      )}
      </div>
      <ConfirmDialog
        open={!!tableConfirm}
        title={tableConfirm?.action === 'delete' ? 'Delete Pipeline'
          : tableConfirm?.action === 'cancel' ? 'Cancel Pipeline'
          : 'Retry Pipeline'}
        message={tableConfirm ? (() => {
          const n = tableConfirm.tables.length
          const ref = n === 1
            ? `"${tableConfirm.tables[0].source_database}.${tableConfirm.tables[0].source_table}"`
            : `${n} tables`
          if (tableConfirm.action === 'delete')
            return `This will permanently delete the pipeline for ${ref} and release ${n === 1 ? 'the table' : 'the tables'} back to the inventory. This action cannot be undone.`
          if (tableConfirm.action === 'cancel')
            return `Cancel the in-progress migration for ${ref}? Any running jobs will be stopped and progress on ${n === 1 ? 'this table' : 'these tables'} will be lost.`
          return `Retry the migration for ${ref}? This starts ${n === 1 ? 'the pipeline' : 'each pipeline'} over from the beginning and discards any progress from the previous run. This action cannot be undone.`
        })() : ''}
        confirmLabel={tableConfirm?.action === 'delete' ? 'Delete'
          : tableConfirm?.action === 'cancel' ? 'Cancel Migration'
          : 'Retry'}
        cancelLabel={tableConfirm?.action === 'cancel' ? 'Keep Running' : 'Cancel'}
        variant="destructive"
        onConfirm={() => {
          const c = tableConfirm
          setTableConfirm(null)
          if (!c || c.tables.length === 0) return
          const tables = c.tables
          const n = tables.length
          const label = n === 1 ? tables[0].source_table : `${n} tables`
          if (c.action === 'delete') {
            Promise.allSettled(tables.map(t => removeTable.mutateAsync({ planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table })
              .catch((err: any) => { showToast(err?.message || `Failed to remove ${t.source_table}`, 'error'); throw err })))
              .then(results => { const ok = results.filter(r => r.status === 'fulfilled').length; if (ok > 0) showToast(`Removed ${ok === n ? label : `${ok} of ${n} table(s)`}`, 'success') })
          } else if (c.action === 'cancel') {
            Promise.allSettled(tables.map(t => cancelTable.mutateAsync({ planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table })
              .catch((err: any) => { showToast(err?.message || `Failed to cancel ${t.source_table}`, 'error'); throw err })))
              .then(results => { const ok = results.filter(r => r.status === 'fulfilled').length; if (ok > 0) showToast(`Cancelled ${ok === n ? label : `${ok} of ${n} table(s)`}`, 'success') })
          } else {
            setRetryOverlay(label)
            // Fire all retries and wait for every one to settle before clearing
            // the overlay. Using mutateAsync + Promise.allSettled avoids relying
            // on per-mutate onSuccess/onError callbacks, which react-query does
            // not reliably invoke when the same mutation is called N times in a
            // loop (only the last call's callbacks are retained) — that left the
            // overlay stuck until a hard refresh.
            Promise.allSettled(
              tables.map(t => retryTable.mutateAsync({ planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table })
                .catch((err: any) => { showToast(err?.message || `Failed to retry ${t.source_table}`, 'error'); throw err }))
            ).then(results => {
              const ok = results.filter(r => r.status === 'fulfilled').length
              if (ok > 0) {
                showToast(`Retrying ${ok === n ? label : `${ok} of ${n} table(s)`}`, 'success')
                syncPlan.mutate(plan.id)
                setTimeout(() => setRetryOverlay(null), 2000)
              } else {
                setRetryOverlay(null)
              }
            })
          }
          setSelectedRows(new Set())
        }}
        onCancel={() => setTableConfirm(null)}
      />
    </div>
  )
}
