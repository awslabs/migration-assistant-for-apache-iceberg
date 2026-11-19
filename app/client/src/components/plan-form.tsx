// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useGroups } from '@/hooks/use-groups'
import { useManageableGroups } from '@/hooks/use-permissions'
import { useState } from 'react'
import { Loader2, X, Search } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import type { MigrationPlan } from '@/types'

interface PlanFormProps {
  initial?: MigrationPlan
  onSubmit: (data: any) => void
  onCancel: () => void
  isPending: boolean
  submitLabel: string
}

export function PlanForm({ initial, onSubmit, onCancel, isPending, submitLabel }: PlanFormProps) {
  const { t } = useTranslation()
  const { data: groups } = useGroups()
  const manageableGroups = useManageableGroups()
  const [name, setName] = useState(initial?.name ?? '')
  const [desc, setDesc] = useState(initial?.description ?? '')
  const [target, setTarget] = useState((initial as any)?.target ?? 's3_tables')
  const [tableBucketArn, setTableBucketArn] = useState((initial as any)?.table_bucket_arn ?? '')
  const [managed, setManaged] = useState(initial?.migration_types?.includes('s3tables_managed') ?? false)
  const [ft, setFt] = useState(initial?.migration_types?.includes('rewrite') ?? false)
  const [inc, setInc] = useState(initial?.migration_types?.includes('incremental') ?? false)
  const [morph, setMorph] = useState(initial?.migration_types?.includes('morph') ?? false)
  const [addFiles, setAddFiles] = useState(initial?.migration_types?.includes('add_files') ?? false)
  const [engine, setEngine] = useState((initial as any)?.execution_engine ?? 'athena')
  const [swapTables, setSwapTables] = useState((initial as any)?.swap_tables ?? false)
  const [owners, setOwners] = useState<string[]>(initial?.owners ?? [])
  const [ownerS, setOwnerS] = useState('')

  const s3TablesSelected = target === 's3_tables'
  const athenaSelected = engine === 'athena'
  const fg = groups?.filter(g => manageableGroups.includes(g.name) && !owners.includes(g.name) && g.name.toLowerCase().includes(ownerS.toLowerCase())) ?? []
  const handleTargetChange = (t: string) => {
    setTarget(t)
    if (t === 's3_tables') {
      setMorph(false)
      setAddFiles(false)
    } else {
      // Managed Migration targets Amazon S3 Tables only.
      setManaged(false)
    }
  }
  const handleEngineChange = (e: string) => {
    setEngine(e)
    if (e === 'athena') {
      setAddFiles(false)
    }
  }

  const noStrategy = !managed && !ft && !inc && !morph && !addFiles

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim() || noStrategy) return
    const types: string[] = []
    if (managed) types.push('s3tables_managed')
    if (ft) types.push('rewrite')
    if (inc) types.push('incremental')
    if (morph) types.push('morph')
    if (addFiles) types.push('add_files')
    onSubmit({
      name, description: desc, target, migration_types: types,
      execution_engine: engine,
      swap_tables: swapTables,
      table_bucket_arn: tableBucketArn,
      owners,
    })
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-8">
      <div className="space-y-3">
        <label className="text-sm font-medium">{t('plans.labels.plan_name')}</label>
        <p className="text-xs text-muted-foreground">{t('plans.labels.plan_unique_name')}</p>
        <Input value={name} onChange={e => setName(e.target.value)} placeholder="Q1 Analytics Migration" required />
      </div>

      <div className="space-y-3">
        <label className="text-sm font-medium">{t('common.labels.description')}</label>
        <p className="text-xs text-muted-foreground">{t('plans.labels.plan_context')}</p>
        <textarea value={desc} onChange={e => setDesc(e.target.value)} placeholder="Describe the migration plan..."
          className="flex min-h-[100px] w-full rounded-md border border-input bg-transparent px-3 py-2 text-sm shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring" />
      </div>

      <div className="space-y-3">
        <div>
          <label className="text-sm font-medium">{t('form.labels.target')}</label>
          <p className="text-xs text-muted-foreground">{initial ? t('form.labels.target_locked') : t('form.actions.choose_target')}</p>
        </div>
        {initial ? (
          <div className="rounded-lg border-2 border-muted bg-muted/30 p-4">
            <span className="text-sm font-medium">{target === 's3_tables' ? t('form.labels.amazon_s3_tables') : target === 's3_iceberg' ? t('form.labels.apache_iceberg_on_s3') : target}</span>
          </div>
        ) : (
        <div className="grid gap-3 sm:grid-cols-2">
          <button type="button" onClick={() => handleTargetChange('s3_tables')}
            className={`relative text-left rounded-lg border-2 p-4 transition-all cursor-pointer ${target === 's3_tables' ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/30'}`}>
            <span className="absolute -top-2.5 right-3 rounded-full bg-blue-600 px-2 py-0.5 text-[10px] font-medium text-white">{t('form.labels.recommended')}</span>
            <div className="flex items-start gap-3">
              <div className={`mt-0.5 h-4 w-4 rounded-full border-2 flex items-center justify-center shrink-0 ${target === 's3_tables' ? 'border-primary' : 'border-muted-foreground/40'}`}>
                {target === 's3_tables' && <div className="h-2 w-2 rounded-full bg-primary" />}
              </div>
              <div>
                <span className="text-sm font-medium">{t('form.labels.amazon_s3_tables')}</span>
                <p className="text-xs text-muted-foreground mt-1">{t('form.descriptions.target_s3_tables_desc')}</p>
              </div>
            </div>
          </button>
          <button type="button" onClick={() => handleTargetChange('s3_iceberg')}
            className={`text-left rounded-lg border-2 p-4 transition-all cursor-pointer ${target === 's3_iceberg' ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/30'}`}>
            <div className="flex items-start gap-3">
              <div className={`mt-0.5 h-4 w-4 rounded-full border-2 flex items-center justify-center shrink-0 ${target === 's3_iceberg' ? 'border-primary' : 'border-muted-foreground/40'}`}>
                {target === 's3_iceberg' && <div className="h-2 w-2 rounded-full bg-primary" />}
              </div>
              <div>
                <span className="text-sm font-medium">{t('form.labels.apache_iceberg_on_s3')}</span>
                <p className="text-xs text-muted-foreground mt-1">{t('form.descriptions.target_s3_iceberg_desc')}</p>
              </div>
            </div>
          </button>
        </div>
        )}
      </div>

      {target === 's3_tables' && (
        <div className="space-y-3">
          <label className="text-sm font-medium">{t('form.labels.s3_table_bucket_arn')}</label>
          <p className="text-xs text-muted-foreground">{t('form.descriptions.bucket_arn_desc')}</p>
          <Input value={tableBucketArn} onChange={e => setTableBucketArn(e.target.value)}
            placeholder="arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket"
            className="font-mono text-xs" required={target === 's3_tables'} />
        </div>
      )}

      <div className="space-y-3">
        <div>
          <label className="text-sm font-medium">{t('form.labels.execution_engine')}</label>
          <p className="text-xs text-muted-foreground">{t('form.actions.choose_engine')}</p>
        </div>
        <div className="grid gap-3 sm:grid-cols-2">
          <button type="button" onClick={() => handleEngineChange('athena')}
            className={`text-left rounded-lg border-2 p-4 transition-all cursor-pointer ${engine === 'athena' ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/30'}`}>
            <div className="flex items-start gap-3">
              <div className={`mt-0.5 h-4 w-4 rounded-full border-2 flex items-center justify-center shrink-0 ${engine === 'athena' ? 'border-primary' : 'border-muted-foreground/40'}`}>
                {engine === 'athena' && <div className="h-2 w-2 rounded-full bg-primary" />}
              </div>
              <div>
                <span className="text-sm font-medium">{t('form.labels.amazon_athena')}</span>
                <p className="text-xs text-muted-foreground mt-1">{t('form.descriptions.engine_athena_desc')}</p>
              </div>
            </div>
          </button>
          <button type="button" onClick={() => handleEngineChange('emr_serverless')}
            className={`text-left rounded-lg border-2 p-4 transition-all cursor-pointer ${engine === 'emr_serverless' ? 'border-primary bg-primary/5' : 'border-border hover:border-primary/30'}`}>
            <div className="flex items-start gap-3">
              <div className={`mt-0.5 h-4 w-4 rounded-full border-2 flex items-center justify-center shrink-0 ${engine === 'emr_serverless' ? 'border-primary' : 'border-muted-foreground/40'}`}>
                {engine === 'emr_serverless' && <div className="h-2 w-2 rounded-full bg-primary" />}
              </div>
              <div>
                <span className="text-sm font-medium">{t('form.labels.emr_serverless')}</span>
                <p className="text-xs text-muted-foreground mt-1">{t('form.descriptions.engine_emr_desc')}</p>
              </div>
            </div>
          </button>
        </div>
      </div>

      <div className="space-y-4">
        <div>
          <label className="text-sm font-medium">{t('form.labels.migration_type')}</label>
          <p className="text-xs text-muted-foreground">{t('form.actions.choose_strategies')}</p>
        </div>
        <div className="space-y-3">
          <label className={`flex items-start gap-3 ${s3TablesSelected ? 'cursor-pointer' : 'opacity-40 cursor-not-allowed'}`}>
            <input type="checkbox" checked={managed} onChange={() => { if (s3TablesSelected) setManaged(!managed) }} disabled={!s3TablesSelected} className="rounded mt-0.5" />
            <div>
              <span className="text-sm font-medium">{t('form.labels.s3tables_managed')}</span>
              <p className="text-xs text-muted-foreground">{t('form.descriptions.s3tables_managed_desc')}</p>
              {!s3TablesSelected && <p className="text-xs text-amber-600 mt-1">{t('form.descriptions.s3tables_managed_target_note')}</p>}
            </div>
          </label>
          <label className="flex items-start gap-3 cursor-pointer">
            <input type="checkbox" checked={ft} onChange={() => setFt(!ft)} className="rounded mt-0.5" />
            <div>
              <span className="text-sm font-medium">{t('form.labels.rewrite')}</span>
              <p className="text-xs text-muted-foreground">{t('form.descriptions.rewrite_desc')}</p>
            </div>
          </label>
          <label className="flex items-start gap-3 cursor-pointer">
            <input type="checkbox" checked={inc} onChange={() => setInc(!inc)} className="rounded mt-0.5" />
            <div>
              <span className="text-sm font-medium">{t('form.labels.incremental')}</span>
              <p className="text-xs text-muted-foreground">{t('form.descriptions.incremental_desc')}</p>
            </div>
          </label>
          <label className={`flex items-start gap-3 ${s3TablesSelected ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer'}`}>
            <input type="checkbox" checked={morph} onChange={() => { if (!s3TablesSelected) setMorph(!morph) }} disabled={s3TablesSelected} className="rounded mt-0.5" />
            <div>
              <span className="text-sm font-medium">{t('form.labels.morph')}</span>
              <p className="text-xs text-muted-foreground">{t('form.descriptions.morph_desc')}</p>
              {s3TablesSelected && <p className="text-xs text-amber-600 mt-1">{t('form.descriptions.s3_tables_swap_note')}</p>}
            </div>
          </label>
          <label className={`flex items-start gap-3 ${(s3TablesSelected || athenaSelected) ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer'}`}>
            <input type="checkbox" checked={addFiles} onChange={() => { if (!s3TablesSelected && !athenaSelected) setAddFiles(!addFiles) }} disabled={s3TablesSelected || athenaSelected} className="rounded mt-0.5" />
            <div>
              <span className="text-sm font-medium">{t('form.actions.add_files')}</span>
              <p className="text-xs text-muted-foreground">{t('form.actions.add_files_desc')}</p>
              {s3TablesSelected && <p className="text-xs text-amber-600 mt-1">{t('form.descriptions.s3_tables_swap_note')}</p>}
              {!s3TablesSelected && athenaSelected && <p className="text-xs text-amber-600 mt-1">{t('form.actions.add_files_engine_note')}</p>}
            </div>
          </label>
        </div>
        {noStrategy && <p className="text-xs text-destructive">{t('form.descriptions.no_strategy_error')}</p>}
      </div>

      <div className="space-y-3">
        <label className="flex items-start gap-3 cursor-pointer">
          <input type="checkbox" checked={swapTables} onChange={() => setSwapTables(!swapTables)} className="rounded mt-0.5" />
          <div>
            <span className="text-sm font-medium">{t('form.actions.replace_tables')}</span>
            <p className="text-xs text-muted-foreground">{t('form.descriptions.swap_tables_desc')}</p>
          </div>
        </label>
      </div>

      <div className="space-y-3">
        <div>
          <label className="text-sm font-medium">{t('form.labels.owners')}</label>
          <p className="text-xs text-muted-foreground">{t('form.descriptions.owners_desc')}</p>
        </div>
        {owners.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {owners.map(o => (
              <span key={o} className="inline-flex items-center gap-1 rounded-md bg-secondary px-2 py-1 text-xs">
                {o}
                <button type="button" onClick={() => setOwners(p => p.filter(x => x !== o))} className="hover:text-destructive cursor-pointer">
                  <X className="h-3 w-3" />
                </button>
              </span>
            ))}
          </div>
        )}
        <div className="relative max-w-sm">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input placeholder="Search groups..." value={ownerS} onChange={e => setOwnerS(e.target.value)} className="pl-9 pr-8" />
          {ownerS && <button onClick={() => setOwnerS('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"><X className="h-4 w-4" /></button>}
        </div>
        {ownerS && fg.length > 0 && (
          <div className="max-w-sm rounded-md border bg-popover shadow-sm max-h-32 overflow-y-auto">
            {fg.slice(0, 5).map(g => (
              <button key={g.name} type="button" className="w-full text-left px-3 py-2 text-sm hover:bg-accent cursor-pointer"
                onClick={() => { setOwners(p => [...p, g.name]); setOwnerS('') }}>
                {g.name} <span className="text-muted-foreground">({g.members.length} members)</span>
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="flex gap-2 pt-2">
        <Button type="submit" disabled={isPending || !name.trim() || noStrategy}>
          {isPending && <Loader2 className="h-4 w-4 animate-spin" />}
          {submitLabel}
        </Button>
        <Button type="button" variant="outline" onClick={onCancel}>{t('common.actions.cancel')}</Button>
      </div>
    </form>
  )
}
