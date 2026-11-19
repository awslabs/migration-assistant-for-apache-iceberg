// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { useToast } from '@/components/ui/toast'
import { CopyButton } from '@/components/ui/copy-button'
import { usePlan, useFinalizePlan, useUpdatePlan, useRemoveTableFromPlan, useUpdateTableType, useDeletePlan, useUpdateTableSpec, useTableSpec, useApplyRecommendations } from '@/hooks/use-plans'
import { useTables } from '@/hooks/use-migrations'
import { useMigrationRules, getCompatibleTypes } from '@/hooks/use-validation'
import { useAssistantStream } from '@/hooks/use-assistant-stream'
import type { AnalysisProgress } from '@/hooks/use-assistant-stream'
import { usePlanAccess } from '@/hooks/use-permissions'
import { PlanForm } from '@/components/plan-form'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { useState, useMemo, useEffect, useRef } from 'react'
import { useParams, useNavigate } from '@tanstack/react-router'
import { Loader2, X, Search, CheckCircle, Pencil, Trash2, ChevronLeft, ChevronRight, Bot, ArrowUpDown, ArrowUp, ArrowDown, AlertTriangle, Check, XCircle, Plus, Sparkles, MoreHorizontal, ChevronDown, Download } from 'lucide-react'
import { downloadAnalysisReport } from '@/lib/analysis-report'
import { useTranslation } from 'react-i18next'
import type { MigrationPlan, TableInfo } from '@/types'

const mtL: Record<string,string> = {rewrite:'Rewrite',incremental:'Incremental',morph:'Morph',add_files:'Add Files',s3tables_managed:'Managed Migration'}
const targetL: Record<string,string> = {s3_tables:'S3 Tables',s3_iceberg:'Iceberg on S3'}
const fmtBadge: Record<string, 'default'|'secondary'|'outline'|'success'|'warning'> = {
  csv:'outline', avro:'secondary', parquet:'default', json:'outline', orc:'outline', hudi:'warning', delta:'secondary', iceberg:'success'}
type SortDir = 'asc'|'desc'|null

function PlanTableList({plan,tables,isDraft,isEditable,onDetailOpenChange}:{plan:MigrationPlan;tables?:TableInfo[];isDraft:boolean;isEditable:boolean;onDetailOpenChange?:(open:boolean)=>void}) {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const { showToast } = useToast()
  const removeTable = useRemoveTableFromPlan()
  const updateType = useUpdateTableType()
  const updateSpec = useUpdateTableSpec()
  const { data: rules } = useMigrationRules()
  const [tblSearch,setTblSearch] = useState(''); const [tblPage,setTblPage] = useState(0)
  const [sortCol,setSortCol] = useState<string|null>(null); const [sortDir,setSortDir] = useState<SortDir>(null)
  const [editingKey,setEditingKey] = useState<string|null>(null)
  const [editValue,setEditValue] = useState('')
  const [analysisPanel, setAnalysisPanel] = useState<any>(null)
  const [removeConfirm, setRemoveConfirm] = useState<any>(null)
  const [panelWidth, setPanelWidth] = useState(480)
  const [selectedRows, setSelectedRows] = useState<Set<string>>(new Set())
  const [actionsOpen, setActionsOpen] = useState(false)
  const panelScrollRef = useRef<HTMLDivElement>(null)
  const [panelTypeEditing, setPanelTypeEditing] = useState(false)
  const [panelTypeValue, setPanelTypeValue] = useState('')
  const [specEditing, setSpecEditing] = useState(false)
  const [specCollapsed, setSpecCollapsed] = useState(true)
  const [specDraft, setSpecDraft] = useState('')
  const [specError, setSpecError] = useState('')
  const tblPS = 10
  const { data: backendSpec } = useTableSpec(
    plan.id,
    analysisPanel?.source_database ?? '',
    analysisPanel?.source_table ?? '',
    !!analysisPanel
  )
  const tblMap = useMemo(()=>{const m:Record<string,TableInfo>={};tables?.forEach(t=>{m[`${t.database}.${t.table_name}`]=t});return m},[tables])

  // Auto-assign types based on plan config + validation rules
  // Only reassigns when current type is INVALID (not in compatible list)
  // Manual user overrides are preserved
  useEffect(() => {
    if (!isDraft || !plan.tables.length || !tables?.length || !rules) return
    const types = plan.migration_types
    plan.tables.forEach(t => {
      const info = tblMap[`${t.source_database}.${t.source_table}`]
      if (!info) return
      const compatible = getCompatibleTypes(rules, info.format, types, info.partitioned)
      if (compatible.length === 0) return
      // Only reassign if current type is not in the compatible list
      if (!compatible.includes(t.migration_type)) {
        const assigned = autoAssign(compatible)
        updateType.mutate({ planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table, migType: assigned })
      }
    })
  }, [plan.migration_types.join(','), tables?.length, rules])

  // Notify the parent page whether the analysis pane is open, so it can break
  // the layout out to full width while the pane is docked side-by-side.
  useEffect(() => { onDetailOpenChange?.(!!analysisPanel) }, [analysisPanel])

  const hSort=(c:string)=>{if(sortCol===c){setSortDir(sortDir==='asc'?'desc':sortDir==='desc'?null:'asc');if(sortDir==='desc')setSortCol(null)}else{setSortCol(c);setSortDir('asc')}}
  const SI=({c}:{c:string})=>{if(sortCol!==c)return<ArrowUpDown className="h-3 w-3 ml-1 opacity-40"/>;return sortDir==='asc'?<ArrowUp className="h-3 w-3 ml-1"/>:<ArrowDown className="h-3 w-3 ml-1"/>}
  let fl = plan.tables.filter(t=>t.source_table.toLowerCase().includes(tblSearch.toLowerCase())||t.source_database.toLowerCase().includes(tblSearch.toLowerCase()))
  if(sortCol&&sortDir){fl=[...fl].sort((a,b)=>{const ai=tblMap[`${a.source_database}.${a.source_table}`];const bi=tblMap[`${b.source_database}.${b.source_table}`]
    let av:any,bv:any;if(sortCol==='source_table'){av=a.source_table;bv=b.source_table}else if(sortCol==='source_database'){av=a.source_database;bv=b.source_database}
    else if(sortCol==='format'){av=ai?.format??'';bv=bi?.format??''}else if(sortCol==='migration_type'){av=a.migration_type;bv=b.migration_type}
    else{av='';bv=''}
    const c=typeof av==='number'?av-bv:String(av).localeCompare(String(bv));return sortDir==='desc'?-c:c})}
  const tp=Math.max(1,Math.ceil(fl.length/tblPS));const pg=fl.slice(tblPage*tblPS,(tblPage+1)*tblPS)
  const analyzingLabel = t('common.labels.analyzing_status')

  return (
    <>
    <div className="flex items-start gap-4">
      <div className="space-y-3 flex-1 min-w-0">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium">{plan.tables.length} table(s)</span>
        <div className="flex items-center gap-2">
          {/* Actions dropdown — shown when rows are selected */}
          {isEditable && selectedRows.size > 0 && (() => {
            return (
              <div className="relative">
                <Button variant="outline" size="sm" className="h-8 text-xs gap-1" onClick={() => setActionsOpen(!actionsOpen)}>
                  <MoreHorizontal className="h-3.5 w-3.5" />Actions ({selectedRows.size})
                  <ChevronDown className={`h-3 w-3 transition-transform ${actionsOpen ? 'rotate-180' : ''}`} />
                </Button>
                {actionsOpen && <>
                  <div className="fixed inset-0 z-30" onClick={() => setActionsOpen(false)} />
                  <div className="absolute right-0 top-full mt-1 z-40 w-48 rounded-md border bg-popover shadow-md py-1 text-sm">
                    <button className="w-full px-3 py-1.5 text-left hover:bg-accent flex items-center gap-2 text-destructive cursor-pointer"
                      onClick={() => {
                        setActionsOpen(false)
                        const selected = plan.tables.filter(t => selectedRows.has(`${t.source_database}.${t.source_table}`))
                        if (selected.length === 1) {
                          setRemoveConfirm(selected[0])
                        } else {
                          // Bulk remove: confirm first one, then queue rest
                          setRemoveConfirm(selected[0])
                        }
                      }}>
                      <Trash2 className="h-3.5 w-3.5" />Remove ({selectedRows.size})
                    </button>
                  </div>
                </>}
              </div>
            )
          })()}
          {isEditable&&<Button variant="outline" size="sm" onClick={()=>navigate({to:'/inventory'})}><Plus className="h-3 w-3"/>{t('plans.actions.add_tables')}</Button>}
          {plan.tables.length>5&&<div className="relative max-w-xs"><Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground"/>
            <Input placeholder="Search tables..." value={tblSearch} onChange={e=>{setTblSearch(e.target.value);setTblPage(0)}} className="pl-9 pr-8 h-8 text-xs"/>
            {tblSearch&&<button onClick={()=>setTblSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"><X className="h-3 w-3"/></button>}</div>}
        </div>
      </div>
      {fl.length>0&&<div className="rounded-md border overflow-x-auto"><table className="w-full min-w-[720px] text-sm"><thead><tr className="border-b bg-muted/50">
        {isEditable && <th className="px-3 py-2 w-8"><input type="checkbox" className="rounded border-muted-foreground/50 cursor-pointer"
          checked={pg.length > 0 && pg.every(t => selectedRows.has(`${t.source_database}.${t.source_table}`))}
          onChange={e => {
            const next = new Set(selectedRows)
            pg.forEach(t => { const k = `${t.source_database}.${t.source_table}`; e.target.checked ? next.add(k) : next.delete(k) })
            setSelectedRows(next)
          }} /></th>}
        <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={()=>hSort('source_table')}><span className="inline-flex items-center">{t('common.labels.table')}<SI c="source_table"/></span></th>
        <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={()=>hSort('source_database')}><span className="inline-flex items-center">{t('common.labels.database')}<SI c="source_database"/></span></th>
        <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={()=>hSort('format')}><span className="inline-flex items-center">{t('common.labels.format')}<SI c="format"/></span></th>
        <th className="px-3 py-2 text-left font-medium cursor-pointer select-none" onClick={()=>hSort('migration_type')}><span className="inline-flex items-center">{t('common.labels.type')}<SI c="migration_type"/></span></th>
        <th className="px-3 py-2 text-left font-medium">{t('form.labels.recommended')}</th>
        <th className="px-3 py-2 text-left font-medium">{t('form.labels.estimates')}</th>
      </tr></thead><tbody>{pg.map((t,i)=>{
        const info=tblMap[`${t.source_database}.${t.source_table}`]
        const compatible = info ? getCompatibleTypes(rules, info.format, plan.migration_types, info.partitioned) : plan.migration_types
        const allEligible = info ? getCompatibleTypes(rules, info.format, plan.migration_types, info.partitioned) : plan.migration_types
        const hasWarning = compatible.length === 0
        const rowKey = `${t.source_database}.${t.source_table}`
        return(
        <tr key={i} className={`border-b last:border-0 hover:bg-muted/30 ${hasWarning ? 'bg-destructive/5' : ''}`}>
          {isEditable && <td className="px-3 py-2"><input type="checkbox" className="rounded border-muted-foreground/50 cursor-pointer"
            checked={selectedRows.has(rowKey)}
            onChange={e => { const next = new Set(selectedRows); e.target.checked ? next.add(rowKey) : next.delete(rowKey); setSelectedRows(next) }} /></td>}
          <td className="px-3 py-2 font-medium"><button className="text-left underline text-primary hover:text-primary/80 cursor-pointer" onClick={()=>{setAnalysisPanel(t);setSpecEditing(false);setSpecError('');setPanelTypeEditing(false)}}>{t.source_table}</button></td>
          <td className="px-3 py-2 text-muted-foreground">{t.source_database}</td>
          <td className="px-3 py-2">{info?<Badge variant={fmtBadge[info.format]??'outline'}>{info.format}</Badge>:'—'}</td>
          <td className="px-3 py-2">{(()=>{
            const key=`${t.source_database}.${t.source_table}`
            const isEditing = editingKey === key
            if(hasWarning) return <span className="inline-flex items-center gap-1 text-xs text-destructive"><AlertTriangle className="h-3 w-3"/>Incompatible</span>
            if(!isEditable) return <Badge variant="outline">{mtL[t.migration_type]??t.migration_type}</Badge>
            if(isEditing) return (
              <div className="inline-flex items-center gap-1">
                <Select value={editValue} onChange={e=>setEditValue(e.target.value)} className="text-xs h-7 w-28">
                  {allEligible.map(mt=><option key={mt} value={mt}>{mtL[mt]??mt}</option>)}
                </Select>
                <button onClick={()=>{updateType.mutate({planId:plan.id,sourceDb:t.source_database,sourceTable:t.source_table,migType:editValue},{onSuccess:()=>showToast(`Migration type for ${t.source_database}.${t.source_table} updated to ${mtL[editValue]??editValue}`,'success'),onError:(err:any)=>showToast(err?.message||'Failed to update type','error')});setEditingKey(null)}}
                  className="text-emerald-600 hover:text-emerald-700 cursor-pointer" aria-label="Confirm"><Check className="h-4 w-4"/></button>
                <button onClick={()=>setEditingKey(null)}
                  className="text-destructive hover:text-destructive/80 cursor-pointer" aria-label="Cancel"><XCircle className="h-4 w-4"/></button>
              </div>)
            // Display mode
            return (
              <div className="inline-flex items-center gap-1.5">
                <Badge variant="outline" className="cursor-pointer" onClick={()=>{setEditingKey(key);setEditValue(t.migration_type)}}>{mtL[t.migration_type]??t.migration_type}</Badge>
                <button onClick={()=>{setEditingKey(key);setEditValue(t.migration_type)}}
                  className="text-muted-foreground hover:text-foreground cursor-pointer" aria-label="Edit type"><Pencil className="h-3 w-3"/></button>
              </div>)
          })()}</td>
          <td className="px-3 py-2 text-xs">{t.analysis_status==='analyzing'?<span className="text-amber-600">{analyzingLabel}</span>:t.recommended_type?<Badge variant="outline" className={t.recommended_type===t.migration_type?"border-emerald-500 text-emerald-600":"border-amber-500 text-amber-600"}>{mtL[t.recommended_type]??t.recommended_type}</Badge>:<span className="text-muted-foreground">—</span>}</td>
          <td className="px-3 py-2 text-xs">{(t.estimated_size_gb!=null||t.estimated_cost_usd!=null||t.estimated_time_min!=null)?<div className="space-y-0.5 text-muted-foreground">{t.estimated_size_gb!=null&&<div>~{t.estimated_size_gb} GB</div>}{t.estimated_cost_usd!=null&&<div>&lt;${t.estimated_cost_usd.toFixed(2)}</div>}{t.estimated_time_min!=null&&<div>&lt;{Math.round(t.estimated_time_min)} min</div>}</div>:<span className="text-muted-foreground">—</span>}</td>
        </tr>)})}</tbody></table></div>}
      {fl.length>tblPS&&<div className="flex items-center justify-end gap-2 text-xs text-muted-foreground">
        {tblPage*tblPS+1}–{Math.min((tblPage+1)*tblPS,fl.length)} of {fl.length}
        <Button variant="outline" size="icon" className="h-6 w-6" onClick={()=>setTblPage(p=>p-1)} disabled={tblPage===0}><ChevronLeft className="h-3 w-3"/></Button>
        <Button variant="outline" size="icon" className="h-6 w-6" onClick={()=>setTblPage(p=>p+1)} disabled={tblPage>=tp-1}><ChevronRight className="h-3 w-3"/></Button>
      </div>}
      </div>
      {analysisPanel && (
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
              <h3 className="text-sm font-semibold">{analysisPanel.source_database}.{analysisPanel.source_table}</h3>
              <p className="text-xs text-muted-foreground">{t('form.labels.analysis')} &amp; {t('detail.labels.spec')}</p>
            </div>
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setAnalysisPanel(null)}><X className="h-4 w-4" /></Button>
          </div>
          <div ref={panelScrollRef} className="flex-1 overflow-y-auto p-4 space-y-6 text-sm">
            {/* Agent Reasoning */}
            <div className="space-y-3">
              <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{t('detail.labels.agent_reasoning')}</h4>
              {(!analysisPanel.analysis_status || analysisPanel.analysis_status === 'pending') ? (
                <div className="rounded-md border border-dashed bg-muted/20 p-4 text-center text-xs text-muted-foreground">
                  {t('plan_detail.messages.analysis_not_run')}
                </div>
              ) : (
              <div className="space-y-2">
                <div className="flex justify-between items-center"><span className="text-muted-foreground">{t('form.labels.migration_type')}</span>{isEditable && !panelTypeEditing ? (
                  <div className="inline-flex items-center gap-1.5">
                    <Badge variant="outline">{mtL[analysisPanel.migration_type] ?? analysisPanel.migration_type}</Badge>
                    <button onClick={() => { setPanelTypeEditing(true); setPanelTypeValue(analysisPanel.migration_type) }} className="text-muted-foreground hover:text-foreground cursor-pointer" aria-label="Edit type"><Pencil className="h-3 w-3" /></button>
                  </div>
                ) : panelTypeEditing ? (
                  <div className="inline-flex items-center gap-1">
                    <Select value={panelTypeValue} onChange={e => setPanelTypeValue(e.target.value)} className="text-xs h-7 w-28">
                      {plan.migration_types.map(mt=><option key={mt} value={mt}>{mtL[mt]??mt}</option>)}
                    </Select>
                    <button onClick={() => { updateType.mutate({ planId: plan.id, sourceDb: analysisPanel.source_database, sourceTable: analysisPanel.source_table, migType: panelTypeValue }, { onSuccess: () => { const label = `${analysisPanel.source_database}.${analysisPanel.source_table}`; setPanelTypeEditing(false); setAnalysisPanel(null); showToast(`Type for ${label} updated to ${mtL[panelTypeValue] ?? panelTypeValue}`, 'success') }, onError: (err: any) => showToast(err?.message || 'Failed to update type', 'error') }) }}
                      className="text-emerald-600 hover:text-emerald-700 cursor-pointer" aria-label="Confirm"><Check className="h-4 w-4" /></button>
                    <button onClick={() => setPanelTypeEditing(false)}
                      className="text-destructive hover:text-destructive/80 cursor-pointer" aria-label="Cancel"><XCircle className="h-4 w-4" /></button>
                  </div>
                ) : (
                  <Badge variant="outline">{mtL[analysisPanel.migration_type] ?? analysisPanel.migration_type}</Badge>
                )}</div>
                {analysisPanel.recommended_type && <div className="flex justify-between"><span className="text-muted-foreground">{t('form.labels.recommended')}</span><Badge variant="outline" className={analysisPanel.recommended_type === analysisPanel.migration_type ? 'border-emerald-500 text-emerald-600' : 'border-amber-500 text-amber-600'}>{mtL[analysisPanel.recommended_type] ?? analysisPanel.recommended_type}</Badge></div>}
                {analysisPanel.recommended_batch_size != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.batch_size')}</span><span>{analysisPanel.recommended_batch_size} partitions/batch</span></div>}
                {analysisPanel.estimated_size_gb != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.estimated_size')}</span><span>~{analysisPanel.estimated_size_gb} GB</span></div>}
                {analysisPanel.estimated_cost_usd != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.estimated_cost')}</span><span>&lt;${analysisPanel.estimated_cost_usd.toFixed(2)}</span></div>}
                {analysisPanel.estimated_time_min != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.estimated_time')}</span><span>&lt;{Math.round(analysisPanel.estimated_time_min)} min</span></div>}
                <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.status')}</span><Badge variant={analysisPanel.analysis_status === 'completed' ? 'success' : analysisPanel.analysis_status === 'failed' ? 'destructive' : 'secondary'}>{analysisPanel.analysis_status}</Badge></div>
              </div>
              )}
              {analysisPanel.analysis_summary && (
                <div className="space-y-1">
                  <span className="text-muted-foreground text-xs font-medium">{t('detail.labels.summary')}</span>
                  <div className="rounded-md border bg-muted/30 p-3 text-xs whitespace-pre-wrap">
                    {analysisPanel.analysis_summary}
                  </div>
                </div>
              )}
            </div>

            {/* Migration Spec */}
            <div className="space-y-3">
              <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{t('detail.labels.migration_spec')}</h4>
              <div className="space-y-2">
                <div className="space-y-1">
                  <span className="text-xs font-medium text-muted-foreground">{t('detail.labels.source')}</span>
                  <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1">
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.database')}</span><span className="font-mono">{analysisPanel.source_database}</span></div>
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.table')}</span><span className="font-mono">{analysisPanel.source_table}</span></div>
                    {analysisPanel.source_format && <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.format')}</span><Badge variant={fmtBadge[analysisPanel.source_format] ?? 'outline'}>{analysisPanel.source_format}</Badge></div>}
                    {analysisPanel.source_location && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('common.labels.location')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{analysisPanel.source_location}</span><CopyButton value={analysisPanel.source_location} label="location" /></span></div>}
                    {analysisPanel.source_account && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('aws.labels.aws_account')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{analysisPanel.source_account}</span><CopyButton value={analysisPanel.source_account} label="account ID" /></span></div>}
                    {analysisPanel.source_compression && <div className="flex justify-between"><span className="text-muted-foreground">{t('detail.labels.compression')}</span><span>{analysisPanel.source_compression}</span></div>}
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('detail.labels.partitioned')}</span><span>{analysisPanel.source_partitioned ? 'Yes' : 'No'}</span></div>
                    {analysisPanel.source_partition_keys?.length > 0 && <div className="pt-1"><span className="text-muted-foreground">{t('detail.labels.partition_keys')}</span><div className="mt-1 flex flex-wrap gap-1">{analysisPanel.source_partition_keys.map((pk: any, i: number) => <Badge key={i} variant="secondary" className="text-xs">{pk.name} ({pk.type})</Badge>)}</div></div>}
                  </div>
                </div>
                <div className="space-y-1">
                  <span className="text-xs font-medium text-muted-foreground">{t('form.labels.target')}</span>
                  <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1">
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.database')}</span><span className="font-mono">{analysisPanel.target_database}</span></div>
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.table')}</span><span className="font-mono">{analysisPanel.target_table}</span></div>
                    {analysisPanel.target_type && <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.type')}</span><Badge variant="secondary">{targetL[analysisPanel.target_type] ?? analysisPanel.target_type}</Badge></div>}
                    {analysisPanel.target_location && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('common.labels.location')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{analysisPanel.target_location}</span><CopyButton value={analysisPanel.target_location} label="location" /></span></div>}
                    {analysisPanel.target_bucket_arn && <div className="flex justify-between gap-2"><span className="text-muted-foreground shrink-0">{t('form.labels.bucket_arn')}</span><span className="flex items-center gap-1 min-w-0"><span className="font-mono truncate">{analysisPanel.target_bucket_arn}</span><CopyButton value={analysisPanel.target_bucket_arn} label="bucket ARN" /></span></div>}
                  </div>
                </div>
                <div className="space-y-1">
                  <span className="text-xs font-medium text-muted-foreground">{t('detail.labels.strategy')}</span>
                  <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1">
                    <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.type')}</span><Badge variant="outline">{mtL[analysisPanel.migration_type] ?? analysisPanel.migration_type}</Badge></div>
                    {analysisPanel.recommended_batch_size != null && <div className="flex justify-between"><span className="text-muted-foreground">{t('pipeline.labels.batch_size')}</span><span>{analysisPanel.recommended_batch_size}</span></div>}
                  </div>
                </div>
                {(analysisPanel.partition_spec?.length > 0 || analysisPanel.sort_order?.length > 0) && (
                  <div className="space-y-1">
                    <span className="text-xs font-medium text-muted-foreground">{t('detail.labels.iceberg_properties')}</span>
                    <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-2">
                      {analysisPanel.partition_spec?.length > 0 && <div><span className="text-muted-foreground">{t('detail.labels.partition_spec')}</span><div className="mt-1 flex flex-wrap gap-1">{analysisPanel.partition_spec.map((p: any, i: number) => <Badge key={i} variant="secondary" className="text-xs">{p.transform}({p.source_column})</Badge>)}</div></div>}
                      {analysisPanel.sort_order?.length > 0 && <div><span className="text-muted-foreground">{t('detail.labels.sort_order')}</span><div className="mt-1 flex flex-wrap gap-1">{analysisPanel.sort_order.map((s: any, i: number) => <Badge key={i} variant="secondary" className="text-xs">{s.column} {s.direction ?? 'asc'}</Badge>)}</div></div>}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Raw Spec JSON */}
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <button onClick={() => setSpecCollapsed(!specCollapsed)} className="flex items-center gap-1 cursor-pointer">
                  <ChevronRight className={`h-3 w-3 transition-transform ${specCollapsed ? '' : 'rotate-90'}`} />
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{t('detail.labels.spec_json')}</h4>
                </button>
                {isEditable && !specEditing && (
                  <Button variant="outline" size="sm" className="h-6 text-xs" onClick={() => {
                    const spec = backendSpec ?? {
                      version: "1.0",
                      source: {
                        database: analysisPanel.source_database,
                        table_name: analysisPanel.source_table,
                      },
                      target: {
                        type: analysisPanel.target_type ?? plan.target,
                        database: analysisPanel.target_database,
                        table_name: analysisPanel.target_table,
                      },
                      strategy: { type: analysisPanel.migration_type },
                      iceberg_properties: {},
                    }
                    setSpecDraft(JSON.stringify(spec, null, 2))
                    setSpecError('')
                    setSpecCollapsed(false)
                    const scrollTop = panelScrollRef.current?.scrollTop ?? 0
                    setSpecEditing(true)
                    requestAnimationFrame(() => {
                      if (panelScrollRef.current) panelScrollRef.current.scrollTop = scrollTop
                    })
                  }}><Pencil className="h-3 w-3 mr-1" />Edit</Button>
                )}
              </div>
              {!specCollapsed && (specEditing ? (
                <div className="space-y-2">
                  <textarea
                    className="w-full rounded-md border bg-muted/30 p-3 text-xs font-mono whitespace-pre resize-y min-h-[200px] focus:outline-none focus:ring-2 focus:ring-ring"
                    value={specDraft}
                    onChange={e => { setSpecDraft(e.target.value); setSpecError('') }}
                    rows={16}
                  />
                  {specError && <p className="text-xs text-destructive">{specError}</p>}
                  <div className="flex items-center gap-2">
                    <Button size="sm" className="h-7 text-xs" disabled={updateSpec.isPending} onClick={() => {
                      try {
                        const parsed = JSON.parse(specDraft)
                        updateSpec.mutate({
                          planId: plan.id,
                          sourceDb: analysisPanel.source_database,
                          sourceTable: analysisPanel.source_table,
                          specJson: parsed,
                        }, {
                          onSuccess: () => { const label = `${analysisPanel.source_database}.${analysisPanel.source_table}`; setSpecEditing(false); setAnalysisPanel(null); showToast(`Spec for ${label} saved successfully`, 'success') },
                          onError: (err: any) => setSpecError(err?.message || 'Failed to save'),
                        })
                      } catch { setSpecError('Invalid JSON') }
                    }}>{updateSpec.isPending ? <Loader2 className="h-3 w-3 animate-spin" /> : <Check className="h-3 w-3" />}Save</Button>
                    <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => { setSpecEditing(false); setSpecError('') }}>{t('common.actions.cancel')}</Button>
                  </div>
                </div>
              ) : (
                <pre className="rounded-md border bg-muted/30 p-3 text-xs font-mono whitespace-pre-wrap overflow-x-auto">{JSON.stringify(backendSpec ?? { loading: true }, null, 2)}</pre>
              ))}
            </div>
          </div>
        </div>
      )}
      </div>
      <ConfirmDialog
        open={!!removeConfirm}
        title="Remove Table"
        message={removeConfirm ? (selectedRows.size > 1
          ? `Remove ${selectedRows.size} selected table(s) from this plan? They will be released back to the inventory.`
          : `Remove "${removeConfirm.source_database}.${removeConfirm.source_table}" from this plan? It will be released back to the inventory.`) : ''}
        confirmLabel="Remove"
        cancelLabel="Cancel"
        variant="destructive"
        onConfirm={() => {
          const t = removeConfirm
          setRemoveConfirm(null)
          if (!t) return
          if (selectedRows.size > 1) {
            // Bulk remove all selected
            const selected = plan.tables.filter(tbl => selectedRows.has(`${tbl.source_database}.${tbl.source_table}`))
            selected.forEach(tbl => removeTable.mutate(
              { planId: plan.id, sourceDb: tbl.source_database, sourceTable: tbl.source_table }
            ))
            showToast(`Removed ${selected.length} table(s) from plan "${plan.name}"`, 'success')
            setSelectedRows(new Set())
          } else {
            removeTable.mutate(
              { planId: plan.id, sourceDb: t.source_database, sourceTable: t.source_table },
              { onSuccess: () => showToast(`Removed ${t.source_table} from plan`, 'success'), onError: (err: any) => showToast(err?.message || 'Failed to remove table', 'error') }
            )
            setSelectedRows(new Set())
          }
        }}
        onCancel={() => setRemoveConfirm(null)}
      />
    </>)
}

function autoAssign(compatible: string[]): string {
  return compatible[0]
}

const TOOL_LABELS: Record<string, string> = {
  // Glue / catalog
  get_glue_table: 'Fetching table metadata',
  get_glue_partitions: 'Fetching partition info',
  get_glue_column_statistics: 'Fetching column statistics',
  get_glue_table_access: 'Checking table access',
  // Sizing / cost / recommendations
  estimate_table_size: 'Estimating table size',
  estimate_migration_cost: 'Estimating cost & time',
  recommend_migration_type: 'Recommending migration type',
  recommend_batch_size: 'Calculating batch size',
  recommend_add_files_batch_size: 'Calculating batch size',
  recommend_partition_transform: 'Analyzing partitions',
  recommend_sort_order: 'Recommending sort order',
  // Schema
  analyze_schema_compatibility: 'Analyzing schema compatibility',
  check_type_promotion: 'Checking type promotion',
  // S3 / access patterns
  get_s3_access_patterns: 'Checking access patterns',
  get_s3_object_sample: 'Sampling data files',
  list_s3_objects: 'Listing data files',
  // Iceberg
  get_iceberg_table_stats: 'Reading Iceberg metadata',
  get_iceberg_partition_details: 'Reading Iceberg partitions',
  // Hudi
  get_hudi_table_metadata: 'Reading Hudi metadata',
  discover_hudi_partitions: 'Discovering Hudi partitions',
  get_hudi_compaction_status: 'Checking Hudi compaction',
  // Local fallback
  local_analysis: 'Running local analysis',
}

// Turn any tool name into a readable label. Falls back to a title-cased,
// de-underscored form so a newly added agent tool never shows as raw
// snake_case (e.g. "analyze_schema_compatibility" → "Analyze schema
// compatibility") even before it's added to TOOL_LABELS.
function toolLabel(tool: string): string {
  if (TOOL_LABELS[tool]) return TOOL_LABELS[tool]
  if (!tool) return 'Working'
  const spaced = tool.replace(/_/g, ' ').trim()
  return spaced.charAt(0).toUpperCase() + spaced.slice(1)
}

interface AnalysisDerivedState {
  total: number
  analyzing: number
  completed: number
  withRec: number
  isAnalyzing: boolean
  hasResults: boolean
}

function AnalysisProgressPanel({ progress, analysisState, plan, onClose }: { progress: AnalysisProgress; analysisState: AnalysisDerivedState; plan: MigrationPlan; onClose: () => void }) {
  const { t } = useTranslation()
  const { toolCalls, batch, batchesTotal } = progress
  const { showToast } = useToast()
  const applyRecommendations = useApplyRecommendations()

  // Whether THIS session has a live SSE stream driving the panel. When true we
  // trust the live progress; when false (e.g. after a page refresh) we fall
  // back to state derived from the persisted plan.
  const liveActive = progress.isRunning || !!progress.summary || !!progress.error

  // Effective state = live stream if active, else derived from the DB.
  const isRunning = liveActive ? progress.isRunning : analysisState.isAnalyzing
  const error = progress.error
  const summary = progress.summary || (
    // Reconstruct a summary from the plan when the live one is gone (refresh)
    // but analysis has finished and produced results.
    !liveActive && !analysisState.isAnalyzing && analysisState.hasResults
      ? `Analyzed ${analysisState.withRec} of ${analysisState.total} table(s).`
      : null
  )
  const tablesCount = liveActive ? progress.tablesCount : analysisState.total
  const currentTable = progress.currentTable

  // Count tables where the recommendation differs from current type
  const tablesWithDifferentRec = plan.tables.filter(
    t => t.recommended_type && t.recommended_type !== t.migration_type
  ).length

  // Show only the 6 most recent tool calls — oldest disappears as new ones
  // arrive, so the feed stays a fixed height regardless of how many tables are
  // in the plan (e.g. 126 tables would otherwise render a very long list).
  const recentCalls = toolCalls.slice(-6)

  // Progress %: with a live stream, estimate from tool-call volume; without one
  // (refresh), use the DB-derived completed/total ratio.
  let progressPct: number
  if (liveActive && progress.isRunning) {
    const estimatedTotalCalls = Math.max(tablesCount, 1) * 10
    progressPct = Math.min(Math.round((toolCalls.length / estimatedTotalCalls) * 100), 90)
  } else if (isRunning) {
    const done = analysisState.total - analysisState.analyzing
    progressPct = Math.min(Math.round((done / Math.max(analysisState.total, 1)) * 100), 90)
  } else {
    progressPct = (summary || error) ? 100 : 0
  }

  const panelBg = 'rounded-lg border bg-muted/30 p-4 space-y-3'

  return (
    <div className={panelBg}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Bot className="h-4 w-4 text-primary" />
          <span className="text-sm font-medium">{t('ai.labels.ai_assistant')}</span>
          {isRunning && (
            <Badge variant="warning" className="text-xs animate-pulse">{t('common.loading.analyzing')}</Badge>
          )}
          {!isRunning && summary && !error && (
            <Badge variant="success" className="text-xs">{t('common.labels.complete')}</Badge>
          )}
        </div>
        <Button variant="ghost" size="icon" className="h-6 w-6" onClick={onClose}>
          <X className="h-3 w-3" />
        </Button>
      </div>

      {isRunning && (
        <div className="space-y-3">
          {/* Progress bar */}
          <div className="space-y-1">
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>
                {batchesTotal > 1
                  ? `Analyzing ${tablesCount} table(s) — batch ${batch} of ${batchesTotal}...`
                  : tablesCount > 1
                    ? `Analyzing ${tablesCount} table(s)...`
                    : currentTable
                      ? `Analyzing ${currentTable}`
                      : `Preparing analysis...`}
              </span>
              <span>{progressPct}%</span>
            </div>
            <div className="h-1.5 w-full rounded-full bg-muted overflow-hidden">
              <div
                className="h-full rounded-full bg-primary transition-all duration-500 ease-out"
                style={{ width: `${Math.max(progressPct, 3)}%` }}
              />
            </div>
          </div>

          {/* Tool call activity feed — last 6, oldest disappears */}
          {recentCalls.length > 0 && (
            <div className="space-y-1.5">
              {recentCalls.map((tc, i) => (
                <div key={tc.id} className="flex items-center gap-2 text-xs">
                  {i === recentCalls.length - 1 && tc.status === 'calling' ? (
                    <Loader2 className="h-3 w-3 animate-spin text-amber-500 shrink-0" />
                  ) : (
                    <CheckCircle className="h-3 w-3 text-emerald-500 shrink-0" />
                  )}
                  <span className={i === recentCalls.length - 1 && tc.status === 'calling' ? 'text-foreground' : 'text-muted-foreground'}>
                    {toolLabel(tc.tool)}
                  </span>
                  {tc.table && (
                    <span className="text-muted-foreground/60 font-mono truncate ml-auto text-[10px]">
                      {tc.table.split('.').pop()}
                    </span>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* No live tool-call feed. Either we're just starting (live stream,
              no calls yet) or we reconnected to an in-progress run after a
              refresh (no live stream — show a monitoring message + DB counts). */}
          {recentCalls.length === 0 && (
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Loader2 className="h-3 w-3 animate-spin" />
              <span>
                {liveActive
                  ? t('common.loading.initializing_analysis')
                  : `Analysis in progress — ${analysisState.completed} of ${analysisState.total} table(s) done so far. Progress updates automatically.`}
              </span>
            </div>
          )}
        </div>
      )}

      {/* Completed state */}
      {!isRunning && summary && (
        <div className="space-y-3">
          <div className="rounded-md bg-background border p-3 text-sm">{summary}</div>
          <div className="flex flex-wrap items-center gap-2">
            {tablesWithDifferentRec > 0 && (
              <Button
                size="sm"
                disabled={applyRecommendations.isPending}
                onClick={() => applyRecommendations.mutate(plan.id, {
                  onSuccess: () => showToast(`Applied recommendations to ${tablesWithDifferentRec} table(s) in "${plan.name}"`, 'success'),
                  onError: (err: any) => showToast(err?.message || 'Failed to apply recommendations', 'error'),
                })}
              >
                {applyRecommendations.isPending
                  ? <Loader2 className="h-3 w-3 animate-spin" />
                  : <Sparkles className="h-3 w-3" />}
                Apply Recommendations ({tablesWithDifferentRec})
              </Button>
            )}
            {/* Download a full, formatted analysis report (HTML or Markdown) */}
            <Button
              size="sm"
              variant="outline"
              onClick={() => downloadAnalysisReport(plan, summary, 'html')}
              title="Download a full analysis report as a styled HTML file"
            >
              <Download className="h-3 w-3" />
              Download Report (HTML)
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => downloadAnalysisReport(plan, summary, 'md')}
              title="Download a full analysis report as Markdown"
            >
              <Download className="h-3 w-3" />
              Markdown
            </Button>
          </div>
        </div>
      )}

      {/* Error state */}
      {!isRunning && error && (
        <div className="rounded-md bg-destructive/10 border border-destructive/20 p-3 text-sm text-destructive">
          {error}
        </div>
      )}
    </div>
  )
}

export function PlanDetailPage() {
  const { t } = useTranslation()
  const { planId } = useParams({ strict: false }) as { planId: string }
  const navigate = useNavigate()
  const { showToast } = useToast()
  const { data: plan, isLoading } = usePlan(planId)
  const { data: tables } = useTables()
  const updatePlan = useUpdatePlan()
  const finalizePlan = useFinalizePlan()
  const deletePlan = useDeletePlan()
  const [editing, setEditing] = useState(false)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false)
  const [showFinalizeConfirm, setShowFinalizeConfirm] = useState(false)
  const [assistantOpen, setAssistantOpen] = useState(false)
  const [menuOpen, setMenuOpen] = useState(false)
  // Whether the per-table analysis pane (inside PlanTableList) is open — lifted
  // up so the page can break out to full width while it's showing.
  const [tableDetailOpen, setTableDetailOpen] = useState(false)
  // Tracks a manual close of the panel so we don't re-open it on every poll.
  const [assistantDismissed, setAssistantDismissed] = useState(false)
  const { progress, startAnalysis, reset } = useAssistantStream()
  const access = usePlanAccess(plan)

  // Derive analysis state from the PERSISTED plan (DB source of truth), so the
  // UI survives a page refresh even though the live SSE stream + component state
  // are gone. The backend background task keeps running and keeps writing
  // per-table analysis_status; this reads that back.
  const analysisState = useMemo(() => {
    const tbls = plan?.tables ?? []
    const analyzing = tbls.filter(t => t.analysis_status === 'analyzing').length
    const withRec = tbls.filter(t => t.recommended_type).length
    const completed = tbls.filter(t => t.analysis_status === 'completed').length
    return {
      total: tbls.length,
      analyzing,
      completed,
      withRec,
      isAnalyzing: analyzing > 0,
      hasResults: withRec > 0 || completed > 0,
    }
  }, [plan])

  // Auto-open the panel when the DB says analysis is in progress (reconnect
  // after a refresh), unless the user explicitly closed it. Live runs open it
  // via the button. Once analysis stops, we leave it as-is (user controls it).
  useEffect(() => {
    if (analysisState.isAnalyzing && !assistantDismissed) {
      setAssistantOpen(true)
    }
  }, [analysisState.isAnalyzing, assistantDismissed])

  if (isLoading) return <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
  if (!plan) return <div className="py-12 text-center text-muted-foreground">{t('errors.labels.message')}</div>

  const canManage = access === 'manage'
  const isDraft = plan.status === 'draft' || plan.status === 'ready'
  const isEditable = canManage && (plan.status === 'draft' || plan.status === 'ready' || plan.status === 'staged')

  if (editing) return (
    <Card><CardHeader><CardTitle>{t('detail.labels.edit_prefix')} {plan.name}</CardTitle></CardHeader><CardContent>
      <PlanForm initial={plan} onSubmit={d => updatePlan.mutate({ planId: plan.id, data: d }, { onSuccess: () => { setEditing(false); showToast(`Plan "${plan.name}" updated successfully`, 'success') }, onError: (err: any) => showToast(err?.message || 'Failed to update plan', 'error') })}
        onCancel={() => setEditing(false)} isPending={updatePlan.isPending} submitLabel="Save Changes" />
    </CardContent></Card>)

  return (
    <div className={tableDetailOpen
      ? // Break out of the layout's max-w container to use (almost) the full
        // window width when the analysis pane is docked side-by-side.
        'space-y-6 relative left-1/2 right-1/2 -mx-[50vw] w-screen px-4'
      : 'space-y-6'}>
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3"><CardTitle>{plan.name}</CardTitle><Badge variant={plan.status === 'completed' ? 'success' : plan.status === 'failed' ? 'destructive' : plan.status === 'in_progress' ? 'default' : plan.status === 'cancelled' ? 'outline' : 'secondary'}>{plan.status.replace('_', ' ')}</Badge></div>
            <div className="flex items-center gap-2">
              {isDraft && canManage && (() => {
                // Fold the plan-level actions into a single Actions dropdown.
                const analyzing = progress.isRunning || analysisState.isAnalyzing
                const items: { key: string; label: string; icon: React.ReactNode; onClick: () => void; variant?: 'destructive'; disabled?: boolean }[] = [
                  { key: 'edit', label: t('common.actions.edit'), icon: <Pencil className="h-3.5 w-3.5" />, onClick: () => setEditing(true) },
                  { key: 'analyze', label: analyzing ? t('common.loading.analyzing_ellipsis') : t('ai.labels.ai_assistant'), icon: <Bot className="h-3.5 w-3.5" />, disabled: analyzing,
                    onClick: () => { setAssistantDismissed(false); setAssistantOpen(true); reset(); startAnalysis(plan.id) } },
                ]
                if (!assistantOpen && !analyzing && analysisState.hasResults) {
                  items.push({ key: 'view', label: 'View analysis', icon: <Sparkles className="h-3.5 w-3.5" />, onClick: () => { setAssistantDismissed(false); setAssistantOpen(true) } })
                }
                items.push({ key: 'finalize', label: t('plans.actions.finalize'), icon: <CheckCircle className="h-3.5 w-3.5" />, disabled: finalizePlan.isPending || plan.tables.length === 0, onClick: () => setShowFinalizeConfirm(true) })
                items.push({ key: 'delete', label: t('common.actions.delete'), icon: <Trash2 className="h-3.5 w-3.5" />, variant: 'destructive', disabled: deletePlan.isPending, onClick: () => setShowDeleteConfirm(true) })
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
            </div>
          </div>
          {plan.description && <CardDescription>{plan.description}</CardDescription>}
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-4 text-sm">
            <div><span className="text-muted-foreground">{t('detail.labels.target_label')}</span> <Badge variant="outline" className="border-blue-300 text-blue-700 dark:border-blue-700 dark:text-blue-400">{targetL[plan.target] ?? plan.target}</Badge></div>
            <div><span className="text-muted-foreground">{t('detail.labels.engine_label')}</span> <Badge variant="outline" className="border-emerald-300 text-emerald-700 dark:border-emerald-700 dark:text-emerald-400">{plan.execution_engine === 'emr_serverless' ? 'EMR Serverless' : plan.execution_engine === 'athena' ? 'Athena' : plan.execution_engine}</Badge></div>
            <div><span className="text-muted-foreground">{t('detail.labels.types_label')}</span> <span className="inline-flex gap-1 ml-1">{plan.migration_types.map(mt => <Badge key={mt} variant="outline" className="border-purple-300 text-purple-700 dark:border-purple-700 dark:text-purple-400">{mtL[mt] ?? mt}</Badge>)}</span></div>
            <div><span className="text-muted-foreground">{t('detail.labels.created_on')}</span> {new Date(plan.created_at).toLocaleDateString()} by {plan.created_by}</div>
          </div>
          {plan.owners.length > 0 && <div className="text-sm"><span className="text-muted-foreground">{t('form.labels.owners')}</span> {plan.owners.join(', ')}</div>}
          {assistantOpen && (
            <AnalysisProgressPanel
              progress={progress}
              analysisState={analysisState}
              plan={plan}
              onClose={() => { setAssistantOpen(false); setAssistantDismissed(true) }}
            />
          )}
          <PlanTableList plan={plan} tables={tables} isDraft={isDraft} isEditable={isEditable} onDetailOpenChange={setTableDetailOpen} />
        </CardContent>
      </Card>
      <ConfirmDialog
        open={showFinalizeConfirm}
        title="Finalize Migration Plan"
        message={`Are you sure you want to finalize "${plan.name}"? This will move the plan to the migration stage. This action cannot be undone.`}
        confirmLabel="Finalize"
        cancelLabel="Go Back"
        variant="default"
        onConfirm={() => { setShowFinalizeConfirm(false); finalizePlan.mutate(plan.id, { onSuccess: () => { showToast(`Plan "${plan.name}" finalized successfully`, 'success'); navigate({ to: '/migrations' }) }, onError: (err: any) => showToast(err?.message || 'Failed to finalize plan', 'error') }) }}
        onCancel={() => setShowFinalizeConfirm(false)}
      />
      <ConfirmDialog
        open={showDeleteConfirm}
        title="Delete Migration Plan"
        message={`Are you sure you want to delete "${plan.name}"? All ${plan.tables.length} table(s) will be released back to the inventory. This action cannot be undone.`}
        confirmLabel="Delete Plan"
        cancelLabel="Keep Plan"
        variant="destructive"
        onConfirm={() => { setShowDeleteConfirm(false); deletePlan.mutate(plan.id, { onSuccess: () => { showToast(`Plan "${plan.name}" deleted`, 'success'); navigate({ to: '/plans' }) }, onError: (err: any) => showToast(err?.message || 'Failed to delete plan', 'error') }) }}
        onCancel={() => setShowDeleteConfirm(false)}
      />
    </div>)
}
