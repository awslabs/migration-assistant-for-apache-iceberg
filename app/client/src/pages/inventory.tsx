// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { useToast } from '@/components/ui/toast'
import { useTables } from '@/hooks/use-migrations'
import { usePlans, useAddTablesBatch, useTablesInPlans } from '@/hooks/use-plans'
import { useGroups } from '@/hooks/use-groups'
import { useSources, useRefreshSource } from '@/hooks/use-sources'
import { useMigrationRules, getCompatibleTypes } from '@/hooks/use-validation'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Loader2, Search, ChevronLeft, ChevronRight, CheckSquare, Filter, X, ArrowUpDown, ArrowUp, ArrowDown, AlertTriangle, ChevronDown, ChevronRight as ChevronRightIcon, Eye, Database, Table2, Server, RefreshCw } from 'lucide-react'
import { useState, useMemo, useRef, useEffect } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { useQueryClient } from '@tanstack/react-query'
import { useAuth } from '@/hooks/use-auth'
import { useTranslation } from 'react-i18next'
import { api } from '@/lib/api'

/**
 * Target table naming prefix for Iceberg-on-S3 migrations.
 * Configurable via VITE_TARGET_TABLE_PREFIX env var.
 * Default: "ima__" (prefix) — target becomes "ima__<source_table>"
 */
const TARGET_TABLE_PREFIX = import.meta.env.VITE_TARGET_TABLE_PREFIX || 'ima__'

const fmtBadge: Record<string, 'default'|'secondary'|'outline'|'success'|'warning'> = {
  csv:'outline', avro:'secondary', parquet:'default', json:'outline', orc:'outline', hudi:'warning', delta:'secondary', iceberg:'success'}
const mtL: Record<string,string> = {rewrite:'Rewrite',incremental:'Incremental',morph:'Morph',add_files:'Add Files',s3tables_managed:'Managed Migration'}
const PAGE_SIZES = [10, 25, 50, 100]
const MAX_NAME_DISPLAY_LEN = 100
const truncateName = (s: string) => s.length > MAX_NAME_DISPLAY_LEN ? `${s.slice(0, MAX_NAME_DISPLAY_LEN)}…` : s
type SortDir = 'asc' | 'desc' | null
const FACET_FIELDS = [
  { field: 'table', desc: 'Table name', examples: ['table=user_events'] },
  { field: 'database', desc: 'Database name', examples: ['database=analytics'] },
  { field: 'format', desc: 'File format', examples: ['format=parquet'] },
  { field: 'account', desc: 'AWS Account ID', examples: ['account=111222333444'] },
  { field: 'owner', desc: 'Owner group', examples: ['owner=data-engineering'] },
  { field: 'status', desc: 'in_plan / available', examples: ['status=available'] },
]
function parseFacets(query: string) {
  const facets: {field:string;op:'='|':';value:string}[] = []; let ft = ''
  const parts = query.match(/(\w+[=:][^\s]+)|([^\s]+)/g) ?? []
  for (const p of parts) {
    const m = p.match(/^(table|database|format|account|owner|status)([=:])(.+)$/i)
    if (m) facets.push({field:m[1].toLowerCase(),op:m[2] as '='|':',value:m[3].toLowerCase()})
    else ft += (ft?' ':'')+p
  }
  return {facets, freeText:ft.toLowerCase()}
}
function FacetSearchInput({value,onChange,className}:{value:string;onChange:(v:string)=>void;className?:string}) {
  const { t } = useTranslation()
  const [focused,setFocused] = useState(false); const [showA,setShowA] = useState(false)
  const ref = useRef<HTMLDivElement>(null); const iRef = useRef<HTMLInputElement>(null)
  useEffect(()=>{const h=(e:MouseEvent)=>{if(ref.current&&!ref.current.contains(e.target as Node))setShowA(false)};document.addEventListener('mousedown',h);return()=>document.removeEventListener('mousedown',h)},[])
  const shouldShow = focused&&showA&&!value.match(/\w+[=:]$/)
  const ins = (f:string) => {const pre=value&&!value.endsWith(' ')?value+' ':value;onChange(pre+f+'=');setShowA(false);iRef.current?.focus()}
  return (<div className={`relative ${className??''}`} ref={ref}>
    <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground"/>
    <Input ref={iRef} placeholder="Search... (click for facet options)" value={value} onChange={e=>onChange(e.target.value)}
      onFocus={()=>{setFocused(true);setShowA(true)}} onBlur={()=>setFocused(false)} className="pl-9 pr-8"/>
    {value&&<button onClick={()=>onChange('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"><X className="h-4 w-4"/></button>}
    {shouldShow&&<div className="absolute left-0 top-full mt-1 w-full max-w-lg rounded-md border bg-popover shadow-lg z-50 p-2">
      <p className="text-xs text-muted-foreground mb-2 px-2">{t('inventory.labels.facet_help_prefix')} <code className="bg-muted px-1 rounded">=</code> {t('inventory.labels.facet_help_exact')} <code className="bg-muted px-1 rounded">:</code> {t('inventory.labels.facet_help_contains')}</p>
      {FACET_FIELDS.map(f=><button key={f.field} type="button" className="w-full text-left px-3 py-2 rounded-sm text-sm hover:bg-accent cursor-pointer flex items-center justify-between"
        onMouseDown={e=>{e.preventDefault();ins(f.field)}}><div><span className="font-medium font-mono">{f.field}</span><span className="text-muted-foreground ml-2">— {f.desc}</span></div>
        <span className="text-xs text-muted-foreground font-mono">{f.examples[0]}</span></button>)}
    </div>}
  </div>)
}

export function InventoryPage() {
  const { t } = useTranslation()
  const {data:tables,isLoading} = useTables()
  const {data:plans} = usePlans()
  const {data:tablesInPlans} = useTablesInPlans()
  const {data:rules} = useMigrationRules()
  const {data:groups} = useGroups()
  const {data:sources} = useSources()
  const { isAdmin } = useAuth()
  const addTablesBatch = useAddTablesBatch()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [search,setSearch] = useState(''); const [pageSize,setPageSize] = useState(10)
  const [page,setPage] = useState(0); const [selected,setSelected] = useState<Set<string>>(new Set())
  const [migratableOnly,setMigratableOnly] = useState(false)
  const [excludeIceberg,setExcludeIceberg] = useState(false)
  const [sortCol,setSortCol] = useState<string|null>(null); const [sortDir,setSortDir] = useState<SortDir>(null)
  const [addError,setAddError] = useState('')
  // Tree navigation
  const [treeFilter,setTreeFilter] = useState<{source?:string;database?:string}|null>(null)
  const [expandedSources,setExpandedSources] = useState<Set<string>>(new Set())
  const [contextMenu,setContextMenu] = useState<{x:number;y:number;sourceId:string;sourceName:string}|null>(null)
  const refreshSource = useRefreshSource()
  const { showToast } = useToast()
  // Action dialogs
  const [showActions,setShowActions] = useState(false)
  const [dialog,setDialog] = useState<'plan'|'owner'|'delete'|null>(null)
  const [targetPlan,setTargetPlan] = useState('')
  const [targetOwner,setTargetOwner] = useState('')
  const [addingToPlan,setAddingToPlan] = useState(false)
  const [detailPanel,setDetailPanel] = useState<any>(null)
  const actRef = useRef<HTMLDivElement>(null)
  useEffect(()=>{const h=(e:MouseEvent)=>{if(actRef.current&&!actRef.current.contains(e.target as Node))setShowActions(false)};document.addEventListener('mousedown',h);return()=>document.removeEventListener('mousedown',h)},[])

  const inPlanMap: Record<string,{name:string;id:string}> = useMemo(()=>{
    const raw = tablesInPlans ?? {}; const m: Record<string,{name:string;id:string}> = {}
    for (const [k,v] of Object.entries(raw)) { try { m[k]=JSON.parse(v as string) } catch { m[k]={name:v as string,id:''} } }
    return m
  },[tablesInPlans])

  const filtered = useMemo(()=>{
    const {facets,freeText} = parseFacets(search); let list = tables ?? []
    for (const f of facets) {
      list = list.filter(t=>{const key=`${t.database}.${t.table_name}`; let fv=''
        if(f.field==='table')fv=t.table_name.toLowerCase(); else if(f.field==='database')fv=t.database.toLowerCase()
        else if(f.field==='format')fv=t.format.toLowerCase(); else if(f.field==='account')fv=t.aws_account.toLowerCase()
        else if(f.field==='owner')fv=t.owner.toLowerCase()
        else if(f.field==='status')fv=inPlanMap[key]?'in_plan':'available'
        return f.op==='='?fv===f.value:fv.includes(f.value)})}
    if(freeText)list=list.filter(t=>t.table_name.toLowerCase().includes(freeText)||t.database.toLowerCase().includes(freeText)||t.format.toLowerCase().includes(freeText)||t.aws_account.includes(freeText)||t.owner.toLowerCase().includes(freeText))
    if(migratableOnly)list=list.filter(t=>!inPlanMap[`${t.database}.${t.table_name}`])
    if(excludeIceberg)list=list.filter(t=>t.format.toLowerCase()!=='iceberg')
    if(treeFilter?.database)list=list.filter(t=>t.database===treeFilter.database)
    else if(treeFilter?.source){
      const src=sources?.find(s=>s.name===treeFilter.source)
      if(src)list=list.filter(t=>t.aws_account===src.aws_account_id)
    }
    if(sortCol&&sortDir){list=[...list].sort((a,b)=>{const av=(a as any)[sortCol]??'';const bv=(b as any)[sortCol]??'';const c=typeof av==='number'?av-bv:typeof av==='boolean'?(av===bv?0:av?-1:1):String(av).localeCompare(String(bv));return sortDir==='desc'?-c:c})}
    return list
  },[tables,search,migratableOnly,excludeIceberg,treeFilter,sources,sortCol,sortDir,inPlanMap])

  const selectable=filtered.filter(t=>!inPlanMap[`${t.database}.${t.table_name}`])
  const totalPages=Math.max(1,Math.ceil(filtered.length/pageSize))
  const paginated=filtered.slice(page*pageSize,(page+1)*pageSize)
  const yesLabel = t('common.labels.yes')
  const noLabel = t('common.labels.no')
  const allKeys=selectable.map(t=>`${t.database}.${t.table_name}`)
  const allSelected=allKeys.length>0&&allKeys.every(k=>selected.has(k))
  const toggle=(key:string)=>setSelected(p=>{const n=new Set(p);n.has(key)?n.delete(key):n.add(key);return n})
  const toggleAll=()=>allSelected?setSelected(new Set()):setSelected(new Set(allKeys))
  const handleSort=(col:string)=>{if(sortCol===col){setSortDir(sortDir==='asc'?'desc':sortDir==='desc'?null:'asc');if(sortDir==='desc')setSortCol(null)}else{setSortCol(col);setSortDir('asc')}}
  const SI=({col}:{col:string})=>{if(sortCol!==col)return<ArrowUpDown className="h-3 w-3 ml-1 opacity-40"/>;return sortDir==='asc'?<ArrowUp className="h-3 w-3 ml-1"/>:<ArrowDown className="h-3 w-3 ml-1"/>}

  const selectedKeys = () => Array.from(selected).map(k => { const [db,tbl]=k.split('.'); return {database:db,table_name:tbl} })

  const handleAddToPlan = () => {
    if(addingToPlan)return
    if(!targetPlan||selected.size===0)return; setAddError('')
    const plan=plans?.find(p=>p.id===targetPlan); if(!plan)return
    const incompatible:string[]=[]; const icebergRejected:string[]=[]; const notOwned:string[]=[]; const toAdd:{db:string;tbl:string;migType:string}[]=[]
    const planOwners = plan.owners ?? []
    selected.forEach(key=>{const[db,tbl]=key.split('.'); const info=tables?.find(t=>t.database===db&&t.table_name===tbl)
      if(!info)return
      // Table's owner must match one of the plan's owner groups
      if(info.owner && planOwners.length > 0 && !planOwners.includes(info.owner)){
        notOwned.push(`${db}.${tbl} (owner: ${info.owner})`)
        return
      }
      // Iceberg tables can only be added to plans targeting S3 Tables or Iceberg on S3
      if(info.format==='iceberg' && plan.target!=='s3_tables' && plan.target!=='s3_iceberg'){
        icebergRejected.push(`${db}.${tbl}`)
        return
      }
      // Iceberg to S3 Tables/Iceberg-on-S3 is always a rewrite — skip normal validation
      if(info.format==='iceberg' && (plan.target==='s3_tables' || plan.target==='s3_iceberg')){
        toAdd.push({db,tbl,migType:'rewrite'})
        return
      }
      const compatible=getCompatibleTypes(rules,info.format,plan.migration_types,info.partitioned)
      if(compatible.length===0){incompatible.push(`${db}.${tbl} (${info.format})`)} else{toAdd.push({db,tbl,migType:compatible[0]})}})
    const errors:string[]=[]
    if(notOwned.length>0)errors.push(`Table owner does not match plan ownership (${planOwners.join(', ')}): ${notOwned.join(', ')}`)
    if(icebergRejected.length>0)errors.push(`Iceberg tables can only be added to plans targeting "S3 Tables" or "Iceberg on S3": ${icebergRejected.join(', ')}`)
    if(incompatible.length>0)errors.push(`Incompatible format: ${incompatible.join(', ')}`)
    if(errors.length>0)setAddError(errors.join('. '))
    if(toAdd.length>0){
      setAddingToPlan(true)
      const batch = toAdd.map(({db,tbl,migType})=>({
        source_database:db, source_table:tbl, target_database:db,
        target_table: plan.target === 's3_tables' ? tbl : `${TARGET_TABLE_PREFIX}${tbl}`,
        migration_type: migType,
      }))
      addTablesBatch.mutateAsync({planId:targetPlan,tables:batch})
        .then((res)=>{
          if(res?.skipped?.length){
            showToast(`Added ${res.added} table(s); skipped ${res.skipped.length} (${res.skipped[0].reason})`, 'success')
          }
          setSelected(new Set());setTargetPlan('');setDialog(null);navigate({to:`/plans/${targetPlan}`})
        })
        .catch((err:any)=>{setAddError(err?.message||'Failed to add tables to plan')})
        .finally(()=>setAddingToPlan(false))}
  }

  const handleChangeOwner = () => {
    if(!targetOwner||selected.size===0)return
    api.post('/tables/change-owner',{tables:selectedKeys(),owner:targetOwner}).then(()=>{
      queryClient.invalidateQueries({queryKey:['tables']}); setSelected(new Set()); setTargetOwner(''); setDialog(null)})
  }

  const handleDelete = () => {
    if(selected.size===0)return
    api.post('/tables/bulk-delete',{tables:selectedKeys()}).then(()=>{
      queryClient.invalidateQueries({queryKey:['tables']}); queryClient.invalidateQueries({queryKey:['tables-in-plans']})
      setSelected(new Set()); setDialog(null)})
  }

  // Build tree data: source → databases
  const treeData = useMemo(() => {
    if (!sources || !tables) return []
    return sources.map(src => {
      const srcTables = tables.filter(t => t.aws_account === src.aws_account_id)
      const databases = [...new Set(srcTables.map(t => t.database))].sort()
      return { id: src.id, name: src.name, accountId: src.aws_account_id, databases, tableCount: srcTables.length }
    })
  }, [sources, tables])

  const toggleSource = (name: string) => {
    setExpandedSources(prev => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name); else next.add(name)
      return next
    })
  }

  return (
    <div className="flex gap-4">
      {/* Full-screen overlay while adding tables to a plan — blocks interaction
          and signals progress during the (multi-request) add + navigate. */}
      {addingToPlan && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/70 backdrop-blur-sm">
          <div className="flex flex-col items-center gap-3 rounded-lg border bg-background p-8 shadow-lg">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
            <span className="text-sm font-medium">{t('inventory.actions.add_to_plan')}…</span>
            <span className="text-xs text-muted-foreground">{selected.size} table(s)</span>
          </div>
        </div>
      )}
      {/* Left tree panel */}
      <aside className="w-56 shrink-0 border-r pr-4">
        <div className="sticky top-4 space-y-1">
          <button
            onClick={() => { setTreeFilter(null); setPage(0) }}
            className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs font-medium transition-colors cursor-pointer ${
              !treeFilter ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:bg-accent/50'}`}
          >
            <Table2 className="h-3.5 w-3.5" />{t('inventory.labels.all_tables')}
            <span className="ml-auto text-[10px] text-muted-foreground">{tables?.length ?? 0}</span>
          </button>
          {treeData.map(src => (
            <div key={src.name}>
              <button
                onClick={() => { toggleSource(src.name); setTreeFilter({ source: src.name }); setPage(0) }}
                onContextMenu={(e) => { e.preventDefault(); setContextMenu({ x: e.clientX, y: e.clientY, sourceId: src.id, sourceName: src.name }) }}
                className={`flex w-full items-center gap-1.5 rounded-md px-2 py-1.5 text-xs font-medium transition-colors cursor-pointer ${
                  treeFilter?.source === src.name && !treeFilter?.database ? 'bg-accent text-accent-foreground' : 'text-muted-foreground hover:bg-accent/50'}`}
              >
                {expandedSources.has(src.name) ? <ChevronDown className="h-3 w-3 shrink-0" /> : <ChevronRightIcon className="h-3 w-3 shrink-0" />}
                <Server className="h-3 w-3 shrink-0" />
                <span className="truncate">{src.name}</span>
                <span className="ml-auto text-[10px] text-muted-foreground">{src.tableCount}</span>
              </button>
              {expandedSources.has(src.name) && (
                <div className="ml-4 space-y-0.5 mt-0.5">
                  {src.databases.map(db => {
                    const dbCount = (tables ?? []).filter(t => t.database === db && t.aws_account === src.accountId).length
                    return (
                      <button
                        key={db}
                        onClick={() => { setTreeFilter({ source: src.name, database: db }); setPage(0) }}
                        className={`flex w-full items-center gap-1.5 rounded-md px-2 py-1 text-[11px] transition-colors cursor-pointer ${
                          treeFilter?.database === db ? 'bg-accent text-accent-foreground font-medium' : 'text-muted-foreground hover:bg-accent/50'}`}
                      >
                        <Database className="h-3 w-3 shrink-0" />
                        <span className="truncate">{db}</span>
                        <span className="ml-auto text-[10px] text-muted-foreground">{dbCount}</span>
                      </button>
                    )
                  })}
                </div>
              )}
            </div>
          ))}
        </div>
        {/* Context menu for source right-click */}
        {contextMenu && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => setContextMenu(null)} onContextMenu={(e) => { e.preventDefault(); setContextMenu(null) }} />
            <div className="fixed z-50 rounded-md border bg-popover p-1 shadow-md" style={{ left: contextMenu.x, top: contextMenu.y }}>
              <button
                className="w-full text-left px-3 py-1.5 text-xs rounded-sm hover:bg-accent cursor-pointer flex items-center gap-2"
                onClick={() => {
                  const id = contextMenu.sourceId
                  const name = contextMenu.sourceName
                  setContextMenu(null)
                  showToast(`Refreshing ${name}... this might take a few minutes`, 'success')
                  refreshSource.mutate(id, {
                    onSuccess: () => { showToast(`${name} refreshed`, 'success'); queryClient.invalidateQueries({ queryKey: ['tables'] }) },
                    onError: (err: any) => showToast(err?.message || 'Refresh failed', 'error'),
                  })
                }}
              >
                <RefreshCw className="h-3 w-3" />{t('inventory.actions.refresh_source')}
              </button>
            </div>
          </>
        )}
      </aside>

      {/* Right content */}
      <div className="flex-1 min-w-0 space-y-6">
      <div><h2 className="text-xl font-semibold tracking-tight">{t('nav.labels.inventory')}</h2>
        <p className="text-muted-foreground text-sm">{t('inventory.actions.browse_tables')}{treeFilter?.database ? ` — ${treeFilter.database}` : treeFilter?.source ? ` — ${treeFilter.source}` : ''}</p></div>
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div className="flex items-center gap-3 flex-1">
          <FacetSearchInput value={search} onChange={v=>{setSearch(v);setPage(0)}} className="max-w-lg flex-1"/>
          <Button variant={migratableOnly?'default':'outline'} size="sm" onClick={()=>{setMigratableOnly(!migratableOnly);setPage(0)}}>
            <Filter className="h-3 w-3"/>{migratableOnly?t('inventory.actions.show_all'):t('inventory.labels.not_in_plan')}</Button>
          <Button variant={excludeIceberg?'default':'outline'} size="sm" onClick={()=>{setExcludeIceberg(!excludeIceberg);setPage(0)}}>
            <Filter className="h-3 w-3"/>{t('inventory.actions.exclude_iceberg')}</Button>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" size="sm" onClick={toggleAll} disabled={allKeys.length===0}>
            <CheckSquare className="h-3 w-3"/>{allSelected?t('inventory.actions.deselect_all'):t('inventory.actions.select_all')}</Button>
          {selected.size>0&&(
            <>
              <Button size="sm" onClick={()=>setDialog('plan')}>{t('inventory.actions.add_to_plan')} ({selected.size})</Button>
              {isAdmin && <div className="relative" ref={actRef}>
                <Button variant="outline" size="sm" onClick={()=>setShowActions(!showActions)}>
                  {t('common.labels.actions')} <ChevronDown className="h-3 w-3 ml-1"/>
                </Button>
                {showActions&&<div className="absolute right-0 top-full mt-1 w-48 rounded-md border bg-popover p-1 shadow-md z-50">
                  <button className="w-full text-left px-3 py-2 text-sm rounded-sm hover:bg-accent cursor-pointer" onClick={()=>{setShowActions(false);setDialog('owner')}}>{t('inventory.actions.change_ownership')}</button>
                  <button className="w-full text-left px-3 py-2 text-sm rounded-sm hover:bg-accent text-destructive cursor-pointer" onClick={()=>{setShowActions(false);setDialog('delete')}}>{t('common.actions.delete')}</button>
                </div>}
              </div>}
            </>
          )}
        </div>
      </div>
      {/* Add to Plan dialog — card-based selection */}
      {dialog==='plan'&&<div className="fixed inset-0 z-50 flex items-center justify-center"><div className="fixed inset-0 bg-black/50" onClick={()=>{if(!addingToPlan)setDialog(null)}}/>
        <div className="relative z-50 w-full max-w-2xl rounded-lg border bg-background p-6 shadow-lg space-y-4">
          <h3 className="text-base font-semibold">{t('inventory.actions.add_tables_to_plan', { count: selected.size })}</h3>
          <p className="text-sm text-muted-foreground">{t('inventory.actions.select_plan')}</p>
          {plans?.filter(p=>p.status==='draft').length===0 ? (
            <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">{t('inventory.messages.no_draft_plans')}</div>
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 max-h-[320px] overflow-y-auto">
              {plans?.filter(p=>p.status==='draft').map(p=>(
                <button key={p.id} type="button"
                  className={`text-left rounded-lg border p-4 transition-all cursor-pointer hover:border-primary/50 ${targetPlan===p.id ? 'border-primary bg-primary/5 ring-1 ring-primary' : ''}`}
                  onClick={()=>setTargetPlan(p.id)}>
                  <p className="text-sm font-medium">{p.name}</p>
                  <p className="text-xs text-muted-foreground mt-1">{p.tables.length} table(s) · {p.migration_types.join(', ')}</p>
                  <div className="flex gap-1 mt-2">
                    <Badge variant="outline" className="text-[10px] px-1.5 py-0 border-blue-300 text-blue-700 dark:border-blue-700 dark:text-blue-400">{p.target === 's3_tables' ? 'S3 Tables' : 'Iceberg on S3'}</Badge>
                    {p.migration_types.map(mt => <Badge key={mt} variant="outline" className="text-[10px] px-1.5 py-0 border-purple-300 text-purple-700 dark:border-purple-700 dark:text-purple-400">{mtL[mt] ?? mt}</Badge>)}
                  </div>
                </button>
              ))}
            </div>
          )}
          <div className="flex justify-end gap-2 pt-2 border-t">
            {addError && <div className="flex-1 text-xs text-destructive flex items-center gap-1"><AlertTriangle className="h-3 w-3 shrink-0"/>{addError}</div>}
            <Button variant="outline" size="sm" disabled={addingToPlan} onClick={()=>{setDialog(null);setAddError('')}}>{t('common.actions.cancel')}</Button>
            <Button size="sm" disabled={!targetPlan||addingToPlan} onClick={handleAddToPlan}>{addingToPlan && <Loader2 className="h-3 w-3 animate-spin" />}{t('inventory.actions.add_to_plan')}</Button>
          </div>
        </div></div>}

      {/* Change Ownership dialog */}
      {dialog==='owner'&&<div className="fixed inset-0 z-50 flex items-center justify-center"><div className="fixed inset-0 bg-black/50" onClick={()=>setDialog(null)}/>
        <div className="relative z-50 w-full max-w-md rounded-lg border bg-background p-6 shadow-lg space-y-4">
          <h3 className="text-base font-semibold">{t('inventory.actions.change_owner_for', { count: selected.size })}</h3>
          <Select value={targetOwner} onChange={e=>setTargetOwner(e.target.value)}>
            <option value="">{t('users.actions.select_group')}</option>
            {groups?.map(g=><option key={g.name} value={g.name}>{g.name}</option>)}
          </Select>
          <div className="flex justify-end gap-2">
            <Button variant="outline" size="sm" onClick={()=>setDialog(null)}>{t('common.actions.cancel')}</Button>
            <Button size="sm" disabled={!targetOwner} onClick={handleChangeOwner}>{t('inventory.actions.change_owner')}</Button>
          </div>
        </div></div>}

      {/* Delete confirmation */}
      <ConfirmDialog open={dialog==='delete'} title="Delete Tables"
        message={`This will permanently delete ${selected.size} table(s) from the inventory. This action cannot be undone. Do you wish to proceed?`}
        confirmLabel="Delete Permanently" variant="destructive"
        onConfirm={handleDelete} onCancel={()=>setDialog(null)}/>

      {isLoading?(<div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground"/></div>
      ):!filtered.length?(<Card><CardContent className="py-8 text-center text-muted-foreground">{search||migratableOnly||excludeIceberg?'No tables match.':'No tables found.'}</CardContent></Card>
      ):(
        <>
          <div className="rounded-md border overflow-x-auto"><table className="w-full text-sm " role="table">
            <thead><tr className="border-b bg-muted/50">
              <th className="px-4 py-3 w-10 shrink-0"><input type="checkbox" checked={allSelected} onChange={toggleAll} disabled={allKeys.length===0} className="rounded"/></th>
              <th className="px-4 py-3 text-left font-medium cursor-pointer select-none" onClick={()=>handleSort('table_name')}><span className="inline-flex items-center">{t('common.labels.table')}<SI col="table_name"/></span></th>
              <th className="px-4 py-3 text-left font-medium cursor-pointer select-none" onClick={()=>handleSort('database')}><span className="inline-flex items-center">{t('common.labels.database')}<SI col="database"/></span></th>
              <th className="px-4 py-3 text-left font-medium cursor-pointer select-none" onClick={()=>handleSort('format')}><span className="inline-flex items-center">{t('common.labels.format')}<SI col="format"/></span></th>
              <th className="px-4 py-3 text-left font-medium cursor-pointer select-none" onClick={()=>handleSort('aws_account')}><span className="inline-flex items-center">{t('aws.labels.aws_account')}<SI col="aws_account"/></span></th>
              <th className="px-4 py-3 text-left font-medium cursor-pointer select-none" onClick={()=>handleSort('partitioned')}><span className="inline-flex items-center">{t('detail.labels.partitioned')}<SI col="partitioned"/></span></th>
              <th className="px-4 py-3 text-left font-medium cursor-pointer select-none" onClick={()=>handleSort('owner')}><span className="inline-flex items-center">{t('common.labels.owner')}<SI col="owner"/></span></th>
              <th className="px-4 py-3 text-left font-medium">{t('common.labels.status')}</th>
              <th className="px-4 py-3 text-left font-medium w-10"></th>
            </tr></thead>
            <tbody>{paginated.map(t=>{
              const key=`${t.database}.${t.table_name}`;const planInfo=inPlanMap[key];const dis=!!planInfo
              return(<tr key={key} className={`border-b last:border-0 ${dis?'opacity-60':'cursor-pointer'} ${selected.has(key)?'bg-accent/50':''}`} onClick={()=>!dis&&toggle(key)}>
                <td className="px-4 py-3"><input type="checkbox" checked={selected.has(key)} disabled={dis} onChange={()=>{}} className="rounded"/></td>
                <td className="px-4 py-3 font-medium truncate" title={t.table_name}>{truncateName(t.table_name)}</td>
                <td className="px-4 py-3 text-muted-foreground truncate" title={t.database}>{truncateName(t.database)}</td>
                <td className="px-4 py-3"><Badge variant={fmtBadge[t.format]??'outline'}>{t.format}</Badge></td>
                <td className="px-4 py-3 font-mono text-xs text-muted-foreground whitespace-nowrap">{t.aws_account}</td>
                <td className="px-4 py-3 text-xs">{t.partitioned?<Badge variant="secondary">{yesLabel}</Badge>:<span className="text-muted-foreground">{noLabel}</span>}</td>
                <td className="px-4 py-3 text-xs">{t.owner}</td>
                <td className="px-4 py-3 text-xs">{
                  planInfo?<button onClick={e=>{e.stopPropagation();navigate({to:'/plans/$planId',params:{planId:planInfo.id}})}} className="cursor-pointer"><Badge variant="secondary" className="hover:bg-primary hover:text-primary-foreground transition-colors">{planInfo.name}</Badge></button>:null}</td>
                <td className="px-4 py-3"><Button variant="ghost" size="icon" className="h-7 w-7" onClick={e=>{e.stopPropagation();setDetailPanel(t)}} title="View details"><Eye className="h-3.5 w-3.5"/></Button></td>
              </tr>)})}</tbody>
          </table></div>
          <div className="flex items-center justify-between pt-4">
            <div className="flex items-center gap-2"><span className="text-sm text-muted-foreground">{t('common.actions.show')}</span>
              <Select value={String(pageSize)} onChange={e=>{setPageSize(Number(e.target.value));setPage(0)}} className="w-20">
                {PAGE_SIZES.map(n=><option key={n} value={n}>{n}</option>)}</Select>
              <span className="text-sm text-muted-foreground">{t('common.labels.per_page')}</span></div>
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">{filtered.length>0?page*pageSize+1:0}–{Math.min((page+1)*pageSize,filtered.length)} {t('common.labels.of')} {filtered.length}</span>
              <Button variant="outline" size="icon" onClick={()=>setPage(p=>p-1)} disabled={page===0}><ChevronLeft className="h-4 w-4"/></Button>
              <Button variant="outline" size="icon" onClick={()=>setPage(p=>p+1)} disabled={page>=totalPages-1}><ChevronRight className="h-4 w-4"/></Button>
            </div>
          </div>
        </>)}
      {/* Detail Panel */}
      {detailPanel && (
        <><div className="fixed inset-0 z-40" onClick={() => setDetailPanel(null)} />
        <div className="fixed top-14 bottom-0 right-0 z-50 w-[420px] border-l bg-background shadow-xl flex flex-col">
          <div className="flex items-center justify-between p-4 border-b">
            <div>
              <h3 className="text-sm font-semibold">{detailPanel.database}.{detailPanel.table_name}</h3>
              <p className="text-xs text-muted-foreground">{t('inventory.labels.table_details')}</p>
            </div>
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setDetailPanel(null)}><X className="h-4 w-4" /></Button>
          </div>
          <div className="flex-1 overflow-y-auto p-4 space-y-4 text-sm">
            <div className="rounded-md border bg-muted/30 p-3 space-y-2">
              <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.database')}</span><span className="font-mono">{detailPanel.database}</span></div>
              <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.table')}</span><span className="font-mono">{detailPanel.table_name}</span></div>
              <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.format')}</span><Badge variant={fmtBadge[detailPanel.format]??'outline'}>{detailPanel.format}</Badge></div>
              <div className="flex justify-between"><span className="text-muted-foreground">{t('aws.labels.aws_account')}</span><span className="font-mono text-xs">{detailPanel.aws_account}</span></div>
              <div className="flex justify-between"><span className="text-muted-foreground">{t('detail.labels.partitioned')}</span><span>{detailPanel.partitioned ? 'Yes' : 'No'}</span></div>
              <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.owner')}</span><span>{detailPanel.owner || '—'}</span></div>
              <div className="flex justify-between"><span className="text-muted-foreground">{t('common.labels.size')}</span><span>{detailPanel.size_bytes > 0 ? `${(detailPanel.size_bytes / (1024**3)).toFixed(2)} GB` : '—'}</span></div>
            </div>
            <div className="space-y-1">
              <span className="text-xs font-medium text-muted-foreground">{t('inventory.labels.s3_location')}</span>
              <div className="rounded-md border bg-muted/30 p-3 text-xs font-mono break-all">{detailPanel.location || '—'}</div>
            </div>
            {inPlanMap[`${detailPanel.database}.${detailPanel.table_name}`] && (
              <div className="space-y-1">
                <span className="text-xs font-medium text-muted-foreground">{t('inventory.actions.assigned_to_plan')}</span>
                <div className="rounded-md border bg-muted/30 p-3">
                  <button onClick={()=>{const info=inPlanMap[`${detailPanel.database}.${detailPanel.table_name}`];if(info?.id)navigate({to:'/plans/$planId',params:{planId:info.id}})}} className="text-sm text-primary hover:underline cursor-pointer">
                    {inPlanMap[`${detailPanel.database}.${detailPanel.table_name}`]?.name}
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
        </>
      )}
    </div>
    </div>)
}
