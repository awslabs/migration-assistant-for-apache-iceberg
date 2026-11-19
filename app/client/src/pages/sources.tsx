// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/toast'
import { CopyButton } from '@/components/ui/copy-button'
import { useSources, useDeleteSource, useUpdateSource, useRefreshSource } from '@/hooks/use-sources'
import { useGroups } from '@/hooks/use-groups'
import { useAuth } from '@/hooks/use-auth'
import { Link } from '@tanstack/react-router'
import { Plus, Loader2, Trash2, Database, ShieldAlert, Pencil, RefreshCw, Check, X, AlertCircle } from 'lucide-react'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { RefreshLogDialog } from '@/components/ui/refresh-log-dialog'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

const typeLabels: Record<string, string> = { aws_glue_data_catalog: 'AWS Glue Data Catalog' }

export function SourcesPage() {
  const { t } = useTranslation()
  const { isAdmin } = useAuth()
  const { showToast } = useToast()
  const { data: sources, isLoading } = useSources()
  const { data: groups } = useGroups()
  const deleteSource = useDeleteSource()
  const updateSource = useUpdateSource()
  const refreshSource = useRefreshSource()
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [editId, setEditId] = useState<string | null>(null)
  const [editArn, setEditArn] = useState('')
  const [editGroup, setEditGroup] = useState('')
  const [saveMsg, setSaveMsg] = useState('')
  const [refreshMsg, setRefreshMsg] = useState('')
  const [refreshingId, setRefreshingId] = useState<Set<string>>(new Set())
  const [refreshLogSourceId, setRefreshLogSourceId] = useState<string | null>(null)
  const [refreshErrorSourceId, setRefreshErrorSourceId] = useState<string | null>(null)

  // Check for creation message from URL
  const urlParams = typeof window !== 'undefined' ? new URLSearchParams(window.location.search) : null
  const [createMsg, setCreateMsg] = useState(urlParams?.get('created') === 'true'
    ? 'New source added. You will have to manually refresh the source to bring all the databases/tables into the inventory.'
    : '')

  const deletingSource = sources?.find(s => s.id === deleteId)

  if (!isAdmin) return (
    <div className="flex flex-col items-center justify-center py-20 text-center">
      <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
      <h2 className="text-xl font-semibold mb-2">{t('auth.labels.access_denied')}</h2>
      <p className="text-muted-foreground">{t('auth.messages.admin_only_sources')}</p>
    </div>)

  const startEdit = (s: { id: string; iam_role_arn: string; default_group: string }) => {
    setEditId(s.id); setEditArn(s.iam_role_arn); setEditGroup(s.default_group); setSaveMsg('')
  }
  const cancelEdit = () => { setEditId(null); setSaveMsg('') }
  const saveEdit = () => {
    if (!editId) return
    const editedName = sources?.find(s => s.id === editId)?.name ?? 'Source'
    updateSource.mutate({ id: editId, data: { iam_role_arn: editArn, default_group: editGroup } }, {
      onSuccess: () => { setEditId(null); setSaveMsg(''); showToast(`Source "${editedName}" updated. Refresh to apply changes.`, 'success') },
      onError: (err: any) => showToast(err?.message || 'Failed to update source', 'error')
    })
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight">{t('nav.labels.sources')}</h2>
          <p className="text-muted-foreground text-sm">{t('sources.actions.manage_sources')}</p>
        </div>
        <Link to="/sources/new"><Button><Plus className="h-4 w-4" />{t('sources.actions.add_source')}</Button></Link>
      </div>

      {saveMsg && (
        <div className="flex items-center gap-2 rounded-md bg-amber-50 border border-amber-200 p-3 text-sm text-amber-800">
          <AlertCircle className="h-4 w-4 shrink-0" />
          {saveMsg}
          <button onClick={() => setSaveMsg('')} className="ml-auto"><X className="h-4 w-4" /></button>
        </div>
      )}

      {createMsg && (
        <div className="flex items-center gap-2 rounded-md bg-blue-50 border border-blue-200 p-3 text-sm text-blue-800">
          <AlertCircle className="h-4 w-4 shrink-0" />
          {createMsg}
          <button onClick={() => setCreateMsg('')} className="ml-auto"><X className="h-4 w-4" /></button>
        </div>
      )}

      {refreshMsg && (
        <div className="flex items-center gap-2 rounded-md bg-emerald-50 border border-emerald-200 p-3 text-sm text-emerald-800">
          <AlertCircle className="h-4 w-4 shrink-0" />
          {refreshMsg}
          {refreshErrorSourceId && (
            <button
              className="underline font-medium shrink-0"
              onClick={() => setRefreshLogSourceId(refreshErrorSourceId)}
            >
              {t('sources.actions.view_refresh_log')}
            </button>
          )}
          <button onClick={() => { setRefreshMsg(''); setRefreshErrorSourceId(null) }} className="ml-auto"><X className="h-4 w-4" /></button>
        </div>
      )}

      {isLoading ? (
        <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
      ) : !sources?.length ? (
        <Card><CardContent className="flex flex-col items-center justify-center py-12 text-center">
          <Database className="h-10 w-10 text-muted-foreground mb-2" />
          <p className="text-muted-foreground">{t('sources.messages.no_sources')}</p>
          <Link to="/sources/new"><Button variant="outline" className="mt-4"><Plus className="h-4 w-4" />{t('sources.actions.add_first_source')}</Button></Link>
        </CardContent></Card>
      ) : (
        <div className="rounded-md border">
          <table className="w-full text-sm">
            <thead><tr className="border-b bg-muted/50">
              <th className="px-4 py-3 text-left font-medium">{t('common.labels.name')}</th>
              <th className="px-4 py-3 text-left font-medium">{t('common.labels.type')}</th>
              <th className="px-4 py-3 text-left font-medium">{t('aws.labels.aws_account')}</th>
              <th className="px-4 py-3 text-left font-medium">{t('aws.labels.iam_role')}</th>
              <th className="px-4 py-3 text-left font-medium">{t('sources.labels.default_ownership')}</th>
              <th className="px-4 py-3 text-left font-medium">{t('sources.labels.last_refreshed')}</th>
              <th className="px-4 py-3 text-right font-medium">{t('common.labels.actions')}</th>
            </tr></thead>
            <tbody>{sources.map(s => {
              const isEditing = editId === s.id
              return (
                <tr key={s.id} className="border-b last:border-0">
                  <td className="px-4 py-3 font-medium">{s.name}</td>
                  <td className="px-4 py-3"><Badge variant="outline">{typeLabels[s.source_type] ?? s.source_type}</Badge></td>
                  <td className="px-4 py-3 font-mono text-xs text-muted-foreground"><span className="inline-flex items-center gap-1">{s.aws_account_id}<CopyButton value={s.aws_account_id} label="account ID" /></span></td>
                  <td className="px-4 py-3">
                    {isEditing ? (
                      <Input value={editArn} onChange={e => setEditArn(e.target.value)} placeholder="arn:aws:iam::..." className="h-7 text-xs font-mono w-56" />
                    ) : (
                      <span className="inline-flex items-center gap-1 text-xs text-muted-foreground font-mono">{s.iam_role_arn || t('common.labels.default')}{s.iam_role_arn && <CopyButton value={s.iam_role_arn} label="role ARN" />}</span>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    {isEditing ? (
                      <select value={editGroup} onChange={e => setEditGroup(e.target.value)}
                        className="h-7 rounded-md border border-input bg-transparent px-2 text-xs">
                        <option value="">{t('common.labels.none_type')}</option>
                        {groups?.map(g => <option key={g.name} value={g.name}>{g.name}</option>)}
                      </select>
                    ) : (
                      s.default_group ? <Badge variant="secondary">{s.default_group}</Badge> : <span className="text-muted-foreground">—</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">
                    {refreshingId.has(s.id) ? <span className="text-amber-600 animate-pulse">{t('common.loading.refreshing')}</span> : s.last_refreshed_at ? new Date(s.last_refreshed_at).toLocaleString() : <span className="text-amber-600">{t('common.labels.never')}</span>}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <div className="flex items-center justify-end gap-1">
                      {isEditing ? (
                        <>
                          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={saveEdit} disabled={updateSource.isPending}>
                            <Check className="h-3.5 w-3.5 text-emerald-600" />
                          </Button>
                          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={cancelEdit}>
                            <X className="h-3.5 w-3.5 text-destructive" />
                          </Button>
                        </>
                      ) : (
                        <>
                          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => startEdit(s)} title="Edit">
                            <Pencil className="h-3.5 w-3.5" />
                          </Button>
                          <Button variant="ghost" size="icon" className="h-7 w-7"
                            onClick={() => { setRefreshingId(prev => new Set(prev).add(s.id)); refreshSource.mutate(s.id, {
                              onSuccess: (data: any) => {
                                setRefreshingId(prev => { const n = new Set(prev); n.delete(s.id); return n })
                                const removed = data.tables_removed ?? 0
                                const orphaned = data.tables_orphaned ?? 0
                                const removedToast = removed > 0 ? `, ${removed} removed` : ''
                                showToast(`Refreshed "${s.name}": ${data.tables_added} added, ${data.tables_updated} updated${removedToast}`, 'success')
                                setRefreshErrorSourceId(data.errors > 0 ? s.id : null)
                                const removedMsg = removed > 0 ? ` ${removed} table(s) removed (deleted from source).` : ''
                                const orphanedMsg = orphaned > 0
                                  ? ` ${orphaned} table(s) deleted from source but kept because they are used by a plan: ${(data.orphaned_tables ?? []).join(', ')}.`
                                  : ''
                                setRefreshMsg(`Refreshed "${s.name}": ${data.databases_scanned} database(s) scanned, ${data.tables_added} table(s) added, ${data.tables_updated} table(s) updated.${removedMsg}${orphanedMsg}${data.errors > 0 ? ` ${data.errors} error(s) logged.` : ''}`)
                              },
                              onError: (err: any) => { setRefreshingId(prev => { const n = new Set(prev); n.delete(s.id); return n }); showToast(`Refresh failed for "${s.name}"`, 'error'); setRefreshErrorSourceId(null); setRefreshMsg(`Refresh failed for "${s.name}": ${err.message}`) }
                            }) }} disabled={refreshingId.has(s.id)} title="Refresh">
                            <RefreshCw className={`h-3.5 w-3.5 ${refreshingId.has(s.id) ? 'animate-spin' : ''}`} />
                          </Button>
                          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setRefreshLogSourceId(s.id)} title="View refresh log">
                            <AlertCircle className="h-3.5 w-3.5" />
                          </Button>
                          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setDeleteId(s.id)} title="Delete">
                            <Trash2 className="h-3.5 w-3.5" />
                          </Button>
                        </>
                      )}
                    </div>
                  </td>
                </tr>
              )
            })}</tbody>
          </table>
        </div>
      )}

      <ConfirmDialog
        open={!!deleteId}
        title="Delete Source"
        message={`Are you sure you want to delete "${deletingSource?.name}"? This will not remove any tables already imported from this source.`}
        confirmLabel="Delete Source"
        variant="destructive"
        onConfirm={() => { if (deleteId) { const deletedName = deletingSource?.name ?? 'Source'; deleteSource.mutate(deleteId, { onSuccess: () => { setDeleteId(null); showToast(`Source "${deletedName}" deleted`, 'success') }, onError: (err: any) => showToast(err?.message || 'Failed to delete source', 'error') }) } }}
        onCancel={() => setDeleteId(null)}
      />

      <RefreshLogDialog
        sourceId={refreshLogSourceId}
        sourceName={sources?.find(s => s.id === refreshLogSourceId)?.name}
        lastRefreshSummary={sources?.find(s => s.id === refreshLogSourceId)?.last_refresh_summary}
        lastRefreshedAt={sources?.find(s => s.id === refreshLogSourceId)?.last_refreshed_at}
        onClose={() => setRefreshLogSourceId(null)}
      />
    </div>
  )
}
