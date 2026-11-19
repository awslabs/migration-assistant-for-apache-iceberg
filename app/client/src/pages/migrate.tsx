// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { usePlans } from '@/hooks/use-plans'
import { Loader2, ArrowRightLeft, Search, X, ChevronLeft, ChevronRight } from 'lucide-react'
import { useNavigate } from '@tanstack/react-router'
import { useState, useMemo } from 'react'
import type { PlanStatus } from '@/types'
import { useTranslation } from 'react-i18next'

const statusV: Record<PlanStatus, 'default'|'secondary'|'success'|'destructive'|'warning'> = {
  draft:'secondary',ready:'default',staged:'default',in_progress:'warning',paused:'secondary',cancelled:'secondary',completed:'success',failed:'destructive'}
const mtL: Record<string,string> = {rewrite:'Rewrite',incremental:'Incremental',morph:'Morph',add_files:'Add Files',s3tables_managed:'Managed Migration'}
const targetL: Record<string,string> = {s3_tables:'S3 Tables',s3_iceberg:'Iceberg on S3'}
const PAGE_SIZE = 10

export function MigratePage() {
  const { t } = useTranslation()
  const { data: plans, isLoading } = usePlans()
  const navigate = useNavigate()
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const migrations = useMemo(() => {
    const list = plans?.filter(p => ['staged','in_progress','paused','cancelled','completed','failed'].includes(p.status)) ?? []
    const filtered = search ? list.filter(p => p.name.toLowerCase().includes(search.toLowerCase())) : list
    return filtered.sort((a, b) => {
      const order: Record<string, number> = { in_progress: 0, staged: 1, paused: 2, failed: 3, completed: 4, cancelled: 5 }
      const oa = order[a.status] ?? 3
      const ob = order[b.status] ?? 3
      if (oa !== ob) return oa - ob
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
    })
  }, [plans, search])
  const totalPages = Math.max(1, Math.ceil(migrations.length / PAGE_SIZE))
  const paginated = migrations.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  return (
    <div className="space-y-6">
      <div><h2 className="text-xl font-semibold tracking-tight">{t('nav.labels.migrations')}</h2>
        <p className="text-muted-foreground text-sm">{t('plans.actions.track_migrations')}</p></div>
      <div className="relative max-w-sm">
        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input placeholder="Search migrations..." value={search} onChange={e => { setSearch(e.target.value); setPage(0) }} className="pl-9 pr-8" />
        {search && <button onClick={() => setSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"><X className="h-4 w-4" /></button>}
      </div>
      {isLoading ? (
        <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
      ) : !migrations.length ? (
        <Card><CardContent className="flex flex-col items-center justify-center py-12 text-center">
          <ArrowRightLeft className="h-10 w-10 text-muted-foreground mb-2" />
          <p className="text-muted-foreground">{search ? t('migrations.messages.no_migrations_match') : t('dashboard.messages.no_migrations_yet')}</p>
        </CardContent></Card>
      ) : (
        <div className="space-y-3">
          {paginated.map(p => (
            <Card key={p.id} className="cursor-pointer hover:border-primary/50 transition-all"
              onClick={() => navigate({ to: '/migrations/$migrationId', params: { migrationId: p.id } })}>
              <CardContent className="py-4 space-y-2">
                <div className="flex items-center justify-between">
                  <p className="text-sm font-medium">{p.name}</p>
                  <Badge variant={statusV[p.status]}>{p.status.replace('_',' ')}</Badge>
                </div>
                {p.description && <p className="text-xs text-muted-foreground line-clamp-2">{p.description}</p>}
                <div className="flex flex-wrap gap-1.5">
                  <Badge variant="outline" className="border-blue-300 text-blue-700 dark:border-blue-700 dark:text-blue-400">{targetL[p.target] ?? p.target}</Badge>
                  {p.migration_types.map(mt => <Badge key={mt} variant="outline" className="border-purple-300 text-purple-700 dark:border-purple-700 dark:text-purple-400">{mtL[mt] ?? mt}</Badge>)}
                </div>
                <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                  <span>{p.tables.length} table(s)</span>
                  <span>{t('common.labels.created')} {new Date(p.created_at).toLocaleDateString()} by {p.created_by}</span>
                  {p.owners.length > 0 && <span>{t('common.labels.owners')} {p.owners.join(', ')}</span>}
                </div>
              </CardContent>
            </Card>
          ))}
          {totalPages > 1 && (
            <div className="flex items-center justify-end gap-2 text-sm text-muted-foreground">
              {t('common.labels.page')} {page + 1}/{totalPages}
              <Button variant="outline" size="icon" onClick={() => setPage(p => p - 1)} disabled={page === 0}><ChevronLeft className="h-4 w-4" /></Button>
              <Button variant="outline" size="icon" onClick={() => setPage(p => p + 1)} disabled={page >= totalPages - 1}><ChevronRight className="h-4 w-4" /></Button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
