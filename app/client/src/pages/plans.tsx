// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { usePlans } from '@/hooks/use-plans'
import { useAuth } from '@/hooks/use-auth'
import { useMemo } from 'react'
import { Link, useNavigate } from '@tanstack/react-router'
import { Plus, Loader2, ClipboardList, X, Search, ChevronLeft, ChevronRight } from 'lucide-react'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

const mtL: Record<string,string> = {rewrite:'Rewrite',incremental:'Incremental',morph:'Morph',add_files:'Add Files',s3tables_managed:'Managed Migration'}
const targetL: Record<string,string> = {s3_tables:'S3 Tables',s3_iceberg:'Iceberg on S3'}
const PAGE_SIZE = 10

export function PlansPage() {
  const { t } = useTranslation()
  const { data: plans, isLoading } = usePlans()
  const { user } = useAuth()
  const navigate = useNavigate()
  const [planSearch, setPlanSearch] = useState('')
  const [page, setPage] = useState(0)

  const allPlans = useMemo(() => {
    const list = plans?.filter(p => p.status === 'draft' || p.status === 'ready') ?? []
    return planSearch ? list.filter(p => p.name.toLowerCase().includes(planSearch.toLowerCase())) : list
  }, [plans, planSearch])
  const totalPages = Math.max(1, Math.ceil(allPlans.length / PAGE_SIZE))
  const paginated = allPlans.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight">{t('plans.labels.migration_plans')}</h2>
          <p className="text-muted-foreground text-sm">{t('plans.actions.create_plans_desc')}</p>
        </div>
        <Link to="/plans/new">{user?.role !== 'viewer' && <Button><Plus className="h-4 w-4" />{t('plans.actions.new_plan')}</Button>}</Link>
      </div>

      <div className="relative max-w-sm">
        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input placeholder="Search plans..." value={planSearch} onChange={e => { setPlanSearch(e.target.value); setPage(0) }} className="pl-9 pr-8" />
        {planSearch && <button onClick={() => setPlanSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"><X className="h-4 w-4" /></button>}
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
      ) : !allPlans.length ? (
        <Card><CardContent className="flex flex-col items-center justify-center py-12 text-center">
          <ClipboardList className="h-10 w-10 text-muted-foreground mb-2" />
          <p className="text-muted-foreground">{planSearch ? t('plans.messages.no_plans_match') : t('plans.messages.no_plans_yet')}</p>
        </CardContent></Card>
      ) : (
        <div className="space-y-3">
          {paginated.map(p => (
            <Card key={p.id} className="cursor-pointer hover:border-primary/50 transition-all"
              onClick={() => navigate({ to: '/plans/$planId', params: { planId: p.id } })}>
              <CardContent className="py-4 space-y-2">
                <div className="flex items-center justify-between">
                  <p className="text-sm font-medium">{p.name}</p>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className="border-blue-300 text-blue-700 dark:border-blue-700 dark:text-blue-400">{targetL[p.target] ?? p.target}</Badge>
                    {p.migration_types.map(mt => <Badge key={mt} variant="outline" className="border-purple-300 text-purple-700 dark:border-purple-700 dark:text-purple-400">{mtL[mt] ?? mt}</Badge>)}
                  </div>
                </div>
                {p.description && <p className="text-xs text-muted-foreground line-clamp-2">{p.description}</p>}
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
