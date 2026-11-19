// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { useTables } from '@/hooks/use-migrations'
import { usePlans } from '@/hooks/use-plans'
import { useSources } from '@/hooks/use-sources'
import { ArrowRightLeft, CheckCircle, Loader2, ClipboardList, Package, TrendingUp, Plus, X, Info } from 'lucide-react'
import { Link } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import type { PlanStatus } from '@/types'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

const planStatusVariant: Record<PlanStatus, 'default' | 'secondary' | 'success' | 'destructive' | 'warning'> = {
  draft: 'secondary', ready: 'default', staged: 'default', in_progress: 'warning', paused: 'secondary', cancelled: 'secondary', completed: 'success', failed: 'destructive',
}
const mtL: Record<string, string> = { rewrite: 'Rewrite', incremental: 'Incremental', morph: 'Morph', add_files: 'Add Files', s3tables_managed: 'Managed Migration' }
const targetL: Record<string, string> = { s3_tables: 'S3 Tables', s3_iceberg: 'Iceberg on S3' }

export function DashboardPage() {
  const { t } = useTranslation()
  const { data: plans, isLoading: plansLoading } = usePlans()
  const { data: tables } = useTables()
  const { data: sources } = useSources()
  const [bannerDismissed, setBannerDismissed] = useState(false)

  const totalTables = tables?.length ?? 0
  const icebergTables = tables?.filter(t => t.format === 'iceberg').length ?? 0
  const nonIcebergTables = totalTables - icebergTables
  const migrationPct = totalTables > 0 ? Math.round((icebergTables / totalTables) * 100) : 0

  const activePlans = plans?.filter(p => ['draft', 'ready'].includes(p.status)) ?? []
  const activeMigrations = (plans?.filter(p => ['staged', 'in_progress', 'paused', 'completed', 'cancelled', 'failed'].includes(p.status)) ?? [])
    .sort((a, b) => {
      // in_progress first, then completed (desc by date), then rest
      const order: Record<string, number> = { in_progress: 0, staged: 1, paused: 2, failed: 3, completed: 4, cancelled: 5 }
      const oa = order[a.status] ?? 3
      const ob = order[b.status] ?? 3
      if (oa !== ob) return oa - ob
      // Within same status group, most recent first
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
    })
  const inProgressMigrations = activeMigrations.filter(m => m.status === 'in_progress')
  const completedMigrations = activeMigrations.filter(m => m.status === 'completed')

  return (
    <div className="space-y-8">
      {!bannerDismissed && sources && sources.length === 0 && (
        <div className="flex items-center justify-between gap-3 rounded-lg border border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-950 px-4 py-3">
          <div className="flex items-center gap-2 text-sm text-blue-800 dark:text-blue-200">
            <Info className="h-4 w-4 shrink-0" />
            <span>{t('dashboard.descriptions.source_banner')}</span>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <Link to="/sources/new"><Button size="sm" variant="default">{t('sources.actions.create_source')}</Button></Link>
            <button onClick={() => setBannerDismissed(true)} className="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-200"><X className="h-4 w-4" /></button>
          </div>
        </div>
      )}
      <div>
        <h2 className="text-xl font-semibold tracking-tight">{t('nav.labels.dashboard')}</h2>
        <p className="text-muted-foreground">{t('dashboard.descriptions.overview')}</p>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-5">
        <Link to="/inventory" className="block">
          <Card className="hover:border-primary/50 transition-colors cursor-pointer h-full">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">{t('inventory.labels.non_iceberg_tables')}</CardTitle>
              <Package className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
            </CardHeader>
            <CardContent><div className="text-2xl font-bold">{nonIcebergTables}</div>
              <p className="text-xs text-muted-foreground">{t('dashboard.labels.of_total', { count: totalTables })}</p></CardContent>
          </Card>
        </Link>
        <Link to="/inventory" className="block">
          <Card className="hover:border-primary/50 transition-colors cursor-pointer h-full">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">{t('dashboard.labels.migration_progress')}</CardTitle>
              <TrendingUp className="h-4 w-4 text-emerald-600" aria-hidden="true" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-emerald-600">{migrationPct}%</div>
              <div className="mt-2 h-2 w-full rounded-full bg-secondary">
                <div className="h-2 rounded-full bg-emerald-600 transition-all" style={{ width: `${migrationPct}%` }} />
              </div>
            </CardContent>
          </Card>
        </Link>
        <Link to="/plans" className="block">
          <Card className="hover:border-primary/50 transition-colors cursor-pointer h-full">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">{t('nav.labels.plans')}</CardTitle>
              <ClipboardList className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
            </CardHeader>
            <CardContent><div className="text-2xl font-bold">{activePlans.length}</div>
              <p className="text-xs text-muted-foreground">{activePlans.filter(p => p.status === 'draft').length} draft</p></CardContent>
          </Card>
        </Link>
        <Link to="/migrations" className="block">
          <Card className="hover:border-primary/50 transition-colors cursor-pointer h-full">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">{t('dashboard.labels.active_migrations')}</CardTitle>
              <ArrowRightLeft className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
            </CardHeader>
            <CardContent><div className="text-2xl font-bold">{inProgressMigrations.length}</div>
              <p className="text-xs text-muted-foreground">{activeMigrations.length} total</p></CardContent>
          </Card>
        </Link>
        <Link to="/migrations" className="block">
          <Card className="hover:border-primary/50 transition-colors cursor-pointer h-full">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <CardTitle className="text-sm font-medium">{t('common.labels.completed')}</CardTitle>
              <CheckCircle className="h-4 w-4 text-emerald-600" aria-hidden="true" />
            </CardHeader>
            <CardContent><div className="text-2xl font-bold text-emerald-600">{completedMigrations.length}</div></CardContent>
          </Card>
        </Link>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div><CardTitle>{t('nav.labels.plans')}</CardTitle><CardDescription>{t('dashboard.labels.plans_in_progress')}</CardDescription></div>
              <div className="flex gap-2">
                <Link to="/plans/new"><Button size="sm"><Plus className="h-3 w-3" />{t('plans.actions.new_plan')}</Button></Link>
                <Link to="/plans"><Button variant="outline" size="sm">{t('common.actions.view_all')}</Button></Link>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {plansLoading ? (
              <div className="flex items-center justify-center py-8"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
            ) : !activePlans.length ? (
              <div className="flex flex-col items-center justify-center py-8 text-center">
                <ClipboardList className="h-10 w-10 text-muted-foreground mb-2" aria-hidden="true" />
                <p className="text-muted-foreground">{t('dashboard.messages.no_active_plans')}</p>
              </div>
            ) : (
              <div className="space-y-3">
                {activePlans.slice(0, 10).map(plan => (
                  <Link key={plan.id} to="/plans/$planId" params={{ planId: plan.id }} className="block">
                    <div className="flex items-center justify-between rounded-lg border p-3 hover:border-primary/50 transition-colors">
                      <div className="space-y-1">
                        <p className="text-sm font-medium">{plan.name}</p>
                        <div className="flex items-center gap-2 text-xs text-muted-foreground">
                          <span>{plan.tables.length} table(s)</span>
                          <span>·</span>
                          <span>{new Date(plan.created_at).toLocaleDateString()}</span>
                          <span>·</span>
                          <Badge variant="outline" className="text-[10px] px-1.5 py-0 border-blue-300 text-blue-700 dark:border-blue-700 dark:text-blue-400">{targetL[plan.target] ?? plan.target}</Badge>
                          {plan.migration_types.map(mt => <Badge key={mt} variant="outline" className="text-[10px] px-1.5 py-0 border-purple-300 text-purple-700 dark:border-purple-700 dark:text-purple-400">{mtL[mt] ?? mt}</Badge>)}
                        </div>
                      </div>
                      <Badge variant={planStatusVariant[plan.status]}>{plan.status}</Badge>
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div><CardTitle>{t('nav.labels.migrations')}</CardTitle><CardDescription>{t('dashboard.labels.active_and_recent')}</CardDescription></div>
              <Link to="/migrations"><Button variant="outline" size="sm">{t('common.actions.view_all')}</Button></Link>
            </div>
          </CardHeader>
          <CardContent>
            {plansLoading ? (
              <div className="flex items-center justify-center py-8"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
            ) : !activeMigrations.length ? (
              <div className="flex flex-col items-center justify-center py-8 text-center">
                <ArrowRightLeft className="h-10 w-10 text-muted-foreground mb-2" aria-hidden="true" />
                <p className="text-muted-foreground">{t('dashboard.messages.no_migrations_yet')}</p>
              </div>
            ) : (
              <div className="space-y-3">
                {activeMigrations.slice(0, 10).map(m => {
                  const tablesCompleted = m.tables.filter(t => t.status === 'completed').length
                  const tablesFailed = m.tables.filter(t => t.status === 'failed').length
                  return (
                    <Link key={m.id} to="/migrations/$migrationId" params={{ migrationId: m.id }} className="block">
                      <div className="flex items-center justify-between rounded-lg border p-3 hover:border-primary/50 transition-colors">
                        <div className="space-y-1">
                          <p className="text-sm font-medium">{m.name}</p>
                          <div className="flex items-center gap-2 text-xs text-muted-foreground">
                            <span>{m.tables.length} table(s)</span>
                            {tablesCompleted > 0 && <span className="text-emerald-600">{tablesCompleted} done</span>}
                            {tablesFailed > 0 && <span className="text-destructive">{tablesFailed} failed</span>}
                            <span>·</span>
                            <Badge variant="outline" className="text-[10px] px-1.5 py-0 border-blue-300 text-blue-700 dark:border-blue-700 dark:text-blue-400">{targetL[m.target] ?? m.target}</Badge>
                            {m.migration_types.map(mt => <Badge key={mt} variant="outline" className="text-[10px] px-1.5 py-0 border-purple-300 text-purple-700 dark:border-purple-700 dark:text-purple-400">{mtL[mt] ?? mt}</Badge>)}
                          </div>
                          {m.status === 'completed' && <p className="text-[10px] text-muted-foreground">{t('dashboard.labels.completed_on')} {new Date(m.created_at).toLocaleDateString()}</p>}
                        </div>
                        <Badge variant={planStatusVariant[m.status]}>{m.status.replace('_', ' ')}</Badge>
                      </div>
                    </Link>
                  )
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
