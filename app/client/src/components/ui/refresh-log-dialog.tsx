// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Loader2, X, AlertCircle, CheckCircle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useRefreshLog } from '@/hooks/use-sources'
import { useTranslation } from 'react-i18next'

interface RefreshLogDialogProps {
  sourceId: string | null
  sourceName?: string
  lastRefreshSummary?: string | null
  lastRefreshedAt?: string | null
  onClose: () => void
}

export function RefreshLogDialog({ sourceId, sourceName, lastRefreshSummary, lastRefreshedAt, onClose }: RefreshLogDialogProps) {
  const { t } = useTranslation()
  const { data: entries, isLoading } = useRefreshLog(sourceId)

  if (!sourceId) return null

  const summary = lastRefreshSummary ? JSON.parse(lastRefreshSummary) : null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/50" onClick={onClose} />
      <div className="relative z-50 w-full max-w-2xl max-h-[80vh] flex flex-col rounded-lg border bg-background p-6 shadow-lg">
        <div className="flex items-start justify-between gap-4 mb-4">
          <div>
            <h3 className="text-base font-semibold">{t('sources.actions.refresh_log')}{sourceName ? ` — ${sourceName}` : ''}</h3>
            <p className="mt-1 text-sm text-muted-foreground">{t('sources.descriptions.last_refresh_summary')}</p>
          </div>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onClose}>
            <X className="h-3.5 w-3.5" />
          </Button>
        </div>

        {/* Last refresh summary */}
        {summary && (
          <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1 mb-4">
            <div className="flex items-center gap-1.5 mb-2 text-sm font-medium">
              <CheckCircle className="h-3.5 w-3.5 text-emerald-600" />
              <span>{t('sources.labels.last_refresh')}{lastRefreshedAt ? ` — ${new Date(lastRefreshedAt).toLocaleString()}` : ''}</span>
            </div>
            <div className="grid grid-cols-3 gap-2">
              <div><span className="text-muted-foreground">{t('sources.labels.databases_scanned')}</span> {summary.databases_scanned}</div>
              <div><span className="text-muted-foreground">{t('sources.labels.tables_added')}</span> {summary.tables_added}</div>
              <div><span className="text-muted-foreground">{t('sources.labels.tables_updated')}</span> {summary.tables_updated}</div>
              <div><span className="text-muted-foreground">{t('sources.labels.tables_removed')}</span> {summary.tables_removed ?? 0}</div>
              <div><span className="text-muted-foreground">{t('sources.labels.tables_orphaned')}</span> {summary.tables_orphaned ?? 0}</div>
              <div><span className="text-muted-foreground">{t('common.labels.errors')}:</span> <span className={summary.errors > 0 ? 'text-destructive font-medium' : ''}>{summary.errors}</span></div>
            </div>
          </div>
        )}

        <div className="flex-1 overflow-y-auto">
          {isLoading ? (
            <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>
          ) : !entries?.length ? (
            <div className="flex flex-col items-center justify-center py-12 text-center text-muted-foreground">
              <AlertCircle className="h-8 w-8 mb-2" />
              <p>{t('sources.messages.no_refresh_errors')}</p>
            </div>
          ) : (
            <div className="space-y-3">
              <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{t('common.labels.errors')}</h4>
              {entries.map(e => (
                <div key={e.id} className="rounded-md border p-3 text-sm">
                  <div className="flex items-center justify-between gap-2 mb-1">
                    <span className="font-medium">{e.error_type}</span>
                    <span className="text-xs text-muted-foreground">{new Date(e.created_at).toLocaleString()}</span>
                  </div>
                  {(e.database_name || e.table_name) && (
                    <div className="text-xs text-muted-foreground mb-1">
                      {e.database_name}{e.table_name ? `.${e.table_name}` : ''}
                    </div>
                  )}
                  <div className="text-xs font-mono text-destructive whitespace-pre-wrap break-words">{e.error_message}</div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="mt-4 flex justify-end">
          <Button variant="outline" size="sm" onClick={onClose}>{t('common.actions.close')}</Button>
        </div>
      </div>
    </div>
  )
}
