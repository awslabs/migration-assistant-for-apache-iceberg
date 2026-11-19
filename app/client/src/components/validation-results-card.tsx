// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useTranslation } from 'react-i18next'
import { Loader2, ShieldCheck, Download } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { usePipelineValidation } from '@/hooks/use-data-validation'
import { downloadValidationReport } from '@/lib/validation-report'
import type {
  ValidationCheck,
  ValidationResult,
  ValidationOverallStatus,
  ValidationCheckStatus,
} from '@/types'

const overallBadge: Record<ValidationOverallStatus, 'success' | 'destructive' | 'warning' | 'secondary'> = {
  passed: 'success',
  failed: 'destructive',
  error: 'destructive',
  running: 'warning',
}

const checkBadge: Record<ValidationCheckStatus, 'success' | 'destructive' | 'secondary'> = {
  passed: 'success',
  failed: 'destructive',
  error: 'destructive',
  skipped: 'secondary',
}

function hasRun(v: unknown): v is { run_id: string } {
  return !!v && typeof v === 'object' && 'run_id' in v
}

interface ValidationResultsCardProps {
  pipelineId: string
  // Optional context to enrich the downloadable report header.
  sourceDatabase?: string
  sourceTable?: string
  targetDatabase?: string
  targetTable?: string
  migrationType?: string
}

export function ValidationResultsCard({
  pipelineId,
  sourceDatabase,
  sourceTable,
  targetDatabase,
  targetTable,
  migrationType,
}: ValidationResultsCardProps) {
  const { t } = useTranslation()
  const { data, isLoading } = usePipelineValidation(pipelineId)

  // No validation has ever run for this pipeline — render nothing (the
  // "Validate" action is how a user starts one).
  if (isLoading) {
    return (
      <div className="rounded-md border bg-muted/30 p-3 text-xs flex items-center gap-2 text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        {t('validation.descriptions.loading')}
      </div>
    )
  }
  if (!hasRun(data)) return null

  const run = data as unknown as {
    overall_status: ValidationOverallStatus
    tier: string
    run_at: string
    completed_at: string | null
    results: ValidationCheck[]
    triggered_by: string
  }

  return (
    <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-2">
      <div className="flex items-center justify-between">
        <span className="flex items-center gap-1.5 font-medium">
          <ShieldCheck className="h-3.5 w-3.5 text-primary" />
          {t('validation.labels.validation')}
        </span>
        <span className="flex items-center gap-1.5">
          {run.overall_status === 'running' && (
            <Loader2 className="h-3 w-3 animate-spin text-amber-500" />
          )}
          <Badge variant={overallBadge[run.overall_status] ?? 'secondary'}>
            {run.overall_status}
          </Badge>
          {run.overall_status !== 'running' && run.results?.length > 0 && (
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              title="Download full report (.html)"
              onClick={() =>
                downloadValidationReport(data as ValidationResult, {
                  pipelineId,
                  sourceDatabase,
                  sourceTable,
                  targetDatabase,
                  targetTable,
                  migrationType,
                })
              }
            >
              <Download className="h-3.5 w-3.5" />
            </Button>
          )}
        </span>
      </div>

      <div className="flex justify-between text-muted-foreground">
        <span>{t('validation.labels.tier')}</span>
        <span className="capitalize">{run.tier}</span>
      </div>
      <div className="flex justify-between text-muted-foreground">
        <span>{t('validation.labels.last_run')}</span>
        <span>
          {run.completed_at
            ? new Date(run.completed_at).toLocaleString()
            : new Date(run.run_at).toLocaleString()}
        </span>
      </div>
      {run.triggered_by && (
        <div className="flex justify-between text-muted-foreground">
          <span>{t('validation.labels.by')}</span>
          <span>{run.triggered_by}</span>
        </div>
      )}

      {run.results?.length > 0 && (
        <div className="pt-1 space-y-1 border-t">
          {run.results.map((c) => (
            <div key={c.check_id} className="flex items-start justify-between gap-2 pt-1">
              <span className="flex items-center gap-1.5 min-w-0">
                <span className="font-mono text-muted-foreground shrink-0">
                  {c.check_id}
                </span>
                <span className="truncate" title={c.detail || c.name}>
                  {c.name}
                </span>
              </span>
              <Badge
                variant={checkBadge[c.status] ?? 'secondary'}
                className="text-[10px] shrink-0"
              >
                {c.status}
              </Badge>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
