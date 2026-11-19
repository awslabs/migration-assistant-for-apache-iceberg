// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Loader2, X, ShieldCheck, AlertTriangle } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { VALIDATION_CATALOG } from '@/hooks/use-data-validation'
import type { ValidationTier, ValidationSkipped } from '@/types'

interface ValidationDialogProps {
  open: boolean
  // Human-readable description of what will be validated, e.g.
  // "3 table(s) in this plan" or "orders (analytics)".
  targetLabel: string
  // Tables that cannot be validated (wrong status), shown so the user knows
  // they'll be excluded. Optional.
  skipped?: ValidationSkipped[]
  isPending?: boolean
  onRun: (tier: ValidationTier) => void
  onClose: () => void
}

export function ValidationDialog({
  open,
  targetLabel,
  skipped = [],
  isPending = false,
  onRun,
  onClose,
}: ValidationDialogProps) {
  const { t } = useTranslation()
  const [tier, setTier] = useState<ValidationTier>('basic')

  if (!open) return null

  const checks = VALIDATION_CATALOG[tier]

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/50" onClick={onClose} />
      <div className="relative z-50 w-full max-w-2xl max-h-[85vh] flex flex-col rounded-lg border bg-background p-6 shadow-lg">
        <div className="flex items-start justify-between gap-4 mb-4">
          <div>
            <h3 className="text-base font-semibold flex items-center gap-2">
              <ShieldCheck className="h-4 w-4 text-primary" />
              {t('validation.labels.validate_migration')}
            </h3>
            <p className="mt-1 text-sm text-muted-foreground">
              {t('validation.descriptions.compare_source_target', { target: targetLabel })}
            </p>
          </div>
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onClose}>
            <X className="h-3.5 w-3.5" />
          </Button>
        </div>

        <div className="flex-1 overflow-y-auto space-y-4">
          {/* Tier selection */}
          <div className="space-y-2">
            <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              {t('validation.labels.validation_depth')}
            </span>
            <div className="grid gap-2 sm:grid-cols-2">
              <label
                className={`flex cursor-pointer items-start gap-2 rounded-md border p-3 text-sm ${tier === 'basic' ? 'border-primary bg-primary/5' : 'hover:bg-muted/40'}`}
              >
                <input
                  type="radio"
                  name="validation-tier"
                  className="mt-0.5 cursor-pointer"
                  checked={tier === 'basic'}
                  onChange={() => setTier('basic')}
                />
                <span>
                  <span className="font-medium">{t('validation.labels.basic')}</span>
                  <span className="block text-xs text-muted-foreground">
                    {t('validation.descriptions.basic_desc')}
                  </span>
                </span>
              </label>
              <label
                className={`flex cursor-pointer items-start gap-2 rounded-md border p-3 text-sm ${tier === 'advanced' ? 'border-primary bg-primary/5' : 'hover:bg-muted/40'}`}
              >
                <input
                  type="radio"
                  name="validation-tier"
                  className="mt-0.5 cursor-pointer"
                  checked={tier === 'advanced'}
                  onChange={() => setTier('advanced')}
                />
                <span>
                  <span className="font-medium">{t('validation.labels.advanced')}</span>
                  <span className="block text-xs text-muted-foreground">
                    {t('validation.descriptions.advanced_desc')}
                  </span>
                </span>
              </label>
            </div>
          </div>

          {/* Check list */}
          <div className="space-y-2">
            <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              {t('validation.descriptions.checks_count', { count: checks.length })}
            </span>
            <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-1.5">
              {checks.map((c) => (
                <div key={c.id} className="flex items-center justify-between gap-2">
                  <span className="flex items-center gap-2">
                    <span className="font-mono text-muted-foreground">{c.id}</span>
                    <span>{c.name}</span>
                  </span>
                  {c.note && (
                    <span className="text-[11px] text-muted-foreground italic">
                      {c.note}
                    </span>
                  )}
                </div>
              ))}
            </div>
            <p className="text-[11px] text-muted-foreground">
              {t('validation.descriptions.applicable_subset')}
            </p>
          </div>

          {/* Ineligible tables */}
          {skipped.length > 0 && (
            <div className="space-y-2">
              <span className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-amber-600">
                <AlertTriangle className="h-3.5 w-3.5" />
                {t('validation.descriptions.excluded_count', { count: skipped.length })}
              </span>
              <div className="rounded-md border border-amber-200 bg-amber-50 dark:border-amber-800 dark:bg-amber-950/30 p-3 text-xs space-y-1.5">
                {skipped.map((s) => (
                  <div key={s.pipeline_id} className="flex items-center justify-between gap-2">
                    <span className="truncate">
                      {s.source_table
                        ? `${s.source_table} (${s.source_database})`
                        : s.pipeline_id}
                    </span>
                    <span className="flex items-center gap-1.5 shrink-0">
                      <Badge variant="warning" className="text-[10px]">
                        {s.status}
                      </Badge>
                      <span className="text-muted-foreground">{s.reason}</span>
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="mt-6 flex justify-end gap-2">
          <Button variant="outline" size="sm" onClick={onClose} disabled={isPending}>
            {t('common.actions.cancel')}
          </Button>
          <Button size="sm" onClick={() => onRun(tier)} disabled={isPending}>
            {isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
            {t('validation.labels.run_validation')}
          </Button>
        </div>
      </div>
    </div>
  )
}
