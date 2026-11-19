// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { ShieldCheck } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import type { ValidationState } from '@/types'

const STYLE: Record<ValidationState, string> = {
  none: 'text-muted-foreground/50',
  stale: 'text-muted-foreground/50',
  passed: 'text-emerald-600',
  failed: 'text-red-600',
}

const LABEL: Record<ValidationState, string> = {
  none: 'validation.labels.status_not_run',
  stale: 'validation.labels.status_stale',
  passed: 'validation.labels.status_passed',
  failed: 'validation.labels.status_failed',
}

// Grey = not validated (or a new batch landed since), green = passed, red = failed.
export function ValidationStatusIcon({ state = 'none' }: { state?: ValidationState }) {
  const { t } = useTranslation()
  const label = t(LABEL[state])
  return (
    <span title={label} aria-label={label} className="inline-flex shrink-0">
      <ShieldCheck className={`h-4 w-4 ${STYLE[state]}`} />
    </span>
  )
}
