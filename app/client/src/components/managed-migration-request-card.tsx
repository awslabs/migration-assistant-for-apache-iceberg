// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronRight, FileJson } from 'lucide-react'

import { CopyButton } from '@/components/ui/copy-button'
import { usePipelineDetail } from '@/hooks/use-pipeline'

/** Read the pipeline's persisted metadata bag (spec_json._metadata). */
function parseMeta(specJson: string | undefined): Record<string, any> {
  if (!specJson) return {}
  try {
    const spec = JSON.parse(specJson)
    return spec?._metadata ?? {}
  } catch {
    return {}
  }
}

/** Each _metadata value is itself a JSON string; parse it back to an object. */
function parseJsonField<T>(v: unknown, fallback: T): T {
  if (v == null) return fallback
  if (typeof v === 'string') {
    try {
      return JSON.parse(v) as T
    } catch {
      return fallback
    }
  }
  return v as T
}

/**
 * Displays the captured AWS StartTableMigration request payload for a Managed
 * Migration pipeline, shown below the Spec JSON section on the pipeline detail
 * page. Read-only: reads spec_json._metadata.start_request (captured by the
 * engine when the migration was submitted) and lets the operator view/copy it.
 * Renders only for s3tables_managed pipelines that have a captured payload.
 */
export function ManagedMigrationRequestCard({
  pipelineId,
}: {
  pipelineId: string
}) {
  const { t } = useTranslation()
  const { data: pipeline } = usePipelineDetail(pipelineId)

  const [collapsed, setCollapsed] = useState(true)

  if (!pipeline) return null
  if (pipeline.migration_type !== 's3tables_managed') return null

  const meta = parseMeta(pipeline.spec_json)
  const startRequest = parseJsonField<Record<string, any> | null>(meta.start_request, null)

  // Nothing captured yet (migration not submitted, or an older pipeline).
  if (!startRequest) return null

  const pretty = JSON.stringify(startRequest, null, 2)

  return (
    <div className="rounded-md border bg-muted/30 p-3 text-xs space-y-3">
      <div className="flex items-center justify-between">
        <button onClick={() => setCollapsed(!collapsed)} className="flex items-center gap-1 cursor-pointer">
          <ChevronRight className={`h-3 w-3 transition-transform ${collapsed ? '' : 'rotate-90'}`} />
          <span className="flex items-center gap-1.5 font-semibold uppercase tracking-wide text-muted-foreground">
            <FileJson className="h-3.5 w-3.5" />
            {t('managed_migration.labels.request_payload')}
          </span>
        </button>
        <CopyButton value={pretty} label="request payload" />
      </div>

      {!collapsed && (
        <>
          <p className="text-muted-foreground">
            {t('managed_migration.descriptions.request_payload_desc')}
          </p>
          <pre className="rounded-md border bg-background p-3 text-xs font-mono whitespace-pre-wrap overflow-x-auto">{pretty}</pre>
        </>
      )}
    </div>
  )
}
