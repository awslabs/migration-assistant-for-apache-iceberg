// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type {
  ValidationResult,
  ValidationRunResponse,
  ValidationTier,
  PlanValidationStatus,
} from '@/types'

// Latest validation run for a pipeline. Polls while a run is in progress so the
// results card updates live, then stops.
export function usePipelineValidation(pipelineId: string | null | undefined) {
  return useQuery({
    queryKey: ['validation-pipeline', pipelineId],
    queryFn: () =>
      api.get<ValidationResult | Record<string, never>>(
        `/validation/data/pipeline/${pipelineId}`,
      ),
    enabled: !!pipelineId,
    refetchInterval: (query) => {
      const status = (query.state.data as ValidationResult | undefined)
        ?.overall_status
      return status === 'running' ? 5000 : false
    },
  })
}


// Validation state of every migrated table in a plan, keyed by pipeline id.
// Polled so the icon flips to grey when a new batch lands and to green/red when
// a run finishes.
export function usePlanValidationStatus(planId: string | null | undefined) {
  return useQuery({
    queryKey: ['validation-plan-status', planId],
    queryFn: () =>
      api.get<PlanValidationStatus>(`/validation/data/plan/${planId}/status`),
    enabled: !!planId,
    refetchInterval: 10000,
  })
}

// Validate every eligible table in a plan.
export function useRunPlanValidation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, tier }: { planId: string; tier: ValidationTier }) =>
      api.post<ValidationRunResponse>(
        `/validation/data/plan/${planId}/run?tier=${tier}`,
        {},
      ),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['validation-plan-status'] })
      // Nudge each started pipeline's validation query so cards begin polling.
      data.started?.forEach((s) =>
        queryClient.invalidateQueries({
          queryKey: ['validation-pipeline', s.pipeline_id],
        }),
      )
    },
  })
}

// Validate a specific subset of a plan's tables (by pipeline id).
export function useRunTablesValidation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      planId,
      pipelineIds,
      tier,
    }: {
      planId: string
      pipelineIds: string[]
      tier: ValidationTier
    }) =>
      api.post<ValidationRunResponse>(
        `/validation/data/plan/${planId}/tables/run`,
        { pipeline_ids: pipelineIds, tier },
      ),
    onSuccess: (data) => {
      data.started?.forEach((s) =>
        queryClient.invalidateQueries({
          queryKey: ['validation-pipeline', s.pipeline_id],
        }),
      )
    },
  })
}

// Client-side catalog of the checks each tier runs, mirrored from the engine's
// core/validation_checks.py so the dialog can preview them without a round-trip.
// Some checks only apply to certain strategies/layouts; the engine decides the
// final set per table — this is an informational preview.
export interface CatalogCheck {
  id: string
  name: string
  note?: string
}

export const VALIDATION_CATALOG: Record<ValidationTier, CatalogCheck[]> = {
  basic: [
    { id: 'V1', name: 'Row count' },
    { id: 'V2', name: 'Schema conformance' },
    { id: 'V3', name: 'Partition count', note: 'partitioned tables only' },
    { id: 'V4', name: 'Null counts (key columns)' },
    { id: 'V5', name: 'Value bounds (key columns)' },
    { id: 'V6', name: 'Morph manifest integrity', note: 'morph only' },
  ],
  advanced: [
    { id: 'V1', name: 'Row count' },
    { id: 'V2', name: 'Schema conformance' },
    { id: 'V3', name: 'Partition count', note: 'partitioned tables only' },
    { id: 'V4', name: 'Null counts (key columns)' },
    { id: 'V5', name: 'Value bounds (key columns)' },
    { id: 'V6', name: 'Morph manifest integrity', note: 'morph only' },
    { id: 'V7', name: 'Aggregate parity', note: 'numeric key columns' },
    { id: 'V8', name: 'Per-partition row counts', note: 'partitioned tables only' },
  ],
}
