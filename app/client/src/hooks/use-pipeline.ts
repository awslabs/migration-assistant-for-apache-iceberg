// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { PipelineDetail, PipelineBatchResponse } from '@/types'

export function usePipelineDetail(pipelineId: string | null | undefined) {
  return useQuery({
    queryKey: ['pipeline', pipelineId],
    queryFn: () => api.get<PipelineDetail>(`/pipeline/${pipelineId}`),
    enabled: !!pipelineId,
    refetchInterval: (query) => {
      const status = query.state.data?.status
      return status === 'running' ? 5000 : false
    },
  })
}

export function usePipelineBatches(pipelineId: string | null | undefined) {
  return useQuery({
    queryKey: ['pipeline-batches', pipelineId],
    queryFn: () => api.get<PipelineBatchResponse>(`/pipeline/${pipelineId}/batches`),
    enabled: !!pipelineId,
    refetchInterval: (query) => {
      const data = query.state.data
      if (!data) return false
      const hasRunning = data.batches.some(b => b.status === 'running')
      const hasPending = data.batches.some(b => b.status === 'pending')
      return (hasRunning || hasPending) ? 5000 : false
    },
  })
}

export interface PipelineCosts {
  pipeline_id: string
  athena_bytes_scanned: number
  athena_query_count: number
  athena_cost_usd: number
  emr_vcpu_hours: number
  emr_memory_gb_hours?: number
  emr_storage_gb_hours?: number
  emr_cost_usd: number
  total_cost_usd: number
}

export interface PlanCosts {
  plan_id: string
  athena_bytes_scanned: number
  athena_query_count: number
  athena_cost_usd: number
  emr_vcpu_hours: number
  emr_cost_usd: number
  total_cost_usd: number
}

export function usePipelineCosts(pipelineId: string | null | undefined) {
  return useQuery({
    queryKey: ['pipeline-costs', pipelineId],
    queryFn: () => api.get<PipelineCosts>(`/pipeline/${pipelineId}/costs`),
    enabled: !!pipelineId,
  })
}

export function usePlanCosts(planId: string) {
  return useQuery({
    queryKey: ['plan-costs', planId],
    queryFn: () => api.get<PlanCosts>(`/pipeline/plan/${planId}/costs`),
    enabled: !!planId,
  })
}

// Approve a Managed Migration plan-review pause (resume the migration).
export function useApproveMigration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (pipelineId: string) =>
      api.post<PipelineDetail>(`/pipeline/${pipelineId}/approve-migration`, {}),
    onSuccess: (_data, pipelineId) => {
      queryClient.invalidateQueries({ queryKey: ['pipeline', pipelineId] })
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}
