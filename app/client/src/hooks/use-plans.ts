// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { MigrationPlan, CreatePlanRequest } from '@/types'

export function usePlans() {
  return useQuery({
    queryKey: ['plans'],
    queryFn: () => api.get<MigrationPlan[]>('/plans'),
  })
}

export function usePlan(id: string) {
  return useQuery({
    queryKey: ['plans', id],
    queryFn: () => api.get<MigrationPlan>(`/plans/${id}`),
    // While an AI analysis is running, its progress lives only in the backend
    // background task + the per-table analysis_status in the DB. Poll so the
    // plan (and any panel derived from it) reflects that progress even without
    // a live SSE stream — e.g. after a page refresh. Stops polling once no
    // table is 'analyzing'.
    refetchInterval: (query) => {
      const plan = query.state.data as MigrationPlan | undefined
      const analyzing = plan?.tables?.some(t => t.analysis_status === 'analyzing')
      return analyzing ? 4000 : false
    },
  })
}

export function useCreatePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (data: CreatePlanRequest) =>
      api.post<MigrationPlan>('/plans', data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useTablesInPlans() {
  return useQuery({
    queryKey: ['tables-in-plans'],
    queryFn: () => api.get<Record<string, string>>('/plans/meta/tables-in-plans'),
  })
}


export interface BatchTableItem {
  source_database: string
  source_table: string
  target_database: string
  target_table: string
  migration_type: string
}

export interface AddTablesBatchResult {
  added: number
  skipped: { table: string; reason: string }[]
}

// Add many tables to a plan in a single transactional request. Replaces the
// old per-table fan-out (2N requests, each reloading the whole plan) with one
// cheap call that returns a minimal summary.
export function useAddTablesBatch() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, tables }: { planId: string; tables: BatchTableItem[] }) =>
      api.post<AddTablesBatchResult>(`/plans/${planId}/tables/batch`, { tables }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
      queryClient.invalidateQueries({ queryKey: ['tables-in-plans'] })
    },
  })
}

export function useRemoveTableFromPlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable }: { planId: string; sourceDb: string; sourceTable: string }) =>
      api.delete<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
      queryClient.invalidateQueries({ queryKey: ['tables-in-plans'] })
    },
  })
}

export function useUpdatePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, data }: { planId: string; data: CreatePlanRequest }) =>
      api.put<MigrationPlan>(`/plans/${planId}`, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useFinalizePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/finalize`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useExecutePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/execute`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useFinalizeMigration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/finalize-migration`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useArchivePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/archive`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useCancelPlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/cancel`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useReplan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/replan`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function usePausePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/pause`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useResumePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/resume`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useApplyRecommendations() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<MigrationPlan>(`/plans/${planId}/apply-recommendations`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useUpdateTableType() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable, migType }: { planId: string; sourceDb: string; sourceTable: string; migType: string }) =>
      api.put<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/type`, { migration_type: migType }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useUpdateTableSpec() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable, specJson }: { planId: string; sourceDb: string; sourceTable: string; specJson: Record<string, unknown> }) =>
      api.put<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/spec`, { spec_json: specJson }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useTableSpec(planId: string, sourceDb: string, sourceTable: string, enabled: boolean = true) {
  return useQuery({
    queryKey: ['table-spec', planId, sourceDb, sourceTable],
    queryFn: () => api.get<Record<string, unknown>>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/spec`),
    enabled: enabled && !!planId && !!sourceDb && !!sourceTable,
  })
}

export function useDeletePlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.delete<void>(`/plans/${planId}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
      queryClient.invalidateQueries({ queryKey: ['tables-in-plans'] })
    },
  })
}


export function usePauseTable() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable }: { planId: string; sourceDb: string; sourceTable: string }) =>
      api.post<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/pause`, {}),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['plans'] }) },
  })
}

export function useResumeTable() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable }: { planId: string; sourceDb: string; sourceTable: string }) =>
      api.post<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/resume`, {}),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['plans'] }) },
  })
}

export function useCancelTable() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable }: { planId: string; sourceDb: string; sourceTable: string }) =>
      api.post<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/cancel`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
      queryClient.invalidateQueries({ queryKey: ['tables-in-plans'] })
    },
  })
}

export function useSyncPlan() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (planId: string) =>
      api.post<{ plan_id: string; tables_checked: number; tables_updated: number }>(`/plans/${planId}/sync`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}

export function useSyncNow() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (pipelineId: string) =>
      api.post<{ detail: string }>(`/pipeline/${pipelineId}/sync-now`, {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['plans'] })
    },
  })
}


export function useRetryTable() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable }: { planId: string; sourceDb: string; sourceTable: string }) =>
      api.post<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/retry`, {}),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['plans'] }) },
  })
}

export function useResumeTableFromFailure() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ planId, sourceDb, sourceTable }: { planId: string; sourceDb: string; sourceTable: string }) =>
      api.post<MigrationPlan>(`/plans/${planId}/tables/${sourceDb}/${sourceTable}/resume-from-failure`, {}),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['plans'] }) },
  })
}
