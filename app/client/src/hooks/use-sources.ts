// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { SourceInfo, CreateSourceRequest, RefreshLogEntry } from '@/types'

export function useSources() {
  return useQuery({
    queryKey: ['sources'],
    queryFn: () => api.get<SourceInfo[]>('/sources'),
  })
}

export function useCreateSource() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (d: CreateSourceRequest) => api.post<SourceInfo>('/sources', d),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['sources'] }) },
  })
}

export function useDeleteSource() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.delete<void>(`/sources/${id}`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['sources'] }) },
  })
}


export function useUpdateSource() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: { iam_role_arn: string; default_group: string } }) =>
      api.put<SourceInfo>(`/sources/${id}`, data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['sources'] }) },
  })
}

export function useRefreshSource() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => api.post<SourceInfo>(`/sources/${id}/refresh`, {}),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['sources'] }) },
  })
}

export function useRefreshLog(sourceId: string | null) {
  return useQuery({
    queryKey: ['sources', sourceId, 'refresh-log'],
    queryFn: () => api.get<RefreshLogEntry[]>(`/sources/${sourceId}/refresh-log`),
    enabled: !!sourceId,
  })
}
