// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { GroupInfo, CreateGroupRequest, AddGroupMemberRequest } from '@/types'

export function useGroups() {
  return useQuery({ queryKey: ['groups'], queryFn: () => api.get<GroupInfo[]>('/groups') })
}
export function useCreateGroup() {
  const qc = useQueryClient()
  return useMutation({ mutationFn: (d: CreateGroupRequest) => api.post<GroupInfo>('/groups', d),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['groups'] }) } })
}
export function useAddGroupMember() {
  const qc = useQueryClient()
  return useMutation({ mutationFn: ({ group, data }: { group: string; data: AddGroupMemberRequest }) =>
    api.post<GroupInfo>(`/groups/${group}/members`, data),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['groups'] }) } })
}
export function useRemoveGroupMember() {
  const qc = useQueryClient()
  return useMutation({ mutationFn: ({ group, username }: { group: string; username: string }) =>
    api.delete<GroupInfo>(`/groups/${group}/members/${username}`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['groups'] }) } })
}
export function useDeleteGroup() {
  const qc = useQueryClient()
  return useMutation({ mutationFn: (name: string) => api.delete<void>(`/groups/${name}`),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['groups'] }) } })
}
