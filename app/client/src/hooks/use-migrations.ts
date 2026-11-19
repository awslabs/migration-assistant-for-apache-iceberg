// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import type { TableInfo } from '@/types'

export function useTables() {
  return useQuery({
    queryKey: ['tables'],
    queryFn: () => api.get<TableInfo[]>('/tables'),
  })
}
