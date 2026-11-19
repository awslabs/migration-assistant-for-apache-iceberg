// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'

export type MigrationRules = Record<string, string[]>

export function useMigrationRules() {
  return useQuery({
    queryKey: ['migration-rules'],
    queryFn: () => api.get<MigrationRules>('/validation/rules'),
    staleTime: 60_000,
  })
}

export function getEligibleTypes(
  rules: MigrationRules | undefined,
  format: string,
): string[] {
  if (!rules) return []
  return rules[format.toLowerCase()] ?? []
}

export function getCompatibleTypes(
  rules: MigrationRules | undefined,
  format: string,
  planTypes: string[],
  partitioned?: boolean,
): string[] {
  const eligible = getEligibleTypes(rules, format)
  let compatible = planTypes.filter((t) => eligible.includes(t))
  // incremental and add_files require partitioned tables
  if (partitioned === false) {
    compatible = compatible.filter((t) => t !== 'incremental' && t !== 'add_files')
  }
  return compatible
}
