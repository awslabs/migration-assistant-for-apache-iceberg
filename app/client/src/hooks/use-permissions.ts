// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useMemo } from 'react'
import { useAuth } from './use-auth'
import { useGroups } from './use-groups'
import type { MigrationPlan } from '@/types'

export type PlanAccess = 'manage' | 'view' | 'none'

/**
 * Returns the user's access level for a specific plan.
 *
 * - "manage": user can perform all actions (edit, finalize, execute, etc.)
 * - "view": user can only read (no mutation buttons)
 * - "none": user shouldn't see this plan (handled by backend filtering)
 */
export function usePlanAccess(plan: MigrationPlan | undefined | null): PlanAccess {
  const { user, isAdmin } = useAuth()
  const { data: groups } = useGroups()

  return useMemo(() => {
    if (!plan || !user) return 'none'
    if (isAdmin) return 'manage'

    const planOwners = plan.owners || []

    // Plans with no owners — everyone can view but not manage
    if (planOwners.length === 0) return 'view'

    // Build user's group memberships from groups data
    if (!groups) return 'none'
    const memberships: Record<string, string> = {}
    for (const g of groups) {
      const member = g.members.find(m => m.username === user.username)
      if (member) {
        memberships[g.name] = member.role
      }
    }

    // Find the highest access level across owning groups
    let bestAccess: PlanAccess = 'none'
    for (const groupName of planOwners) {
      const role = memberships[groupName]
      if (role === 'manage') return 'manage'
      if (role === 'view') bestAccess = 'view'
    }

    return bestAccess
  }, [plan, user, isAdmin, groups])
}

/**
 * Returns the list of group names the current user can assign as plan owners.
 * Admins get all groups. Others get only groups where they have "manage" role.
 */
export function useManageableGroups(): string[] {
  const { user, isAdmin } = useAuth()
  const { data: groups } = useGroups()

  return useMemo(() => {
    if (!groups || !user) return []
    if (isAdmin) return groups.map(g => g.name)
    return groups
      .filter(g => g.members.some(m => m.username === user.username && m.role === 'manage'))
      .map(g => g.name)
  }, [groups, user, isAdmin])
}
