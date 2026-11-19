// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useToast } from '@/components/ui/toast'
import { useCreatePlan } from '@/hooks/use-plans'
import { useNavigate } from '@tanstack/react-router'
import { PlanForm } from '@/components/plan-form'
import { useTranslation } from 'react-i18next'

export function PlanCreatePage() {
  const { t } = useTranslation()
  const createPlan = useCreatePlan()
  const navigate = useNavigate()
  const { showToast } = useToast()

  return (
    <div className="max-w-3xl mx-auto">
      <Card>
        <CardHeader>
          <CardTitle>{t('plans.actions.create_migration_plan')}</CardTitle>
          <CardDescription>{t('plans.actions.configure_plan')}</CardDescription>
        </CardHeader>
        <CardContent>
          <PlanForm
            onSubmit={d => createPlan.mutate(d, { onSuccess: (plan) => { showToast(`Plan "${plan.name}" created successfully`, 'success'); navigate({ to: '/plans/$planId', params: { planId: plan.id } }) }, onError: (err: any) => showToast(err?.message || 'Failed to create plan', 'error') })}
            onCancel={() => navigate({ to: '/plans' })}
            isPending={createPlan.isPending}
            submitLabel="Create Plan"
          />
        </CardContent>
      </Card>
    </div>
  )
}
