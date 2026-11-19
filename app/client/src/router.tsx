// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  createRouter,
  createRootRoute,
  createRoute,
  Outlet,
} from '@tanstack/react-router'
import { Layout } from '@/components/layout'
import { DashboardPage } from '@/pages/dashboard'
import { InventoryPage } from '@/pages/inventory'
import { PlansPage } from '@/pages/plans'
import { PlanDetailPage } from '@/pages/plan-detail'
import { PlanCreatePage } from '@/pages/plan-create'
import { MigratePage } from '@/pages/migrate'
import { MigrationDetailPage } from '@/pages/migration-detail'
import { UsersPage } from '@/pages/users'
import { SourcesPage } from '@/pages/sources'
import { SourceCreatePage } from '@/pages/source-create'
import { SettingsPage } from '@/pages/settings'
import { AccountPage } from '@/pages/account'

const rootRoute = createRootRoute({
  component: () => (
    <Layout>
      <Outlet />
    </Layout>
  ),
})

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: DashboardPage,
})

const inventoryRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/inventory',
  component: InventoryPage,
})

const plansRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/plans',
  component: PlansPage,
})

const planCreateRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/plans/new',
  component: PlanCreatePage,
})

const planDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/plans/$planId',
  component: PlanDetailPage,
})

const migrateRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/migrations',
  component: MigratePage,
})

const migrationDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/migrations/$migrationId',
  component: MigrationDetailPage,
})

const usersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/users',
  component: UsersPage,
})

const sourcesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/sources',
  component: SourcesPage,
  validateSearch: (search: Record<string, unknown>) => ({
    created: (search.created as string) || undefined,
  }),
})

const sourceCreateRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/sources/new',
  component: SourceCreatePage,
})

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/settings',
  component: SettingsPage,
})

const accountRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/account',
  component: AccountPage,
})

const routeTree = rootRoute.addChildren([
  indexRoute, inventoryRoute, plansRoute, planCreateRoute, planDetailRoute, migrateRoute, migrationDetailRoute, usersRoute, sourcesRoute, sourceCreateRoute, settingsRoute, accountRoute,
])

export const router = createRouter({ routeTree })

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}
