// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Link, useRouterState } from '@tanstack/react-router'
import {
  LayoutDashboard, Package, ClipboardList, ArrowRightLeft,
  Settings, Users, LogOut, ChevronDown, Home, ChevronRight, Database,
  AlertTriangle, User, Sun, Moon,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { useAuth } from '@/hooks/use-auth'
import { useThemePreference } from '@/hooks/use-theme-preference'
import { useQuery } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { IceLogo } from '@/components/logo'
import { useState, useRef, useEffect } from 'react'
import { useTranslation } from 'react-i18next'

const navItems = [
  { to: '/' as const, label: 'Dashboard', icon: LayoutDashboard },
  { to: '/inventory' as const, label: 'Inventory', icon: Package },
  { to: '/plans' as const, label: 'Plans', icon: ClipboardList },
  { to: '/migrations' as const, label: 'Migrations', icon: ArrowRightLeft },
]

export function Layout({ children }: { children: React.ReactNode }) {
  const { t } = useTranslation()
  const router = useRouterState()
  const currentPath = router.location.pathname
  const { user, isAdmin, logout } = useAuth()
  const { theme, toggle: toggleTheme } = useThemePreference()
  const [settingsOpen, setSettingsOpen] = useState(false)
  const [userMenuOpen, setUserMenuOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const userMenuRef = useRef<HTMLDivElement>(null)

  const { data: engineHealth } = useQuery({
    queryKey: ['engine-health'],
    queryFn: () => api.get<{ status: string; engine: string; detail?: string }>('/health/engine'),
    refetchInterval: 30000, // check every 30s
    retry: false,
  })
  const engineDown = engineHealth?.engine === 'unreachable' || engineHealth?.engine === 'unhealthy'

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setSettingsOpen(false)
      }
      if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) {
        setUserMenuOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  return (
    <div className="min-h-screen bg-background">
      <nav className="border-b bg-card" role="navigation" aria-label="Main navigation">
        <div className="mx-auto max-w-[1600px] px-4 sm:px-6 lg:px-8">
          <div className="flex h-14 items-center justify-between">
            <div className="flex items-center gap-2">
              <Link to="/" className="flex items-center gap-2 hover:opacity-80 transition-opacity">
                <IceLogo className="h-8 w-8" />
                <span className="text-lg font-semibold">{t('app.branding.app_name')}</span>
              </Link>
            </div>
            <div className="flex items-center gap-1">
              {navItems.map(({ to, label, icon: Icon }) => (
                <Link key={to} to={to}
                  className={cn(
                    'flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                    currentPath === to
                      ? 'bg-accent text-accent-foreground'
                      : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
                  )}>
                  <Icon className="h-4 w-4" aria-hidden="true" />
                  {label}
                </Link>
              ))}

              {isAdmin && (
                <div className="relative" ref={dropdownRef}>
                  <button
                    onClick={() => setSettingsOpen(!settingsOpen)}
                    className={cn(
                      'flex items-center gap-1 rounded-md px-3 py-2 text-sm font-medium transition-colors cursor-pointer',
                      currentPath.startsWith('/users') || currentPath.startsWith('/sources') || currentPath.startsWith('/settings')
                        ? 'bg-accent text-accent-foreground'
                        : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground'
                    )}
                    aria-expanded={settingsOpen}
                    aria-haspopup="true"
                  >
                    <Settings className="h-4 w-4" aria-hidden="true" />
                    {t('common.actions.manage')}
                    <ChevronDown className={cn('h-3 w-3 transition-transform', settingsOpen && 'rotate-180')} aria-hidden="true" />
                  </button>
                  {settingsOpen && (
                    <div className="absolute right-0 top-full mt-1 w-48 rounded-md border bg-popover p-1 shadow-md z-50">
                      <Link
                        to="/sources"
                        search={{ created: undefined }}
                        onClick={() => setSettingsOpen(false)}
                        className="flex items-center gap-2 rounded-sm px-3 py-2 text-sm transition-colors hover:bg-accent hover:text-accent-foreground"
                      >
                        <Database className="h-4 w-4" aria-hidden="true" />
                        {t('nav.labels.sources')}
                      </Link>
                      <Link
                        to="/users"
                        onClick={() => setSettingsOpen(false)}
                        className="flex items-center gap-2 rounded-sm px-3 py-2 text-sm transition-colors hover:bg-accent hover:text-accent-foreground"
                      >
                        <Users className="h-4 w-4" aria-hidden="true" />
                        {t('nav.labels.user_management')}
                      </Link>
                      <Link
                        to="/settings"
                        onClick={() => setSettingsOpen(false)}
                        className="flex items-center gap-2 rounded-sm px-3 py-2 text-sm transition-colors hover:bg-accent hover:text-accent-foreground"
                      >
                        <Settings className="h-4 w-4" aria-hidden="true" />
                        {t('nav.labels.settings')}
                      </Link>
                    </div>
                  )}
                </div>
              )}

              <div className="ml-4 flex items-center gap-2 border-l pl-4">
                <Button variant="ghost" size="icon" onClick={toggleTheme} aria-label="Toggle theme">
                  {theme === 'light' ? <Moon className="h-4 w-4" /> : <Sun className="h-4 w-4" />}
                </Button>
                <div className="relative" ref={userMenuRef}>
                  <button
                    onClick={() => setUserMenuOpen(!userMenuOpen)}
                    className="flex items-center gap-2 rounded-md px-2 py-1.5 text-sm font-medium text-muted-foreground hover:bg-accent hover:text-accent-foreground transition-colors cursor-pointer"
                    aria-expanded={userMenuOpen}
                    aria-haspopup="true"
                  >
                    <User className="h-4 w-4" aria-hidden="true" />
                    {user?.username}
                    <ChevronDown className={cn('h-3 w-3 transition-transform', userMenuOpen && 'rotate-180')} aria-hidden="true" />
                  </button>
                  {userMenuOpen && (
                    <div className="absolute right-0 top-full mt-1 w-44 rounded-md border bg-popover p-1 shadow-md z-50">
                      <Link
                        to="/account"
                        onClick={() => setUserMenuOpen(false)}
                        className="flex items-center gap-2 rounded-sm px-3 py-2 text-sm transition-colors hover:bg-accent hover:text-accent-foreground"
                      >
                        <Settings className="h-4 w-4" aria-hidden="true" />
                        {t('nav.labels.settings')}
                      </Link>
                      <button
                        onClick={() => { setUserMenuOpen(false); logout() }}
                        className="flex w-full items-center gap-2 rounded-sm px-3 py-2 text-sm transition-colors hover:bg-accent hover:text-accent-foreground cursor-pointer"
                      >
                        <LogOut className="h-4 w-4" aria-hidden="true" />
                        {t('nav.actions.logout')}
                      </button>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </nav>
      <div className="border-b bg-card/50">
        <div className="mx-auto max-w-[1600px] px-4 sm:px-6 lg:px-8">
          <nav className="flex items-center gap-1 py-2 text-xs text-muted-foreground" aria-label="Breadcrumb">
            <Link to="/" className="hover:text-foreground transition-colors"><Home className="h-3.5 w-3.5" /></Link>
            {currentPath !== '/' && currentPath.split('/').filter(Boolean).map((segment, i, arr) => {
              const path = '/' + arr.slice(0, i + 1).join('/')
              const isUuid = /^[0-9a-f]{8}-/.test(segment)
              const label = isUuid ? 'Details' : segment.charAt(0).toUpperCase() + segment.slice(1)
              const isLast = i === arr.length - 1
              return (
                <span key={path} className="flex items-center gap-1">
                  <ChevronRight className="h-3 w-3" />
                  {isLast ? (
                    <span className="text-foreground font-medium">{label}</span>
                  ) : (
                    <Link to={path as '/'} className="hover:text-foreground transition-colors">{label}</Link>
                  )}
                </span>
              )
            })}
          </nav>
        </div>
      </div>
      <main className="mx-auto max-w-[1600px] px-4 py-6 sm:px-6 lg:px-8">
        {engineDown && (
          <div className="mb-4 flex items-center gap-2 rounded-md border border-amber-300 bg-amber-50 dark:border-amber-700 dark:bg-amber-950 p-3 text-sm text-amber-800 dark:text-amber-200">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            <span>
              <strong>{t('engine.labels.engine_unavailable')}</strong> — pipeline status, batch progress, and cost data may not load. Check that the engine is running
              {isAdmin ? (
                <>
                  , and that the engine URL is set in{' '}
                  <Link to="/settings" className="font-medium underline underline-offset-2 hover:text-foreground">
                    {t('nav.labels.settings')}
                  </Link>
                  {' '}if not already configured.
                </>
              ) : (
                '. If this persists, ask an admin to verify the engine URL in Settings.'
              )}
            </span>
          </div>
        )}
        {children}
      </main>
    </div>
  )
}
