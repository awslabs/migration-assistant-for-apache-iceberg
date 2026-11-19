// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/toast'
import { useAuth } from '@/hooks/use-auth'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { Loader2, Pencil, Check, X, RotateCcw, Settings as SettingsIcon, ShieldAlert } from 'lucide-react'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

interface Setting {
  key: string
  label: string
  value: string
  default: string
  group: string
  description: string
  source: 'database' | 'environment' | 'default'
  updated_at: string | null
  options: string[] | null
}

function useSettings() {
  return useQuery({
    queryKey: ['settings'],
    queryFn: () => api.get<Setting[]>('/settings'),
  })
}

function useUpdateSetting() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ key, value }: { key: string; value: string }) =>
      api.put(`/settings/${key}`, { value }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['settings'] }),
  })
}

function useResetSetting() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (key: string) => api.delete(`/settings/${key}`),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['settings'] }),
  })
}

function SettingRow({ setting }: { setting: Setting }) {
  const { t } = useTranslation()
  const { showToast } = useToast()
  const updateSetting = useUpdateSetting()
  const resetSetting = useResetSetting()
  const [editing, setEditing] = useState(false)
  const [editValue, setEditValue] = useState('')

  const startEdit = () => {
    setEditValue(setting.value)
    setEditing(true)
  }

  const save = () => {
    updateSetting.mutate(
      { key: setting.key, value: editValue },
      {
        onSuccess: () => { setEditing(false); showToast(`${setting.label} updated`, 'success') },
        onError: (err: any) => showToast(err?.message || 'Failed to update', 'error'),
      }
    )
  }

  const reset = () => {
    resetSetting.mutate(setting.key, {
      onSuccess: () => showToast(`${setting.label} reset to default`, 'success'),
      onError: (err: any) => showToast(err?.message || 'Failed to reset', 'error'),
    })
  }

  return (
    <div className="flex items-start justify-between gap-4 py-3 border-b last:border-0">
      <div className="flex-1 min-w-0 space-y-1">
        <span className="text-sm font-medium">{setting.label}</span>
        <p className="text-xs text-muted-foreground">{setting.description}</p>
        <p className="text-[10px] text-muted-foreground font-mono">{setting.key}</p>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        {editing ? (
          <>
            {setting.options ? (
              <select
                value={editValue}
                onChange={e => setEditValue(e.target.value)}
                className="h-8 w-64 rounded-md border border-input bg-transparent px-3 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-ring"
                autoFocus
              >
                {setting.options.map(opt => (
                  <option key={opt} value={opt}>{opt}</option>
                ))}
              </select>
            ) : (
              <Input
                value={editValue}
                onChange={e => setEditValue(e.target.value)}
                className="h-8 w-64 text-xs font-mono"
                onKeyDown={e => { if (e.key === 'Enter') save(); if (e.key === 'Escape') setEditing(false) }}
                autoFocus
              />
            )}
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={save} disabled={updateSetting.isPending}>
              <Check className="h-3.5 w-3.5 text-emerald-600" />
            </Button>
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setEditing(false)}>
              <X className="h-3.5 w-3.5 text-destructive" />
            </Button>
          </>
        ) : (
          <>
            <span className="text-xs font-mono text-muted-foreground max-w-[280px] truncate" title={setting.value}>
              {setting.value || <span className="italic text-muted-foreground/50">{t('common.labels.not_set')}</span>}
            </span>
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={startEdit} title="Edit">
              <Pencil className="h-3.5 w-3.5" />
            </Button>
            {setting.source === 'database' && (
              <Button variant="ghost" size="icon" className="h-7 w-7" onClick={reset} title="Reset to default">
                <RotateCcw className="h-3.5 w-3.5" />
              </Button>
            )}
          </>
        )}
      </div>
    </div>
  )
}

export function SettingsPage() {
  const { t } = useTranslation()
  const { isAdmin } = useAuth()
  const { data: settings, isLoading } = useSettings()
  const [activeGroup, setActiveGroup] = useState<string | null>(null)

  if (!isAdmin) return (
    <div className="flex flex-col items-center justify-center py-20 text-center">
      <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
      <h2 className="text-xl font-semibold mb-2">{t('auth.labels.access_denied')}</h2>
      <p className="text-muted-foreground">{t('settings.messages.admin_only_settings')}</p>
    </div>)

  if (isLoading) return <div className="flex items-center justify-center py-12"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>

  // Group settings by category
  const groups: Record<string, Setting[]> = {}
  for (const s of settings ?? []) {
    if (!groups[s.group]) groups[s.group] = []
    groups[s.group].push(s)
  }

  const groupNames = Object.keys(groups)
  const selectedGroup = activeGroup && groups[activeGroup] ? activeGroup : groupNames[0] ?? null

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-xl font-semibold tracking-tight">{t('nav.labels.settings')}</h2>
        <p className="text-muted-foreground text-sm">{t('settings.actions.configure_engine')}</p>
      </div>

      <div className="flex gap-6">
        {/* Left navigation */}
        <aside className="w-56 shrink-0">
          <nav className="space-y-1 sticky top-6">
            <h3 className="text-sm font-semibold text-muted-foreground mb-3 px-3">{t('common.labels.categories')}</h3>
            {groupNames.map(group => (
              <button
                key={group}
                onClick={() => setActiveGroup(group)}
                className={`flex w-full items-center justify-between gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors cursor-pointer ${
                  selectedGroup === group
                    ? 'bg-accent text-accent-foreground'
                    : 'text-muted-foreground hover:bg-accent/50 hover:text-accent-foreground'
                }`}
              >
                <span className="flex items-center gap-2">
                  <SettingsIcon className="h-4 w-4" />
                  {group}
                </span>
                <Badge variant="secondary" className="text-[10px]">{groups[group].length}</Badge>
              </button>
            ))}
          </nav>
        </aside>

        {/* Right content */}
        <div className="flex-1 min-w-0">
          {selectedGroup && (
            <Card>
              <CardHeader className="pb-3">
                <div className="flex items-center gap-2">
                  <SettingsIcon className="h-4 w-4 text-muted-foreground" />
                  <CardTitle className="text-base">{selectedGroup}</CardTitle>
                  <Badge variant="secondary" className="text-[10px]">{groups[selectedGroup].length} {t('nav.labels.settings').toLowerCase()}</Badge>
                </div>
              </CardHeader>
              <CardContent className="pt-0">
                <div className="divide-y">
                  {groups[selectedGroup].map(s => <SettingRow key={s.key} setting={s} />)}
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  )
}
