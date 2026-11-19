// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/toast'
import { useAuth } from '@/hooks/use-auth'
import { useThemePreference } from '@/hooks/use-theme-preference'
import { useQuery, useMutation } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { Bell, Key, Users, Palette, Loader2, Sun, Moon, Check } from 'lucide-react'
import { useState, useEffect } from 'react'
import { cn } from '@/lib/utils'
import { useTranslation } from 'react-i18next'

type Section = 'notifications' | 'password' | 'groups' | 'preferences'

const menuItems: { id: Section; label: string; icon: React.ElementType }[] = [
  { id: 'notifications', label: 'Notifications', icon: Bell },
  { id: 'password', label: 'Manage Password', icon: Key },
  { id: 'groups', label: 'Group Membership', icon: Users },
  { id: 'preferences', label: 'Preferences', icon: Palette },
]

export function AccountPage() {
  const { t } = useTranslation()
  const [activeSection, setActiveSection] = useState<Section>('notifications')

  return (
    <div className="flex gap-6">
      {/* Left menu */}
      <aside className="w-56 shrink-0">
        <nav className="space-y-1 sticky top-6">
          <h2 className="text-sm font-semibold text-muted-foreground mb-3 px-3">{t('account.labels.account_settings')}</h2>
          {menuItems.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => setActiveSection(id)}
              className={cn(
                'flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium transition-colors cursor-pointer',
                activeSection === id
                  ? 'bg-accent text-accent-foreground'
                  : 'text-muted-foreground hover:bg-accent/50 hover:text-accent-foreground'
              )}
            >
              <Icon className="h-4 w-4" />
              {label}
            </button>
          ))}
        </nav>
      </aside>

      {/* Right content pane */}
      <div className="flex-1 min-w-0">
        {activeSection === 'notifications' && <NotificationsSection />}
        {activeSection === 'password' && <PasswordSection />}
        {activeSection === 'groups' && <GroupsSection />}
        {activeSection === 'preferences' && <PreferencesSection />}
      </div>
    </div>
  )
}

/* ─── Notifications ─── */

interface NotificationPrefs {
  pipeline_completed: boolean
  pipeline_failed: boolean
  sync_out_of_sync: boolean
  plan_completed: boolean
}

function NotificationsSection() {
  const { t } = useTranslation()
  const { user } = useAuth()
  const { showToast } = useToast()
  const [email, setEmail] = useState('')
  const [emailLoaded, setEmailLoaded] = useState(false)
  const [prefs, setPrefs] = useState<NotificationPrefs>({
    pipeline_completed: true,
    pipeline_failed: true,
    sync_out_of_sync: true,
    plan_completed: true,
  })
  const [dirty, setDirty] = useState(false)

  // Load existing prefs from server
  const { data: serverPrefs } = useQuery({
    queryKey: ['notification-prefs', user?.username],
    queryFn: () => api.get<NotificationPrefs & { username: string }>(`/auth/users/${user!.username}/notifications`),
    enabled: !!user,
  })

  // Load email from server (survives page refresh)
  const { data: profileData } = useQuery({
    queryKey: ['user-profile', user?.username],
    queryFn: () => api.get<{ email: string; theme_preference: string }>(`/auth/users/${user!.username}/profile`),
    enabled: !!user,
  })

  useEffect(() => {
    if (profileData?.email && !emailLoaded) {
      setEmail(profileData.email)
      setEmailLoaded(true)
    }
  }, [profileData, emailLoaded])

  useEffect(() => {
    if (serverPrefs) {
      setPrefs({
        pipeline_completed: serverPrefs.pipeline_completed,
        pipeline_failed: serverPrefs.pipeline_failed,
        sync_out_of_sync: serverPrefs.sync_out_of_sync,
        plan_completed: serverPrefs.plan_completed,
      })
    }
  }, [serverPrefs])

  const savePrefs = useMutation({
    mutationFn: async () => {
      await api.put(`/auth/users/${user!.username}/email`, { email })
      await api.put(`/auth/users/${user!.username}/notifications`, prefs)
    },
    onSuccess: () => {
      setDirty(false)
      showToast('Notification preferences saved', 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to save', 'error'),
  })

  const togglePref = (key: keyof NotificationPrefs) => {
    setPrefs(p => ({ ...p, [key]: !p[key] }))
    setDirty(true)
  }

  const notifOptions: { key: keyof NotificationPrefs; label: string; description: string }[] = [
    { key: 'pipeline_completed', label: 'Pipeline Completed', description: 'When a pipeline finishes successfully' },
    { key: 'pipeline_failed', label: 'Pipeline Failed', description: 'When a pipeline encounters an error' },
    { key: 'sync_out_of_sync', label: 'Out of Sync', description: 'When an incremental pipeline detects new changes' },
    { key: 'plan_completed', label: 'Plan Completed', description: 'When all tables in a migration plan are done' },
  ]

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Bell className="h-5 w-5 text-muted-foreground" />
          <CardTitle className="text-lg">{t('account.labels.notifications')}</CardTitle>
        </div>
        <CardDescription>{t('account.descriptions.notification_desc')}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-2">
          <label htmlFor="notif-email" className="text-sm font-medium">{t('users.labels.email_address')}</label>
          <div className="flex gap-2 max-w-md">
            <Input
              id="notif-email"
              type="email"
              placeholder="you@example.com"
              value={email}
              onChange={e => { setEmail(e.target.value); setDirty(true) }}
            />
          </div>
        </div>
        <div className="space-y-3 pt-2">
          {notifOptions.map(opt => (
            <div key={opt.key} className="flex items-center justify-between rounded-md border px-4 py-3">
              <div>
                <p className="text-sm font-medium">{opt.label}</p>
                <p className="text-xs text-muted-foreground">{opt.description}</p>
              </div>
              <button
                onClick={() => togglePref(opt.key)}
                className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors ${prefs[opt.key] ? 'bg-primary' : 'bg-muted'}`}
                role="switch"
                aria-checked={prefs[opt.key]}
              >
                <span className={`pointer-events-none inline-block h-5 w-5 rounded-full bg-background shadow-lg ring-0 transition-transform ${prefs[opt.key] ? 'translate-x-5' : 'translate-x-0'}`} />
              </button>
            </div>
          ))}
        </div>
        <div className="pt-2">
          <Button onClick={() => savePrefs.mutate()} disabled={!dirty || savePrefs.isPending} size="sm">
            {savePrefs.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : !dirty ? <Check className="h-4 w-4" /> : null}
            {!dirty && !savePrefs.isPending ? t('account.labels.saved') : t('account.actions.save_preferences')}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}

/* ─── Manage Password ─── */

function PasswordSection() {
  const { t } = useTranslation()
  const { user } = useAuth()
  const { showToast } = useToast()
  const [currentPassword, setCurrentPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')

  const changePassword = useMutation({
    mutationFn: (data: { username: string; current_password: string; new_password: string }) =>
      api.post<{ detail: string }>('/auth/change-password', data),
    onSuccess: () => {
      showToast('Password changed successfully', 'success')
      setCurrentPassword('')
      setNewPassword('')
      setConfirmPassword('')
    },
    onError: (err: Error) => {
      showToast(err.message || 'Failed to change password', 'error')
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (newPassword !== confirmPassword) {
      showToast('New passwords do not match', 'error')
      return
    }
    changePassword.mutate({
      username: user!.username,
      current_password: currentPassword,
      new_password: newPassword,
    })
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Key className="h-5 w-5 text-muted-foreground" />
          <CardTitle className="text-lg">{t('account.actions.manage_password')}</CardTitle>
        </div>
        <CardDescription>{t('account.actions.update_password')}</CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-4 max-w-md">
          <div className="space-y-2">
            <label htmlFor="current-pw" className="text-sm font-medium">{t('users.labels.current_password')}</label>
            <Input id="current-pw" type="password" value={currentPassword} onChange={e => setCurrentPassword(e.target.value)} />
          </div>
          <div className="space-y-2">
            <label htmlFor="new-pw" className="text-sm font-medium">{t('users.actions.new_password')}</label>
            <Input id="new-pw" type="password" value={newPassword} onChange={e => setNewPassword(e.target.value)} />
          </div>
          <div className="space-y-2">
            <label htmlFor="confirm-pw" className="text-sm font-medium">{t('users.labels.confirm_new_password')}</label>
            <Input id="confirm-pw" type="password" value={confirmPassword} onChange={e => setConfirmPassword(e.target.value)} />
          </div>
          <p className="text-xs text-muted-foreground">
            {t('users.descriptions.password_requirements')}
          </p>
          <Button type="submit" size="sm" disabled={changePassword.isPending || !currentPassword || !newPassword || !confirmPassword}>
            {changePassword.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
            {t('account.actions.change_password')}
          </Button>
        </form>
      </CardContent>
    </Card>
  )
}

/* ─── Group Membership ─── */

function GroupsSection() {
  const { t } = useTranslation()
  const { user } = useAuth()

  const { data: groups, isLoading } = useQuery({
    queryKey: ['user-groups', user?.username],
    queryFn: () => api.get<{ name: string; description: string; role: string }[]>(`/auth/users/${user!.username}/groups`),
    enabled: !!user,
  })

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Users className="h-5 w-5 text-muted-foreground" />
          <CardTitle className="text-lg">{t('users.labels.group_membership')}</CardTitle>
        </div>
        <CardDescription>{t('users.descriptions.group_membership_desc')}</CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> {t('users.labels.loading_groups')}
          </div>
        ) : groups && groups.length > 0 ? (
          <div className="space-y-2">
            {groups.map(g => (
              <div key={g.name} className="flex items-center justify-between rounded-md border px-4 py-3">
                <div>
                  <p className="text-sm font-medium">{g.name}</p>
                  {g.description && <p className="text-xs text-muted-foreground">{g.description}</p>}
                </div>
                <Badge variant="secondary">{g.role}</Badge>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">{t('users.messages.no_group_membership')}</p>
        )}
      </CardContent>
    </Card>
  )
}

/* ─── Preferences ─── */

function PreferencesSection() {
  const { t } = useTranslation()
  const { theme, toggle: handleToggle } = useThemePreference()
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Palette className="h-5 w-5 text-muted-foreground" />
          <CardTitle className="text-lg">{t('account.labels.preferences')}</CardTitle>
        </div>
        <CardDescription>{t('account.labels.customize_experience')}</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-between rounded-md border px-4 py-3 max-w-md">
          <div className="flex items-center gap-3">
            {theme === 'light' ? <Sun className="h-5 w-5 text-amber-500" /> : <Moon className="h-5 w-5 text-indigo-400" />}
            <div>
              <p className="text-sm font-medium">{t('account.labels.appearance')}</p>
              <p className="text-xs text-muted-foreground">{t('account.labels.currently_using')} {theme}</p>
            </div>
          </div>
          <Button variant="outline" size="sm" onClick={handleToggle}>
            {t('account.actions.switch_to')} {theme === 'light' ? 'dark' : 'light'}
          </Button>
        </div>
      </CardContent>
    </Card>
  )
}
