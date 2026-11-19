// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useAuth } from '@/hooks/use-auth'
import { api } from '@/lib/api'
import { useMutation } from '@tanstack/react-query'
import { useState } from 'react'
import { Loader2 } from 'lucide-react'
import { IceLogo } from '@/components/logo'
import { useTranslation } from 'react-i18next'

/**
 * Forced first-login password change (T5).
 *
 * Shown by AuthGate when the authenticated user has `must_change_password`
 * (the seeded/default admin or an admin-reset credential). Until the change
 * succeeds, every protected API route returns 403 PASSWORD_CHANGE_REQUIRED, so
 * this screen is the only thing the user can interact with. On success the
 * backend clears the flag and issues a fresh session; we re-fetch the user via
 * refreshUser() to drop straight into the app.
 */
export function ForceChangePasswordPage() {
  const { user, refreshUser, logout } = useAuth()
  const { t } = useTranslation()
  const [currentPassword, setCurrentPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState('')

  const changePassword = useMutation({
    mutationFn: (data: { username: string; current_password: string; new_password: string }) =>
      api.post<{ detail: string }>('/auth/change-password', data),
    onSuccess: async () => {
      // Flag is cleared server-side; re-read identity to leave the gate.
      await refreshUser()
    },
    onError: (err: Error) => {
      setError(err.message || 'Failed to change password')
    },
  })

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    if (newPassword !== confirmPassword) {
      setError('New passwords do not match')
      return
    }
    changePassword.mutate({
      username: user!.username,
      current_password: currentPassword,
      new_password: newPassword,
    })
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-background px-4">
      <Card className="w-full max-w-xl">
        <CardHeader className="text-center">
          <div className="flex items-center justify-center gap-2 mb-2">
            <IceLogo className="h-8 w-8" />
            <CardTitle className="text-xl sm:whitespace-nowrap">{t('app.branding.app_name')}</CardTitle>
          </div>
          <CardTitle className="text-lg">{t('account.actions.change_password')}</CardTitle>
          <CardDescription>
            {t('force_password.messages.must_set_password')}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <label htmlFor="current-pw" className="text-sm font-medium">{t('users.labels.current_password')}</label>
              <Input id="current-pw" type="password" placeholder="••••••••" value={currentPassword} onChange={e => setCurrentPassword(e.target.value)} />
            </div>
            <div className="space-y-2">
              <label htmlFor="new-pw" className="text-sm font-medium">{t('users.actions.new_password')}</label>
              <Input id="new-pw" type="password" placeholder="••••••••" value={newPassword} onChange={e => setNewPassword(e.target.value)} />
            </div>
            <div className="space-y-2">
              <label htmlFor="confirm-pw" className="text-sm font-medium">{t('users.labels.confirm_new_password')}</label>
              <Input id="confirm-pw" type="password" placeholder="••••••••" value={confirmPassword} onChange={e => setConfirmPassword(e.target.value)} />
            </div>
            <p className="text-xs text-muted-foreground">
              {t('users.descriptions.password_requirements')}
            </p>
            {error && (
              <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive" role="alert">{error}</div>
            )}
            <Button
              type="submit"
              className="w-full"
              disabled={changePassword.isPending || !currentPassword || !newPassword || !confirmPassword}
            >
              {changePassword.isPending && <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />}
              {t('account.actions.change_password')}
            </Button>
            <Button type="button" variant="ghost" className="w-full" onClick={() => logout()}>
              {t('common.actions.sign_out', 'Sign out')}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
