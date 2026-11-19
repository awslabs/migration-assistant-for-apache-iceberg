// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
import { useCallback } from 'react'
import { useMutation } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useAuth } from '@/hooks/use-auth'
import { useTheme } from '@/hooks/use-theme'
import { useToast } from '@/components/ui/toast'

/**
 * Switch between light and dark and remember the choice on the server.
 *
 * The saved preference is re-applied on every page load (via /auth/me), so a
 * switch that is only kept in the browser would be undone by the next refresh.
 */
export function useThemePreference() {
  const { user } = useAuth()
  const { theme, setTheme } = useTheme()
  const { showToast } = useToast()

  const save = useMutation({
    mutationFn: (next: string) =>
      api.put(`/auth/users/${user!.username}/theme`, { theme: next }),
    onError: (err: Error) => showToast(err.message || 'Failed to save theme', 'error'),
  })

  const toggle = useCallback(() => {
    const next = theme === 'light' ? 'dark' : 'light'
    setTheme(next)
    if (user) save.mutate(next)
  }, [theme, setTheme, user, save])

  return { theme, toggle }
}
