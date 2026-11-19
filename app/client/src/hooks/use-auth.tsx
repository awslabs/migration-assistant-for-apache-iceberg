// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from 'react'
import { api, UnauthorizedError } from '@/lib/api'
import { useTheme } from '@/hooks/use-theme'

export type UserRole = 'admin' | 'editor' | 'viewer'

export interface User {
  username: string
  name: string
  role: UserRole
  email: string
  theme_preference: string
  // True when the account must set a new password before it can use the app
  // (seeded/default admin or an admin-reset credential). The app routes such a
  // user to a forced change-password screen (T5).
  must_change_password?: boolean
}

interface AuthContextType {
  user: User | null
  isLoading: boolean
  isAdmin: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  // Re-fetch the current user from /auth/me. Used after a forced password
  // change to clear the must_change_password gate without a full reload.
  refreshUser: () => Promise<void>
}

const AuthContext = createContext<AuthContextType | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const { setTheme } = useTheme()

  // Apply a server-provided theme preference through the theme state, so the
  // page, the theme cache and the Preferences screen always agree. 'system'
  // keeps whatever the browser already has. Identity itself is never persisted
  // client-side (M7): the session lives in an HttpOnly cookie and is restored
  // from the server via /auth/me.
  const applyThemePreference = useCallback((pref: string | undefined) => {
    if (pref === 'light' || pref === 'dark') setTheme(pref)
  }, [setTheme])

  // Restore the session from the server on load. The session token is in an
  // HttpOnly cookie the browser sends automatically; if it is missing or
  // expired, /auth/me returns 401 and we treat the user as logged out.
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const me = await api.get<User>('/auth/me')
        if (!cancelled) {
          setUser(me)
          applyThemePreference(me.theme_preference)
        }
      } catch {
        // 401 (no/expired session) or network error -> not authenticated.
        if (!cancelled) setUser(null)
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [applyThemePreference])

  const login = useCallback(async (username: string, password: string) => {
    // On success the server sets the HttpOnly session cookie; we only keep the
    // returned profile in memory for rendering.
    const res = await api.post<User>('/auth/login', { username, password })
    setUser(res)
    applyThemePreference(res.theme_preference)
  }, [applyThemePreference])

  const logout = useCallback(() => {
    // Clear UI state immediately, then best-effort revoke the server session.
    setUser(null)
    void api.post('/auth/logout', {}).catch(() => { /* already gone */ })
  }, [])

  const refreshUser = useCallback(async () => {
    // Re-read identity from the server (e.g. after a forced password change,
    // which clears must_change_password and issues a fresh session cookie).
    const me = await api.get<User>('/auth/me')
    setUser(me)
    applyThemePreference(me.theme_preference)
  }, [applyThemePreference])

  const isAdmin = user?.role === 'admin'

  return (
    <AuthContext.Provider value={{ user, isLoading, isAdmin, login, logout, refreshUser }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}

// Re-exported so components can distinguish an auth failure from other errors.
export { UnauthorizedError }
