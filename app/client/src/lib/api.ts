// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

const BASE_URL = '/api'

/**
 * Raised when the server rejects a request for lack of a valid session (401).
 * The app listens for this to drop the user back to the login screen instead
 * of surfacing a generic error, since M7 auth is cookie-based and a 401 means
 * the session cookie is missing, expired, or was invalidated server-side.
 */
export class UnauthorizedError extends Error {
  constructor(message = 'Authentication required') {
    super(message)
    this.name = 'UnauthorizedError'
  }
}

/**
 * Raised when the server refuses a request because the account must change its
 * password first (403 with detail `PASSWORD_CHANGE_REQUIRED`, T5). The app
 * listens for this to route the user to the forced change-password screen
 * rather than surfacing a generic error.
 */
export class PasswordChangeRequiredError extends Error {
  constructor(message = 'Password change required') {
    super(message)
    this.name = 'PasswordChangeRequiredError'
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  // Authentication is carried by the HttpOnly `ima_session` cookie (M7). It is
  // not readable by JS and must be sent with every request, so credentials are
  // always included. No identity header is attached client-side.
  const res = await fetch(`${BASE_URL}${path}`, {
    ...options,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...(options?.headers ?? {}),
    },
  })
  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: res.statusText }))
    if (res.status === 401) {
      throw new UnauthorizedError(error.detail || 'Authentication required')
    }
    if (res.status === 403 && error.detail === 'PASSWORD_CHANGE_REQUIRED') {
      throw new PasswordChangeRequiredError()
    }
    throw new Error(error.detail || 'Request failed')
  }
  const text = await res.text()
  return text ? JSON.parse(text) : ({} as T)
}

export const api = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body: unknown) =>
    request<T>(path, { method: 'POST', body: JSON.stringify(body) }),
  put: <T>(path: string, body: unknown) =>
    request<T>(path, { method: 'PUT', body: JSON.stringify(body) }),
  delete: <T>(path: string) => request<T>(path, { method: 'DELETE' }),
}
