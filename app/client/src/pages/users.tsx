// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { useToast } from '@/components/ui/toast'
import { useUsers, useCreateUser, useDeleteUser } from '@/hooks/use-users'
import { useGroups, useCreateGroup, useAddGroupMember, useRemoveGroupMember, useDeleteGroup } from '@/hooks/use-groups'
import { useAuth } from '@/hooks/use-auth'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { useState } from 'react'
import { Plus, Trash2, Loader2, ShieldAlert, X, Search, Users, FolderOpen, Shield, RotateCcw, Copy, Check } from 'lucide-react'
import { useMutation, useQueryClient as __useQueryClient } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import { api } from '@/lib/api'
import type { UserRole, GroupMemberRole } from '@/types'

const roleV: Record<UserRole, 'default'|'secondary'|'outline'> = { admin:'default', editor:'secondary', viewer:'outline' }
const grV: Record<string, 'default'|'secondary'> = { manage:'default', view:'secondary' }
type Tab = 'users' | 'groups' | 'roles'

export function UsersPage() {
  const { t } = useTranslation()
  const { isAdmin, user: cur } = useAuth()
  const { showToast } = useToast()
  const { data: users, isLoading: uLoad } = useUsers()
  const { data: groups, isLoading: gLoad } = useGroups()
  const deleteUser = useDeleteUser()
  const createGroup = useCreateGroup(); const deleteGroup = useDeleteGroup()
  const addMember = useAddGroupMember(); const removeMember = useRemoveGroupMember()
  const [tab, setTab] = useState<Tab>('roles')

  // User form
  const [showUF, setShowUF] = useState(false)

  // Group form
  const [showGF, setShowGF] = useState(false)
  const [gN, setGN] = useState(''); const [gD, setGD] = useState('')

  // Role assignment
  const [selUser, setSelUser] = useState('')
  const [selGroup, setSelGroup] = useState('')
  const [selRole, setSelRole] = useState<GroupMemberRole>('view')
  const [userSearch, setUserSearch] = useState('')
  const [confirmAction, setConfirmAction] = useState<{ type: 'user'|'group'; id: string } | null>(null)

  if (!isAdmin) return (
    <div className="flex flex-col items-center justify-center py-20 text-center">
      <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
      <h2 className="text-xl font-semibold mb-2">{t('auth.labels.access_denied')}</h2>
      <p className="text-muted-foreground">{t('auth.messages.admin_only_users')}</p>
    </div>)

  const resetUF = () => { setShowUF(false) }
  const hCreateGroup = (e: React.FormEvent) => {
    e.preventDefault(); if (!gN.trim()) return
    const groupName = gN
    createGroup.mutate({ name:gN, description:gD }, { onSuccess: () => { setShowGF(false); setGN(''); setGD(''); showToast(`Group "${groupName}" created successfully`, 'success') }, onError: (err: any) => showToast(err?.message || 'Failed to create group', 'error') })
  }
  const hAssign = () => {
    if (!selUser || !selGroup) return
    const assignedUser = selUser, assignedGroup = selGroup
    addMember.mutate({ group: selGroup, data: { username: selUser, role: selRole } },
      { onSuccess: () => { setSelUser(''); setSelGroup(''); setUserSearch(''); showToast(`User "${assignedUser}" assigned to group "${assignedGroup}"`, 'success') }, onError: (err: any) => showToast(err?.message || 'Failed to assign user', 'error') })
  }

  // Build a flat list of all user-group assignments
  const assignments = (groups ?? []).flatMap(g =>
    g.members.map(m => ({ group: g.name, username: m.username, name: m.name, role: m.role as GroupMemberRole }))
  )
  const filteredAssignments = userSearch
    ? assignments.filter(a => a.username.includes(userSearch.toLowerCase()) || a.name.toLowerCase().includes(userSearch.toLowerCase()) || a.group.toLowerCase().includes(userSearch.toLowerCase()))
    : assignments

  const tabs = [
    { id: 'roles' as Tab, label: t('users.labels.user_roles'), icon: Shield },
    { id: 'groups' as Tab, label: t('common.labels.groups'), icon: FolderOpen },
    { id: 'users' as Tab, label: t('users.labels.users'), icon: Users },
  ]

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-xl font-semibold tracking-tight">{t('common.actions.manage')}</h2>
        <p className="text-muted-foreground text-sm">{t('users.actions.manage_users')}</p>
      </div>

      <div className="flex gap-6">
        {/* Left navigation */}
        <aside className="w-52 shrink-0">
          <nav className="space-y-1 sticky top-6">
            <h3 className="text-sm font-semibold text-muted-foreground mb-3 px-3">{t('nav.labels.user_management')}</h3>
            {tabs.map(t => (
              <button
                key={t.id}
                onClick={() => setTab(t.id)}
                className={`flex w-full items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium transition-colors cursor-pointer ${
                  tab === t.id
                    ? 'bg-accent text-accent-foreground'
                    : 'text-muted-foreground hover:bg-accent/50 hover:text-accent-foreground'
                }`}
              >
                <t.icon className="h-4 w-4" />{t.label}
              </button>
            ))}
          </nav>
        </aside>

        {/* Right content */}
        <div className="flex-1 min-w-0">

      {/* ===== USERS TAB ===== */}
      {tab === 'users' && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">{t('users.actions.manage_users_desc')}</p>
            <Button size="sm" onClick={() => setShowUF(true)}><Plus className="h-3 w-3" /> {t('users.actions.add_user')}</Button>
          </div>
          {showUF && <CreateUserDialog onClose={resetUF} onSuccess={(username) => { resetUF(); showToast(`User "${username}" created successfully`, 'success') }} />}
          {uLoad ? <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /> : (
            <div className="rounded-md border"><table className="w-full text-sm">
              <thead><tr className="border-b bg-muted/50">
                <th className="px-4 py-3 text-left font-medium">{t('common.labels.username')}</th>
                <th className="px-4 py-3 text-left font-medium">{t('common.labels.name')}</th>
                <th className="px-4 py-3 text-left font-medium">{t('users.labels.system_role')}</th>
                <th className="px-4 py-3 text-left font-medium">{t('common.labels.groups')}</th>
                <th className="px-4 py-3 text-right font-medium">{t('common.labels.actions')}</th>
              </tr></thead>
              <tbody>{users?.map(u => {
                const ug = groups?.filter(g => g.members.some(m => m.username === u.username)) ?? []
                return (
                  <tr key={u.username} className="border-b last:border-0">
                    <td className="px-4 py-3 font-medium">{u.username}</td>
                    <td className="px-4 py-3">{u.name}</td>
                    <td className="px-4 py-3">
                      <RoleSelect username={u.username} currentRole={u.role} disabled={u.username === cur?.username} />
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex flex-wrap gap-1">
                        {ug.map(g => <Badge key={g.name} variant="outline">{g.name}</Badge>)}
                        {ug.length === 0 && <span className="text-xs text-muted-foreground">—</span>}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex items-center justify-end gap-1">
                        <ResetPasswordButton username={u.username} />
                        <Button variant="ghost" size="icon" onClick={() => setConfirmAction({ type: 'user', id: u.username })}
                          disabled={u.username === cur?.username || deleteUser.isPending}><Trash2 className="h-4 w-4" /></Button>
                      </div>
                    </td>
                  </tr>)
              })}</tbody>
            </table></div>
          )}
        </div>
      )}

      {/* ===== GROUPS TAB ===== */}
      {tab === 'groups' && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">{t('users.descriptions.group_desc')}</p>
            <Button size="sm" onClick={() => setShowGF(!showGF)}><Plus className="h-3 w-3" /> {t('users.actions.new_group')}</Button>
          </div>
          {showGF && (
            <Card><CardContent className="pt-6">
              <form onSubmit={hCreateGroup} className="space-y-4">
                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="space-y-1"><label className="text-sm font-medium">{t('users.labels.group_name')}</label>
                    <Input value={gN} onChange={e=>setGN(e.target.value)} placeholder="data-engineering" required/></div>
                  <div className="space-y-1"><label className="text-sm font-medium">{t('common.labels.description')}</label>
                    <Input value={gD} onChange={e=>setGD(e.target.value)} placeholder="Optional description"/></div>
                </div>
                <div className="flex gap-2">
                  <Button type="submit" disabled={createGroup.isPending}>{t('users.actions.create_group')}</Button>
                  <Button type="button" variant="outline" onClick={() => setShowGF(false)}>{t('common.actions.cancel')}</Button>
                </div>
              </form>
            </CardContent></Card>
          )}
          {gLoad ? <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /> : !groups?.length ? (
            <Card><CardContent className="py-8 text-center text-muted-foreground">{t('users.messages.no_groups')}</CardContent></Card>
          ) : (
            <div className="rounded-md border"><table className="w-full text-sm">
              <thead><tr className="border-b bg-muted/50">
                <th className="px-4 py-3 text-left font-medium">{t('users.labels.group_name_header')}</th>
                <th className="px-4 py-3 text-left font-medium">{t('common.labels.description')}</th>
                <th className="px-4 py-3 text-left font-medium">{t('common.labels.members')}</th>
                <th className="px-4 py-3 text-right font-medium">{t('common.labels.actions')}</th>
              </tr></thead>
              <tbody>{groups.map(g => (
                <tr key={g.name} className="border-b last:border-0">
                  <td className="px-4 py-3 font-medium">{g.name}</td>
                  <td className="px-4 py-3 text-muted-foreground">{g.description || '—'}</td>
                  <td className="px-4 py-3"><Badge variant="outline">{g.members.length}</Badge></td>
                  <td className="px-4 py-3 text-right">
                    {g.name === 'admins' ? (
                      <span className="text-xs text-muted-foreground">{t('common.labels.protected')}</span>
                    ) : (
                      <Button variant="ghost" size="icon" onClick={() => setConfirmAction({ type: 'group', id: g.name })} disabled={deleteGroup.isPending}>
                        <Trash2 className="h-4 w-4" /></Button>
                    )}
                  </td>
                </tr>
              ))}</tbody>
            </table></div>
          )}
        </div>
      )}

      {/* ===== USER ROLES TAB ===== */}
      {tab === 'roles' && (
        <div className="space-y-4">
          <p className="text-sm text-muted-foreground">{t('users.actions.assign_users_desc')}</p>

          <Card><CardHeader><CardTitle className="text-base">{t('users.actions.assign_user_to_group')}</CardTitle></CardHeader>
            <CardContent>
              <div className="flex items-end gap-3 flex-wrap">
                <div className="space-y-1 flex-1 min-w-[150px]">
                  <label className="text-sm font-medium">{t('users.labels.user')}</label>
                  <Select value={selUser} onChange={e => setSelUser(e.target.value)}>
                    <option value="">{t('users.actions.select_user')}</option>
                    {users?.map(u => <option key={u.username} value={u.username}>{u.name} ({u.username})</option>)}
                  </Select>
                </div>
                <div className="space-y-1 flex-1 min-w-[150px]">
                  <label className="text-sm font-medium">{t('common.labels.group')}</label>
                  <Select value={selGroup} onChange={e => setSelGroup(e.target.value)}>
                    <option value="">{t('users.actions.select_group')}</option>
                    {groups?.map(g => <option key={g.name} value={g.name}>{g.name}</option>)}
                  </Select>
                </div>
                <div className="space-y-1 w-32">
                  <label className="text-sm font-medium">{t('common.labels.role')}</label>
                  <Select value={selRole} onChange={e => setSelRole(e.target.value as GroupMemberRole)}>
                    <option value="view">{t('common.actions.view')}</option>
                    <option value="manage">{t('common.actions.manage')}</option>
                  </Select>
                </div>
                <Button onClick={hAssign} disabled={!selUser || !selGroup || addMember.isPending}>
                  {addMember.isPending && <Loader2 className="h-4 w-4 animate-spin" />}{t('common.actions.assign')}
                </Button>
              </div>
              {addMember.isError && <div className="mt-3 rounded-md bg-destructive/10 p-3 text-sm text-destructive">{addMember.error.message}</div>}
            </CardContent>
          </Card>

          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">{t('users.labels.current_assignments')} ({assignments.length})</span>
              {assignments.length > 5 && (
                <div className="relative max-w-xs">
                  <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input placeholder="Filter by user or group..." value={userSearch} onChange={e => setUserSearch(e.target.value)} className="pl-9 pr-8 h-8 text-xs" />
                  {userSearch && <button onClick={() => setUserSearch('')} className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"><X className="h-3 w-3" /></button>}
                </div>
              )}
            </div>
            {filteredAssignments.length > 0 ? (
              <div className="rounded-md border"><table className="w-full text-sm">
                <thead><tr className="border-b bg-muted/50">
                  <th className="px-4 py-3 text-left font-medium">{t('users.labels.user')}</th>
                  <th className="px-4 py-3 text-left font-medium">{t('common.labels.name')}</th>
                  <th className="px-4 py-3 text-left font-medium">{t('common.labels.group')}</th>
                  <th className="px-4 py-3 text-left font-medium">{t('common.labels.role')}</th>
                  <th className="px-4 py-3 text-right font-medium">{t('common.labels.actions')}</th>
                </tr></thead>
                <tbody>{filteredAssignments.map((a) => (
                  <tr key={`${a.group}-${a.username}`} className="border-b last:border-0">
                    <td className="px-4 py-3 font-medium">{a.username}</td>
                    <td className="px-4 py-3">{a.name}</td>
                    <td className="px-4 py-3"><Badge variant="outline">{a.group}</Badge></td>
                    <td className="px-4 py-3"><Badge variant={grV[a.role] ?? 'outline'}>{a.role}</Badge></td>
                    <td className="px-4 py-3 text-right">
                      <Button variant="ghost" size="icon" className="h-7 w-7"
                        onClick={() => removeMember.mutate({ group: a.group, username: a.username }, { onSuccess: () => showToast(`Removed "${a.username}" from group "${a.group}"`, 'success'), onError: (err: any) => showToast(err?.message || 'Failed to remove member', 'error') })}
                        disabled={removeMember.isPending}><Trash2 className="h-3 w-3" /></Button>
                    </td>
                  </tr>
                ))}</tbody>
              </table></div>
            ) : (
              <Card><CardContent className="py-8 text-center text-muted-foreground">
                {userSearch ? t('users.messages.no_assignments_match') : t('users.messages.no_assignments_yet')}
              </CardContent></Card>
            )}
          </div>
        </div>
      )}
      <ConfirmDialog
        open={!!confirmAction}
        title={confirmAction?.type === 'user' ? 'Delete User' : 'Delete Group'}
        message={confirmAction ? (confirmAction.type === 'user'
          ? `Permanently delete user "${confirmAction.id}"? They will lose access immediately. This action cannot be undone.`
          : `Permanently delete group "${confirmAction.id}"? Members will lose the access granted through this group. This action cannot be undone.`) : ''}
        confirmLabel="Delete"
        cancelLabel="Cancel"
        variant="destructive"
        onConfirm={() => {
          const c = confirmAction
          setConfirmAction(null)
          if (!c) return
          if (c.type === 'user') {
            deleteUser.mutate(c.id, { onSuccess: () => showToast(`User "${c.id}" deleted`, 'success'), onError: (err: any) => showToast(err?.message || 'Failed to delete user', 'error') })
          } else {
            deleteGroup.mutate(c.id, { onSuccess: () => showToast(`Group "${c.id}" deleted`, 'success'), onError: (err: any) => showToast(err?.message || 'Failed to delete group', 'error') })
          }
        }}
        onCancel={() => setConfirmAction(null)}
      />
    </div>
    </div>
    </div>
  )
}

/* ─── Inline Role Editor ─── */

function RoleSelect({ username, currentRole, disabled }: { username: string; currentRole: UserRole; disabled: boolean }) {
  const { t } = useTranslation()
  const { showToast } = useToast()
  const queryClient = __useQueryClient()
  const updateRole = useMutation({
    mutationFn: (role: string) => api.put<{ detail: string }>(`/auth/users/${username}/role`, { role }),
    onSuccess: (_data, role) => {
      queryClient.invalidateQueries({ queryKey: ['users'] })
      showToast(`Role for "${username}" updated to ${role}`, 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to update role', 'error'),
  })

  if (disabled) {
    return <Badge variant={roleV[currentRole]}>{currentRole}</Badge>
  }

  return (
    <Select
      value={currentRole}
      onChange={e => updateRole.mutate(e.target.value)}
      className="text-xs h-7 w-24"
      disabled={updateRole.isPending}
    >
      <option value="admin">{t('common.roles.admin')}</option>
      <option value="editor">{t('common.roles.editor')}</option>
      <option value="viewer">{t('common.roles.viewer')}</option>
    </Select>
  )
}


/* ─── Reset Password Button (inline in users table) ─── */

function ResetPasswordButton({ username }: { username: string }) {
  const { t } = useTranslation()
  const { showToast } = useToast()
  const [generatedPw, setGeneratedPw] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  const resetPw = useMutation({
    mutationFn: () => api.post<{ new_password: string }>(`/auth/users/${username}/reset-password`, {}),
    onSuccess: (data) => {
      setGeneratedPw(data.new_password)
      showToast(`Password reset for ${username}`, 'success')
    },
    onError: (err: Error) => showToast(err.message || 'Failed to reset password', 'error'),
  })

  const handleCopy = () => {
    if (!generatedPw) return
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(generatedPw).then(() => {
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      })
    } else {
      // Fallback for non-HTTPS contexts (e.g. local dev with custom domains)
      const textarea = document.createElement('textarea')
      textarea.value = generatedPw
      textarea.style.position = 'fixed'
      textarea.style.opacity = '0'
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      document.body.removeChild(textarea)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  if (generatedPw) {
    return (
      <div className="flex items-center gap-1">
        <code className="text-xs bg-muted px-1.5 py-0.5 rounded font-mono max-w-[120px] truncate" title={generatedPw}>
          {generatedPw}
        </code>
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={handleCopy} title="Copy password">
          {copied ? <Check className="h-3.5 w-3.5 text-green-600" /> : <Copy className="h-3.5 w-3.5" />}
        </Button>
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => setGeneratedPw(null)} title="Dismiss">
          <X className="h-3.5 w-3.5" />
        </Button>
      </div>
    )
  }

  return (
    <Button variant="outline" size="sm" className="h-7 text-xs" onClick={() => resetPw.mutate()} disabled={resetPw.isPending}>
      {resetPw.isPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <RotateCcw className="h-3.5 w-3.5" />}
      {t('users.actions.reset_password')}
    </Button>
  )
}


/* ─── Create User Dialog ─── */

function CreateUserDialog({ onClose, onSuccess }: { onClose: () => void; onSuccess: (username: string) => void }) {
  const { t } = useTranslation()
  const createUser = useCreateUser()
  const { showToast } = useToast()
  const [uN, setUN] = useState('')
  const [uFN, setUFN] = useState('')
  const [uP, setUP] = useState('')
  const [uEmail, setUEmail] = useState('')
  const [uR, setUR] = useState<UserRole>('viewer')
  const [copied, setCopied] = useState(false)

  const generatePassword = () => {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*'
    let pw = ''
    // Ensure at least one of each required type
    pw += 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'[Math.floor(Math.random() * 26)]
    pw += 'abcdefghijklmnopqrstuvwxyz'[Math.floor(Math.random() * 26)]
    pw += '0123456789'[Math.floor(Math.random() * 10)]
    pw += '!@#$%^&*'[Math.floor(Math.random() * 8)]
    for (let i = 0; i < 8; i++) pw += chars[Math.floor(Math.random() * chars.length)]
    // Shuffle
    pw = pw.split('').sort(() => Math.random() - 0.5).join('')
    setUP(pw)
  }

  const handleCopyPw = () => {
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(uP).then(() => {
        setCopied(true)
        setTimeout(() => setCopied(false), 2000)
      })
    } else {
      const textarea = document.createElement('textarea')
      textarea.value = uP
      textarea.style.position = 'fixed'
      textarea.style.opacity = '0'
      document.body.appendChild(textarea)
      textarea.select()
      document.execCommand('copy')
      document.body.removeChild(textarea)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!uN.trim() || !uFN.trim() || !uP.trim()) return
    createUser.mutate(
      { username: uN, name: uFN, password: uP, role: uR },
      {
        onSuccess: () => {
          // Save email if provided
          if (uEmail.trim()) {
            api.put(`/auth/users/${uN}/email`, { email: uEmail.trim() }).catch(() => {})
          }
          onSuccess(uN)
        },
        onError: (err: any) => showToast(err?.message || 'Failed to create user', 'error'),
      }
    )
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/50" onClick={onClose} />
      <div className="relative z-50 w-full max-w-lg rounded-lg border bg-background p-6 shadow-lg">
        <h3 className="text-lg font-semibold mb-1">{t('users.actions.add_user')}</h3>
        <p className="text-sm text-muted-foreground mb-4">{t('users.actions.create_user_desc')}</p>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-1">
              <label className="text-sm font-medium">{t('users.labels.username_field')}</label>
              <Input placeholder="jdoe" value={uN} onChange={e => setUN(e.target.value)} required />
            </div>
            <div className="space-y-1">
              <label className="text-sm font-medium">{t('users.labels.full_name')}</label>
              <Input placeholder="Jane Doe" value={uFN} onChange={e => setUFN(e.target.value)} required />
            </div>
          </div>
          <div className="space-y-1">
            <label className="text-sm font-medium">{t('common.labels.email')}</label>
            <Input type="email" placeholder="jane@example.com (optional)" value={uEmail} onChange={e => setUEmail(e.target.value)} />
          </div>
          <div className="space-y-1">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">{t('users.labels.password_field')}</label>
              <Button type="button" variant="ghost" size="sm" className="h-7 text-xs" onClick={generatePassword}>
                {t('users.labels.auto_generate')}
              </Button>
            </div>
            <div className="flex gap-2">
              <Input type="text" placeholder="Min 8 chars, upper, lower, digit, special" value={uP} onChange={e => setUP(e.target.value)} required className="font-mono text-sm" />
              {uP && (
                <Button type="button" variant="outline" size="icon" className="h-9 w-9 shrink-0" onClick={handleCopyPw} title="Copy password">
                  {copied ? <Check className="h-4 w-4 text-green-600" /> : <Copy className="h-4 w-4" />}
                </Button>
              )}
            </div>
            <p className="text-xs text-muted-foreground">{t('users.descriptions.password_requirements')}</p>
          </div>
          <div className="space-y-1">
            <label className="text-sm font-medium">{t('users.labels.system_role')}</label>
            <Select value={uR} onChange={e => setUR(e.target.value as UserRole)}>
              <option value="viewer">{t('common.roles.viewer')}</option>
              <option value="editor">{t('common.roles.editor')}</option>
              <option value="admin">{t('common.roles.admin')}</option>
            </Select>
          </div>
          {createUser.isError && <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{createUser.error.message}</div>}
          <div className="flex justify-end gap-2 pt-2">
            <Button type="button" variant="outline" onClick={onClose}>{t('common.actions.cancel')}</Button>
            <Button type="submit" disabled={createUser.isPending}>
              {createUser.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              {t('users.actions.create_user')}
            </Button>
          </div>
        </form>
      </div>
    </div>
  )
}
