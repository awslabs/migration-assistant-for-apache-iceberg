// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast } from '@/components/ui/toast'
import { useCreateSource } from '@/hooks/use-sources'
import { useGroups } from '@/hooks/use-groups'
import { useNavigate } from '@tanstack/react-router'
import { useState } from 'react'
import { Loader2, Database, Check } from 'lucide-react'
import { useTranslation } from 'react-i18next'

export function SourceCreatePage() {
  const { t } = useTranslation()
  const createSource = useCreateSource()
  const { data: groups } = useGroups()
  const navigate = useNavigate()
  const { showToast } = useToast()
  const [name, setName] = useState('')
  const [sourceType, setSourceType] = useState('aws_glue_data_catalog')
  const [accountId, setAccountId] = useState('')
  const [iamRoleArn, setIamRoleArn] = useState('')
  const [defaultGroup, setDefaultGroup] = useState('')
  const [dbFilterMode, setDbFilterMode] = useState('exclude')
  const [dbFilterList, setDbFilterList] = useState('')

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (!name.trim() || !accountId.trim()) return
    createSource.mutate(
      { name, source_type: sourceType, aws_account_id: accountId, iam_role_arn: iamRoleArn, default_group: defaultGroup, db_filter_mode: dbFilterMode, db_filter_list: dbFilterList },
      { onSuccess: () => { showToast(`Source "${name}" added successfully`, 'success'); navigate({ to: '/sources', search: { created: 'true' } }) }, onError: (err: any) => showToast(err?.message || 'Failed to add source', 'error') }
    )
  }

  return (
    <div className="max-w-3xl mx-auto space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>{t('sources.actions.add_source')}</CardTitle>
          <CardDescription>{t('sources.actions.connect_source')}</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-8">
            <div className="space-y-2">
              <label className="text-sm font-medium">{t('sources.labels.source_name')}</label>
              <p className="text-xs text-muted-foreground">{t('sources.labels.source_friendly_name')}</p>
              <Input value={name} onChange={e => setName(e.target.value)} placeholder="Production Data Lake" required />
            </div>

            <div className="space-y-3">
              <label className="text-sm font-medium">{t('sources.labels.source_type')}</label>
              <p className="text-xs text-muted-foreground">{t('sources.actions.choose_catalog_type')}</p>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                <button type="button" onClick={() => setSourceType('aws_glue_data_catalog')}
                  className={`relative flex flex-col items-center gap-3 rounded-lg border-2 p-6 transition-all cursor-pointer ${
                    sourceType === 'aws_glue_data_catalog' ? 'border-primary bg-primary/5' : 'border-muted hover:border-muted-foreground/30'}`}>
                  {sourceType === 'aws_glue_data_catalog' && (
                    <div className="absolute top-2 right-2 rounded-full bg-primary p-0.5"><Check className="h-3 w-3 text-primary-foreground" /></div>
                  )}
                  <Database className="h-8 w-8 text-primary" />
                  <div className="text-center">
                    <p className="text-sm font-medium">{t('sources.labels.aws_glue_data_catalog')}</p>
                    <p className="text-xs text-muted-foreground mt-1">{t('sources.actions.discover_tables')}</p>
                  </div>
                </button>
                <div className="flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed border-muted p-6 opacity-50">
                  <Database className="h-8 w-8 text-muted-foreground" />
                  <p className="text-xs text-muted-foreground">{t('sources.labels.more_sources_soon')}</p>
                </div>
              </div>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">{t('aws.labels.aws_account_id')}</label>
              <p className="text-xs text-muted-foreground">{t('aws.descriptions.account_id_desc')}</p>
              <Input value={accountId} onChange={e => setAccountId(e.target.value)} placeholder="123456789012" required className="max-w-xs font-mono" />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">{t('aws.labels.iam_role_arn')}</label>
              <p className="text-xs text-muted-foreground">
                {t('sources.descriptions.iam_role_arn_desc')}
              </p>
              <Input value={iamRoleArn} onChange={e => setIamRoleArn(e.target.value)} placeholder="arn:aws:iam::123456789012:role/GlueCatalogReadRole" className="font-mono text-xs" />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">{t('sources.labels.default_ownership')}</label>
              <p className="text-xs text-muted-foreground">{t('sources.descriptions.default_ownership_desc')}</p>
              <select value={defaultGroup} onChange={e => setDefaultGroup(e.target.value)}
                className="flex h-9 w-full max-w-xs rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring">
                <option value="">{t('common.labels.none_type')}</option>
                {groups?.map(g => <option key={g.name} value={g.name}>{g.name}</option>)}
              </select>
            </div>

            <div className="space-y-3">
              <label className="text-sm font-medium">{t('sources.labels.database_filter')}</label>
              <p className="text-xs text-muted-foreground">{t('sources.actions.choose_db_filter')}</p>
              <div className="flex gap-4">
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input type="radio" name="filterMode" value="exclude" checked={dbFilterMode === 'exclude'} onChange={() => setDbFilterMode('exclude')} className="rounded" />
                  {t('sources.actions.exclude_databases')}
                </label>
                <label className="flex items-center gap-2 text-sm cursor-pointer">
                  <input type="radio" name="filterMode" value="include" checked={dbFilterMode === 'include'} onChange={() => setDbFilterMode('include')} className="rounded" />
                  {t('sources.actions.include_only')}
                </label>
              </div>
              <Input value={dbFilterList} onChange={e => setDbFilterList(e.target.value)}
                placeholder={dbFilterMode === 'exclude' ? 'temp_db, staging, test_db' : 'analytics, warehouse, ml_features'}
              />
              <p className="text-xs text-muted-foreground">
                {dbFilterMode === 'exclude'
                  ? t('sources.descriptions.db_filter_exclude_desc')
                  : t('sources.descriptions.db_filter_include_desc')}
              </p>
            </div>

            {createSource.isError && (
              <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">{createSource.error.message}</div>
            )}

            <div className="flex gap-2 pt-2">
              <Button type="submit" disabled={createSource.isPending || !name.trim() || !accountId.trim()}>
                {createSource.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
                {t('sources.actions.add_source')}
              </Button>
              <Button type="button" variant="outline" onClick={() => navigate({ to: '/sources', search: { created: undefined } })}>{t('common.actions.cancel')}</Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
