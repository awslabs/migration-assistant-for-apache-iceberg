// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

export type TableFormat = 'csv' | 'avro' | 'parquet' | 'json' | 'orc' | 'hudi' | 'delta' | 'iceberg'
export type PlanStatus = 'draft' | 'ready' | 'staged' | 'in_progress' | 'paused' | 'cancelled' | 'completed' | 'failed'
export type UserRole = 'admin' | 'editor' | 'viewer'

export interface TableInfo {
  database: string; table_name: string; format: TableFormat
  location: string; aws_account: string; size_bytes: number
  partitioned: boolean; owner: string
}
export interface PlanTable {
  source_database: string; source_table: string
  target_database: string; target_table: string
  migration_type: string; status: string
  // Spec fields
  source_format: string | null
  source_location: string | null
  source_account: string | null
  source_partitioned: boolean
  source_partition_keys: { name: string; type: string }[] | null
  source_compression: string | null
  target_type: string | null
  target_location: string | null
  target_bucket_arn: string | null
  partition_spec: { source_column: string; transform: string; num_buckets?: number; width?: number }[] | null
  sort_order: { column: string; direction?: string; null_order?: string }[] | null
  table_properties: Record<string, string> | null
  pipeline_id: string | null
  // Agent reasoning
  recommended_type: string | null
  recommended_batch_size: number | null
  estimated_size_gb: number | null
  estimated_cost_usd: number | null
  estimated_time_min: number | null
  analysis_summary: string | null
  analysis_status: string
  error_message: string | null
}
export interface MigrationPlan {
  id: string; name: string; description: string; tables: PlanTable[]
  status: PlanStatus; target: string; migration_types: string[]
  execution_engine: string; swap_tables: boolean
  table_bucket_arn: string
  owners: string[]; created_at: string; created_by: string
}
export interface CreatePlanRequest {
  name: string; description: string; target: string; migration_types: string[]
  execution_engine: string; swap_tables: boolean
  table_bucket_arn: string
  owners: string[]
}
export interface UserInfo { username: string; name: string; role: UserRole }
export interface CreateUserRequest { username: string; name: string; password: string; role: UserRole }
export type GroupMemberRole = 'manage' | 'view'
export interface GroupInfo { name: string; description: string; members: GroupMember[] }
export interface GroupMember { username: string; role: GroupMemberRole; name: string }
export interface CreateGroupRequest { name: string; description: string }
export interface AddGroupMemberRequest { username: string; role: GroupMemberRole }

export interface SourceInfo {
  id: string; name: string; source_type: string
  aws_account_id: string; iam_role_arn: string; default_group: string
  db_filter_mode: string; db_filter_list: string; last_refreshed_at: string | null
  last_refresh_summary: string | null
  created_at: string; created_by: string
}
export interface CreateSourceRequest {
  name: string; source_type: string
  aws_account_id: string; iam_role_arn: string; default_group: string
  db_filter_mode: string; db_filter_list: string
}
export interface RefreshLogEntry {
  id: number; source_id: string
  database_name: string; table_name: string
  error_type: string; error_message: string; created_at: string
}

export interface PipelineDetail {
  id: string
  source_database: string; source_table: string
  target_database: string; target_table: string
  status: string; sync_status: string
  migration_type: string; execution_engine: string
  source_format: string; source_location: string
  source_partitioned: number
  target_type: string; target_bucket_arn: string
  emr_job_run_id: string; error_message: string
  partitions_completed: number; partitions_total: number
  bytes_processed: number; bytes_total: number
  created_at: string; started_at: string; completed_at: string
  updated_at: string; spec_json: string
}

export interface PipelineBatch {
  pipeline_id: string; batch_number: number
  status: string; partitions: string
  partition_count: number; bytes_total: number; bytes_processed: number
  emr_job_run_id: string; error_message: string
  created_at: string; started_at: string; completed_at: string
}

export interface PipelineBatchResponse {
  pipeline_id: string
  summary: Record<string, { count: number; partitions: number }>
  batches: PipelineBatch[]
}

// ── On-demand data validation (source-vs-target checks run after migration) ──

export type ValidationTier = 'basic' | 'advanced'
export type ValidationOverallStatus = 'running' | 'passed' | 'failed' | 'error'
export type ValidationCheckStatus = 'passed' | 'failed' | 'error' | 'skipped'

export interface ValidationCheck {
  check_id: string
  name: string
  status: ValidationCheckStatus
  source_value: string | null
  target_value: string | null
  detail: string
}

export interface ValidationResult {
  run_id: string
  pipeline_id: string
  tier: ValidationTier
  engine: string
  overall_status: ValidationOverallStatus
  results: ValidationCheck[]
  run_at: string
  completed_at: string | null
  triggered_by: string
}

export interface ValidationStarted {
  pipeline_id: string
  run_id: string
  engine: string
}

export interface ValidationSkipped {
  pipeline_id: string
  source_database?: string
  source_table?: string
  status: string
  reason: string
}

export interface ValidationRunResponse {
  started: ValidationStarted[]
  errors: { pipeline_id: string; error: string }[]
  skipped: ValidationSkipped[]
}

export type ValidationState = 'none' | 'passed' | 'failed' | 'stale'
export interface PipelineValidationStatus {
  pipeline_id: string
  state: ValidationState
  validated_at?: string | null
  last_batch_at?: string | null
}
export type PlanValidationStatus = Record<string, PipelineValidationStatus>
