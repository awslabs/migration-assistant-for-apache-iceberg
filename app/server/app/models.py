# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pydantic models for the app API."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class TableFormat(str, Enum):
    CSV = "csv"
    AVRO = "avro"
    PARQUET = "parquet"
    JSON = "json"
    ORC = "orc"
    HUDI = "hudi"
    DELTA = "delta"
    ICEBERG = "iceberg"


class TableInfo(BaseModel):
    database: str
    table_name: str
    format: TableFormat
    location: str
    aws_account: str
    size_bytes: int = 0
    partitioned: bool = False
    owner: str = "data-engineering"


class PlanStatus(str, Enum):
    DRAFT = "draft"
    READY = "ready"
    STAGED = "staged"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


class PlanTable(BaseModel):
    source_database: str = Field(min_length=1)
    source_table: str = Field(min_length=1)
    target_database: str = Field(min_length=1)
    target_table: str = Field(min_length=1)
    migration_type: str = "rewrite"
    status: str = "pending"
    # Spec fields — populated from agent analysis or manual input
    source_format: Optional[str] = None
    source_location: Optional[str] = None
    source_account: Optional[str] = None
    source_partitioned: bool = False
    source_partition_keys: Optional[list[dict]] = None
    source_compression: Optional[str] = None
    target_type: Optional[str] = None
    target_location: Optional[str] = None
    target_bucket_arn: Optional[str] = None
    partition_spec: Optional[list[dict]] = None
    sort_order: Optional[list[dict]] = None
    table_properties: Optional[dict] = None
    pipeline_id: Optional[str] = None
    # Agent reasoning — not part of the spec
    recommended_type: Optional[str] = None
    recommended_batch_size: Optional[int] = None
    estimated_size_gb: Optional[float] = None
    estimated_cost_usd: Optional[float] = None
    estimated_time_min: Optional[float] = None
    analysis_summary: Optional[str] = None
    analysis_status: str = "pending"
    error_message: Optional[str] = None
    # Schema evolution fields
    schema_compatible: Optional[bool] = None
    schema_issues: Optional[list[dict]] = None


class CreatePlanRequest(BaseModel):
    name: str = Field(min_length=1)
    description: str = ""
    target: str = "s3_tables"
    migration_types: list[str] = ["rewrite"]
    execution_engine: str = "athena"
    swap_tables: bool = False
    max_table_size_gb: Optional[int] = None
    table_bucket_arn: str = ""
    owners: list[str] = []


class MigrationPlan(BaseModel):
    id: str
    name: str
    description: str
    tables: list[PlanTable]
    status: PlanStatus = PlanStatus.DRAFT
    target: str = "s3_tables"
    migration_types: list[str] = ["rewrite"]
    execution_engine: str = "athena"
    swap_tables: bool = False
    table_bucket_arn: str = ""
    owners: list[str] = []
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = "admin"


class UserRole(str, Enum):
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class UserInfo(BaseModel):
    username: str
    name: str
    role: UserRole


class CreateUserRequest(BaseModel):
    username: str = Field(min_length=1)
    name: str = Field(min_length=1)
    password: str = Field(min_length=1)
    role: UserRole = UserRole.VIEWER


class AddToPlanRequest(BaseModel):
    source_database: str = Field(min_length=1)
    source_table: str = Field(min_length=1)
    target_database: str = Field(min_length=1)
    target_table: str = Field(min_length=1)


class GroupInfo(BaseModel):
    name: str
    description: str = ""
    members: list[dict] = []  # [{username, role}]


class CreateGroupRequest(BaseModel):
    name: str = Field(min_length=1)
    description: str = ""


class GroupMemberRole(str, Enum):
    MANAGE = "manage"
    VIEW = "view"


class AddGroupMemberRequest(BaseModel):
    username: str = Field(min_length=1)
    role: GroupMemberRole = GroupMemberRole.VIEW


class SourceInfo(BaseModel):
    id: str
    name: str
    source_type: str = "aws_glue_data_catalog"
    aws_account_id: str
    iam_role_arn: str = ""
    default_group: str = ""
    db_filter_mode: str = "exclude"
    db_filter_list: str = ""
    last_refreshed_at: Optional[str] = None
    last_refresh_summary: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    created_by: str = "admin"


class CreateSourceRequest(BaseModel):
    name: str = Field(min_length=1)
    source_type: str = "aws_glue_data_catalog"
    aws_account_id: str = Field(min_length=1)
    iam_role_arn: str = ""
    default_group: str = ""
    db_filter_mode: str = "exclude"  # "include" or "exclude"
    db_filter_list: str = ""


class UpdateSourceRequest(BaseModel):
    iam_role_arn: str = ""
    default_group: str = ""


class RefreshLogEntry(BaseModel):
    id: int
    source_id: str
    database_name: Optional[str] = ""
    table_name: Optional[str] = ""
    error_type: str
    error_message: str
    created_at: str
