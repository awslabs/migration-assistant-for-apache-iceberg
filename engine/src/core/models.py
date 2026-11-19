# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Data models for the engine."""

from enum import Enum


class MigrationType(str, Enum):
    REWRITE = "rewrite"
    INCREMENTAL = "incremental"
    MORPH = "morph"
    ADD_FILES = "add_files"
    # Managed Migration — delegates Hive-Parquet → S3 Tables to the AWS
    # S3 Tables Migration API. Reconciled by the strategy itself rather than
    # the state machine.
    S3TABLES_MANAGED = "s3tables_managed"


class MigrationStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TargetType(str, Enum):
    S3_TABLES = "s3_tables"
    S3_ICEBERG = "s3_iceberg"
