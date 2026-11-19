# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Database layer for the engine."""

import os

_DB_BACKEND = os.getenv("ENGINE_DB_BACKEND", "sql")  # "sql" or "dynamodb"
_TRACKING_BACKEND = os.getenv("ENGINE_TRACKING_BACKEND", "dynamodb")

if _DB_BACKEND == "dynamodb":
    from .dynamo import PipelineRepo, BatchRepo, ProgressRepo, create_tables_if_not_exist

    def init_db():
        create_tables_if_not_exist()
        print("Engine DB initialized (dynamodb)")

    def get_conn():
        return None  # Not used with DynamoDB
else:
    from .connection import get_conn, init_db
    from .pipeline import PipelineRepo
    from .progress import ProgressRepo
    from .batch import BatchRepo

# Object tracking always uses its own backend setting
if _TRACKING_BACKEND == "dynamodb":
    from .object_tracking_dynamo import ObjectTrackingRepo
else:
    from .object_tracking import ObjectTrackingRepo

from .costs import CostRepo
from .validation_results import ValidationRepo

__all__ = [
    "get_conn", "init_db",
    "PipelineRepo", "ObjectTrackingRepo", "ProgressRepo", "BatchRepo",
    "CostRepo", "ValidationRepo",
]
