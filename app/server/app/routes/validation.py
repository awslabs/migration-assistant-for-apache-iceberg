# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Migration validation endpoints."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import validation_service, table_service

router = APIRouter(prefix="/validation", tags=["validation"])


@router.get("/rules")
def get_rules():
    """Get all migration eligibility rules."""
    return validation_service.get_all_rules()


@router.get("/eligible/{fmt}")
def get_eligible_types(fmt: str):
    """Get eligible migration types for a format."""
    types = validation_service.get_eligible_types(fmt)
    return {"format": fmt, "eligible_types": types}


@router.get("/table/{database}/{table_name}/{migration_type}")
def validate_table(database: str, table_name: str, migration_type: str):
    """Validate if a specific table is eligible for a migration type."""
    table = table_service.get_table(database, table_name)
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    return validation_service.validate_table(table, migration_type)


class BatchValidationRequest(BaseModel):
    tables: list[dict]  # [{database, table_name}]
    migration_types: list[str]


@router.post("/batch")
def validate_batch(request: BatchValidationRequest):
    """Validate a batch of tables against plan migration types."""
    table_infos = []
    for t in request.tables:
        info = table_service.get_table(t["database"], t["table_name"])
        if info:
            table_infos.append(info)
    return validation_service.validate_tables_for_plan(
        table_infos, request.migration_types
    )
