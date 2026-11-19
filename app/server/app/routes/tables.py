# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Table browsing endpoints."""
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from app.deps import require_admin
from app.models import TableInfo
from app.services import table_service

router = APIRouter(prefix="/tables", tags=["tables"])

@router.get("", response_model=list[TableInfo])
def list_tables(database: Optional[str] = None, format: Optional[str] = None):
    return table_service.list_tables(database=database, format=format)

@router.get("/{database}/{table_name}", response_model=TableInfo)
def get_table(database: str, table_name: str):
    table = table_service.get_table(database, table_name)
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")
    return table

class BulkTableRequest(BaseModel):
    tables: list[dict]  # [{database, table_name}]

class ChangeOwnerRequest(BaseModel):
    tables: list[dict]
    owner: str

@router.post("/bulk-delete", dependencies=[Depends(require_admin)])
def bulk_delete(request: BulkTableRequest):
    count = table_service.delete_tables(request.tables)
    return {"deleted": count}

@router.post("/change-owner", dependencies=[Depends(require_admin)])
def change_owner(request: ChangeOwnerRequest):
    count = table_service.update_owner(request.tables, request.owner)
    return {"updated": count}
