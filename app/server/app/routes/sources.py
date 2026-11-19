# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Source management endpoints."""
from fastapi import APIRouter, HTTPException, Depends
from app.deps import require_admin
from app.models import SourceInfo, CreateSourceRequest, UpdateSourceRequest, RefreshLogEntry
from app.services import source_service

router = APIRouter(prefix="/sources", tags=["sources"])


@router.get("", response_model=list[SourceInfo], dependencies=[Depends(require_admin)])
def list_sources():
    return source_service.list_sources()


@router.get("/{source_id}", response_model=SourceInfo, dependencies=[Depends(require_admin)])
def get_source(source_id: str):
    source = source_service.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


@router.post("", response_model=SourceInfo, status_code=201, dependencies=[Depends(require_admin)])
def create_source(request: CreateSourceRequest):
    return source_service.create_source(request)


@router.put("/{source_id}", response_model=SourceInfo, dependencies=[Depends(require_admin)])
def update_source(source_id: str, request: UpdateSourceRequest):
    source = source_service.update_source(source_id, request.iam_role_arn, request.default_group)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


@router.post("/{source_id}/refresh", dependencies=[Depends(require_admin)])
def refresh_source(source_id: str):
    try:
        result = source_service.refresh_source(source_id)
        if not result:
            raise HTTPException(status_code=404, detail="Source not found")
        return result
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Refresh failed: {str(e)}")


@router.get("/{source_id}/refresh-log", response_model=list[RefreshLogEntry], dependencies=[Depends(require_admin)])
def get_refresh_log(source_id: str):
    source = source_service.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source_service.get_refresh_log(source_id)


@router.delete("/{source_id}", dependencies=[Depends(require_admin)])
def delete_source(source_id: str):
    if not source_service.delete_source(source_id):
        raise HTTPException(status_code=404, detail="Source not found")
    return {"detail": "Source deleted"}
