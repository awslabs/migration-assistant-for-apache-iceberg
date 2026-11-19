# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Group management endpoints."""
from fastapi import APIRouter, HTTPException, Depends
from app.deps import require_admin
from app.models import GroupInfo, CreateGroupRequest, AddGroupMemberRequest
from app.services import group_service

router = APIRouter(prefix="/groups", tags=["groups"])

@router.get("", response_model=list[GroupInfo])
def list_groups():
    return group_service.list_groups()

@router.post("", response_model=GroupInfo, status_code=201, dependencies=[Depends(require_admin)])
def create_group(request: CreateGroupRequest):
    try:
        return group_service.create_group(request)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.post("/{group_name}/members", response_model=GroupInfo, dependencies=[Depends(require_admin)])
def add_member(group_name: str, request: AddGroupMemberRequest):
    try:
        result = group_service.add_member(group_name, request)
        if not result:
            raise HTTPException(status_code=404, detail="Group not found")
        return result
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

@router.delete("/{group_name}/members/{username}", response_model=GroupInfo, dependencies=[Depends(require_admin)])
def remove_member(group_name: str, username: str):
    result = group_service.remove_member(group_name, username)
    if not result:
        raise HTTPException(status_code=404, detail="Group not found")
    return result

@router.delete("/{group_name}", dependencies=[Depends(require_admin)])
def delete_group(group_name: str):
    try:
        if not group_service.delete_group(group_name):
            raise HTTPException(status_code=404, detail="Group not found")
        return {"detail": "Group deleted"}
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
