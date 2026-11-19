# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Progress API routes."""

from fastapi import APIRouter
from pydantic import BaseModel

from ...db import ProgressRepo, PipelineRepo

router = APIRouter(prefix="/progress", tags=["progress"])
progress_repo = ProgressRepo()
pipeline_repo = PipelineRepo()


class UpsertProgressRequest(BaseModel):
    partition_key: str
    partition_value: str
    objects_total: int = 0
    bytes_total: int = 0


class CompleteProgressRequest(BaseModel):
    objects_processed: int = 0
    bytes_processed: int = 0
    manifest_file: str = ""
    snapshot_id: int = 0


@router.get("/{pipeline_id}")
def list_progress(pipeline_id: str, status: str = None):
    return progress_repo.list_by_pipeline(pipeline_id, status=status)


@router.post("/{pipeline_id}")
def upsert_progress(pipeline_id: str, req: UpsertProgressRequest):
    return progress_repo.upsert(
        pipeline_id, req.partition_key, req.partition_value,
        req.objects_total, req.bytes_total,
    )


@router.post("/{pipeline_id}/partition/{progress_id}/start")
def start_partition(pipeline_id: str, progress_id: int):
    progress_repo.mark_running(progress_id)
    return {"status": "running"}


@router.post("/{pipeline_id}/partition/{progress_id}/complete")
def complete_partition(pipeline_id: str, progress_id: int,
                       req: CompleteProgressRequest):
    progress_repo.mark_completed(
        progress_id, req.objects_processed, req.bytes_processed,
        req.manifest_file, req.snapshot_id,
    )
    # Recompute sync status
    sync = progress_repo.compute_sync_status(pipeline_id)
    pipeline_repo.update_sync_status(pipeline_id, sync)
    return {"status": "completed", "sync_status": sync}


@router.post("/{pipeline_id}/partition/{progress_id}/fail")
def fail_partition(pipeline_id: str, progress_id: int,
                   error: str = ""):
    progress_repo.mark_failed(progress_id, error)
    sync = progress_repo.compute_sync_status(pipeline_id)
    pipeline_repo.update_sync_status(pipeline_id, sync)
    return {"status": "failed", "sync_status": sync}


@router.get("/{pipeline_id}/sync-status")
def get_sync_status(pipeline_id: str):
    sync = progress_repo.compute_sync_status(pipeline_id)
    return {"pipeline_id": pipeline_id, "sync_status": sync}
