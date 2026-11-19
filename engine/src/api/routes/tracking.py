# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Object tracking API routes — receives S3 events."""

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

from ...db import ObjectTrackingRepo

router = APIRouter(prefix="/tracking", tags=["tracking"])
repo = ObjectTrackingRepo()


class S3EventRequest(BaseModel):
    pipeline_id: str
    bucket: str
    key: str
    event_type: str  # "PUT" or "DELETE"
    size_bytes: int = 0
    etag: str = ""
    last_modified: str = ""
    partition_values: Optional[dict] = None


class BatchS3EventRequest(BaseModel):
    events: list[S3EventRequest]


@router.post("/event")
def record_event(req: S3EventRequest):
    """Record a single S3 object event."""
    return repo.record_event(
        pipeline_id=req.pipeline_id,
        bucket=req.bucket,
        key=req.key,
        event_type=req.event_type,
        size_bytes=req.size_bytes,
        etag=req.etag,
        last_modified=req.last_modified,
        partition_values=req.partition_values,
    )


@router.post("/events")
def record_events(req: BatchS3EventRequest):
    """Record a batch of S3 object events (from Lambda)."""
    results = []
    for event in req.events:
        r = repo.record_event(
            pipeline_id=event.pipeline_id,
            bucket=event.bucket,
            key=event.key,
            event_type=event.event_type,
            size_bytes=event.size_bytes,
            etag=event.etag,
            last_modified=event.last_modified,
            partition_values=event.partition_values,
        )
        results.append(r)
    return {"recorded": len(results)}


@router.get("/{pipeline_id}/pending")
def get_pending(pipeline_id: str, limit: int = 1000):
    return repo.get_pending(pipeline_id, limit=limit)


@router.get("/{pipeline_id}/stats")
def get_stats(pipeline_id: str):
    return repo.get_stats(pipeline_id)


@router.post("/{event_id}/processed")
def mark_processed(event_id: int):
    repo.mark_processed(event_id)
    return {"status": "processed"}


@router.post("/{event_id}/failed")
def mark_failed(event_id: int):
    repo.mark_failed(event_id)
    return {"status": "failed"}
