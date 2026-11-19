# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Settings endpoints (admin only)."""
import logging

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from app.services import settings_service
from app.deps import require_admin

log = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("")
def list_settings(_admin: str = Depends(require_admin)):
    """Get all settings with current values and metadata (admin only)."""
    return settings_service.list_settings()


class UpdateSettingRequest(BaseModel):
    value: str


@router.put("/{key}")
def update_setting(key: str, request: UpdateSettingRequest, admin_user: str = Depends(require_admin)):
    """Update a setting value (admin only)."""
    # Validate key exists in registry
    valid_keys = {s["key"] for s in settings_service.SETTINGS_REGISTRY}
    if key not in valid_keys:
        raise HTTPException(status_code=400, detail=f"Unknown setting: {key}")

    value = request.value
    if key == settings_service.LOGS_BUCKET_KEY:
        # Accept a bucket name, s3:// URI or ARN; store just the bucket name.
        # It is interpolated into s3:// URIs the engine uses, so reject anything
        # that is not a plausible bucket name.
        value = settings_service.normalize_bucket_name(value)
        if not settings_service.is_valid_bucket_name(value):
            raise HTTPException(
                status_code=400,
                detail="Logs Bucket must be a valid S3 bucket name "
                       "(3-63 chars: lowercase letters, digits, '.' or '-'), "
                       "e.g. my-ima-logs")

    log.info("Setting '%s' updated by user '%s'", key, admin_user)
    result = settings_service.update_setting(key, value)

    # Push ENGINE_* settings to the engine so they take effect immediately.
    # The global region and the logs bucket each fan out to the several
    # engine-side keys derived from them.
    if key == settings_service.REGION_KEY:
        _push_settings_to_engine(settings_service.region_payload())
    elif key == settings_service.LOGS_BUCKET_KEY:
        _push_settings_to_engine(settings_service.logs_payload())
    elif key.startswith("ENGINE_"):
        _push_setting_to_engine(key, value)

    return result


@router.delete("/{key}")
def reset_setting(key: str, admin_user: str = Depends(require_admin)):
    """Reset a setting to its default (admin only)."""
    log.info("Setting '%s' reset to default by user '%s'", key, admin_user)
    settings_service.delete_setting(key)

    # Push the default/env value to the engine
    if key == settings_service.REGION_KEY:
        _push_settings_to_engine(settings_service.region_payload())
    elif key == settings_service.LOGS_BUCKET_KEY:
        # Cleared: nothing to push (an empty value would not overwrite the
        # engine's current locations anyway); the engine keeps its last values
        # until it restarts and re-reads its stack defaults.
        _push_settings_to_engine(settings_service.logs_payload())
    elif key.startswith("ENGINE_"):
        default_value = settings_service.get_setting(key) or ""
        _push_setting_to_engine(key, default_value)

    return {"detail": f"Setting '{key}' reset to default"}


def _push_setting_to_engine(key: str, value: str):
    """Push a single setting update to the engine (best effort)."""
    _push_settings_to_engine({key: value})


def _push_settings_to_engine(payload: dict[str, str]):
    """Push one or more setting updates to the engine (best effort)."""
    if not payload:
        return
    import httpx
    from app import engine_client
    engine_url = settings_service.get_setting("ENGINE_URL") or "http://localhost:8001"
    try:
        engine_client.post(f"{engine_url}/api/settings", json=payload, timeout=5.0)
    except httpx.RequestError as e:
        # Best effort — the engine may be down; it re-syncs settings on startup.
        log.debug("Could not push settings %s to engine: %s", sorted(payload), e)
