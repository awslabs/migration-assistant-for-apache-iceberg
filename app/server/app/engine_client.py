# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""HTTP helpers for calling the engine API.

Every engine route except ``/api/health`` requires a bearer token. The token is
read from the ``ENGINE_API_TOKEN`` environment variable only. It is
deliberately not a database setting, so it is never shown in the settings UI or
pushed to the engine.

These functions mirror ``httpx.get/post/put/delete`` and raise the same
``httpx`` exceptions, so callers keep their existing error handling.
"""
import logging
import os

import httpx

log = logging.getLogger(__name__)

# Name of the env var that holds the engine bearer token (not the token itself).
AUTH_ENV_VAR = "ENGINE_API_TOKEN"

_warned_missing_token = False


def auth_headers() -> dict:
    """Authorization header for engine calls (empty if no token is configured)."""
    global _warned_missing_token
    token = os.environ.get(AUTH_ENV_VAR, "").strip()
    if not token:
        if not _warned_missing_token:
            log.warning("%s is not set; engine requests will be rejected", AUTH_ENV_VAR)
            _warned_missing_token = True
        return {}
    return {"Authorization": f"Bearer {token}"}


def _request(method: str, url: str, **kwargs) -> httpx.Response:
    headers = {**auth_headers(), **(kwargs.pop("headers", None) or {})}
    return httpx.request(method, url, headers=headers, **kwargs)


def get(url: str, **kwargs) -> httpx.Response:
    return _request("GET", url, **kwargs)


def post(url: str, **kwargs) -> httpx.Response:
    return _request("POST", url, **kwargs)


def put(url: str, **kwargs) -> httpx.Response:
    return _request("PUT", url, **kwargs)


def delete(url: str, **kwargs) -> httpx.Response:
    return _request("DELETE", url, **kwargs)
