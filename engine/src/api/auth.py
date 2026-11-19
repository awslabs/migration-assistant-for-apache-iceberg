# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bearer-token authentication for the engine API.

The engine is a server-to-server service. Every route except the health check
requires ``Authorization: Bearer <token>`` where the token matches the
``ENGINE_API_TOKEN`` environment variable (provisioned from a secret store,
never committed).

The check fails closed: if no token is configured the engine rejects every
protected request instead of running unauthenticated.
"""
import hmac
import logging
import os

from fastapi import HTTPException, Request

log = logging.getLogger(__name__)

# Name of the env var that holds the bearer token (not the token itself).
AUTH_ENV_VAR = "ENGINE_API_TOKEN"


def _configured_token() -> str:
    return os.environ.get(AUTH_ENV_VAR, "").strip()


def require_api_token(request: Request) -> None:
    """FastAPI dependency that rejects requests without a valid bearer token."""
    expected = _configured_token()
    if not expected:
        log.error("%s is not set; rejecting request to %s", AUTH_ENV_VAR, request.url.path)
        raise HTTPException(status_code=503, detail="Engine API token is not configured")

    header = request.headers.get("authorization", "")
    scheme, _, presented = header.partition(" ")
    if scheme.lower() != "bearer" or not presented:
        raise HTTPException(
            status_code=401, detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"})

    if not hmac.compare_digest(presented.strip().encode(), expected.encode()):
        raise HTTPException(
            status_code=401, detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"})
