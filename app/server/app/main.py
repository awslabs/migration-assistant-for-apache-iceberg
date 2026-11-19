# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Migration Assistant app - FastAPI backend."""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from slowapi.errors import RateLimitExceeded
from app.database import init_db
from app.deps import require_usable_session
from app.rate_limit import limiter, rate_limit_exceeded_handler
from app.services.plan_service import EngineActionError
from app.routes import auth, groups, plans, sources, tables, validation, assistant, schema, pipeline, settings, validation_data

log = logging.getLogger(__name__)

# CORS: Allowed origins configured via CORS_ALLOWED_ORIGINS env var.
# In production, set this to the actual frontend domain(s), comma-separated.
# Example: CORS_ALLOWED_ORIGINS=https://ima.example.com,https://app.example.com
# In development, defaults to common local dev servers.
_DEFAULT_DEV_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:3000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:3000",
]
CORS_ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
] or _DEFAULT_DEV_ORIGINS

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Push all ENGINE_* settings to the engine on startup (best effort)
    _sync_settings_to_engine()
    yield


def _sync_settings_to_engine():
    """Push all ENGINE_* settings from DB to the engine."""
    import httpx
    from app import engine_client
    from app.services.settings_service import (
        get_setting, region_payload, logs_payload, SETTINGS_REGISTRY)
    engine_url = get_setting("ENGINE_URL") or "http://localhost:8001"
    payload = {}
    for s in SETTINGS_REGISTRY:
        if s["key"].startswith("ENGINE_") and s["key"] != "ENGINE_URL":
            val = get_setting(s["key"])
            if val:
                payload[s["key"]] = val
    # The single global region fans out to every engine-side per-service region
    # key (Athena, EMR, tracking, S3 Tables, XTable), which are no longer in the
    # registry individually.
    payload.update(region_payload())
    # Likewise the single logs bucket fans out to the Athena output location,
    # EMR log URI and migration-reports bucket.
    payload.update(logs_payload())
    if payload:
        try:
            engine_client.post(f"{engine_url}/api/settings", json=payload, timeout=5.0)
        except httpx.RequestError as e:
            # Best effort on startup — the engine may not be running yet; it
            # picks up settings on its own startup sync.
            log.debug("Could not push settings to engine at startup: %s", e)

app = FastAPI(title="Migration Assistant for Apache Iceberg", version="0.1.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)


@app.exception_handler(EngineActionError)
def engine_action_error_handler(request: Request, exc: EngineActionError):
    """An engine action was not confirmed: report it instead of a 500."""
    return JSONResponse(status_code=409, content={"detail": str(exc)})
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    # Session auth is carried by the HttpOnly `ima_session` cookie (see deps.py),
    # sent automatically with allow_credentials=True. No custom identity header.
    allow_headers=["Content-Type"],
)
# ── Deny-by-default authorization ────────────────────────────────────────
# Every router below requires an authenticated caller with a *usable* session:
# authenticated AND not in a forced password-change state. The `auth`
# router manages its own per-route auth because /auth/login must stay public
# and /auth/change-password must remain reachable while a forced change is
# pending. Health endpoints are defined directly on `app` and are public.
# Individual routes add finer-grained checks (require_admin, require_plan_*).
_authn = [Depends(require_usable_session)]

app.include_router(auth.router, prefix="/api")
app.include_router(groups.router, prefix="/api", dependencies=_authn)
app.include_router(tables.router, prefix="/api", dependencies=_authn)
app.include_router(plans.router, prefix="/api", dependencies=_authn)
app.include_router(sources.router, prefix="/api", dependencies=_authn)
app.include_router(validation.router, prefix="/api", dependencies=_authn)
app.include_router(assistant.router, prefix="/api", dependencies=_authn)
app.include_router(schema.router, prefix="/api", dependencies=_authn)
app.include_router(pipeline.router, prefix="/api", dependencies=_authn)
app.include_router(settings.router, prefix="/api", dependencies=_authn)
app.include_router(validation_data.router, prefix="/api", dependencies=_authn)

@app.get("/api/health")
def health_check():
    return {"status": "ok"}


@app.get("/api/health/engine")
def engine_health():
    """Check if the engine is reachable."""
    import httpx
    from app.services.settings_service import get_setting
    engine_url = get_setting("ENGINE_URL") or "http://localhost:8001"
    try:
        resp = httpx.get(f"{engine_url}/api/health", timeout=5.0)
        if resp.status_code == 200:
            return {"status": "ok", "engine": "connected"}
        return {"status": "degraded", "engine": "unhealthy", "detail": f"Status {resp.status_code}"}
    except Exception as e:
        return {"status": "degraded", "engine": "unreachable", "detail": str(e)[:200]}

# Serve frontend static files (production — when built into container)
_STATIC_DIR = Path(__file__).parent.parent / "static"
if _STATIC_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=str(_STATIC_DIR / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve the SPA index.html for all non-API routes."""
        file_path = _STATIC_DIR / full_path
        if file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_STATIC_DIR / "index.html"))
