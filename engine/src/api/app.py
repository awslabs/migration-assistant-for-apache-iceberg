# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Engine API — FastAPI application."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from ..db import init_db
from ..core.reconciler import reconcile_loop
from .auth import require_api_token
from .routes import pipeline, tracking, progress, validation

log = logging.getLogger(__name__)


def _configure_logging():
    level = os.getenv("ENGINE_LOG_LEVEL", "INFO").upper()
    log_format = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format=log_format,
        datefmt=date_format,
    )

    # Write logs to file if ENGINE_LOG_FILE is set
    log_file = os.getenv("ENGINE_LOG_FILE", "")
    if log_file:
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setLevel(getattr(logging, level, logging.INFO))
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
        logging.getLogger().addHandler(file_handler)

    # Quiet down noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()
    init_db()
    log.info("Engine API starting up")

    # Log existing pipeline summary
    from ..db import PipelineRepo
    repo = PipelineRepo()
    all_pipelines = repo.list_all()
    if all_pipelines:
        status_counts = {}
        for p in all_pipelines:
            status_counts[p["status"]] = status_counts.get(p["status"], 0) + 1
        log.info("Pipeline summary: %d total — %s",
                 len(all_pipelines),
                 ", ".join(f"{s}={c}" for s, c in sorted(status_counts.items())))
        for p in all_pipelines:
            log.info("  ├─ %s | %s.%s → %s.%s | status=%s | type=%s",
                     p["id"][:8],
                     p.get("source_database", "?"), p.get("source_table", "?"),
                     p.get("target_database", ""),
                     p.get("target_table", ""),
                     p.get("status", "?"), p.get("migration_type", "?"))
    else:
        log.info("No existing pipelines found")

    # Spawn the background reconcile loop
    task = asyncio.create_task(reconcile_loop())
    log.info("Background reconcile loop spawned")

    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        log.info("Engine API shut down")


app = FastAPI(
    title="IMA Engine",
    description="Iceberg table conversion engine — rewrite, incremental, and morph strategies",
    version="0.1.0",
    lifespan=lifespan,
)

# NOTE: No CORS middleware here by design. The Engine API is an internal,
# server-to-server service — it is reached only by the app backend over AWS
# Service Connect (http://engine.local:8001), scoped to the VPC CIDR and
# SG-to-SG, never from a browser. CORS is a browser-enforced control, so a
# cross-origin allowlist has no legitimate consumer here; a wildcard allow-list
# only widened the attack surface. Browser-facing CORS lives in the app backend
# (app/server/app/main.py), origin-scoped via CORS_ALLOWED_ORIGINS.

# Every router below requires a bearer token (see api/auth.py). Only
# /api/health is open, for container liveness checks.
_protected = [Depends(require_api_token)]
app.include_router(pipeline.router, prefix="/api", dependencies=_protected)
app.include_router(tracking.router, prefix="/api", dependencies=_protected)
app.include_router(validation.router, prefix="/api", dependencies=_protected)


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "engine"}


@app.post("/api/settings", dependencies=_protected)
def update_settings(settings: dict):
    """Accept runtime settings pushed from the app backend.
    Sets values as env vars so all engine modules pick them up."""
    updated = []
    skipped = []
    for key, value in settings.items():
        # The API token is never accepted over the API it protects.
        if not key.startswith("ENGINE_") or key == "ENGINE_API_TOKEN":
            continue
        str_value = str(value) if value is not None else ""
        # Do not let an empty/blank push clobber an already-set value.
        # The app periodically re-pushes settings; a blank field in that
        # payload must not wipe a working value (e.g. the EMR execution
        # role ARN), which would surface downstream as a min-length error.
        if str_value.strip() == "" and os.environ.get(key):
            skipped.append(key)
            log.info("Setting skipped (blank would overwrite existing): %s", key)
            continue
        os.environ[key] = str_value
        updated.append(key)
        log.info("Setting updated: %s", key)
    return {"updated": updated, "skipped": skipped}
app.include_router(progress.router, prefix="/api", dependencies=_protected)

