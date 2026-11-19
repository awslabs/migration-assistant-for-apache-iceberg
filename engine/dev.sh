#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Start the engine API server in dev mode with SQLite.

export ENGINE_DB_ENGINE=sqlite
# The engine API requires a bearer token. Export your own to reuse it elsewhere.
export ENGINE_API_TOKEN="${ENGINE_API_TOKEN:-$(openssl rand -hex 32)}"
export ENGINE_LOG_LEVEL=DEBUG
export ENGINE_RECONCILE_INTERVAL_SECONDS=10

cd "$(dirname "$0")"
uv run ima-engine serve --host 127.0.0.1 --port 8001
