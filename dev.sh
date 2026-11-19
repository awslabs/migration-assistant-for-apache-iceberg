#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
set -e

BACKEND_PID=""
FRONTEND_PID=""
AGENT_PID=""
ENGINE_PID=""
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Shared secret between the app backend and the engine API. A fresh random
# token is generated per run unless you provide one.
export ENGINE_API_TOKEN="${ENGINE_API_TOKEN:-$(openssl rand -hex 32)}"

cleanup() {
  echo ""
  echo "Shutting down..."
  [ -n "$FRONTEND_PID" ] && kill "$FRONTEND_PID" 2>/dev/null && wait "$FRONTEND_PID" 2>/dev/null
  [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null && wait "$BACKEND_PID" 2>/dev/null
  [ -n "$AGENT_PID" ] && kill "$AGENT_PID" 2>/dev/null && wait "$AGENT_PID" 2>/dev/null
  [ -n "$ENGINE_PID" ] && kill "$ENGINE_PID" 2>/dev/null && wait "$ENGINE_PID" 2>/dev/null
  echo "Stopped."
}

start_backend() {
  echo "Starting backend on :8000..."
  (cd "$ROOT_DIR/app/server" && AGENT_URL=http://localhost:8082/invocations ENGINE_URL=http://localhost:8001 uv run uvicorn app.main:app --host 127.0.0.1 --port 8000) &
  BACKEND_PID=$!
}

start_frontend() {
  echo "Starting frontend on :5173..."
  (cd "$ROOT_DIR/app/client" && npm run dev) &
  FRONTEND_PID=$!
}

start_agent() {
  echo "Starting agent on :8082..."
  (cd "$ROOT_DIR/agent" && AGENT_PORT=8082 uv run python src/main.py) &
  AGENT_PID=$!
}

start_engine() {
  echo "Starting engine on :8001..."
  (cd "$ROOT_DIR/engine" && ENGINE_DB_BACKEND=dynamodb ENGINE_TRACKING_BACKEND=dynamodb ENGINE_LOG_LEVEL="${ENGINE_LOG_LEVEL:-INFO}" ENGINE_LOG_FILE="$ROOT_DIR/engine.log" ENGINE_RECONCILE_INTERVAL_SECONDS=10 uv run ima-engine serve --host 127.0.0.1 --port 8001) &
  ENGINE_PID=$!
}

wait_for_engine() {
  echo "Waiting for the engine to be ready..."
  for i in $(seq 1 30); do
    if curl -sf http://localhost:8001/api/health >/dev/null 2>&1; then
      echo "The engine is ready."
      return 0
    fi
    sleep 1
  done
  echo "Warning: the engine did not become ready in 30s, continuing anyway..."
}

stop_all() {
  [ -n "$FRONTEND_PID" ] && kill "$FRONTEND_PID" 2>/dev/null && wait "$FRONTEND_PID" 2>/dev/null
  [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null && wait "$BACKEND_PID" 2>/dev/null
  [ -n "$AGENT_PID" ] && kill "$AGENT_PID" 2>/dev/null && wait "$AGENT_PID" 2>/dev/null
  [ -n "$ENGINE_PID" ] && kill "$ENGINE_PID" 2>/dev/null && wait "$ENGINE_PID" 2>/dev/null
  FRONTEND_PID=""
  BACKEND_PID=""
  AGENT_PID=""
  ENGINE_PID=""
}

restart() {
  echo ""
  echo "Restarting..."
  stop_all
  sleep 1
  start_engine
  wait_for_engine
  start_backend
  start_frontend
  start_agent
  echo "Restarted. Press 'r' to restart, 'q' to quit."
}

trap cleanup EXIT INT TERM

start_engine
wait_for_engine
start_backend
start_frontend
start_agent

echo ""
echo "========================================="
echo "  Engine running"
echo "  Frontend:  http://localhost:5173"
echo "  Backend:   http://localhost:8000"
echo "  Agent:     http://localhost:8082"
echo "  Engine:    http://localhost:8001"
echo "  API docs:  http://localhost:8000/docs"
echo "========================================="
echo "Press 'r' to restart, 'q' to quit."
echo ""

while true; do
  read -rsn1 key
  case "$key" in
    r|R) restart ;;
    q|Q) exit 0 ;;
  esac
done
