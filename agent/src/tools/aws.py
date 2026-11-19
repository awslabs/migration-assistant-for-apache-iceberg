# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared AWS client construction for the agent (region, timeouts, retries, reuse).

Centralizes boto3 client creation so every AWS call the agent makes gets:
  * a pinned region — the agent tools previously built clients with a bare
    ``boto3.client(service)`` and no ``region_name``. In the AgentCore runtime,
    if the container has no resolvable region, botocore cannot build the
    service endpoint and requests fail with the tell-tale
    ``Read timeout on endpoint URL: "None"``. Pinning the region here removes
    that failure mode entirely.
  * bounded connect/read timeouts — a hung API call can't stall a whole table
    analysis for the botocore default indefinitely.
  * adaptive retries — smooths transient throttling while the agent fans out
    across many S3 listings / footer reads during analysis.
  * a larger connection pool — the default (10) throttles the agent's
    concurrent S3/Glue reads.
  * client reuse — botocore clients are thread-safe and relatively expensive to
    construct; cache one per (service, region) instead of rebuilding on every
    tool call.

Timeouts are deliberately MORE generous than the engine's (engine read_timeout
defaults to 15s, tuned for short control-plane polls). The agent does heavy
S3 enumeration (list_objects_v2 pagination over large tables) and Iceberg/Hudi
manifest + Parquet footer reads, so an aggressive read timeout would abort
legitimate large-table scans. The read timeout here is the botocore default
(60s) and is env-tunable for very large tables.

Tunables (env):
  AGENT_AWS_CONNECT_TIMEOUT   default 10  (seconds)
  AGENT_AWS_READ_TIMEOUT      default 60  (seconds)
  AGENT_AWS_MAX_ATTEMPTS      default 5
  AGENT_AWS_MAX_POOL          default 50  (max_pool_connections)

Region resolution order: AWS_REGION -> AWS_DEFAULT_REGION -> "us-east-1".
"""

import os
import threading

import boto3
from botocore.config import Config

_CONNECT_TIMEOUT = int(os.getenv("AGENT_AWS_CONNECT_TIMEOUT", "10"))
_READ_TIMEOUT = int(os.getenv("AGENT_AWS_READ_TIMEOUT", "60"))
_MAX_ATTEMPTS = int(os.getenv("AGENT_AWS_MAX_ATTEMPTS", "5"))
_MAX_POOL = int(os.getenv("AGENT_AWS_MAX_POOL", "50"))


def _resolve_region() -> str:
    """Resolve the AWS region for agent clients.

    Reads AWS_REGION then AWS_DEFAULT_REGION, falling back to us-east-1. This is
    the single source of truth so no tool can accidentally build a region-less
    client (which produces the ``endpoint URL: "None"`` failure).
    """
    return (
        os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "us-east-1"
    )


REGION = _resolve_region()

# Shared, immutable botocore Config applied to every agent client.
BOTO_CONFIG = Config(
    connect_timeout=_CONNECT_TIMEOUT,
    read_timeout=_READ_TIMEOUT,
    retries={"mode": "adaptive", "max_attempts": _MAX_ATTEMPTS},
    max_pool_connections=_MAX_POOL,
)

_clients: dict[tuple[str, str], object] = {}
_clients_lock = threading.Lock()


def get_client(service: str, region: str | None = None):
    """Return a cached boto3 client for ``service`` with region + shared Config.

    Clients are thread-safe; reusing them avoids repeated credential/session
    resolution on every tool call. Cached per (service, region) so an explicit
    region override still works if a tool ever needs one.
    """
    resolved = region or REGION
    key = (service, resolved)
    client = _clients.get(key)
    if client is None:
        with _clients_lock:
            client = _clients.get(key)
            if client is None:
                client = boto3.client(service, region_name=resolved, config=BOTO_CONFIG)
                _clients[key] = client
    return client
