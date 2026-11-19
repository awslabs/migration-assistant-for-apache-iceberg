# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared AWS client construction (timeouts, retries, pooling, reuse).

Centralizes boto3 client creation so every AWS call in the engine gets:
  * bounded connect/read timeouts — a hung API call can't hold a reconcile
    worker (or a morph footer-read thread) for the botocore default 60s;
  * adaptive retries — smooths transient throttling under load;
  * a larger connection pool — the default (10) throttles high-concurrency
    reconcile/morph fan-out;
  * client reuse — botocore clients are thread-safe and relatively expensive to
    construct, so we cache one per (service, region). This replaces the previous
    pattern of building a fresh client on every poll / footer read.

Tunables (env):
  ENGINE_AWS_CONNECT_TIMEOUT   default 5   (seconds)
  ENGINE_AWS_READ_TIMEOUT      default 15  (seconds)
  ENGINE_AWS_MAX_ATTEMPTS      default 3
  ENGINE_AWS_MAX_POOL          default 50  (max_pool_connections)
"""

import os
import threading

import boto3
from botocore.config import Config

_CONNECT_TIMEOUT = int(os.getenv("ENGINE_AWS_CONNECT_TIMEOUT", "5"))
_READ_TIMEOUT = int(os.getenv("ENGINE_AWS_READ_TIMEOUT", "15"))
_MAX_ATTEMPTS = int(os.getenv("ENGINE_AWS_MAX_ATTEMPTS", "3"))
_MAX_POOL = int(os.getenv("ENGINE_AWS_MAX_POOL", "50"))

# Shared, immutable botocore Config applied to every engine client.
BOTO_CONFIG = Config(
    connect_timeout=_CONNECT_TIMEOUT,
    read_timeout=_READ_TIMEOUT,
    retries={"mode": "adaptive", "max_attempts": _MAX_ATTEMPTS},
    max_pool_connections=_MAX_POOL,
)

_clients: dict[tuple[str, str], object] = {}
_clients_lock = threading.Lock()


def get_client(service: str, region: str = "us-east-1"):
    """Return a cached boto3 client for (service, region) with the shared Config.

    Clients are thread-safe; reusing them avoids the cost of constructing a new
    client (credential/session resolution) on every reconcile poll or footer
    read. Cached per (service, region) so multi-region use still works.
    """
    key = (service, region or "us-east-1")
    client = _clients.get(key)
    if client is None:
        with _clients_lock:
            client = _clients.get(key)
            if client is None:
                client = boto3.client(
                    service, region_name=key[1], config=BOTO_CONFIG)
                _clients[key] = client
    return client
