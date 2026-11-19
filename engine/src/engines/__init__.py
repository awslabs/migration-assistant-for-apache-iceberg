# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Execution engine adapters — Athena, EMR Serverless."""

from .athena import AthenaClient
from .emr import EmrServerlessClient

__all__ = ["AthenaClient", "EmrServerlessClient"]
