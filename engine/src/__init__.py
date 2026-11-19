# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Iceberg Migration Engine.

Supports three migration strategies:
  - Rewrite:      Full table CTAS into a new Iceberg table
  - Incremental:  Partition-by-partition rewrite with change tracking
  - Morph:        File tracking + dynamic manifest generation (no data movement)
"""
