# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Paginated DynamoDB reads.

A single Query or Scan returns at most 1 MB of data (and evaluates `Limit`
items before any filter). Callers that need every matching item must follow
`LastEvaluatedKey`; these helpers do that.
"""


def query_all(table, **kwargs) -> list[dict]:
    """Every item a Query matches, across all pages."""
    return list(_iterate(table.query, kwargs))


def scan_all(table, **kwargs) -> list[dict]:
    """Every item a Scan matches, across all pages."""
    return list(_iterate(table.scan, kwargs))


def scan_first(table, **kwargs):
    """The first item a filtered Scan matches, or None. Stops at the first hit."""
    for item in _iterate(table.scan, kwargs):
        return item
    return None


def count_all(call, **kwargs) -> int:
    """Total Count for a Select="COUNT" Query/Scan, summed across pages."""
    total = 0
    params = dict(kwargs, Select="COUNT")
    while True:
        resp = call(**params)
        total += resp.get("Count", 0)
        key = resp.get("LastEvaluatedKey")
        if not key:
            return total
        params["ExclusiveStartKey"] = key


def _iterate(call, kwargs):
    params = dict(kwargs)
    while True:
        resp = call(**params)
        yield from resp.get("Items", [])
        key = resp.get("LastEvaluatedKey")
        if not key:
            return
        params["ExclusiveStartKey"] = key
