# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQL injection mitigation via input validation.

Neither Spark SQL nor Athena support parameterized queries (no bind
variables / prepared statements), so SQL is necessarily constructed via
string interpolation. This module provides validation functions that MUST
be applied at the boundary — where database names, table names, column
names, and literal values first enter a SQL string — to reject inputs
containing SQL injection metacharacters.

Design:
  - validate_identifier(name) — for database/table/column names
  - validate_value(val) — for literal values in WHERE clauses

All functions raise ValueError on invalid input so the caller fails
loudly rather than silently executing potentially dangerous SQL.
"""

import re

# Identifiers (database, table, column names): allow alphanumeric,
# underscores, hyphens, and dots (for qualified names). Reject anything
# else — especially quotes, semicolons, comments, and whitespace that
# could break out of the identifier context.
_SAFE_IDENTIFIER_RE = re.compile(r'^[a-zA-Z0-9_.\-]+$')

# Maximum identifier length (Glue/Hive limit is 255)
_MAX_IDENTIFIER_LEN = 255

# Characters that are never valid inside a SQL literal value and indicate
# injection attempts: semicolons (statement separators), double-dash
# (line comments), slash-star (block comments).
_INJECTION_PATTERNS = re.compile(r'[;\x00]|--|/\*')


def validate_identifier(name: str) -> str:
    """Validate a SQL identifier (database, table, or column name).

    Returns the validated name unchanged if safe.
    Raises ValueError if the name contains SQL metacharacters.

    This is the primary defense against injection via identifier
    interpolation in f-string SQL construction.
    """
    if not name:
        raise ValueError("SQL identifier cannot be empty")
    if len(name) > _MAX_IDENTIFIER_LEN:
        raise ValueError(
            f"SQL identifier exceeds max length ({len(name)} > {_MAX_IDENTIFIER_LEN}): "
            f"{name[:50]}...")
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(
            f"SQL identifier contains invalid characters: {name!r}. "
            f"Only alphanumeric, underscores, hyphens, and dots are allowed.")
    return name


def validate_value(val: str) -> str:
    """Validate a literal value for use in a SQL WHERE clause.

    Allows: alphanumeric, spaces, hyphens, underscores, dots, colons,
    slashes (for S3 paths/dates/timestamps), single quotes (for string
    literals — but not unbalanced ones), and common punctuation.

    Rejects: semicolons, null bytes, comment patterns (-- and /*).

    Returns the validated value unchanged if safe.
    Raises ValueError if the value contains injection patterns.
    """
    if val is None:
        return ""
    val_str = str(val)
    if _INJECTION_PATTERNS.search(val_str):
        raise ValueError(
            f"SQL value contains potential injection pattern: {val_str!r}")
    # Check for unbalanced single quotes (odd number = injection attempt)
    if val_str.count("'") % 2 != 0:
        raise ValueError(
            f"SQL value has unbalanced quotes: {val_str!r}")
    return val_str
