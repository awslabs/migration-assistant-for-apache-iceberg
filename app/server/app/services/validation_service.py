# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Migration eligibility validation service.

Reads rules from migration_rules.yaml. The config is
reloaded on every call so changes take effect without
restarting the server.
"""

import yaml
from pathlib import Path
from app.models import TableInfo

CONFIG_PATH = Path(__file__).parent.parent / "config" / "migration_rules.yaml"


def _load_rules() -> dict[str, list[str]]:
    with open(CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("formats", {})


def get_eligible_types(fmt: str) -> list[str]:
    """Return the list of migration types a format supports."""
    rules = _load_rules()
    return rules.get(fmt.lower(), [])


def get_all_rules() -> dict[str, list[str]]:
    """Return the full rules config."""
    return _load_rules()


def validate_table(
    table: TableInfo,
    migration_type: str,
) -> dict:
    """Validate whether a table is eligible for a migration type.

    Returns a dict with:
      - eligible: bool
      - format: the table's format
      - requested_type: the requested migration type
      - allowed_types: list of types this format supports
      - reason: human-readable explanation if not eligible
    """
    allowed = get_eligible_types(table.format.value if hasattr(table.format, 'value') else table.format)
    fmt = table.format.value if hasattr(table.format, 'value') else table.format

    if migration_type not in allowed:
        return {
            "eligible": False,
            "format": fmt,
            "requested_type": migration_type,
            "allowed_types": allowed,
            "reason": (
                f"Format '{fmt}' does not support '{migration_type}'. "
                f"Allowed types: {', '.join(allowed) if allowed else 'none'}."
            ),
        }

    return {
        "eligible": True,
        "format": fmt,
        "requested_type": migration_type,
        "allowed_types": allowed,
        "reason": None,
    }


def validate_tables_for_plan(
    tables: list[TableInfo],
    migration_types: list[str],
) -> list[dict]:
    """Validate a batch of tables against plan migration types.

    For each table, determines the best migration type and
    whether it's eligible.
    """
    results = []
    for table in tables:
        fmt = table.format.value if hasattr(table.format, 'value') else table.format
        allowed = get_eligible_types(fmt)

        compatible = [mt for mt in migration_types if mt in allowed]
        if not compatible:
            results.append({
                "database": table.database,
                "table_name": table.table_name,
                "format": fmt,
                "eligible": False,
                "assigned_type": None,
                "reason": f"No compatible migration type. Format supports: {', '.join(allowed)}.",
            })
            continue

        # Auto-assign logic: prefer first compatible type
        assigned = compatible[0]

        results.append({
            "database": table.database,
            "table_name": table.table_name,
            "format": fmt,
            "eligible": True,
            "assigned_type": assigned,
            "reason": None,
        })

    return results
