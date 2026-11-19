# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Schema evolution tools for Iceberg migration analysis.

Maps source Hive/Glue types to Iceberg types, detecting issues with:
- Decimal precision and scale boundaries
- Timestamp timezone semantics
- Nested struct/array/map type compatibility
- Type promotion safety
"""

import re
from strands import tool


# ---------------------------------------------------------------------------
# Iceberg type constants
# ---------------------------------------------------------------------------

_ICEBERG_BOOLEAN = "boolean"
_ICEBERG_INT = "int"
_ICEBERG_LONG = "long"
_ICEBERG_FLOAT = "float"
_ICEBERG_DOUBLE = "double"
_ICEBERG_STRING = "string"
_ICEBERG_BINARY = "binary"
_ICEBERG_DATE = "date"
_ICEBERG_TIMESTAMP = "timestamp"
_ICEBERG_TIMESTAMPTZ = "timestamptz"

_ICEBERG_MAX_DECIMAL_PRECISION = 38

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

_DECIMAL_RE = re.compile(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE)
_CHAR_RE = re.compile(r"(var)?char\s*\(\s*(\d+)\s*\)", re.IGNORECASE)
_STRUCT_RE = re.compile(r"struct\s*<(.+)>", re.IGNORECASE | re.DOTALL)
_ARRAY_RE = re.compile(r"array\s*<(.+)>", re.IGNORECASE | re.DOTALL)
_MAP_RE = re.compile(r"map\s*<(.+?)\s*,\s*(.+)>", re.IGNORECASE | re.DOTALL)

# ---------------------------------------------------------------------------
# Primitive type mapping
# ---------------------------------------------------------------------------

_PRIMITIVE_MAP = {
    "boolean": _ICEBERG_BOOLEAN,
    "bool": _ICEBERG_BOOLEAN,
    "tinyint": _ICEBERG_INT,
    "smallint": _ICEBERG_INT,
    "int": _ICEBERG_INT,
    "integer": _ICEBERG_INT,
    "bigint": _ICEBERG_LONG,
    "long": _ICEBERG_LONG,
    "float": _ICEBERG_FLOAT,
    "double": _ICEBERG_DOUBLE,
    "real": _ICEBERG_FLOAT,
    "string": _ICEBERG_STRING,
    "varchar": _ICEBERG_STRING,
    "char": _ICEBERG_STRING,
    "binary": _ICEBERG_BINARY,
    "bytes": _ICEBERG_BINARY,
    "date": _ICEBERG_DATE,
    "timestamp": _ICEBERG_TIMESTAMP,
    "timestamp_ntz": _ICEBERG_TIMESTAMP,
    "timestamp_ltz": _ICEBERG_TIMESTAMPTZ,
    "timestamptz": _ICEBERG_TIMESTAMPTZ,
    "timestamp with time zone": _ICEBERG_TIMESTAMPTZ,
    "timestamp without time zone": _ICEBERG_TIMESTAMP,
    "timestamp with local time zone": _ICEBERG_TIMESTAMPTZ,
    "uuid": "uuid",
}

# Safe type promotions in Iceberg
_SAFE_PROMOTIONS = {
    (_ICEBERG_INT, _ICEBERG_LONG),
    (_ICEBERG_FLOAT, _ICEBERG_DOUBLE),
    (_ICEBERG_DATE, _ICEBERG_TIMESTAMP),
    (_ICEBERG_TIMESTAMP, _ICEBERG_TIMESTAMPTZ),
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_struct_fields(fields_str: str) -> list[tuple[str, str]]:
    """Parse struct fields handling nested angle brackets and parentheses."""
    fields = []
    angle_depth = 0
    paren_depth = 0
    current = ""

    for char in fields_str:
        if char == "<":
            angle_depth += 1
            current += char
        elif char == ">":
            angle_depth -= 1
            current += char
        elif char == "(":
            paren_depth += 1
            current += char
        elif char == ")":
            paren_depth -= 1
            current += char
        elif char == "," and angle_depth == 0 and paren_depth == 0:
            fields.append(current.strip())
            current = ""
        else:
            current += char

    if current.strip():
        fields.append(current.strip())

    result = []
    for f in fields:
        parts = f.split(":", 1)
        if len(parts) == 2:
            result.append((parts[0].strip(), parts[1].strip()))
        else:
            result.append((f"_field_{len(result)}", parts[0].strip()))

    return result


def _map_single_type(source_type: str) -> dict:
    """Map a single source type to Iceberg, returning a dict with mapping info."""
    normalized = source_type.strip().lower()

    # Decimal
    decimal_match = _DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        if precision > _ICEBERG_MAX_DECIMAL_PRECISION:
            return {
                "source_type": source_type,
                "iceberg_type": f"decimal({_ICEBERG_MAX_DECIMAL_PRECISION}, {min(scale, _ICEBERG_MAX_DECIMAL_PRECISION)})",
                "exact": False,
                "issue": "error",
                "note": f"Precision {precision} exceeds Iceberg max ({_ICEBERG_MAX_DECIMAL_PRECISION}). Potential data loss.",
            }
        if scale > precision:
            return {
                "source_type": source_type,
                "iceberg_type": f"decimal({precision}, {precision})",
                "exact": False,
                "issue": "warning",
                "note": f"Scale ({scale}) exceeds precision ({precision}); clamped.",
            }
        return {
            "source_type": source_type,
            "iceberg_type": f"decimal({precision}, {scale})",
            "exact": True,
        }

    # Bare decimal
    if normalized == "decimal":
        return {
            "source_type": source_type,
            "iceberg_type": "decimal(38, 18)",
            "exact": False,
            "issue": "error",
            "note": (
                "No precision/scale specified in catalog. Defaulting to decimal(38, 18). "
                "Actual data may have precision exceeding Iceberg's max of 38 — verify "
                "source DDL or run DESCRIBE on the table to confirm precision bounds."
            ),
        }

    # Char/varchar
    if _CHAR_RE.match(normalized):
        return {
            "source_type": source_type,
            "iceberg_type": _ICEBERG_STRING,
            "exact": True,
            "note": "Length constraint dropped; Iceberg strings are unbounded.",
        }

    # Struct
    struct_match = _STRUCT_RE.match(normalized)
    if struct_match:
        fields = _parse_struct_fields(struct_match.group(1))
        mapped_fields = []
        all_exact = True
        nested_issues = []
        max_severity = None  # Track worst nested severity
        for fname, ftype in fields:
            fm = _map_single_type(ftype)
            mapped_fields.append(f"{fname}: {fm['iceberg_type']}")
            if not fm.get("exact", True):
                all_exact = False
            if fm.get("issue"):
                nested_issues.append(f"{fname}: {fm.get('note', '')}")
                # Propagate the worst severity
                if fm["issue"] == "error":
                    max_severity = "error"
                elif fm["issue"] == "warning" and max_severity != "error":
                    max_severity = "warning"
        iceberg_struct = "struct<" + ", ".join(mapped_fields) + ">"
        result = {
            "source_type": source_type,
            "iceberg_type": iceberg_struct,
            "exact": all_exact,
        }
        if nested_issues:
            result["issue"] = max_severity or "warning"
            result["note"] = "Nested field issues: " + "; ".join(nested_issues)
        return result

    # Array
    array_match = _ARRAY_RE.match(normalized)
    if array_match:
        elem = _map_single_type(array_match.group(1).strip())
        return {
            "source_type": source_type,
            "iceberg_type": f"list({elem['iceberg_type']})",
            "exact": elem.get("exact", True),
            "issue": elem.get("issue"),
            "note": elem.get("note"),
        }

    # Map
    map_match = _MAP_RE.match(normalized)
    if map_match:
        key = _map_single_type(map_match.group(1).strip())
        val = _map_single_type(map_match.group(2).strip())
        exact = key.get("exact", True) and val.get("exact", True)
        notes = []
        if key.get("note"):
            notes.append(f"Key: {key['note']}")
        if val.get("note"):
            notes.append(f"Value: {val['note']}")
        # Propagate worst severity from key/value
        max_severity = None
        for part in (key, val):
            if part.get("issue") == "error":
                max_severity = "error"
            elif part.get("issue") == "warning" and max_severity != "error":
                max_severity = "warning"
        result = {
            "source_type": source_type,
            "iceberg_type": f"map({key['iceberg_type']}, {val['iceberg_type']})",
            "exact": exact,
        }
        if notes:
            result["issue"] = max_severity or "warning"
            result["note"] = " | ".join(notes)
        return result

    # Primitive
    iceberg_type = _PRIMITIVE_MAP.get(normalized)
    if iceberg_type:
        return {
            "source_type": source_type,
            "iceberg_type": iceberg_type,
            "exact": True,
        }

    # Unknown
    return {
        "source_type": source_type,
        "iceberg_type": _ICEBERG_STRING,
        "exact": False,
        "issue": "warning",
        "note": f"Unknown type '{source_type}'; mapped to string as fallback.",
    }


def _infer_timestamp(col_name: str, source_type: str, table_parameters: dict = None) -> dict:
    """Infer timestamp vs timestamptz based on context."""
    normalized = source_type.strip().lower()

    # Explicit timezone types
    if normalized in ("timestamp_ltz", "timestamptz", "timestamp with time zone",
                      "timestamp with local time zone"):
        return {
            "source_type": source_type,
            "iceberg_type": _ICEBERG_TIMESTAMPTZ,
            "exact": True,
            "note": "Explicit timezone-aware timestamp.",
        }

    if normalized in ("timestamp_ntz", "timestamp without time zone"):
        return {
            "source_type": source_type,
            "iceberg_type": _ICEBERG_TIMESTAMP,
            "exact": True,
            "note": "Explicit timezone-naive timestamp.",
        }

    # Bare 'timestamp' — apply heuristics
    if table_parameters:
        if "isAdjustedToUTC" in str(table_parameters):
            return {
                "source_type": source_type,
                "iceberg_type": _ICEBERG_TIMESTAMPTZ,
                "exact": False,
                "issue": "warning",
                "note": "Parquet isAdjustedToUTC detected. Mapped to timestamptz. Verify timezone semantics.",
            }
        spark_tz = table_parameters.get("spark.sql.session.timeZone", "")
        if spark_tz:
            return {
                "source_type": source_type,
                "iceberg_type": _ICEBERG_TIMESTAMPTZ,
                "exact": False,
                "issue": "warning",
                "note": f"spark.sql.session.timeZone={spark_tz} detected. Mapped to timestamptz.",
            }

    # Column name heuristics
    col_lower = col_name.lower()
    if any(hint in col_lower for hint in ("_utc", "_tz", "utc_", "tz_")):
        return {
            "source_type": source_type,
            "iceberg_type": _ICEBERG_TIMESTAMPTZ,
            "exact": False,
            "issue": "warning",
            "note": f"Column name '{col_name}' suggests timezone-aware data. Mapped to timestamptz.",
        }

    # Default: no timezone
    return {
        "source_type": source_type,
        "iceberg_type": _ICEBERG_TIMESTAMP,
        "exact": True,
        "note": (
            "Hive 'timestamp' mapped to Iceberg timestamp (no timezone). "
            "If data stores UTC values, consider timestamptz."
        ),
    }


# ---------------------------------------------------------------------------
# Agent tools
# ---------------------------------------------------------------------------

@tool
def analyze_schema_compatibility(
    columns: list,
    table_parameters: dict = None,
    partition_keys: list = None,
) -> dict:
    """Analyze schema compatibility for Iceberg migration.

    Maps all source column types to their Iceberg equivalents, detecting
    issues with decimal precision, timestamp timezone handling, and nested
    struct/array/map types.

    Args:
        columns: List of column dicts with 'name' and 'type' keys (from get_glue_table output)
        table_parameters: Table parameters dict (from get_glue_table output). Used for timestamp timezone inference.
        partition_keys: List of partition key dicts with 'name' and 'type' keys (from get_glue_table output)
    """
    all_columns = list(columns)
    if partition_keys:
        all_columns.extend(partition_keys)

    mappings = []
    issues = []
    error_count = 0
    warning_count = 0

    for col in all_columns:
        col_name = col.get("name", "")
        col_type = col.get("type", "string")
        normalized = col_type.strip().lower()

        # Use timestamp-specific inference for timestamp types
        if "timestamp" in normalized and "struct" not in normalized:
            mapping = _infer_timestamp(col_name, col_type, table_parameters)
        else:
            mapping = _map_single_type(col_type)

        mapping["column_name"] = col_name
        mappings.append(mapping)

        if mapping.get("issue") == "error":
            error_count += 1
            issues.append({
                "column": col_name,
                "severity": "error",
                "source_type": col_type,
                "iceberg_type": mapping["iceberg_type"],
                "message": mapping.get("note", "Incompatible type."),
            })
        elif mapping.get("issue") == "warning":
            warning_count += 1
            issues.append({
                "column": col_name,
                "severity": "warning",
                "source_type": col_type,
                "iceberg_type": mapping["iceberg_type"],
                "message": mapping.get("note", "Approximate mapping."),
            })

    compatible = error_count == 0

    if error_count > 0:
        summary = (
            f"Schema has {error_count} incompatible type(s) requiring manual intervention "
            f"and {warning_count} warning(s). Review errors before migration."
        )
    elif warning_count > 0:
        summary = (
            f"Schema is compatible with {warning_count} advisory note(s). "
            "Migration can proceed; review warnings for semantic differences."
        )
    else:
        summary = "Schema is fully compatible. All types map directly to Iceberg equivalents."

    return {
        "compatible": compatible,
        "total_columns": len(all_columns),
        "errors": error_count,
        "warnings": warning_count,
        "summary": summary,
        "column_mappings": mappings,
        "issues": issues if issues else None,
    }


@tool
def check_type_promotion(
    current_columns: list,
    new_columns: list,
) -> dict:
    """Check if schema evolution (type promotion) between two versions is safe.

    Use this when a source table's schema has changed and you need to verify
    the new types are compatible with the existing Iceberg table.

    Args:
        current_columns: Current Iceberg schema as list of dicts with 'name' and 'type' keys
        new_columns: New/desired schema as list of dicts with 'name' and 'type' keys
    """
    current_map = {c["name"]: c["type"] for c in current_columns}
    new_map = {c["name"]: c["type"] for c in new_columns}

    promotions = []
    added_columns = []
    dropped_columns = []
    incompatible = []

    for col_name, new_type in new_map.items():
        if col_name not in current_map:
            added_columns.append({
                "column": col_name,
                "type": new_type,
                "note": "New column. Existing rows will have null values.",
            })
            continue

        current_type = current_map[col_name]
        current_mapped = _map_single_type(current_type)
        new_mapped = _map_single_type(new_type)

        if current_mapped["iceberg_type"] == new_mapped["iceberg_type"]:
            continue  # Same type

        # Check safe promotions
        pair = (current_mapped["iceberg_type"], new_mapped["iceberg_type"])
        if pair in _SAFE_PROMOTIONS:
            promotions.append({
                "column": col_name,
                "from": current_type,
                "to": new_type,
                "iceberg_from": current_mapped["iceberg_type"],
                "iceberg_to": new_mapped["iceberg_type"],
                "safe": True,
                "note": "Safe type widening. No data loss.",
            })
            continue

        # Check decimal widening
        if "decimal" in current_mapped["iceberg_type"] and "decimal" in new_mapped["iceberg_type"]:
            c_match = _DECIMAL_RE.search(current_type.lower())
            n_match = _DECIMAL_RE.search(new_type.lower())
            if c_match and n_match:
                c_prec, c_scale = int(c_match.group(1)), int(c_match.group(2))
                n_prec, n_scale = int(n_match.group(1)), int(n_match.group(2))
                if n_prec >= c_prec and n_scale >= c_scale:
                    promotions.append({
                        "column": col_name,
                        "from": current_type,
                        "to": new_type,
                        "iceberg_from": current_mapped["iceberg_type"],
                        "iceberg_to": new_mapped["iceberg_type"],
                        "safe": True,
                        "note": f"Safe decimal widening: ({c_prec},{c_scale}) → ({n_prec},{n_scale}).",
                    })
                    continue
                else:
                    incompatible.append({
                        "column": col_name,
                        "from": current_type,
                        "to": new_type,
                        "iceberg_from": current_mapped["iceberg_type"],
                        "iceberg_to": new_mapped["iceberg_type"],
                        "note": (
                            f"Unsafe decimal narrowing: ({c_prec},{c_scale}) → ({n_prec},{n_scale}). "
                            "May cause data loss."
                        ),
                    })
                    continue

        # Incompatible change
        incompatible.append({
            "column": col_name,
            "from": current_type,
            "to": new_type,
            "iceberg_from": current_mapped["iceberg_type"],
            "iceberg_to": new_mapped["iceberg_type"],
            "note": "Incompatible type change. Not a safe Iceberg promotion.",
        })

    for col_name in current_map:
        if col_name not in new_map:
            dropped_columns.append({
                "column": col_name,
                "type": current_map[col_name],
                "note": "Column removed. Data in existing files is preserved but inaccessible.",
            })

    safe = len(incompatible) == 0

    return {
        "safe": safe,
        "safe_promotions": promotions if promotions else None,
        "added_columns": added_columns if added_columns else None,
        "dropped_columns": dropped_columns if dropped_columns else None,
        "incompatible_changes": incompatible if incompatible else None,
        "summary": (
            f"Schema evolution is {'safe' if safe else 'UNSAFE'}. "
            f"{len(promotions)} safe promotion(s), "
            f"{len(added_columns)} new column(s), "
            f"{len(dropped_columns)} dropped column(s), "
            f"{len(incompatible)} incompatible change(s)."
        ),
    }
