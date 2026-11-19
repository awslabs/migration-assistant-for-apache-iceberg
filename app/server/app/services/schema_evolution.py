# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Schema evolution service — maps source types to Iceberg types.

Handles:
- Decimal precision and scale validation
- Timestamp timezone handling (timestamp vs timestamptz)
- Nested struct, array, and map type parsing and mapping
- Type promotion rules (safe widening)
- Schema compatibility assessment for migrations
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class EvolutionSeverity(str, Enum):
    """Severity of a schema evolution issue."""
    INFO = "info"          # No action needed, informational
    WARNING = "warning"    # Safe but worth noting (e.g., precision widening)
    ERROR = "error"        # Incompatible, requires manual intervention


@dataclass
class TypeMapping:
    """A single source-to-Iceberg type mapping."""
    source_type: str
    iceberg_type: str
    is_exact: bool = True
    notes: Optional[str] = None


@dataclass
class EvolutionIssue:
    """A schema evolution issue for a specific column."""
    column: str
    severity: EvolutionSeverity
    source_type: str
    target_type: str
    message: str
    recommendation: Optional[str] = None


@dataclass
class SchemaMapping:
    """Complete schema mapping result for a table."""
    columns: list[TypeMapping] = field(default_factory=list)
    issues: list[EvolutionIssue] = field(default_factory=list)
    compatible: bool = True
    summary: Optional[str] = None


# ---------------------------------------------------------------------------
# Iceberg type constants
# ---------------------------------------------------------------------------

ICEBERG_BOOLEAN = "boolean"
ICEBERG_INT = "int"
ICEBERG_LONG = "long"
ICEBERG_FLOAT = "float"
ICEBERG_DOUBLE = "double"
ICEBERG_STRING = "string"
ICEBERG_BINARY = "binary"
ICEBERG_DATE = "date"
ICEBERG_TIMESTAMP = "timestamp"       # without timezone
ICEBERG_TIMESTAMPTZ = "timestamptz"   # with timezone
ICEBERG_UUID = "uuid"
ICEBERG_FIXED = "fixed"

# ---------------------------------------------------------------------------
# Regex patterns for parsing Hive/Glue type strings
# ---------------------------------------------------------------------------

_DECIMAL_RE = re.compile(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE)
_CHAR_RE = re.compile(r"(var)?char\s*\(\s*(\d+)\s*\)", re.IGNORECASE)
_STRUCT_RE = re.compile(r"struct\s*<(.+)>", re.IGNORECASE | re.DOTALL)
_ARRAY_RE = re.compile(r"array\s*<(.+)>", re.IGNORECASE | re.DOTALL)
_MAP_RE = re.compile(r"map\s*<(.+?)\s*,\s*(.+)>", re.IGNORECASE | re.DOTALL)
_FIXED_RE = re.compile(r"fixed\s*\(\s*(\d+)\s*\)", re.IGNORECASE)

# Iceberg max decimal precision
ICEBERG_MAX_DECIMAL_PRECISION = 38
ICEBERG_MAX_DECIMAL_SCALE = 38


# ---------------------------------------------------------------------------
# Primitive type mapping table
# ---------------------------------------------------------------------------

_PRIMITIVE_MAP: dict[str, str] = {
    # Boolean
    "boolean": ICEBERG_BOOLEAN,
    "bool": ICEBERG_BOOLEAN,

    # Integer types
    "tinyint": ICEBERG_INT,
    "smallint": ICEBERG_INT,
    "int": ICEBERG_INT,
    "integer": ICEBERG_INT,
    "bigint": ICEBERG_LONG,
    "long": ICEBERG_LONG,

    # Floating point
    "float": ICEBERG_FLOAT,
    "double": ICEBERG_DOUBLE,
    "real": ICEBERG_FLOAT,

    # String types
    "string": ICEBERG_STRING,
    "varchar": ICEBERG_STRING,
    "char": ICEBERG_STRING,

    # Binary
    "binary": ICEBERG_BINARY,
    "bytes": ICEBERG_BINARY,

    # Date/time
    "date": ICEBERG_DATE,
    "timestamp": ICEBERG_TIMESTAMP,
    "timestamp_ntz": ICEBERG_TIMESTAMP,
    "timestamp_ltz": ICEBERG_TIMESTAMPTZ,
    "timestamptz": ICEBERG_TIMESTAMPTZ,
    "timestamp with time zone": ICEBERG_TIMESTAMPTZ,
    "timestamp without time zone": ICEBERG_TIMESTAMP,
    "timestamp with local time zone": ICEBERG_TIMESTAMPTZ,

    # UUID
    "uuid": ICEBERG_UUID,
}


# ---------------------------------------------------------------------------
# Type promotion rules (safe widening)
# ---------------------------------------------------------------------------

# Maps (source_iceberg_type, target_iceberg_type) -> is_safe_promotion
_SAFE_PROMOTIONS: set[tuple[str, str]] = {
    (ICEBERG_INT, ICEBERG_LONG),
    (ICEBERG_FLOAT, ICEBERG_DOUBLE),
    (ICEBERG_DATE, ICEBERG_TIMESTAMP),
    (ICEBERG_TIMESTAMP, ICEBERG_TIMESTAMPTZ),
}


def is_safe_promotion(source_iceberg: str, target_iceberg: str) -> bool:
    """Check if promoting from source to target Iceberg type is safe."""
    if source_iceberg == target_iceberg:
        return True
    return (source_iceberg, target_iceberg) in _SAFE_PROMOTIONS


# ---------------------------------------------------------------------------
# Core type mapping functions
# ---------------------------------------------------------------------------

def map_type(source_type: str) -> TypeMapping:
    """Map a single Hive/Glue type string to its Iceberg equivalent.

    Handles primitives, decimals, chars, structs, arrays, and maps.
    """
    normalized = source_type.strip().lower()

    # Check decimal with precision/scale
    decimal_match = _DECIMAL_RE.match(normalized)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        return _map_decimal(precision, scale, source_type)

    # Check char/varchar with length
    char_match = _CHAR_RE.match(normalized)
    if char_match:
        return TypeMapping(
            source_type=source_type,
            iceberg_type=ICEBERG_STRING,
            is_exact=True,
            notes="Iceberg uses unbounded strings; char length constraint is dropped.",
        )

    # Check struct
    struct_match = _STRUCT_RE.match(normalized)
    if struct_match:
        return _map_struct(struct_match.group(1), source_type)

    # Check array
    array_match = _ARRAY_RE.match(normalized)
    if array_match:
        return _map_array(array_match.group(1), source_type)

    # Check map
    map_match = _MAP_RE.match(normalized)
    if map_match:
        return _map_map(map_match.group(1), map_match.group(2), source_type)

    # Check fixed-length binary
    fixed_match = _FIXED_RE.match(normalized)
    if fixed_match:
        length = int(fixed_match.group(1))
        return TypeMapping(
            source_type=source_type,
            iceberg_type=f"fixed({length})",
            is_exact=True,
        )

    # Bare decimal without precision
    if normalized == "decimal":
        return TypeMapping(
            source_type=source_type,
            iceberg_type="decimal(38, 18)",
            is_exact=False,
            notes=(
                "No precision/scale specified in catalog. Defaulting to decimal(38, 18). "
                "Actual data may have precision exceeding Iceberg's max of 38 — verify "
                "source DDL or run DESCRIBE on the table to confirm precision bounds."
            ),
        )

    # Primitive lookup
    iceberg_type = _PRIMITIVE_MAP.get(normalized)
    if iceberg_type:
        return TypeMapping(
            source_type=source_type,
            iceberg_type=iceberg_type,
            is_exact=True,
        )

    # Unknown type — pass through as string with a warning
    return TypeMapping(
        source_type=source_type,
        iceberg_type=ICEBERG_STRING,
        is_exact=False,
        notes=f"Unknown source type '{source_type}'; mapped to string as fallback.",
    )


def _map_decimal(precision: int, scale: int, source_type: str) -> TypeMapping:
    """Map a decimal type, validating precision and scale bounds."""
    if precision > ICEBERG_MAX_DECIMAL_PRECISION:
        return TypeMapping(
            source_type=source_type,
            iceberg_type=f"decimal({ICEBERG_MAX_DECIMAL_PRECISION}, {min(scale, ICEBERG_MAX_DECIMAL_SCALE)})",
            is_exact=False,
            notes=(
                f"Source precision ({precision}) exceeds Iceberg maximum ({ICEBERG_MAX_DECIMAL_PRECISION}). "
                f"Clamped to decimal({ICEBERG_MAX_DECIMAL_PRECISION}, {min(scale, ICEBERG_MAX_DECIMAL_SCALE)}). "
                "Potential data loss for very large values."
            ),
        )
    if scale > precision:
        return TypeMapping(
            source_type=source_type,
            iceberg_type=f"decimal({precision}, {precision})",
            is_exact=False,
            notes=f"Scale ({scale}) exceeds precision ({precision}); clamped scale to {precision}.",
        )
    return TypeMapping(
        source_type=source_type,
        iceberg_type=f"decimal({precision}, {scale})",
        is_exact=True,
    )


def _map_struct(fields_str: str, source_type: str) -> TypeMapping:
    """Map a struct type, recursively mapping each field."""
    fields = _parse_struct_fields(fields_str)
    mapped_fields = []
    all_exact = True
    has_error = False

    for field_name, field_type in fields:
        field_mapping = map_type(field_type)
        mapped_fields.append(f"{field_name}: {field_mapping.iceberg_type}")
        if not field_mapping.is_exact:
            all_exact = False
        # Check if nested field has error-level issues
        if field_mapping.notes and ("exceeds" in field_mapping.notes.lower()
                                     or "data loss" in field_mapping.notes.lower()
                                     or "no precision" in field_mapping.notes.lower()):
            has_error = True

    iceberg_struct = "struct<" + ", ".join(mapped_fields) + ">"
    notes = None
    if not all_exact:
        if has_error:
            notes = "Nested field(s) have precision overflow or unknown precision. Potential data loss in nested fields."
        else:
            notes = "One or more nested fields required type adjustment."
    return TypeMapping(
        source_type=source_type,
        iceberg_type=iceberg_struct,
        is_exact=all_exact,
        notes=notes,
    )


def _map_array(element_str: str, source_type: str) -> TypeMapping:
    """Map an array type, recursively mapping the element type."""
    element_mapping = map_type(element_str.strip())
    iceberg_array = f"list({element_mapping.iceberg_type})"
    return TypeMapping(
        source_type=source_type,
        iceberg_type=iceberg_array,
        is_exact=element_mapping.is_exact,
        notes=element_mapping.notes,
    )


def _map_map(key_str: str, value_str: str, source_type: str) -> TypeMapping:
    """Map a map type, recursively mapping key and value types."""
    key_mapping = map_type(key_str.strip())
    value_mapping = map_type(value_str.strip())
    iceberg_map = f"map({key_mapping.iceberg_type}, {value_mapping.iceberg_type})"
    is_exact = key_mapping.is_exact and value_mapping.is_exact
    notes = None
    if not is_exact:
        parts = []
        if not key_mapping.is_exact and key_mapping.notes:
            parts.append(f"Key: {key_mapping.notes}")
        if not value_mapping.is_exact and value_mapping.notes:
            parts.append(f"Value: {value_mapping.notes}")
        notes = " | ".join(parts) if parts else None
    return TypeMapping(
        source_type=source_type,
        iceberg_type=iceberg_map,
        is_exact=is_exact,
        notes=notes,
    )


def _parse_struct_fields(fields_str: str) -> list[tuple[str, str]]:
    """Parse struct field definitions, handling nested angle brackets and parentheses.

    Input: "field1:string,field2:struct<a:int,b:string>,field3:decimal(10,2)"
    Output: [("field1", "string"), ("field2", "struct<a:int,b:string>"), ("field3", "decimal(10,2)")]
    """
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
        # Split on first colon only (field names don't contain colons)
        parts = f.split(":", 1)
        if len(parts) == 2:
            result.append((parts[0].strip(), parts[1].strip()))
        else:
            # Malformed — treat as unnamed string field
            result.append((f"_unnamed_{len(result)}", parts[0].strip()))

    return result


# ---------------------------------------------------------------------------
# Timestamp timezone inference
# ---------------------------------------------------------------------------

def infer_timestamp_type(
    column_name: str,
    source_type: str,
    table_parameters: dict = None,
    serde: str = None,
) -> TypeMapping:
    """Infer whether a timestamp column should map to timestamp or timestamptz.

    Heuristics:
    1. Explicit type annotations (timestamp_ltz, timestamp with time zone) → timestamptz
    2. Hive/Glue default 'timestamp' → depends on context:
       - If table uses Parquet with isAdjustedToUTC=true → timestamptz
       - If column name contains 'utc' or 'tz' → timestamptz
       - Otherwise → timestamp (no timezone)
    3. Source format hints from SerDe or table parameters
    """
    normalized = source_type.strip().lower()

    # Explicit timezone types
    if normalized in ("timestamp_ltz", "timestamptz", "timestamp with time zone",
                      "timestamp with local time zone"):
        return TypeMapping(
            source_type=source_type,
            iceberg_type=ICEBERG_TIMESTAMPTZ,
            is_exact=True,
            notes="Explicit timezone-aware timestamp type.",
        )

    if normalized in ("timestamp_ntz", "timestamp without time zone"):
        return TypeMapping(
            source_type=source_type,
            iceberg_type=ICEBERG_TIMESTAMP,
            is_exact=True,
            notes="Explicit timezone-naive timestamp type.",
        )

    # For bare 'timestamp', apply heuristics
    if normalized == "timestamp":
        # Check Parquet-specific metadata
        if table_parameters:
            # Spark/Hive Parquet tables may have this parameter
            if "isAdjustedToUTC" in str(table_parameters):
                return TypeMapping(
                    source_type=source_type,
                    iceberg_type=ICEBERG_TIMESTAMPTZ,
                    is_exact=False,
                    notes=(
                        "Source uses Parquet with isAdjustedToUTC metadata. "
                        "Mapped to timestamptz. Verify timezone semantics match your queries."
                    ),
                )

            # Check for Spark SQL timestamp type configuration
            spark_tz = table_parameters.get("spark.sql.session.timeZone", "")
            if spark_tz:
                return TypeMapping(
                    source_type=source_type,
                    iceberg_type=ICEBERG_TIMESTAMPTZ,
                    is_exact=False,
                    notes=(
                        f"Source table has spark.sql.session.timeZone={spark_tz}. "
                        "Mapped to timestamptz. Values are stored relative to this timezone."
                    ),
                )

        # Column name heuristics
        col_lower = column_name.lower()
        if any(hint in col_lower for hint in ("_utc", "_tz", "utc_", "tz_")):
            return TypeMapping(
                source_type=source_type,
                iceberg_type=ICEBERG_TIMESTAMPTZ,
                is_exact=False,
                notes=(
                    f"Column name '{column_name}' suggests timezone-aware data. "
                    "Mapped to timestamptz. Verify with sample data."
                ),
            )

        # Default: bare timestamp → timestamp (no timezone)
        return TypeMapping(
            source_type=source_type,
            iceberg_type=ICEBERG_TIMESTAMP,
            is_exact=True,
            notes=(
                "Hive 'timestamp' has no timezone concept. Mapped to Iceberg timestamp "
                "(without timezone). If your data actually stores UTC values, consider "
                "using timestamptz and adjusting queries accordingly."
            ),
        )

    # Fallback to generic mapping
    return map_type(source_type)


# ---------------------------------------------------------------------------
# Full schema mapping
# ---------------------------------------------------------------------------

def map_schema(
    columns: list[dict],
    table_parameters: dict = None,
    serde: str = None,
) -> SchemaMapping:
    """Map an entire table schema from Glue/Hive types to Iceberg types.

    Args:
        columns: List of {"name": str, "type": str} from Glue catalog
        table_parameters: Table-level parameters (for timezone inference)
        serde: SerDe library name (for format-specific behavior)

    Returns:
        SchemaMapping with column mappings and any evolution issues
    """
    result = SchemaMapping()

    for col in columns:
        col_name = col["name"]
        col_type = col["type"]
        normalized = col_type.strip().lower()

        # Use specialized timestamp inference for timestamp types
        if "timestamp" in normalized and "struct" not in normalized:
            mapping = infer_timestamp_type(col_name, col_type, table_parameters, serde)
        else:
            mapping = map_type(col_type)

        result.columns.append(mapping)

        # Generate issues for non-exact mappings
        if not mapping.is_exact:
            severity = EvolutionSeverity.WARNING
            # Escalate to error if precision loss is possible
            notes_lower = (mapping.notes or "").lower()
            if ("exceeds" in notes_lower or "data loss" in notes_lower
                    or "no precision" in notes_lower):
                severity = EvolutionSeverity.ERROR
                result.compatible = False

            result.issues.append(EvolutionIssue(
                column=col_name,
                severity=severity,
                source_type=col_type,
                target_type=mapping.iceberg_type,
                message=mapping.notes or "Type mapping is approximate.",
                recommendation=_get_recommendation(col_name, col_type, mapping),
            ))

    # Generate summary
    error_count = sum(1 for i in result.issues if i.severity == EvolutionSeverity.ERROR)
    warning_count = sum(1 for i in result.issues if i.severity == EvolutionSeverity.WARNING)

    if error_count > 0:
        result.summary = (
            f"Schema has {error_count} incompatible type(s) requiring manual intervention "
            f"and {warning_count} warning(s). Review errors before proceeding with migration."
        )
    elif warning_count > 0:
        result.summary = (
            f"Schema is compatible with {warning_count} advisory note(s). "
            "Migration can proceed; review warnings for potential semantic differences."
        )
    else:
        result.summary = "Schema is fully compatible. All types map directly to Iceberg equivalents."

    return result


def _get_recommendation(col_name: str, source_type: str, mapping: TypeMapping) -> Optional[str]:
    """Generate a recommendation for a non-exact type mapping."""
    normalized = source_type.strip().lower()

    if "decimal" in normalized and "exceeds" in (mapping.notes or ""):
        return (
            "Consider whether values actually use the full precision. "
            "If not, reduce precision in the source before migration. "
            "If yes, split into multiple columns or use string representation."
        )

    if normalized == "decimal" and "no precision" in (mapping.notes or "").lower():
        return (
            "Run DESCRIBE FORMATTED or check the original DDL to determine actual "
            "precision/scale. If precision exceeds 38, data loss may occur during migration. "
            "Consider: ALTER TABLE ... CHANGE COLUMN ... decimal(38, s) to cap precision."
        )

    if "timestamp" in normalized:
        return (
            "Verify timezone semantics with sample data. Run a query like: "
            "SELECT DISTINCT typeof(col), min(col), max(col) FROM table "
            "to confirm whether values include timezone offsets."
        )

    if "unknown" in (mapping.notes or "").lower():
        return (
            f"Column '{col_name}' has unrecognized type '{source_type}'. "
            "Inspect actual data values and choose an appropriate Iceberg type manually."
        )

    return None


# ---------------------------------------------------------------------------
# Type promotion validation
# ---------------------------------------------------------------------------

def validate_type_promotion(
    source_columns: list[dict],
    target_columns: list[dict],
) -> list[EvolutionIssue]:
    """Validate type promotions between source and target schemas.

    Used when evolving an existing Iceberg table schema (e.g., adding columns
    from a new source version or widening types).

    Args:
        source_columns: Current schema [{"name": str, "type": str}]
        target_columns: Desired schema [{"name": str, "type": str}]

    Returns:
        List of evolution issues (empty if all promotions are safe)
    """
    issues = []
    source_map = {c["name"]: c["type"] for c in source_columns}
    target_map = {c["name"]: c["type"] for c in target_columns}

    for col_name, target_type in target_map.items():
        if col_name not in source_map:
            # New column — always safe in Iceberg (nullable by default)
            issues.append(EvolutionIssue(
                column=col_name,
                severity=EvolutionSeverity.INFO,
                source_type="(new column)",
                target_type=target_type,
                message=f"New column '{col_name}' will be added. Existing data will have null values.",
            ))
            continue

        source_type = source_map[col_name]
        source_mapping = map_type(source_type)
        target_mapping = map_type(target_type)

        if source_mapping.iceberg_type == target_mapping.iceberg_type:
            continue  # Same type, no issue

        if is_safe_promotion(source_mapping.iceberg_type, target_mapping.iceberg_type):
            issues.append(EvolutionIssue(
                column=col_name,
                severity=EvolutionSeverity.INFO,
                source_type=source_type,
                target_type=target_type,
                message=(
                    f"Safe type promotion: {source_mapping.iceberg_type} → {target_mapping.iceberg_type}. "
                    "No data loss."
                ),
            ))
        else:
            # Check decimal widening specifically
            if "decimal" in source_mapping.iceberg_type and "decimal" in target_mapping.iceberg_type:
                issue = _validate_decimal_promotion(col_name, source_type, target_type)
                if issue:
                    issues.append(issue)
            else:
                issues.append(EvolutionIssue(
                    column=col_name,
                    severity=EvolutionSeverity.ERROR,
                    source_type=source_type,
                    target_type=target_type,
                    message=(
                        f"Incompatible type change: {source_mapping.iceberg_type} → "
                        f"{target_mapping.iceberg_type}. This is not a safe promotion."
                    ),
                    recommendation=(
                        "Create a new column with the desired type and migrate data, "
                        "or use a view to cast at read time."
                    ),
                ))

    # Check for dropped columns
    for col_name in source_map:
        if col_name not in target_map:
            issues.append(EvolutionIssue(
                column=col_name,
                severity=EvolutionSeverity.WARNING,
                source_type=source_map[col_name],
                target_type="(dropped)",
                message=(
                    f"Column '{col_name}' exists in source but not in target. "
                    "Iceberg supports column deletion but data in existing files is preserved."
                ),
            ))

    return issues


def _validate_decimal_promotion(
    col_name: str, source_type: str, target_type: str
) -> Optional[EvolutionIssue]:
    """Validate decimal precision/scale widening."""
    source_match = _DECIMAL_RE.search(source_type.lower())
    target_match = _DECIMAL_RE.search(target_type.lower())

    if not source_match or not target_match:
        return None

    s_prec, s_scale = int(source_match.group(1)), int(source_match.group(2))
    t_prec, t_scale = int(target_match.group(1)), int(target_match.group(2))

    # Safe: widening precision or scale (as long as scale doesn't decrease)
    if t_prec >= s_prec and t_scale >= s_scale:
        return EvolutionIssue(
            column=col_name,
            severity=EvolutionSeverity.INFO,
            source_type=source_type,
            target_type=target_type,
            message=(
                f"Safe decimal widening: decimal({s_prec},{s_scale}) → "
                f"decimal({t_prec},{t_scale}). No data loss."
            ),
        )

    # Unsafe: narrowing precision or scale
    return EvolutionIssue(
        column=col_name,
        severity=EvolutionSeverity.ERROR,
        source_type=source_type,
        target_type=target_type,
        message=(
            f"Unsafe decimal change: decimal({s_prec},{s_scale}) → "
            f"decimal({t_prec},{t_scale}). Reducing precision or scale may cause data loss."
        ),
        recommendation=(
            "Keep the wider precision/scale, or verify that no values exceed "
            "the narrower bounds before proceeding."
        ),
    )
