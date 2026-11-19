# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Schema evolution endpoints.

Provides type mapping analysis and schema compatibility checks
for Iceberg migrations.
"""

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import schema_evolution

router = APIRouter(prefix="/schema", tags=["schema"])


class ColumnDef(BaseModel):
    name: str
    type: str


class SchemaAnalysisRequest(BaseModel):
    """Request to analyze schema compatibility for Iceberg migration."""
    columns: list[ColumnDef] = Field(min_length=1)
    table_parameters: dict = Field(default_factory=dict)
    serde: str | None = None


class TypePromotionRequest(BaseModel):
    """Request to validate type promotions between schema versions."""
    source_columns: list[ColumnDef] = Field(min_length=1)
    target_columns: list[ColumnDef] = Field(min_length=1)


class SingleTypeRequest(BaseModel):
    """Request to map a single source type to Iceberg."""
    source_type: str = Field(min_length=1)
    column_name: str = ""
    table_parameters: dict = Field(default_factory=dict)


@router.post("/analyze")
def analyze_schema(request: SchemaAnalysisRequest):
    """Analyze full schema compatibility for Iceberg migration.

    Maps all source column types to Iceberg equivalents, detecting issues
    with decimal precision, timestamp timezone handling, and nested types.
    """
    columns = [{"name": c.name, "type": c.type} for c in request.columns]
    result = schema_evolution.map_schema(
        columns=columns,
        table_parameters=request.table_parameters,
        serde=request.serde,
    )
    return {
        "compatible": result.compatible,
        "summary": result.summary,
        "column_mappings": [
            {
                "source_type": m.source_type,
                "iceberg_type": m.iceberg_type,
                "exact": m.is_exact,
                "notes": m.notes,
            }
            for m in result.columns
        ],
        "issues": [
            {
                "column": i.column,
                "severity": i.severity.value,
                "source_type": i.source_type,
                "target_type": i.target_type,
                "message": i.message,
                "recommendation": i.recommendation,
            }
            for i in result.issues
        ],
    }


@router.post("/type-promotion")
def check_type_promotion(request: TypePromotionRequest):
    """Validate type promotions between two schema versions.

    Checks whether evolving from source to target schema is safe
    in Iceberg (type widening, new columns, dropped columns).
    """
    source = [{"name": c.name, "type": c.type} for c in request.source_columns]
    target = [{"name": c.name, "type": c.type} for c in request.target_columns]
    issues = schema_evolution.validate_type_promotion(source, target)
    errors = [i for i in issues if i.severity == schema_evolution.EvolutionSeverity.ERROR]
    return {
        "safe": len(errors) == 0,
        "issues": [
            {
                "column": i.column,
                "severity": i.severity.value,
                "source_type": i.source_type,
                "target_type": i.target_type,
                "message": i.message,
                "recommendation": i.recommendation,
            }
            for i in issues
        ],
    }


@router.post("/map-type")
def map_single_type(request: SingleTypeRequest):
    """Map a single source type to its Iceberg equivalent.

    Useful for quick lookups and UI type previews.
    """
    normalized = request.source_type.strip().lower()

    # Use timestamp inference if it's a timestamp type
    if "timestamp" in normalized:
        mapping = schema_evolution.infer_timestamp_type(
            column_name=request.column_name,
            source_type=request.source_type,
            table_parameters=request.table_parameters,
        )
    else:
        mapping = schema_evolution.map_type(request.source_type)

    return {
        "source_type": mapping.source_type,
        "iceberg_type": mapping.iceberg_type,
        "exact": mapping.is_exact,
        "notes": mapping.notes,
    }


@router.get("/type-rules")
def get_type_rules():
    """Get the complete type mapping reference and promotion rules.

    Returns all supported source types and their Iceberg equivalents,
    plus the list of safe type promotions.
    """
    return {
        "primitive_mappings": {
            source: iceberg
            for source, iceberg in sorted(schema_evolution._PRIMITIVE_MAP.items())
        },
        "safe_promotions": [
            {"from": s, "to": t}
            for s, t in sorted(schema_evolution._SAFE_PROMOTIONS)
        ],
        "decimal": {
            "max_precision": schema_evolution.ICEBERG_MAX_DECIMAL_PRECISION,
            "max_scale": schema_evolution.ICEBERG_MAX_DECIMAL_SCALE,
            "note": "Decimal widening (increasing precision or scale) is safe. Narrowing is not.",
        },
        "timestamp": {
            "types": {
                "timestamp": "No timezone — local time semantics",
                "timestamptz": "With timezone — UTC-normalized",
            },
            "promotion": "timestamp → timestamptz is a safe promotion",
            "note": (
                "Hive 'timestamp' defaults to no-timezone. Timezone inference uses "
                "table parameters (isAdjustedToUTC, spark.sql.session.timeZone) "
                "and column name heuristics (_utc, _tz suffixes)."
            ),
        },
        "complex_types": {
            "struct": "struct<field:type, ...> → struct<field: iceberg_type, ...>",
            "array": "array<type> → list(iceberg_type)",
            "map": "map<key_type, value_type> → map(iceberg_key, iceberg_value)",
            "note": "Nested types are recursively mapped. Each field follows the same rules.",
        },
    }
