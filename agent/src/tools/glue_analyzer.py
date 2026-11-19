# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Glue Catalog tools for table metadata analysis."""

from strands import tool
from tools.aws import get_client


@tool
def get_glue_table(database: str, table_name: str) -> dict:
    """Get Glue table metadata including schema, partitions, format.

    For Iceberg tables, partition information is read from the Iceberg
    metadata JSON (since Glue PartitionKeys is empty for Iceberg tables).

    Args:
        database: Glue database name
        table_name: Table name
    """
    glue = get_client("glue")
    try:
        resp = glue.get_table(DatabaseName=database, Name=table_name)
        t = resp["Table"]
        sd = t.get("StorageDescriptor", {})
        params = t.get("Parameters", {})

        partition_keys = [
            {"name": p["Name"], "type": p["Type"]}
            for p in t.get("PartitionKeys", [])
        ]

        # For Iceberg tables, read partition spec from metadata
        is_iceberg = "iceberg" in params.get("table_type", "").lower()
        iceberg_partition_spec = None
        if is_iceberg and not partition_keys:
            metadata_location = params.get("metadata_location", "")
            if metadata_location:
                iceberg_partition_spec = _read_iceberg_partition_spec(metadata_location)
                if iceberg_partition_spec:
                    # Resolve source-id to column name from the schema
                    columns = [
                        {"name": c["Name"], "type": c["Type"]}
                        for c in sd.get("Columns", [])
                    ]
                    partition_keys = _resolve_iceberg_partition_keys(iceberg_partition_spec, columns)

        result = {
            "database": database,
            "table_name": table_name,
            "location": sd.get("Location", ""),
            "input_format": sd.get("InputFormat", ""),
            "output_format": sd.get("OutputFormat", ""),
            "serde": sd.get("SerdeInfo", {}).get("SerializationLibrary", ""),
            "columns": [
                {"name": c["Name"], "type": c["Type"]}
                for c in sd.get("Columns", [])
            ],
            "partition_keys": partition_keys,
            "parameters": params,
            "table_type": t.get("TableType", ""),
            "create_time": t.get("CreateTime", "").isoformat()
            if t.get("CreateTime") else None,
        }

        if is_iceberg:
            result["is_iceberg"] = True
            if iceberg_partition_spec:
                result["iceberg_partition_spec"] = iceberg_partition_spec

        return result
    except Exception as e:
        return {"error": str(e), "database": database, "table_name": table_name}


def _read_iceberg_partition_spec(metadata_location: str) -> list | None:
    """Read partition spec from Iceberg metadata JSON on S3.

    Returns raw Iceberg partition fields with source-id, transform, name.
    """
    import json
    try:
        if not metadata_location.startswith("s3://"):
            return None
        path = metadata_location[5:]
        bucket = path.split("/", 1)[0]
        key = path.split("/", 1)[1]

        s3 = get_client("s3")
        resp = s3.get_object(Bucket=bucket, Key=key)
        metadata = json.loads(resp["Body"].read())

        default_spec_id = metadata.get("default-spec-id", 0)
        partition_specs = metadata.get("partition-specs", [])

        # Build id → name map from schema fields
        # Handle both V1 format (schema.fields) and V2 format (schemas[current-schema-id].fields)
        _field_id_map = {}
        schema_fields = metadata.get("schema", {}).get("fields", [])
        if not schema_fields:
            # V2 format: schemas array with current-schema-id
            current_schema_id = metadata.get("current-schema-id", 0)
            for s in metadata.get("schemas", []):
                if s.get("schema-id") == current_schema_id:
                    schema_fields = s.get("fields", [])
                    break
            if not schema_fields and metadata.get("schemas"):
                schema_fields = metadata["schemas"][-1].get("fields", [])
        for f in schema_fields:
            _field_id_map[f.get("id")] = f.get("name")

        fields = None
        for spec in partition_specs:
            if spec.get("spec-id") == default_spec_id:
                fields = spec.get("fields", [])
                break
        if fields is None and partition_specs:
            fields = partition_specs[0].get("fields", [])

        if fields is None:
            return None

        # Enrich each field with the resolved source column name
        for f in fields:
            source_id = f.get("source-id")
            if source_id and source_id in _field_id_map:
                f["_source_column"] = _field_id_map[source_id]

        return fields
    except Exception:
        return None


def _resolve_iceberg_partition_keys(iceberg_fields: list, columns: list[dict]) -> list[dict]:
    """Convert raw Iceberg partition spec fields to partition_keys format.

    Maps source-id to actual column names and preserves transforms.
    Returns: [{name: <source_column_name>, type: <transform>}]
    The `name` is the actual source column (resolved from source-id), NOT the
    Iceberg partition field name.
    """
    # Also build a column name set for fallback matching
    col_names = {c["name"].lower() for c in columns}

    result = []
    for f in iceberg_fields:
        # Use the resolved source column name from schema lookup
        source_col = f.get("_source_column")

        if not source_col:
            # Fallback: if field name matches an actual column, use it
            field_name = f.get("name", "unknown")
            if field_name.lower() in col_names:
                source_col = field_name
            else:
                # Last resort: strip transform suffix from field name
                # e.g. "pickup_datetime_month" → try "pickup_datetime"
                for col in columns:
                    if field_name.startswith(col["name"]):
                        source_col = col["name"]
                        break
                if not source_col:
                    source_col = field_name

        transform = f.get("transform", "identity")
        result.append({
            "name": source_col,
            "type": transform,
        })
    return result


@tool
def get_glue_partitions(database: str, table_name: str, max_results: int = 50) -> dict:
    """Get partition information for a Glue table.

    Args:
        database: Glue database name
        table_name: Table name
        max_results: Max partitions to return
    """
    glue = get_client("glue")
    try:
        resp = glue.get_partitions(
            DatabaseName=database, TableName=table_name, MaxResults=max_results
        )
        partitions = []
        for p in resp.get("Partitions", []):
            partitions.append({
                "values": p["Values"],
                "location": p.get("StorageDescriptor", {}).get("Location", ""),
                "parameters": p.get("Parameters", {}),
            })
        return {
            "database": database,
            "table_name": table_name,
            "partition_count": len(partitions),
            "partitions": partitions,
        }
    except Exception as e:
        return {"error": str(e)}


@tool
def get_glue_column_statistics(database: str, table_name: str, column_names: list = None) -> dict:
    """Fetch Glue Data Catalog column-level statistics for a table.

    Retrieves NumberOfDistinctValues (NDV), NumberOfNulls, min/max values,
    and string length stats from the Glue column statistics API. These
    statistics are generated by Glue crawlers or ANALYZE TABLE commands.

    Use this to inform sort order recommendations — columns with high NDV
    and wide min/max ranges are ideal sort candidates for Iceberg.

    Args:
        database: Glue database name
        table_name: Table name
        column_names: List of column names to fetch stats for. If None, fetches stats for all columns in the table.
    """
    glue = get_client("glue")

    try:
        # If no column names provided, get all columns from the table schema
        if not column_names:
            table_resp = glue.get_table(DatabaseName=database, Name=table_name)
            sd = table_resp["Table"].get("StorageDescriptor", {})
            all_columns = [c["Name"] for c in sd.get("Columns", [])]
            # Also include partition keys
            partition_keys = [p["Name"] for p in table_resp["Table"].get("PartitionKeys", [])]
            column_names = all_columns + partition_keys

        if not column_names:
            return {
                "database": database,
                "table_name": table_name,
                "statistics": {},
                "note": "No columns found in table schema.",
            }

        # Glue API accepts max 100 column names per call
        all_stats = {}
        errors = []
        batch_size = 100

        for i in range(0, len(column_names), batch_size):
            batch = column_names[i:i + batch_size]
            resp = glue.get_column_statistics_for_table(
                DatabaseName=database,
                TableName=table_name,
                ColumnNames=batch,
            )

            for stat in resp.get("ColumnStatisticsList", []):
                col_name = stat["ColumnName"]
                col_type = stat.get("ColumnType", "unknown")
                data = stat.get("StatisticsData", {})
                stat_type = data.get("Type", "")
                analyzed_time = stat.get("AnalyzedTime")

                parsed = {
                    "column_name": col_name,
                    "column_type": col_type,
                    "stat_type": stat_type,
                    "analyzed_time": analyzed_time.isoformat() if analyzed_time else None,
                }

                if stat_type == "LONG":
                    long_data = data.get("LongColumnStatisticsData", {})
                    parsed["ndv"] = long_data.get("NumberOfDistinctValues", 0)
                    parsed["null_count"] = long_data.get("NumberOfNulls", 0)
                    parsed["min_value"] = long_data.get("MinimumValue")
                    parsed["max_value"] = long_data.get("MaximumValue")

                elif stat_type == "DOUBLE":
                    double_data = data.get("DoubleColumnStatisticsData", {})
                    parsed["ndv"] = double_data.get("NumberOfDistinctValues", 0)
                    parsed["null_count"] = double_data.get("NumberOfNulls", 0)
                    parsed["min_value"] = double_data.get("MinimumValue")
                    parsed["max_value"] = double_data.get("MaximumValue")

                elif stat_type == "DATE":
                    date_data = data.get("DateColumnStatisticsData", {})
                    parsed["ndv"] = date_data.get("NumberOfDistinctValues", 0)
                    parsed["null_count"] = date_data.get("NumberOfNulls", 0)
                    min_val = date_data.get("MinimumValue")
                    max_val = date_data.get("MaximumValue")
                    parsed["min_value"] = min_val.isoformat() if min_val else None
                    parsed["max_value"] = max_val.isoformat() if max_val else None

                elif stat_type == "STRING":
                    str_data = data.get("StringColumnStatisticsData", {})
                    parsed["ndv"] = str_data.get("NumberOfDistinctValues", 0)
                    parsed["null_count"] = str_data.get("NumberOfNulls", 0)
                    parsed["max_length"] = str_data.get("MaximumLength")
                    parsed["avg_length"] = str_data.get("AverageLength")

                elif stat_type == "DECIMAL":
                    dec_data = data.get("DecimalColumnStatisticsData", {})
                    parsed["ndv"] = dec_data.get("NumberOfDistinctValues", 0)
                    parsed["null_count"] = dec_data.get("NumberOfNulls", 0)

                elif stat_type == "BOOLEAN":
                    bool_data = data.get("BooleanColumnStatisticsData", {})
                    parsed["null_count"] = bool_data.get("NumberOfNulls", 0)
                    parsed["true_count"] = bool_data.get("NumberOfTrues", 0)
                    parsed["false_count"] = bool_data.get("NumberOfFalses", 0)
                    # NDV for boolean is always 2 (or 3 with nulls)
                    parsed["ndv"] = 2

                elif stat_type == "BINARY":
                    bin_data = data.get("BinaryColumnStatisticsData", {})
                    parsed["null_count"] = bin_data.get("NumberOfNulls", 0)
                    parsed["max_length"] = bin_data.get("MaximumLength")
                    parsed["avg_length"] = bin_data.get("AverageLength")

                all_stats[col_name] = parsed

            for err in resp.get("Errors", []):
                errors.append({
                    "column": err.get("ColumnName"),
                    "error_code": err.get("Error", {}).get("ErrorCode"),
                    "error_message": err.get("Error", {}).get("ErrorMessage"),
                })

        return {
            "database": database,
            "table_name": table_name,
            "columns_requested": len(column_names),
            "columns_with_stats": len(all_stats),
            "statistics": all_stats,
            "errors": errors if errors else None,
            "note": (
                f"Retrieved column statistics for {len(all_stats)}/{len(column_names)} columns."
                + (" Some columns had no statistics available." if errors else "")
            ),
        }

    except glue.exceptions.EntityNotFoundException:
        return {
            "database": database,
            "table_name": table_name,
            "statistics": {},
            "note": "Table not found or no column statistics available.",
        }
    except Exception as e:
        return {
            "database": database,
            "table_name": table_name,
            "statistics": {},
            "error": str(e),
            "note": "Failed to retrieve column statistics. Stats may not have been generated for this table.",
        }



