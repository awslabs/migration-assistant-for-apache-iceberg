# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Athena client — submit, poll, and cancel CTAS/INSERT queries."""

import os
import re as _re
import logging

import boto3

from ..utils.sql_safe import validate_identifier, validate_value
from ..utils.sql_builder import (
    athena_table_ref, athena_insert_into,
    athena_ctas, athena_create_schema,
)

log = logging.getLogger(__name__)


# Athena/Iceberg unsupported types → cast target
# These source types are not supported by Iceberg via Athena CTAS and must be cast.
_ICEBERG_TYPE_CASTS = {
    "smallint": "int",
    "tinyint": "int",
}


def _load_config() -> dict:
    """Settings from environment variables, with local-development defaults."""
    return {
        "region": os.getenv("ENGINE_ATHENA_REGION", "us-east-1"),
        "workgroup": os.getenv("ENGINE_ATHENA_WORKGROUP", "primary"),
        "output_location": os.getenv("ENGINE_ATHENA_OUTPUT_LOCATION", ""),
    }


_config = _load_config()


class AthenaClient:
    """Thin wrapper around the Athena API for CTAS/INSERT queries."""

    def __init__(self, region: str = "", workgroup: str = "",
                 output_location: str = ""):
        self._region = region or _config["region"]
        self._workgroup = workgroup or _config["workgroup"]
        self._output_location = output_location or _config["output_location"]
        # Cached, shared client (timeouts/retries/pool + reuse). See utils.aws.
        from ..utils.aws import get_client
        self._client = get_client("athena", self._region)

    @classmethod
    def from_pipeline(cls, pipeline: dict) -> "AthenaClient":
        """Build a client using the pipeline spec's Athena overrides, if any.

        The app injects the user-configured Athena settings into
        spec.strategy.athena (region/workgroup/output_location) so queries use
        them instead of the engine's deploy-time env defaults. Any absent value
        falls back to the env/config default.
        """
        import json as _json
        cfg: dict = {}
        spec = (pipeline or {}).get("spec_json")
        if isinstance(spec, str):
            try:
                spec = _json.loads(spec)
            except (ValueError, TypeError):
                spec = {}
        if isinstance(spec, dict):
            cfg = ((spec.get("strategy") or {}).get("athena") or {})
        return cls(
            region=cfg.get("region", ""),
            workgroup=cfg.get("workgroup", ""),
            output_location=cfg.get("output_location", ""))

    def _build_select_columns(self, source_db: str, source_table: str) -> str:
        """Build a SELECT column list with CASTs for unsupported Iceberg types.
        
        Fetches schema from Glue catalog. If any column uses an unsupported type
        (e.g., smallint, tinyint), wraps it in CAST(col AS target_type).
        Returns '*' if no casts are needed or schema fetch fails.
        """
        try:
            glue = boto3.client("glue", region_name=self._region)
            resp = glue.get_table(DatabaseName=source_db, Name=source_table)
            columns = resp["Table"]["StorageDescriptor"]["Columns"]
            partition_keys = resp["Table"].get("PartitionKeys", [])
            all_cols = columns + partition_keys
        except Exception as e:
            log.warning("Could not fetch schema for %s.%s, using SELECT *: %s",
                        source_db, source_table, e)
            return "*"

        needs_cast = False
        col_exprs = []
        for col in all_cols:
            col_name = col["Name"]
            col_type = col["Type"].lower().strip()
            target_type = _ICEBERG_TYPE_CASTS.get(col_type)
            if target_type:
                col_exprs.append(f'CAST("{col_name}" AS {target_type}) AS "{col_name}"')
                needs_cast = True
            else:
                col_exprs.append(f'"{col_name}"')

        if not needs_cast:
            return "*"

        return ",\n       ".join(col_exprs)

    def submit_query(self, sql: str, database: str = "",
                     description: str = "", catalog: str = "") -> str:
        """Submit a query and return the query_execution_id."""
        kwargs = {
            "QueryString": sql,
            "WorkGroup": self._workgroup,
        }
        if database or catalog:
            ctx = {}
            if database:
                ctx["Database"] = database
            if catalog:
                ctx["Catalog"] = catalog
            kwargs["QueryExecutionContext"] = ctx
        if self._output_location:
            kwargs["ResultConfiguration"] = {
                "OutputLocation": self._output_location,
            }

        resp = self._client.start_query_execution(**kwargs)
        qid = resp["QueryExecutionId"]
        log.info("Submitted Athena query %s: %s", qid,
                 description or sql[:80])
        return qid

    def get_query_status(self, query_execution_id: str) -> dict:
        """Get the current state of a query execution.

        Returns dict with: state, state_reason, submission_time,
        completion_time, data_scanned_bytes.
        """
        resp = self._client.get_query_execution(
            QueryExecutionId=query_execution_id)
        qe = resp["QueryExecution"]
        status = qe["Status"]
        stats = qe.get("Statistics", {})

        def _iso(dt):
            # SubmissionDateTime / CompletionDateTime are datetime objects.
            # Emit ISO-8601 so callers can store them directly as accurate
            # batch started_at / completed_at (matches the rest of the code's
            # datetime.isoformat() convention). Empty string when absent.
            return dt.isoformat() if dt is not None else ""

        return {
            "state": status["State"],
            "state_reason": status.get("StateChangeReason", ""),
            "submission_time": _iso(status.get("SubmissionDateTime")),
            "completion_time": _iso(status.get("CompletionDateTime")),
            "data_scanned_bytes": stats.get(
                "DataScannedInBytes", 0),
        }

    def cancel_query(self, query_execution_id: str) -> None:
        """Cancel a running query."""
        self._client.stop_query_execution(
            QueryExecutionId=query_execution_id)
        log.info("Cancelled Athena query %s", query_execution_id)

    @staticmethod
    def format_table_ref(database: str, table_name: str,
                         target_type: str = "", table_bucket_arn: str = "") -> str:
        """Format a fully qualified table reference for Athena SQL.

        For s3_tables: "s3tablescatalog/<bucket-name>"."<namespace>"."<table>"
        For regular:   "<database>"."<table>"

        Delegates to sql_builder.athena_table_ref for safe, quoted construction.
        """
        tbl = athena_table_ref(database, table_name, target_type, table_bucket_arn)
        return tbl.sql(dialect="trino")

    @staticmethod
    def build_create_namespace_sql(database: str, table_bucket_arn: str = "") -> str:
        """Build CREATE SCHEMA SQL for S3 Tables namespace.
        
        Returns simple SQL — caller must set the catalog context when submitting.
        """
        if not table_bucket_arn:
            return ""
        return athena_create_schema(database)

    @staticmethod
    def get_s3tables_catalog(table_bucket_arn: str) -> str:
        """Get the S3 Tables catalog name from a bucket ARN."""
        if not table_bucket_arn:
            return ""
        bucket_name = table_bucket_arn.rsplit("/", 1)[-1] if "/" in table_bucket_arn else table_bucket_arn
        return f"s3tablescatalog/{bucket_name}"

    def build_ctas_sql(self, source_db: str, source_table: str,
                       target_db: str, target_table: str,
                       target_location: str,
                       partition_spec: list[dict] = None,
                       sort_order: list[dict] = None,
                       table_properties: dict = None,
                       target_type: str = "",
                       table_bucket_arn: str = "") -> str:
        """Build a CTAS SQL statement for Iceberg."""
        if target_type == "s3_tables":
            # S3 Tables: minimal WITH clause — catalog manages everything
            props = {
                "format": "PARQUET",
            }
        else:
            props = {
                "table_type": "ICEBERG",
                "is_external": "false",
                "format": "PARQUET",
                "write_compression": "ZSTD",
            }
            if target_location:
                props["location"] = f"'{target_location}'"

        if table_properties:
            if table_properties.get("write.parquet.compression-codec"):
                props["write_compression"] = table_properties["write.parquet.compression-codec"].upper()

        if partition_spec:
            # Athena CTAS uses PARTITIONED BY (col_list)
            # For identity transforms, just use column names
            # For time transforms, use Iceberg hidden partitioning
            # For bucket/truncate, include the parameter: bucket(N, col), truncate(N, col)
            part_cols = []
            for ps in partition_spec:
                col = ps["source_column"]
                transform = ps.get("transform", "identity")
                if transform == "identity":
                    part_cols.append(col)
                elif transform == "bucket":
                    n = ps.get("num_buckets", 16)
                    part_cols.append(f"bucket({n}, {col})")
                elif transform == "truncate":
                    w = ps.get("width", 10)
                    part_cols.append(f"truncate({w}, {col})")
                else:
                    # Handle Iceberg metadata format: bucket[N], truncate[N]
                    bucket_m = _re.match(r'^bucket[\(\[](\d+)[\)\]]$', transform)
                    truncate_m = _re.match(r'^truncate[\(\[](\d+)[\)\]]$', transform)
                    if bucket_m:
                        part_cols.append(f"bucket({bucket_m.group(1)}, {col})")
                    elif truncate_m:
                        part_cols.append(f"truncate({truncate_m.group(1)}, {col})")
                    else:
                        # year, month, day, hour, void
                        part_cols.append(f"{transform}({col})")
            props["partitioning"] = (
                "ARRAY[" +
                ", ".join(f"'{p}'" for p in part_cols) +
                "]")

        if sort_order:
            # Athena does NOT support write_ordering in CTAS WITH clause.
            # Sort order must be applied post-creation via ALTER TABLE.
            # The sort_order is handled separately by build_alter_write_order_sql().
            pass

        select_cols = self._build_select_columns(source_db, source_table)
        return athena_ctas(
            target_db=target_db, target_table=target_table,
            source_db=source_db, source_table=source_table,
            select_cols=select_cols,
            where="",
            properties=props,
            target_type=target_type,
            table_bucket_arn=table_bucket_arn,
        )

    def build_create_table_sql(self, source_db: str, source_table: str,
                               target_db: str, target_table: str,
                               target_location: str = "",
                               partition_spec: list[dict] = None,
                               sort_order: list[dict] = None,
                               table_properties: dict = None,
                               target_type: str = "",
                               table_bucket_arn: str = "") -> str:
        """Build a CREATE TABLE ... AS SELECT ... WHERE 1=0 to create an empty Iceberg table with source schema."""
        if target_type == "s3_tables":
            props = {
                "format": "PARQUET",
            }
        else:
            props = {
                "table_type": "ICEBERG",
                "is_external": "false",
                "format": "PARQUET",
                "write_compression": "ZSTD",
            }

        if target_location and target_type != "s3_tables":
            props["location"] = f"'{target_location}'"

        if table_properties:
            if table_properties.get("write.parquet.compression-codec"):
                props["write_compression"] = table_properties["write.parquet.compression-codec"].upper()

        if partition_spec:
            part_cols = []
            for ps in partition_spec:
                col = ps["source_column"]
                transform = ps.get("transform", "identity")
                if transform == "identity":
                    part_cols.append(col)
                elif transform == "bucket":
                    n = ps.get("num_buckets", 16)
                    part_cols.append(f"bucket({n}, {col})")
                elif transform == "truncate":
                    w = ps.get("width", 10)
                    part_cols.append(f"truncate({w}, {col})")
                else:
                    # Handle Iceberg metadata format: bucket[N], truncate[N]
                    bucket_m = _re.match(r'^bucket[\(\[](\d+)[\)\]]$', transform)
                    truncate_m = _re.match(r'^truncate[\(\[](\d+)[\)\]]$', transform)
                    if bucket_m:
                        part_cols.append(f"bucket({bucket_m.group(1)}, {col})")
                    elif truncate_m:
                        part_cols.append(f"truncate({truncate_m.group(1)}, {col})")
                    else:
                        part_cols.append(f"{transform}({col})")
            props["partitioning"] = (
                "ARRAY[" +
                ", ".join(f"'{p}'" for p in part_cols) +
                "]")

        if sort_order:
            # Athena does NOT support write_ordering in CTAS WITH clause.
            # Sort order must be applied post-creation via ALTER TABLE.
            pass

        select_cols = self._build_select_columns(source_db, source_table)
        return athena_ctas(
            target_db=target_db, target_table=target_table,
            source_db=source_db, source_table=source_table,
            select_cols=select_cols,
            where="1=0",
            properties=props,
            target_type=target_type,
            table_bucket_arn=table_bucket_arn,
        )

    def build_alter_write_order_sql(self, target_db: str, target_table: str,
                                    sort_order: list[dict],
                                    target_type: str = "",
                                    table_bucket_arn: str = "") -> str:
        """Build ALTER TABLE to set Iceberg sort order via table properties.

        Athena doesn't support WRITE ORDERED BY syntax.
        Sort order is set via the 'write.sort-order' table property.
        """
        if not sort_order:
            return ""
        order_cols = []
        for sf in sort_order:
            col = sf.get("column", "")
            validate_identifier(col)
            direction = sf.get("direction", "asc").lower()
            null_order = sf.get("null_order", "nulls_last").replace("_", " ").lower()
            order_cols.append(f"{col} {direction} {null_order}")
        sort_expr = ", ".join(order_cols)
        target_ref = self.format_table_ref(target_db, target_table, target_type, table_bucket_arn)
        return f"ALTER TABLE {target_ref} SET TBLPROPERTIES ('write.sort-order' = '{sort_expr}')"

    def build_insert_into_sql(self, source_db: str, source_table: str,
                              target_db: str, target_table: str,
                              partition_filter: str = "",
                              limit: int = 0) -> str:
        """Build an INSERT INTO ... SELECT for batched loading."""
        validate_identifier(source_db)
        validate_identifier(source_table)
        validate_identifier(target_db)
        validate_identifier(target_table)
        if partition_filter:
            validate_value(partition_filter)
        where = partition_filter if partition_filter else ""
        limit_clause = f"\nLIMIT {limit}" if limit > 0 else ""
        sql = athena_insert_into(
            target_db=target_db, target_table=target_table,
            source_db=source_db, source_table=source_table,
            where=where,
        )
        if limit_clause:
            sql += limit_clause
        return sql

    def get_query_results(self, query_execution_id: str, max_results: int = 10) -> list[dict]:
        """Get query results as a list of row dicts."""
        resp = self._client.get_query_results(
            QueryExecutionId=query_execution_id,
            MaxResults=max_results)
        rows = resp.get("ResultSet", {}).get("Rows", [])
        if len(rows) < 2:
            return []
        # First row is headers
        headers = [col.get("VarCharValue", "") for col in rows[0].get("Data", [])]
        results = []
        for row in rows[1:]:
            values = [col.get("VarCharValue", "") for col in row.get("Data", [])]
            results.append(dict(zip(headers, values)))
        return results
