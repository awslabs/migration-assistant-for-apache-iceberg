# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Iceberg manifest builder — generates manifests from tracked file metadata.

Uses pyiceberg to create/update Iceberg tables. For the initial load,
uses pyiceberg's add_files() which reads Parquet footers and extracts
full column statistics (column_sizes, value_counts, null_value_counts,
lower_bounds, upper_bounds) automatically.

For incremental commits (reconcile), builds DataFile objects manually
from the tracked metadata.
"""

import logging
import os

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import (
    IdentityTransform, YearTransform, MonthTransform, DayTransform,
    HourTransform, BucketTransform, TruncateTransform, VoidTransform,
)
from pyiceberg.table.sorting import SortOrder, SortField as IcebergSortField, NullOrder, SortDirection
from pyiceberg.typedef import Record
from pyiceberg.types import (
    BooleanType, DateType, DecimalType, DoubleType, FloatType,
    IntegerType, LongType, NestedField, StringType,
    TimestampType, TimestamptzType, BinaryType,
)

import pyarrow as pa

from .scanner import FileMetadata

log = logging.getLogger(__name__)


class SchemaReconcileError(Exception):
    """Raised when source Parquet types cannot be safely reconciled with the
    catalog schema (an incompatible, non-widening disagreement).

    Distinct from file-read errors (e.g. pyarrow ArrowInvalid, which subclasses
    ValueError) so callers can fail hard on a real type conflict while treating
    an unreadable sample file as a non-fatal, best-effort skip.
    """


# Enable pyiceberg nanosecond → microsecond downcast for Parquet files
# written by older Spark/Hive/Pandas versions that use timestamp[ns].
os.environ.setdefault(
    "PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE", "true")

# Monkey-patch pyiceberg's add_files path to support timestamp[ns].
# The add_files → parquet_file_to_data_file → _check_pyarrow_schema_compatible
# call chain does not pass downcast_ns_timestamp_to_us=True, causing failures
# on Hive tables with nanosecond timestamps. We patch the compatibility check
# to always enable downcast.
#
# Additionally, for Hive-partitioned tables, partition columns are NOT stored
# inside the Parquet files — they exist only in directory paths. The strict
# schema check fails on these "missing" columns. We patch it to tolerate
# mismatches where the missing/extra columns are partition columns.
import pyiceberg.io.pyarrow as _pyarrow_io

_original_check = _pyarrow_io._check_pyarrow_schema_compatible


def _patched_check_pyarrow_schema_compatible(requested_schema, provided_schema, **kwargs):
    kwargs.setdefault("downcast_ns_timestamp_to_us", True)
    try:
        return _original_check(requested_schema, provided_schema, **kwargs)
    except ValueError:
        # If the mismatch is only about missing partition columns in the
        # Parquet file (common for Hive tables), suppress the error.
        # The partition values will be inferred from the file path.
        pass


_pyarrow_io._check_pyarrow_schema_compatible = _patched_check_pyarrow_schema_compatible

# Arrow type → Iceberg type mapping
_ARROW_MAP = {
    pa.bool_(): BooleanType(),
    pa.int8(): IntegerType(), pa.int16(): IntegerType(),
    pa.int32(): IntegerType(), pa.int64(): LongType(),
    pa.uint8(): IntegerType(), pa.uint16(): IntegerType(),
    pa.uint32(): LongType(), pa.uint64(): LongType(),
    pa.float16(): FloatType(), pa.float32(): FloatType(),
    pa.float64(): DoubleType(),
    pa.string(): StringType(), pa.large_string(): StringType(),
    pa.utf8(): StringType(), pa.large_utf8(): StringType(),
    pa.binary(): BinaryType(), pa.large_binary(): BinaryType(),
    pa.date32(): DateType(), pa.date64(): DateType(),
}


def _arrow_to_iceberg(arrow_type: pa.DataType):
    if arrow_type in _ARROW_MAP:
        return _ARROW_MAP[arrow_type]
    if pa.types.is_timestamp(arrow_type):
        return TimestamptzType() if arrow_type.tz else TimestampType()
    if pa.types.is_decimal(arrow_type):
        return DecimalType(precision=arrow_type.precision,
                           scale=arrow_type.scale)
    return BinaryType()


def _glue_type_to_iceberg(glue_type: str):
    """Convert a Glue/Hive type string to an Iceberg type.

    Note: Hive 'timestamp' is timezone-unaware (equivalent to Iceberg TimestampType).
    Only explicit 'timestamptz' or 'timestamp with time zone' maps to TimestamptzType.
    """
    glue_type = glue_type.lower().strip()
    if glue_type in ("string", "varchar", "char"):
        return StringType()
    elif glue_type in ("int", "integer"):
        return IntegerType()
    elif glue_type == "bigint":
        return LongType()
    elif glue_type in ("smallint", "tinyint"):
        return IntegerType()
    elif glue_type == "float":
        return FloatType()
    elif glue_type == "double":
        return DoubleType()
    elif glue_type == "boolean":
        return BooleanType()
    elif glue_type == "date":
        return DateType()
    elif glue_type in ("timestamptz", "timestamp with time zone",
                       "timestamp with local time zone"):
        return TimestamptzType()
    elif glue_type.startswith("timestamp"):
        # Hive 'timestamp' is timezone-unaware
        return TimestampType()
    elif glue_type.startswith("decimal"):
        # decimal(10,2) → DecimalType(10, 2)
        try:
            inner = glue_type[glue_type.index("(") + 1:glue_type.index(")")]
            parts = inner.split(",")
            return DecimalType(precision=int(parts[0].strip()),
                               scale=int(parts[1].strip()) if len(parts) > 1 else 0)
        except (ValueError, IndexError):
            return DecimalType(precision=38, scale=18)
    elif glue_type == "binary":
        return BinaryType()
    else:
        # Default to string for unknown types (array, map, struct, etc.)
        return StringType()


def schema_from_glue(columns: list[dict],
                     partition_keys: list[dict]) -> Schema:
    """Build an Iceberg Schema from Glue catalog column definitions.

    Args:
        columns: List of {"name": ..., "type": ...} from Glue StorageDescriptor.Columns
        partition_keys: List of {"name": ..., "type": ...} from Glue Table.PartitionKeys
    """
    fields = []
    fid = 1
    for col in columns:
        fields.append(NestedField(
            field_id=fid, name=col["name"],
            field_type=_glue_type_to_iceberg(col["type"]),
            required=False))
        fid += 1

    # Add partition columns (they are separate in Hive/Glue)
    for pk in partition_keys:
        fields.append(NestedField(
            field_id=fid, name=pk["name"],
            field_type=_glue_type_to_iceberg(pk["type"]),
            required=False))
        fid += 1

    return Schema(*fields)


# Safe automatic widenings: {(glue_type, parquet_type): widened_type}.
# When the Glue catalog declares a narrower numeric type than the physical
# Parquet data, we widen the target column to the physical type so the load
# does not hit a SchemaColumnConvertNotSupportedException or lose data.
_SAFE_WIDENINGS = {
    (IntegerType, LongType): LongType,
    (FloatType, DoubleType): DoubleType,
    (FloatType, LongType): DoubleType,   # int stored as float-ish edge cases
    (IntegerType, DoubleType): DoubleType,
}


def reconcile_schema_with_parquet(schema: Schema,
                                  parquet_types: dict) -> Schema:
    """Reconcile a Glue-derived Iceberg schema against actual Parquet types.

    Policy (Option B, "widen or fail"):
      - Types agree                     -> keep the Glue type.
      - Column absent from the Parquet  -> keep the Glue type (e.g. Hive-style
        partition columns are not stored in the data files).
      - Glue narrower than Parquet in a
        safe numeric way (int->long,
        float->double)                  -> widen the target column to the
        physical type.
      - Any other disagreement          -> raise ValueError (fail loudly rather
        than silently coerce or corrupt).

    Args:
        schema: the Iceberg Schema built from the Glue catalog.
        parquet_types: {column_name: pyarrow.DataType} from an actual data file.

    Returns:
        A possibly-adjusted Iceberg Schema. Field ids and order are preserved.
    """
    new_fields = []
    mismatches = []
    for f in schema.fields:
        glue_t = f.field_type
        arrow_t = parquet_types.get(f.name)
        if arrow_t is None:
            # Not present in the data file (partition column, etc.) — trust Glue.
            new_fields.append(f)
            continue

        parquet_t = _arrow_to_iceberg(arrow_t)
        if type(glue_t) is type(parquet_t):
            # Same Iceberg type — for parameterized types (decimal) also match params.
            if glue_t == parquet_t:
                new_fields.append(f)
                continue

        widen = _SAFE_WIDENINGS.get((type(glue_t), type(parquet_t)))
        if widen is not None:
            log.info("Schema reconcile: widening column '%s' %s -> %s "
                     "(Parquet physical type)",
                     f.name, type(glue_t).__name__, widen.__name__)
            new_fields.append(NestedField(
                field_id=f.field_id, name=f.name,
                field_type=widen(), required=f.required))
            continue

        # Incompatible disagreement — do not guess.
        mismatches.append(
            f"column '{f.name}': catalog={type(glue_t).__name__}, "
            f"parquet={type(parquet_t).__name__}")
        new_fields.append(f)

    if mismatches:
        raise SchemaReconcileError(
            "Source Parquet types are incompatible with the catalog schema "
            "and cannot be safely reconciled: " + "; ".join(mismatches))

    return Schema(*new_fields)


def _partition_field_name(schema: Schema, col: str, transform_name: str) -> str:
    """Choose a PartitionField name that won't collide with a schema column.

    pyiceberg (>=0.10) rejects a partition field whose name matches an existing
    schema column name. For identity partitions on a column that is (correctly)
    also part of the table schema, the natural name `col` collides. Iceberg
    itself stores such identity partitions with the plain column name, so we
    keep `col` when it does not collide and only disambiguate when it does.
    """
    base = col
    # Non-identity transforms conventionally get a transform suffix (e.g.
    # event_date_month) which usually avoids collision on its own.
    if transform_name != "identity":
        base = f"{col}_{transform_name}"
    existing = {f.name for f in schema.fields}
    if base not in existing:
        return base
    # Disambiguate against schema columns (identity case, or a suffixed name
    # that still happens to collide).
    candidate = f"{base}_part"
    while candidate in existing:
        candidate = f"{candidate}_x"
    return candidate


def build_partition_spec(partition_columns: list[str],
                         schema: Schema) -> PartitionSpec:
    """Build an Iceberg PartitionSpec from column names (identity transforms)."""
    pfields = []
    for i, col in enumerate(partition_columns, start=1000):
        src = schema.find_field(col)
        pfields.append(PartitionField(
            source_id=src.field_id, field_id=i,
            transform=IdentityTransform(),
            name=_partition_field_name(schema, col, "identity")))
    return PartitionSpec(*pfields)


def build_partition_spec_from_transforms(transforms: list[dict],
                                         schema: Schema) -> PartitionSpec:
    """Build an Iceberg PartitionSpec from spec transform definitions.

    Handles: identity, year, month, day, hour, bucket, truncate, void.
    Input format: [{"source_column": "dt", "transform": "month"}, ...]
    """
    if not transforms:
        return PartitionSpec()
    pfields = []
    for i, t in enumerate(transforms, start=1000):
        col = t.get("source_column", "")
        transform_name = t.get("transform", "identity")
        try:
            src = schema.find_field(col)
        except Exception:
            log.warning("Partition column '%s' not found in schema, skipping", col)
            continue
        if transform_name == "identity":
            transform = IdentityTransform()
        elif transform_name == "year":
            transform = YearTransform()
        elif transform_name == "month":
            transform = MonthTransform()
        elif transform_name == "day":
            transform = DayTransform()
        elif transform_name == "hour":
            transform = HourTransform()
        elif transform_name == "bucket":
            n = t.get("num_buckets", 16)
            transform = BucketTransform(num_buckets=n)
        elif transform_name == "truncate":
            w = t.get("width", 10)
            transform = TruncateTransform(width=w)
        elif transform_name == "void":
            transform = VoidTransform()
        else:
            transform = IdentityTransform()
            transform_name = "identity"
        pfields.append(PartitionField(
            source_id=src.field_id, field_id=i,
            transform=transform,
            name=_partition_field_name(schema, col, transform_name)))
    return PartitionSpec(*pfields)


def build_sort_order_from_spec(sort_fields: list[dict],
                               schema: Schema) -> SortOrder:
    """Build an Iceberg SortOrder from spec sort field definitions.

    Input format: [{"column": "event_id", "direction": "asc", "null_order": "nulls_last"}]
    """
    if not sort_fields:
        return SortOrder()
    fields = []
    for sf in sort_fields:
        col = sf.get("column", "")
        try:
            src = schema.find_field(col)
        except Exception:
            log.warning("Sort column '%s' not found in schema, skipping", col)
            continue
        direction = SortDirection.ASC if sf.get("direction", "asc").lower() == "asc" else SortDirection.DESC
        null_order = NullOrder.NULLS_LAST if "last" in sf.get("null_order", "nulls_last").lower() else NullOrder.NULLS_FIRST
        fields.append(IcebergSortField(
            source_id=src.field_id,
            transform=IdentityTransform(),
            direction=direction,
            null_order=null_order))
    return SortOrder(*fields)


def _cast_partition_value(raw_value: str | None, field_type) -> object:
    """Cast a string partition value from a Hive path to the correct Python type.

    Hive partition paths encode values as strings (e.g., year=2024, date=2024-01-15).
    pyiceberg's manifest writer expects properly typed values.
    """
    if raw_value is None or raw_value == "__HIVE_DEFAULT_PARTITION__":
        return None

    if isinstance(field_type, (IntegerType, LongType)):
        try:
            return int(raw_value)
        except (ValueError, TypeError):
            return None
    elif isinstance(field_type, (FloatType, DoubleType)):
        try:
            return float(raw_value)
        except (ValueError, TypeError):
            return None
    elif isinstance(field_type, BooleanType):
        return raw_value.lower() in ("true", "1", "yes")
    elif isinstance(field_type, DateType):
        try:
            # Iceberg stores dates as epoch days (int32)
            from datetime import date
            d = date.fromisoformat(raw_value)
            epoch = date(1970, 1, 1)
            return (d - epoch).days
        except (ValueError, TypeError):
            try:
                return int(raw_value)
            except (ValueError, TypeError):
                return None
    elif isinstance(field_type, (TimestampType, TimestamptzType)):
        try:
            # Iceberg stores timestamps as microseconds since epoch
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(raw_value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
            return int((dt - epoch).total_seconds() * 1_000_000)
        except (ValueError, TypeError):
            return None
    elif isinstance(field_type, DecimalType):
        from decimal import Decimal as D
        try:
            return D(raw_value)
        except Exception:
            return None
    else:
        # StringType and others — keep as string
        return raw_value


def _s3tables_warehouse(table_bucket_arn: str, account_id: str = "") -> str:
    """Build the pyiceberg REST ``warehouse`` for an S3 Tables bucket.

    Format: ``<account_id>:s3tablescatalog/<bucket_name>`` (per the AWS Glue
    Iceberg REST endpoint contract). The account id and bucket name are parsed
    from the S3 Tables bucket ARN when present:
        arn:aws:s3tables:<region>:<account>:bucket/<name>
    Falls back to the supplied account_id if the ARN can't be parsed.
    """
    acct = account_id
    bucket_name = table_bucket_arn
    if table_bucket_arn.startswith("arn:"):
        parts = table_bucket_arn.split(":")
        if len(parts) >= 6:
            acct = parts[4] or account_id
            # parts[5] is like "bucket/<name>"
            bucket_name = parts[5].split("/", 1)[-1]
    return f"{acct}:s3tablescatalog/{bucket_name}"


def _build_s3tables_catalog(table_bucket_arn: str, region: str,
                            account_id: str = ""):
    """Load a pyiceberg REST catalog bound to an S3 Tables bucket.

    Uses the AWS Glue Iceberg REST endpoint with SigV4 signing (signing name
    'glue'), which is the AWS-recommended way for pyiceberg to create/manage
    S3 Tables (managed Iceberg) tables. A plain Glue catalog CANNOT create S3
    Tables tables — it writes a Glue entry without the Iceberg ``table_type``
    marker, so the read-back fails with "Property table_type missing".
    """
    warehouse = _s3tables_warehouse(table_bucket_arn, account_id)
    return load_catalog("s3tablescatalog", **{
        "type": "rest",
        "warehouse": warehouse,
        "uri": f"https://glue.{region}.amazonaws.com/iceberg",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "glue",
        "rest.signing-region": region,
    })


class ManifestBuilder:
    """Builds and commits Iceberg manifests from tracked file metadata."""

    def __init__(self, catalog_name: str = "glue",
                 catalog_type: str = "glue",
                 catalog_props: dict = None,
                 warehouse: str = "",
                 target_type: str = "",
                 table_bucket_arn: str = "",
                 region: str = "us-east-1",
                 account_id: str = ""):
        # S3 Tables (managed Iceberg) requires the Glue Iceberg REST catalog
        # with SigV4 — a plain Glue catalog cannot create these tables. For all
        # other targets (Iceberg-on-S3), keep the existing Glue/DynamoDB/etc.
        # catalog wiring unchanged.
        self._is_s3tables = bool(target_type == "s3_tables" and table_bucket_arn)
        if self._is_s3tables:
            self._catalog = _build_s3tables_catalog(
                table_bucket_arn, region, account_id)
        else:
            self._catalog = load_catalog(
                name=catalog_name,
                **{"type": catalog_type, **(catalog_props or {})})
        self._warehouse = warehouse

    def ensure_table(self, database: str, table_name: str,
                     schema: Schema,
                     partition_spec: PartitionSpec,
                     location: str = "",
                     sort_order: SortOrder = None,
                     properties: dict = None) -> Table:
        """Load or create the target Iceberg table.

        Args:
            database: Target database name.
            table_name: Target table name.
            schema: Iceberg schema.
            partition_spec: Iceberg partition spec.
            location: Explicit table location. If provided, used as-is.
                      If empty, derived from warehouse + database + table_name.
            sort_order: Optional Iceberg sort order.
            properties: Optional table properties dict.
        """
        full = f"{database}.{table_name}"
        try:
            table = self._catalog.load_table(full)
            log.info("Loaded existing table %s (location=%s)",
                     full, table.location())
            return table
        except NoSuchTableError:
            log.info("Creating target table %s", full)
            try:
                self._catalog.create_namespace(database)
            except Exception as e:
                # Namespace likely already exists — creating it is idempotent
                # best-effort setup before creating the table.
                log.debug("create_namespace(%s) ignored: %s", database, e)
            tbl_props = properties or {
                "write.parquet.compression-codec": "zstd",
                "format-version": "2",
            }
            kwargs = dict(
                identifier=full, schema=schema,
                partition_spec=partition_spec,
                properties=tbl_props)
            # S3 Tables manages storage itself — passing an explicit S3 location
            # is invalid for the managed catalog. Only set location for
            # Iceberg-on-S3 targets (mirrors the Athena CTAS path, which omits
            # LOCATION for s3_tables).
            if self._is_s3tables:
                log.info("Creating S3 Tables table %s (managed location)", full)
            else:
                loc = location.rstrip("/") if location else (
                    f"{self._warehouse.rstrip('/')}"
                    f"/{database}/{table_name}")
                log.info("Creating table %s at location %s", full, loc)
                kwargs["location"] = loc
            if sort_order and sort_order.fields:
                kwargs["sort_order"] = sort_order
            return self._catalog.create_table(**kwargs)

    def add_files_to_table(self, table: Table,
                           file_paths: list[str],
                           file_sizes: dict[str, int] = None,
                           on_progress: callable = None,
                           chunk_size: int = 500) -> int:
        """Add existing Parquet files to the Iceberg table in chunks.

        Reads each file's Parquet footer for record count and column stats,
        then commits them as DataFile entries via fast_append in batches
        of `chunk_size` for restartability.

        This bypasses pyiceberg's add_files() which has issues with:
        - INT96 timestamps (common in Hive/Spark Parquet)
        - Partition columns not present in Parquet files (Hive-style)

        Args:
            table: The Iceberg Table object.
            file_paths: List of S3 file paths to add.
            file_sizes: Optional dict mapping file_path → size in bytes.
                        If provided, avoids per-file head_object calls.
            on_progress: Optional callback(files_done, files_total).
            chunk_size: Number of files per commit batch (default 500).

        Returns the number of files added.
        """
        if not file_paths:
            return 0
        log.info("Adding %d files to %s via manual DataFile construction "
                 "(chunk_size=%d)", len(file_paths), table.name(), chunk_size)

        from .scanner import read_parquet_footer, parse_partition_values

        spec = table.spec()
        schema = table.schema()
        partition_columns = [
            schema.find_field(f.source_id).name for f in spec.fields
        ] if spec.fields else []

        # Determine the source prefix from the table location
        # Determine the source prefix from the table location
        # (NOT commonprefix of files — that can eat partition directories)
        table_location = str(table.location()).rstrip("/") + "/"
        prefix = table_location

        # Parallelised Parquet footer reads using a thread pool.
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        total = len(file_paths)
        log_interval = max(1, total // 10)
        progress_interval = max(1, total // 20)
        completed_count = 0
        count_lock = threading.Lock()
        # Footer-read parallelism PER morph load. The engine can run up to
        # ENGINE_MORPH_MAX_CONCURRENT morph loads at once (see MorphStrategy),
        # so the aggregate footer-read thread ceiling on the host is
        # MORPH_MAX_CONCURRENT × MORPH_WORKERS. Keep that product modest
        # (defaults: 6 × 12 = 72) so concurrent morph pipelines can't exhaust
        # host threads / socket pools. Raise only with headroom in mind.
        max_workers = int(os.getenv("ENGINE_MORPH_WORKERS", "12"))

        def _process_file(fp: str):
            try:
                record_count, stats = read_parquet_footer(fp)
            except Exception as e:
                log.warning("Skipping non-Parquet file %s: %s", fp, e)
                return None

            # Get file size — prefer pre-fetched sizes from listing
            if file_sizes and fp in file_sizes:
                file_size = file_sizes[fp]
            else:
                file_size = self._get_file_size(fp)

            # Build partition record from path
            partition_record = Record()
            if spec.fields:
                pv = parse_partition_values(fp, prefix, partition_columns)
                vals = []
                for f in spec.fields:
                    src_field = schema.find_field(f.source_id)
                    raw_val = pv.get(src_field.name)
                    typed_val = _cast_partition_value(raw_val, src_field.field_type)
                    vals.append(typed_val)
                partition_record = Record(*vals)

            return DataFile(
                DataFileContent.DATA,
                fp,
                FileFormat.PARQUET,
                partition_record,
                record_count,
                file_size,
                stats.get("column_sizes") or None,
                stats.get("value_counts") or None,
                stats.get("null_value_counts") or None,
                None,  # nan_value_counts
                None,  # lower_bounds
                None,  # upper_bounds
                None, None, None, None,
            )

        # Process files in chunks for restartability.
        # Each chunk: read footers in parallel → commit as one snapshot.
        total_added = 0
        num_chunks = (total + chunk_size - 1) // chunk_size

        for chunk_idx in range(num_chunks):
            chunk_start = chunk_idx * chunk_size
            chunk_end = min(chunk_start + chunk_size, total)
            chunk_paths = file_paths[chunk_start:chunk_end]

            log.info("Chunk %d/%d: processing files %d-%d of %d",
                     chunk_idx + 1, num_chunks, chunk_start + 1, chunk_end, total)

            data_files = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_process_file, fp): fp
                           for fp in chunk_paths}
                for future in as_completed(futures):
                    result = future.result()
                    if result is not None:
                        data_files.append(result)
                    with count_lock:
                        completed_count += 1
                        i = completed_count
                    if i % log_interval == 0:
                        log.info("  Processing file %d/%d (%.0f%%)",
                                 i, total, (i / total) * 100)
                    if on_progress and i % progress_interval == 0:
                        on_progress(i, total)

            if not data_files:
                log.warning("Chunk %d/%d: no valid data files, skipping commit",
                            chunk_idx + 1, num_chunks)
                continue

            # Commit this chunk via fast_append
            log.info("Chunk %d/%d: committing %d data files...",
                     chunk_idx + 1, num_chunks, len(data_files))
            try:
                with table.transaction() as tx:
                    snap = tx.update_snapshot()
                    app = snap.fast_append()
                    for df in data_files:
                        app.append_data_file(df)
                    app.commit()
                total_added += len(data_files)
                log.info("Chunk %d/%d: committed (%d files total so far)",
                         chunk_idx + 1, num_chunks, total_added)
            except Exception as e:
                log.error("Chunk %d/%d: fast_append commit FAILED: %s",
                          chunk_idx + 1, num_chunks, e)
                raise

            # Report progress after each committed chunk
            if on_progress:
                on_progress(chunk_end, total)

            # Refresh table to pick up the new snapshot for the next chunk
            table.refresh()

        # Final verification
        snap = table.current_snapshot()
        if snap is None:
            raise RuntimeError(
                f"Failed to create snapshot for {table.name()} — "
                f"processed {total} files but no snapshot found")
        log.info("add_files complete for %s — snapshot %s, %d files added "
                 "in %d chunk(s)", table.name(), snap.snapshot_id,
                 total_added, num_chunks)
        return total_added

    def _get_file_size(self, file_path: str) -> int:
        """Get file size in bytes from S3."""
        import boto3
        if file_path.startswith("s3://"):
            path = file_path[5:]
            bucket, _, key = path.partition("/")
            s3 = boto3.client("s3")
            try:
                resp = s3.head_object(Bucket=bucket, Key=key)
                return resp["ContentLength"]
            except Exception:
                return 0
        return 0

    def _build_data_file(self, tracked: FileMetadata,
                         table: Table) -> DataFile:
        """Convert FileMetadata into an Iceberg DataFile.

        Used for incremental commits where we already have metadata
        from the event tracking system.
        """
        spec = table.spec()
        partition_record = None
        if spec.fields:
            vals = []
            for f in spec.fields:
                src_field = table.schema().find_field(f.source_id)
                vals.append(tracked.partition_values.get(src_field.name))
            partition_record = Record(*vals)

        df = DataFile(
            DataFileContent.DATA,
            tracked.file_path,
            FileFormat.PARQUET,
            partition_record or Record(),
            tracked.record_count,
            tracked.file_size_bytes,
            tracked.column_sizes or None,
            tracked.value_counts or None,
            tracked.null_value_counts or None,
            None,  # nan_value_counts
            tracked.lower_bounds or None,
            tracked.upper_bounds or None,
            None, None, None, None,  # key_metadata, split_offsets, equality_ids, sort_order_id
        )
        df._spec_id = spec.spec_id
        return df

    def regenerate(self, table: Table,
                   files: list[FileMetadata]) -> int:
        """Full regeneration — replace all manifests from scratch.

        Uses overwrite to clear any existing state first, then appends
        all files. This makes the operation idempotent — safe to retry.
        """
        if not files:
            return 0
        data_files = [self._build_data_file(f, table) for f in files]
        log.info("Regenerating manifests for %s with %d files",
                 table.name(), len(data_files))

        with table.transaction() as tx:
            snap = tx.update_snapshot()
            ow = snap.overwrite()
            # Clear existing files first (idempotent on retry)
            existing = table.scan().plan_files()
            for task in existing:
                ow.delete_data_file(task.file)
            # Add all discovered files
            for df in data_files:
                ow.append_data_file(df)
            ow.commit()

        log.info("Regeneration complete for %s", table.name())
        return len(data_files)

    def commit_incremental(self, table: Table,
                           new_files: list[FileMetadata],
                           deleted_files: list[FileMetadata]
                           ) -> dict[str, int]:
        """Incremental commit — append new, remove deleted."""
        if not new_files and not deleted_files:
            return {"added": 0, "deleted": 0}
        log.info("Incremental commit for %s: %d adds, %d deletes",
                 table.name(), len(new_files), len(deleted_files))
        with table.transaction() as tx:
            snap = tx.update_snapshot()
            if new_files and not deleted_files:
                app = snap.fast_append()
                for f in new_files:
                    app.append_data_file(self._build_data_file(f, table))
                app.commit()
            elif deleted_files and not new_files:
                ow = snap.overwrite()
                for f in deleted_files:
                    ow.delete_data_file(self._build_data_file(f, table))
                ow.commit()
            else:
                ow = snap.overwrite()
                for f in new_files:
                    ow.append_data_file(self._build_data_file(f, table))
                for f in deleted_files:
                    ow.delete_data_file(self._build_data_file(f, table))
                ow.commit()
        return {"added": len(new_files), "deleted": len(deleted_files)}

    def add_files_incremental(self, table: Table,
                              file_paths: list[str]) -> int:
        """Add new files incrementally with stats from footer reads.

        Used during reconcile for newly created files.
        """
        if not file_paths:
            return 0
        log.info("Incrementally adding %d files to %s",
                 len(file_paths), table.name())
        # Reuse the same logic as add_files_to_table
        return self.add_files_to_table(table, file_paths)
