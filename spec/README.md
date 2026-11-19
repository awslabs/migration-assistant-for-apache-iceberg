# Migration Spec v1.0

JSON specification for Iceberg table conversions. Shared contract between the portal, engine, and AI agent.

See `migration_spec.schema.json` for the full JSON Schema. See `example.json` for a complete example.

## Structure

```json
{
  "version": "1.0",
  "source": { "database", "table_name", "format", "location", "aws_account_id", ... },
  "target": { "type", "database", "table_name", ... },
  "strategy": { "type", "execution_engine?", "iam_role_arn?", "batch_size?", "sync_interval?", "swap_tables?", "unmatched_source_fields_policy?", "incompatible_values_policy?" },
  "iceberg_properties": { "partition_spec?", "sort_order?", "schema_overrides?", "table_properties?" }
}
```

## Strategy Types

| Type | Data Movement | Formats |
|------|--------------|---------|
| `s3tables_managed` | AWS-managed (into S3 Tables) | Parquet (S3 Tables target only) |
| `rewrite` | Full copy (Spark CTAS) | Any |
| `incremental` | Batched partitions (Spark) | Any |
| `add_files` | None (register existing files) | Parquet (partitioned, Iceberg-on-S3 target) |
| `morph` | None (manifest generation) | Parquet, Hudi, Delta |

## Target Types

| Type | Description | Notes |
|------|-------------|-------|
| `s3_tables` | Amazon S3 Tables (managed Iceberg) | Target for `s3tables_managed`; not valid for `morph`/`add_files` |
| `s3_iceberg` | Self-managed Iceberg on S3 | Supports `morph` and `add_files` |

## License

Apache-2.0 — see [LICENSE](../LICENSE).
