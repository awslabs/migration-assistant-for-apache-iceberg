# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Spec — load, validate, and serialize."""

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

SPEC_VERSION = "1.0"


@dataclass
class PartitionKey:
    name: str
    type: str


@dataclass
class Source:
    database: str
    table_name: str
    format: str
    location: str
    aws_account_id: str
    region: str = "us-east-1"
    catalog: str = "glue"
    partitioned: bool = False
    partition_keys: list[PartitionKey] = field(default_factory=list)
    compression: str = ""


@dataclass
class Target:
    type: str  # s3_tables | s3_iceberg
    database: str
    table_name: str
    location: str = ""
    table_bucket_arn: str = ""
    catalog: str = "glue"
    aws_account_id: str = ""
    region: str = "us-east-1"


@dataclass
class Strategy:
    type: str  # rewrite | incremental | morph | add_files | s3tables_managed
    execution_engine: str = "athena"  # athena | emr_serverless
    iam_role_arn: str = ""
    batch_size: int = 5  # partitions per batch (incremental initial load)
    sync_interval: int = 3600  # seconds between sync cycles (incremental)
    swap_tables: bool = False  # whether to swap target table into source location after migration
    # ── Managed Migration (s3tables_managed) options ──
    # Migration policies passed to the S3 Tables Migration API, controlling how
    # the planner handles schema/value mismatches during migration.
    #
    # unmatched_source_fields_policy — source fields with no matching target
    # column. API enum {fail, drop}: "fail" stops the migration; "drop" skips
    # the unmatched fields and proceeds. Default "fail".
    unmatched_source_fields_policy: str = "fail"  # fail | drop
    # incompatible_values_policy — source values that cannot be represented in
    # the target column type. API enum {fail, setNull}: "fail" stops the
    # migration; "setNull" writes NULL for the offending values and proceeds.
    # Default "fail".
    incompatible_values_policy: str = "fail"  # fail | setNull
    # Optional Athena overrides (region / workgroup / output_location) injected
    # by the app from its Settings, so Athena queries use the user-configured
    # values rather than the engine's deploy-time env defaults. Empty/None ⇒ use
    # the AthenaClient env defaults.
    athena: Optional[dict] = None
    # Optional EMR Serverless overrides (application_id / execution_role_arn /
    # region) injected by the app from its Settings, so job submission targets
    # the right REGIONAL application rather than the engine's deploy-time env
    # defaults. Empty/None ⇒ use the EmrServerlessClient env defaults. Without
    # this declared field the spec round-trip through MigrationSpec would drop
    # the block, causing StartJobRun ResourceNotFoundException.
    emr: Optional[dict] = None


@dataclass
class PartitionTransform:
    source_column: str
    transform: str  # identity|year|month|day|hour|bucket|truncate|void
    num_buckets: Optional[int] = None
    width: Optional[int] = None


@dataclass
class SortField:
    column: str
    direction: str = "asc"
    null_order: str = "nulls_last"


@dataclass
class SchemaOverride:
    source_column: str
    target_type: str
    target_name: str = ""


@dataclass
class IcebergProperties:
    partition_spec: list[PartitionTransform] = field(default_factory=list)
    sort_order: list[SortField] = field(default_factory=list)
    schema_overrides: list[SchemaOverride] = field(default_factory=list)
    table_properties: dict[str, str] = field(default_factory=lambda: {
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
    })


@dataclass
class MigrationSpec:
    """The complete migration specification."""
    version: str
    source: Source
    target: Target
    strategy: Strategy
    iceberg_properties: IcebergProperties = field(default_factory=IcebergProperties)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: dict) -> "MigrationSpec":
        src = data["source"]
        src["partition_keys"] = [PartitionKey(**pk) for pk in src.get("partition_keys", [])]

        tgt = data["target"]
        strat = data["strategy"]
        # Filter strategy keys to only accepted fields
        strat_fields = {f.name for f in Strategy.__dataclass_fields__.values()}
        strat = {k: v for k, v in strat.items() if k in strat_fields}

        ip = data.get("iceberg_properties", {})
        # Filter to only accepted fields for each dataclass to handle extra keys from agent
        pt_fields = {"source_column", "transform", "num_buckets", "width"}
        ip["partition_spec"] = [
            PartitionTransform(**{k: v for k, v in pt.items() if k in pt_fields})
            for pt in ip.get("partition_spec", [])
        ]
        sf_fields = {"column", "direction", "null_order"}
        ip["sort_order"] = [
            SortField(**{k: v for k, v in sf.items() if k in sf_fields})
            for sf in ip.get("sort_order", [])
        ]
        so_fields = {"source_column", "target_type", "target_name"}
        ip["schema_overrides"] = [
            SchemaOverride(**{k: v for k, v in so.items() if k in so_fields})
            for so in ip.get("schema_overrides", [])
        ]

        return cls(
            version=data.get("version", SPEC_VERSION),
            source=Source(**src),
            target=Target(**tgt),
            strategy=Strategy(**strat),
            iceberg_properties=IcebergProperties(**ip),
        )

    @classmethod
    def from_json(cls, text: str) -> "MigrationSpec":
        return cls.from_dict(json.loads(text))

    @classmethod
    def load(cls, path: str | Path) -> "MigrationSpec":
        return cls.from_json(Path(path).read_text())
