# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""XTable Lambda client — invokes the XTable Lambda function from the engine."""

import json
import logging
import os

import boto3

log = logging.getLogger(__name__)


class XTableClient:
    """Client for invoking the XTable Lambda function."""

    def __init__(self, lambda_arn: str = "", region: str = ""):
        self._arn = lambda_arn or os.getenv("ENGINE_XTABLE_LAMBDA_ARN", "")
        self._region = region or os.getenv(
            "ENGINE_XTABLE_LAMBDA_REGION",
            os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        self._client = boto3.client("lambda", region_name=self._region)

    def convert(self, source_format: str, table_base_path: str,
                table_name: str, database_name: str = "default",
                sync_mode: str = "FULL") -> dict:
        """Invoke XTable Lambda to convert a table to Iceberg.

        Args:
            source_format: "HUDI" or "DELTA"
            table_base_path: S3 path to the source table root
            table_name: Name for the target Iceberg table
            database_name: Glue database for the target table
            sync_mode: "FULL" (all commits) or "INCREMENTAL" (new only)

        Returns:
            dict with statusCode, body (message, metadata_path, etc.)

        Raises:
            ValueError if Lambda ARN not configured or invocation fails.
        """
        if not self._arn:
            raise ValueError(
                "ENGINE_XTABLE_LAMBDA_ARN not configured. "
                "Set it in Settings or as an environment variable.")

        payload = {
            "source_format": source_format.upper(),
            "target_formats": ["ICEBERG"],
            "table_base_path": table_base_path,
            "table_name": table_name,
            "database_name": database_name,
            "sync_mode": sync_mode,
        }

        log.info("Invoking XTable Lambda: %s → Iceberg (%s.%s) mode=%s",
                 source_format, database_name, table_name, sync_mode)

        try:
            response = self._client.invoke(
                FunctionName=self._arn,
                InvocationType="RequestResponse",  # synchronous
                Payload=json.dumps(payload).encode(),
            )
        except Exception as e:
            log.error("XTable Lambda invocation failed: %s", e)
            raise ValueError(f"XTable Lambda invocation failed: {e}")

        # Parse response
        status_code = response.get("StatusCode", 0)
        if status_code != 200:
            raise ValueError(
                f"XTable Lambda returned status {status_code}")

        # Check for function error
        if response.get("FunctionError"):
            error_payload = response["Payload"].read().decode()
            log.error("XTable Lambda function error: %s", error_payload)
            raise ValueError(
                f"XTable Lambda function error: {error_payload[:500]}")

        result = json.loads(response["Payload"].read().decode())
        log.info("XTable Lambda result: %s", result.get("body", {}).get("message", ""))

        if result.get("statusCode") != 200:
            error_msg = result.get("body", {}).get("message", "Unknown error")
            raise ValueError(f"XTable conversion failed: {error_msg}")

        return result

    def is_configured(self) -> bool:
        """Check if the XTable Lambda ARN is configured."""
        return bool(self._arn)
