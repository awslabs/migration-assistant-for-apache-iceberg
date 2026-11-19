# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tracking Manager — creates and destroys Lambda functions for S3 event tracking.

Each incremental pipeline gets a dedicated Lambda that monitors the source S3 prefix
for object create/delete events and writes them to the DynamoDB object_tracking table.
"""

import logging
import os
import zipfile
import io
import hashlib

import boto3

from .lambda_code import LAMBDA_CODE_TEMPLATE
from ..utils.polling import wait_for_propagation

log = logging.getLogger(__name__)


def _tracking_table() -> str:
    """Resolve the tracking table name at call time (respects settings pushed after startup)."""
    return os.getenv("ENGINE_TRACKING_TABLE", "ima-object-tracking")


def _tracking_region() -> str:
    """Resolve the tracking region at call time."""
    return os.getenv("ENGINE_TRACKING_REGION",
                     os.getenv("AWS_DEFAULT_REGION",
                               os.getenv("AWS_REGION", "us-east-1")))


def _lambda_function_name(pipeline_id: str, source_location: str = "") -> str:
    """Generate a deterministic Lambda function name.

    Includes both a hash of the source location AND the pipeline ID
    to avoid collisions when multiple pipelines target the same source
    (e.g., retry creates a new pipeline for the same table).
    """
    if source_location:
        loc_hash = hashlib.md5(source_location.encode(), usedforsecurity=False).hexdigest()[:8]
        pid_short = pipeline_id[:8]
        return f"ima-track-{loc_hash}-{pid_short}"
    short_id = pipeline_id[:12]
    return f"ima-track-{short_id}"


def _build_lambda_zip() -> bytes:
    """Package the Lambda code into a zip file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", LAMBDA_CODE_TEMPLATE)
    return buf.getvalue()


def _parse_s3_location(location: str) -> tuple[str, str]:
    """Parse s3://bucket/prefix into (bucket, prefix)."""
    if location.startswith("s3://"):
        location = location[5:]
    parts = location.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") + "/" if len(parts) > 1 else ""
    return bucket, prefix


class TrackingManager:
    """Manages Lambda functions for S3 event tracking per pipeline."""

    def __init__(self):
        self._lambda = boto3.client("lambda", region_name=_tracking_region())
        self._s3 = boto3.client("s3", region_name=_tracking_region())

    def create_tracker(self, pipeline_id: str, source_location: str,
                       source_account: str = "") -> dict:
        """Create a Lambda function and S3 event notification for a pipeline.

        Raises ValueError on any failure — caller should fail the pipeline.
        """
        func_name = _lambda_function_name(pipeline_id, source_location)
        bucket, prefix = _parse_s3_location(source_location)

        log.info("[Tracker] Creating tracker for pipeline %s: func=%s, bucket=%s, prefix=%s",
                 pipeline_id[:8], func_name, bucket, prefix)

        if not os.getenv("ENGINE_TRACKING_LAMBDA_ROLE_ARN", ""):
            raise ValueError(
                "ENGINE_TRACKING_LAMBDA_ROLE_ARN not set. "
                "Cannot create tracker without a Lambda execution role.")

        # ── Step 1: Create or update the Lambda function ──
        log.info("[Tracker] Step 1: Creating Lambda function %s (role=%s...)",
                 func_name, os.getenv("ENGINE_TRACKING_LAMBDA_ROLE_ARN", "")[:60])
        zip_bytes = _build_lambda_zip()
        env_vars = {
            "PIPELINE_ID": pipeline_id,
            "TRACKING_TABLE": _tracking_table(),
        }

        try:
            self._lambda.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=os.getenv("ENGINE_TRACKING_LAMBDA_ROLE_ARN", ""),
                Handler="lambda_function.handler",
                Code={"ZipFile": zip_bytes},
                Timeout=30,
                MemorySize=128,
                Environment={"Variables": env_vars},
                Description=f"IMA S3 event tracker for pipeline {pipeline_id[:8]}",
                Tags={"IMA:pipeline_id": pipeline_id},
            )
            log.info("[Tracker] Lambda %s created successfully", func_name)
        except self._lambda.exceptions.ResourceConflictException:
            log.info("[Tracker] Lambda %s already exists, updating...", func_name)
            self._lambda.update_function_code(
                FunctionName=func_name, ZipFile=zip_bytes)
            # Wait for code update to complete before updating config
            log.info("[Tracker] Waiting for code update to complete...")
            wait_for_propagation(5, "Lambda code update propagation")
            try:
                self._lambda.update_function_configuration(
                    FunctionName=func_name,
                    Environment={"Variables": env_vars})
            except self._lambda.exceptions.ResourceConflictException:
                # Still updating — wait more and retry once
                log.info("[Tracker] Lambda still updating, waiting...")
                wait_for_propagation(5, "Lambda ResourceConflict retry")
                self._lambda.update_function_configuration(
                    FunctionName=func_name,
                    Environment={"Variables": env_vars})
            log.info("[Tracker] Lambda %s updated with pipeline_id=%s",
                     func_name, pipeline_id[:8])
        except Exception as e:
            log.error("[Tracker] Failed to create Lambda %s: %s", func_name, e)
            raise ValueError(f"Lambda creation failed for {func_name}: {e}")

        # Wait for function to become active
        log.info("[Tracker] Waiting for Lambda %s to become active...", func_name)
        wait_for_propagation(3, "Lambda function activation")

        # Verify function is active
        try:
            func_info = self._lambda.get_function(FunctionName=func_name)
            func_state = func_info["Configuration"]["State"]
            func_arn = func_info["Configuration"]["FunctionArn"]
            log.info("[Tracker] Lambda %s state=%s, arn=%s", func_name, func_state, func_arn)
            if func_state != "Active":
                log.info("[Tracker] Waiting additional time for Lambda to activate...")
                wait_for_propagation(5, "Lambda activation retry")
        except Exception as e:
            log.error("[Tracker] Failed to get Lambda %s info: %s", func_name, e)
            raise ValueError(f"Cannot verify Lambda {func_name}: {e}")

        # ── Step 2: Add permission for S3 to invoke the Lambda ──
        statement_id = f"s3-invoke-{func_name.replace('ima-track-', '')}"
        log.info("[Tracker] Step 2: Adding S3 invoke permission (statement=%s, bucket=%s)",
                 statement_id, bucket)
        try:
            self._lambda.add_permission(
                FunctionName=func_name,
                StatementId=statement_id,
                Action="lambda:InvokeFunction",
                Principal="s3.amazonaws.com",
                SourceArn=f"arn:aws:s3:::{bucket}",
            )
            log.info("[Tracker] S3 invoke permission added successfully")
        except self._lambda.exceptions.ResourceConflictException:
            log.info("[Tracker] S3 invoke permission already exists (statement=%s)", statement_id)
        except Exception as e:
            log.error("[Tracker] Failed to add S3 invoke permission: %s", e)
            raise ValueError(f"Lambda permission failed for {func_name}: {e}")

        # Wait for permission propagation before S3 validates it
        log.info("[Tracker] Waiting for Lambda permission to propagate...")
        wait_for_propagation(5, "Lambda invoke permission propagation")

        # ── Step 3: Configure S3 bucket notification ──
        notification_id = f"ima-{func_name.replace('ima-track-', '')}"
        log.info("[Tracker] Step 3: Configuring S3 notification (id=%s, bucket=%s, prefix=%s)",
                 notification_id, bucket, prefix)
        try:
            existing = self._s3.get_bucket_notification_configuration(Bucket=bucket)
            lambda_configs = existing.get("LambdaFunctionConfigurations", [])
            log.info("[Tracker] Existing bucket notifications: %d Lambda configs", len(lambda_configs))

            # Remove old notification with same ID if exists
            # Also remove any stale ima notifications whose Lambda no longer exists
            cleaned_configs = []
            for c in lambda_configs:
                config_id = c.get("Id", "")
                if config_id == notification_id:
                    continue  # Will be replaced by new config
                if config_id.startswith("ima-"):
                    # Verify the Lambda still exists
                    try:
                        self._lambda.get_function(FunctionName=c["LambdaFunctionArn"])
                        cleaned_configs.append(c)
                    except self._lambda.exceptions.ResourceNotFoundException:
                        log.info("[Tracker] Removing stale notification %s (Lambda deleted)", config_id)
                    except Exception:
                        cleaned_configs.append(c)  # Keep if we can't verify
                else:
                    cleaned_configs.append(c)
            lambda_configs = cleaned_configs

            new_config = {
                "Id": notification_id,
                "LambdaFunctionArn": func_arn,
                "Events": [
                    "s3:ObjectCreated:*",
                    "s3:ObjectRemoved:*",
                ],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": prefix},
                        ]
                    }
                },
            }
            lambda_configs.append(new_config)

            # Retry with backoff — S3 validates Lambda permission which may take time to propagate
            last_err = None
            for attempt in range(3):
                try:
                    self._s3.put_bucket_notification_configuration(
                        Bucket=bucket,
                        NotificationConfiguration={
                            "LambdaFunctionConfigurations": lambda_configs,
                            "TopicConfigurations": existing.get("TopicConfigurations", []),
                            "QueueConfigurations": existing.get("QueueConfigurations", []),
                        })
                    log.info("[Tracker] S3 notification configured successfully: "
                             "id=%s, prefix=%s, arn=%s", notification_id, prefix, func_arn)
                    last_err = None
                    break
                except Exception as retry_err:
                    last_err = retry_err
                    if attempt < 2:
                        wait = (attempt + 1) * 5
                        log.info("[Tracker] S3 notification validation failed (attempt %d/3), "
                                 "retrying in %ds: %s", attempt + 1, wait, retry_err)
                        wait_for_propagation(wait, "S3 notification validation retry")
            if last_err:
                log.error("[Tracker] Failed to configure S3 notification on %s/%s: %s",
                          bucket, prefix, last_err)
                raise ValueError(
                    f"S3 notification failed on {bucket}/{prefix}: {last_err}")
        except ValueError:
            raise
        except Exception as e:
            log.error("[Tracker] Failed to configure S3 notification on %s/%s: %s",
                      bucket, prefix, e)
            raise ValueError(
                f"S3 notification failed on {bucket}/{prefix}: {e}")

        log.info("[Tracker] ✓ Tracker creation complete for pipeline %s", pipeline_id[:8])
        return {"function_name": func_name, "status": "active",
                "bucket": bucket, "prefix": prefix}

    def destroy_tracker(self, pipeline_id: str, source_location: str) -> dict:
        """Remove the Lambda function and S3 event notification for a pipeline."""
        func_name = _lambda_function_name(pipeline_id, source_location)
        bucket, prefix = _parse_s3_location(source_location)

        log.info("[Tracker] Destroying tracker: func=%s, bucket=%s, prefix=%s",
                 func_name, bucket, prefix)

        # Step 1: Remove S3 notification
        notification_id = f"ima-{func_name.replace('ima-track-', '')}"
        try:
            existing = self._s3.get_bucket_notification_configuration(Bucket=bucket)
            lambda_configs = existing.get("LambdaFunctionConfigurations", [])
            lambda_configs = [c for c in lambda_configs if c.get("Id") != notification_id]

            self._s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={
                    "LambdaFunctionConfigurations": lambda_configs,
                    "TopicConfigurations": existing.get("TopicConfigurations", []),
                    "QueueConfigurations": existing.get("QueueConfigurations", []),
                })
            log.info("[Tracker] Removed S3 notification %s from %s", notification_id, bucket)
        except Exception as e:
            log.warning("[Tracker] Failed to remove S3 notification: %s", e)

        # Step 2: Delete the Lambda function
        try:
            self._lambda.delete_function(FunctionName=func_name)
            log.info("[Tracker] Deleted Lambda function %s", func_name)
        except self._lambda.exceptions.ResourceNotFoundException:
            log.info("[Tracker] Lambda %s already deleted", func_name)
        except Exception as e:
            log.warning("[Tracker] Failed to delete Lambda %s: %s", func_name, e)

        return {"function_name": func_name, "status": "destroyed"}
