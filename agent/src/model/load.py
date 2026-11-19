# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from strands.models import BedrockModel

from tools.aws import REGION

# Uses the global inference profile for Claude Sonnet 5.5
# https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles-support.html
MODEL_ID = "global.anthropic.claude-sonnet-5-5"

def load_model() -> BedrockModel:
    """
    Get Bedrock model client.
    Uses IAM authentication via the execution role. Region is pinned via the
    shared resolver (AWS_REGION -> AWS_DEFAULT_REGION -> us-east-1) so the
    Bedrock endpoint always resolves even if the runtime session has no region.
    """
    return BedrockModel(model_id=MODEL_ID, region_name=REGION)