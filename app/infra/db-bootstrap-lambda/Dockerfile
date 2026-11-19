# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# DB Bootstrap Lambda — CloudFormation custom resource that provisions the
# dedicated IAM-auth application database user. Pure-Python (pg8000), no build deps.
FROM public.ecr.aws/lambda/python:3.12

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

# Lambda manages container lifecycle; no Docker-level health check needed
HEALTHCHECK NONE

CMD ["lambda_function.handler"]
