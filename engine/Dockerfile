# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

FROM public.ecr.aws/docker/library/python:3.13-slim

WORKDIR /app

# Install psycopg2 build deps + curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc libc-dev curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY src/ src/

# Changes on every deploy (deploy.sh passes a timestamp) so the pip layer is
# rebuilt and dependencies with open ranges (boto3 etc.) resolve to the
# latest release instead of a cached older one.
ARG DEPS_REFRESH=0
RUN echo "deps refresh: ${DEPS_REFRESH}" && \
    pip install --no-cache-dir . psycopg2-binary

# Run as non-root user
RUN useradd -r -s /bin/false appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8001

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8001/health || exit 1

CMD ["ima-engine", "serve", "--host", "0.0.0.0", "--port", "8001"]
