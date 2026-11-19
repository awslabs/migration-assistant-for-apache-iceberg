# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# Migration Assistant app — Backend API + Frontend static files
# Single container deployment
#
# PREREQUISITE: Build frontend locally first:
#   cd client && npm run build && cd ..
# The dist/ folder must exist before building this image.

FROM public.ecr.aws/docker/library/python:3.13-slim
WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends curl && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY server/pyproject.toml ./
COPY server/app/ app/
# Changes on every deploy (deploy.sh passes a timestamp) so the pip layer is
# rebuilt and dependencies with open ranges (boto3 etc.) resolve to the
# latest release instead of a cached older one.
ARG DEPS_REFRESH=0
RUN echo "deps refresh: ${DEPS_REFRESH}" && \
    pip install --no-cache-dir -e .

# Copy backend code
COPY server/ .

# Copy pre-built frontend
COPY client/dist/ ./static/

# Run as non-root user
RUN useradd -r -s /bin/false appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
