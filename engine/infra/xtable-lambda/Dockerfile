# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# XTable Lambda Container Image
# Packages Apache XTable (Java) with the Python Lambda runtime. The handler runs
# XTable's RunSync through `java -cp` in a subprocess, so the final image needs
# only a JRE and the bundled jar.
#
# Two stages. The builder compiles the jar and carries the JDK, Maven, git and a
# ~1.7 GB Maven cache; none of that is copied into the final image. Deleting
# build files in a later RUN of the same stage would not make the image smaller,
# because every earlier layer is kept and pushed to ECR.

# ── Stage 1: build the XTable bundled jar ────────────────────────────
FROM public.ecr.aws/lambda/python:3.12 AS builder
# Fail a RUN as soon as any command in a pipe fails (not just the last one).
# The AL2023 base ships bash, so use it as the build shell.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# Install Java 11 (XTable 0.2.0 is compatible with Java 11) — AL2023 uses dnf
RUN dnf install -y java-11-amazon-corretto-devel tar gzip && \
    dnf clean all
# Set JAVA_HOME for build
# AL2023 Corretto installs to /usr/lib/jvm/java-11-amazon-corretto.x86_64
RUN ln -sf /usr/lib/jvm/java-11-amazon-corretto.* /usr/lib/jvm/java-11
ENV JAVA_HOME=/usr/lib/jvm/java-11
ENV PATH="/usr/lib/jvm/java-11/bin:$PATH"
# Verify Java is accessible
RUN java -version && echo "JAVA_HOME=$JAVA_HOME" && ls "$JAVA_HOME/bin/java"
# Install Maven from OS packages (avoids external URL download)
RUN dnf install -y maven && \
    dnf clean all
# Clone and build XTable utilities bundled jar (pinned to 0.2.0 stable release)
# xtable-utilities is NOT declared in the root pom.xml <modules>, so we patch
# the pom to include it, then build the entire project in one pass.
# Split into separate RUN steps to work around Docker buildkit DNS timeouts
# during long builds.
# Step 1: Clone repo and patch pom to include xtable-utilities module
RUN dnf install -y git findutils sed && \
    git clone --depth 1 --branch 0.2.0-incubating \
        https://github.com/apache/incubator-xtable.git /build/xtable && \
    cd /build/xtable && \
    sed -i '/<module>xtable-core<\/module>/a\        <module>xtable-utilities</module>' pom.xml && \
    dnf clean all
# Step 2: Download all dependencies (network-intensive, separate layer)
RUN cd /build/xtable && \
    mvn dependency:go-offline -DskipTests -Dspotless.check.skip=true -Drat.skip=true -q || true
# Step 3: Build offline (no network needed, avoids DNS timeout)
RUN cd /build/xtable && \
    mvn package -DskipTests -Dspotless.check.skip=true -Drat.skip=true -o -q || \
    mvn package -DskipTests -Dspotless.check.skip=true -Drat.skip=true -q && \
    mkdir -p /opt/xtable && \
    cp xtable-utilities/target/xtable-utilities_2.12-0.2.0-incubating-bundled.jar \
       /opt/xtable/xtable-utilities-bundled.jar && \
    test -s /opt/xtable/xtable-utilities-bundled.jar || \
      { echo "ERROR: bundled jar not found or empty"; exit 1; } && \
    rm -rf /build /root/.m2

# ── Stage 2: runtime image ───────────────────────────────────────────
FROM public.ecr.aws/lambda/python:3.12
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
# Headless JRE only: no compiler, Maven or git. The symlink gives the handler its
# fixed path (/usr/lib/jvm/java-11/bin/java) whatever the package's directory is
# called, and the -version call fails the build early if the JVM is not usable.
RUN dnf install -y java-11-amazon-corretto-headless && \
    dnf clean all && \
    ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/java-11 && \
    /usr/lib/jvm/java-11/bin/java -version
ENV JAVA_HOME=/usr/lib/jvm/java-11
ENV PATH="/usr/lib/jvm/java-11/bin:$PATH"
# The only thing taken from the builder. Copied before pip and the handler so a
# change to either does not invalidate this ~1 GB layer.
COPY --from=builder /opt/xtable/xtable-utilities-bundled.jar /opt/xtable/xtable-utilities-bundled.jar
ENV XTABLE_JAR_PATH=/opt/xtable/xtable-utilities-bundled.jar
ENV SPARK_LOCAL_IP=127.0.0.1
# Install Python dependencies
COPY requirements.txt .
# Changes on every deploy (deploy.sh passes a timestamp) so the pip layer is
# rebuilt and dependencies with open ranges (boto3 etc.) resolve to the
# latest release instead of a cached older one.
ARG DEPS_REFRESH=0
RUN echo "deps refresh: ${DEPS_REFRESH}" && \
    pip install --no-cache-dir -r requirements.txt
# Copy handler code
COPY lambda_handler.py ${LAMBDA_TASK_ROOT}/
# Lambda manages container lifecycle; no Docker-level health check needed
HEALTHCHECK NONE
CMD ["lambda_handler.handler"]
