#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ═══════════════════════════════════════════════════════════════════════
# Migration Assistant — single-command deployment script
#
# Usage:
#   ./deploy.sh                    # Full deploy (all stacks)
#   ./deploy.sh --engine-only      # Deploy engine stack only
#   ./deploy.sh --app-only         # Deploy app stack only
#   ./deploy.sh --agent-only       # Deploy agent stack only
#   ./deploy.sh --update-images    # Rebuild & push images, redeploy services
#   ./deploy.sh --teardown         # Delete all stacks
#
# Flags (combine with any build mode above):
#   --with-xtable                  # Also build & push the XTable (Hudi/Delta
#                                    morph) image. It is SKIPPED by default
#                                    because the Java/Maven build is slow; when
#                                    skipped the engine reuses the XTable image
#                                    already in ECR (if any).
#
# Prerequisites:
#   - AWS CLI v2 configured
#   - Finch or Docker installed
#   - Node.js + npm (for frontend build)
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────
# NOTE: AWS_ACCOUNT_ID is resolved lazily in preflight() (below) rather than
# here, so a missing/unauthenticated AWS CLI produces a clear preflight error
# instead of a raw failure on the first command.
export AWS_REGION="${AWS_REGION:-us-east-1}"
export ENV_NAME="${ENV_NAME:-ima}"

# The XTable (Java/Maven) image is slow and disk-hungry to build, so it is NOT
# built by default. Pass --with-xtable to build & push it (needed to add/update
# Hudi/Delta morph support). Can also be forced via BUILD_XTABLE=yes in the env.
# When skipped, deploy_engine reuses whatever XTable image is already in ECR
# (or deploys without morph support if none exists), so this flag only controls
# the (re)build, never breaks the deploy.
BUILD_XTABLE="${BUILD_XTABLE:-no}"

# Container runtime: prefer finch, fall back to docker. Detected here but NOT
# required unless the chosen mode builds images (enforced in preflight()).
CONTAINER_CMD=""
if command -v finch &>/dev/null; then
  CONTAINER_CMD="finch"
elif command -v docker &>/dev/null; then
  CONTAINER_CMD="docker"
fi

# ECR URIs — populated by set_image_uris() once AWS_ACCOUNT_ID is resolved in
# preflight() (they depend on the account id, which we no longer resolve at
# file-load time so preflight can report auth errors cleanly).
ECR_BASE=""
ENGINE_IMAGE=""
APP_IMAGE=""
XTABLE_IMAGE=""
DB_BOOTSTRAP_IMAGE=""

set_image_uris() {
  ECR_BASE="$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"
  ENGINE_IMAGE="$ECR_BASE/ima-engine:latest"
  APP_IMAGE="$ECR_BASE/ima-app:latest"
  XTABLE_IMAGE="$ECR_BASE/ima-xtable-converter:latest"
  DB_BOOTSTRAP_IMAGE="$ECR_BASE/ima-db-bootstrap:latest"
  CFN_ARTIFACTS_BUCKET="ima-cfn-artifacts-${AWS_ACCOUNT_ID}-${AWS_REGION}"
}

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ─── Helper Functions ────────────────────────────────────────────────

log() { echo ""; echo "══════ $1"; }
info() { echo "  → $1"; }
err() { echo "  ✗ ERROR: $1" >&2; exit 1; }

# ─── Preflight ───────────────────────────────────────────────────────
# Validates all prerequisites for the chosen mode UP FRONT with clear messages,
# so the script fails fast instead of erroring out partway through a deploy.
#
# Requirements by mode:
#   - AWS CLI present + authenticated + region: ALL modes
#   - Container runtime (finch/docker):         modes that build images
#   - Node.js + npm:                            modes that build the app frontend
preflight() {
  local mode="$1"
  local needs_container="no" needs_node="no"

  case "$mode" in
    --engine-only|--update-images|full|"")
      needs_container="yes" ;;
  esac
  case "$mode" in
    --app-only|--update-images|full|"")
      needs_container="yes"; needs_node="yes" ;;
  esac

  log "Preflight checks"

  # AWS CLI present (required by every mode, including teardown/agent-only).
  command -v aws &>/dev/null || err "AWS CLI not found on PATH. Install AWS CLI v2 and retry."

  # AWS credentials valid — resolve the account id here (single source of truth).
  # A missing/expired session surfaces as a clear message rather than a raw
  # 'Unable to locate credentials' partway through.
  if ! AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)"; then
    err "AWS credentials are not valid or expired. Configure the AWS CLI (or refresh your session) and retry."
  fi
  export AWS_ACCOUNT_ID
  set_image_uris
  info "AWS account $AWS_ACCOUNT_ID | region $AWS_REGION | env $ENV_NAME"

  # Container runtime — only when the mode builds images.
  if [ "$needs_container" = "yes" ]; then
    [ -n "$CONTAINER_CMD" ] || err "Neither finch nor docker found, but mode '$mode' builds container images. Install one and retry."
    info "Container runtime: $CONTAINER_CMD"

    # Finch (unlike Docker Desktop) needs its VM initialized and running before
    # it can build images. Having the binary on PATH is not enough — catch a
    # missing/stopped VM here with an actionable hint instead of letting the
    # build crash later with 'instance "finch" does not exist'.
    if [ "$CONTAINER_CMD" = "finch" ]; then
      if ! finch vm status 2>/dev/null | grep -qi "running"; then
        err "Finch is selected but its VM is not running. Initialize it once with 'finch vm init', then 'finch vm start' (check with 'finch vm status'). If you meant to use Docker instead, remove finch from your PATH so the script falls back to docker."
      fi
      info "Finch VM: running"
    fi
  fi

  # Node.js + npm — only when the mode builds the app frontend.
  if [ "$needs_node" = "yes" ]; then
    command -v node &>/dev/null || err "Node.js not found on PATH, required to build the app frontend. Install Node.js and retry."
    command -v npm &>/dev/null || err "npm not found on PATH, required to build the app frontend. Install npm and retry."
    info "Node $(node --version) | npm $(npm --version)"
  fi

  info "Preflight OK"
}

get_stack_output() {
  local stack="$1" key="$2"
  aws cloudformation describe-stacks --stack-name "$stack" --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$key'].OutputValue" --output text 2>/dev/null
}

wait_for_stack() {
  local stack="$1" operation="$2"
  info "Waiting for $stack ($operation)..."
  aws cloudformation wait "stack-$operation-complete" --stack-name "$stack" --region "$AWS_REGION"
}

# S3 bucket used to stage CloudFormation templates that exceed the 51,200-byte
# inline limit (`aws cloudformation deploy --s3-bucket`). Deterministic name,
# created on demand and independent of any stack so it also works on a fresh
# (no-stacks-yet) deploy. Populated by set_image_uris() once AWS_ACCOUNT_ID is
# resolved in preflight().
CFN_ARTIFACTS_BUCKET=""

ensure_cfn_bucket() {
  if aws s3api head-bucket --bucket "$CFN_ARTIFACTS_BUCKET" --region "$AWS_REGION" 2>/dev/null; then
    return 0
  fi
  info "Creating CloudFormation artifacts bucket: $CFN_ARTIFACTS_BUCKET"
  # us-east-1 must NOT specify a LocationConstraint; all other regions must.
  if [ "$AWS_REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$CFN_ARTIFACTS_BUCKET" --region "$AWS_REGION" >/dev/null
  else
    aws s3api create-bucket --bucket "$CFN_ARTIFACTS_BUCKET" --region "$AWS_REGION" \
      --create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
  fi
}

# ─── ECR Login ───────────────────────────────────────────────────────

ecr_login() {
  log "ECR Login"
  aws ecr get-login-password --region "$AWS_REGION" | \
    "$CONTAINER_CMD" login --username AWS --password-stdin "$ECR_BASE"
}

# ─── Create ECR Repositories (idempotent) ────────────────────────────

create_ecr_repos() {
  log "Creating ECR repositories (if needed)"
  for repo in ima-engine ima-app ima-xtable-converter ima-db-bootstrap; do
    if aws ecr describe-repositories --repository-names "$repo" --region "$AWS_REGION" &>/dev/null; then
      info "$repo already exists"
    else
      aws ecr create-repository --repository-name "$repo" --region "$AWS_REGION" >/dev/null
      info "Created $repo"
    fi
  done
}

# ─── Build & Push Images ─────────────────────────────────────────────

# AWS Lambda requires the Docker Image Manifest V2 Schema 2 format and rejects
# OCI image-index manifests / provenance attestations, which BuildKit-based
# runtimes (finch, Docker Engine v29+) now emit by default — producing the
# error "The image manifest, config or layer media type ... is not supported".
# These flags suppress the provenance/SBOM attestations that create the OCI
# index and force classic Docker media types on push, so Lambda can read the
# image. They ONLY apply to the two Lambda container images (xtable,
# db-bootstrap); the ECS images (engine, app) run on Fargate, which accepts OCI.
# The flags are BuildKit/buildx-only, so we probe support and fall back to no
# extra flags on an old plain `docker build` (which already produces V2 S2).
_LAMBDA_MANIFEST_FLAGS=""
_lambda_manifest_flags_computed=""
lambda_manifest_flags() {
  if [ -z "$_lambda_manifest_flags_computed" ]; then
    _lambda_manifest_flags_computed="yes"
    if "$CONTAINER_CMD" build --help 2>&1 | grep -q -- "--provenance"; then
      # Dropping provenance/SBOM attestations removes the OCI image index that
      # Lambda rejects; the resulting single-platform image is pushed as Docker
      # V2 Schema 2, which Lambda accepts. Keeps the existing separate push step.
      _LAMBDA_MANIFEST_FLAGS="--provenance=false --sbom=false"
    else
      # NOTE: to stderr — this function's stdout is captured for the build flags.
      info "Runtime '$CONTAINER_CMD' build has no --provenance flag; assuming it emits Docker V2 Schema 2 (Lambda-compatible)" >&2
    fi
  fi
  printf '%s' "$_LAMBDA_MANIFEST_FLAGS"
}

build_engine_image() {
  log "Building engine image"
  "$CONTAINER_CMD" build --platform linux/amd64 \
    --build-arg DEPS_REFRESH="$(date +%s)" \
    -t "$ENGINE_IMAGE" \
    "$ROOT_DIR/engine/"
  "$CONTAINER_CMD" push "$ENGINE_IMAGE"
  info "Pushed $ENGINE_IMAGE"
}

build_app_image() {
  log "Building app image"
  info "Building frontend..."
  (cd "$ROOT_DIR/app/client" && npm ci && npm run build)
  "$CONTAINER_CMD" build --platform linux/amd64 \
    --build-arg DEPS_REFRESH="$(date +%s)" \
    -t "$APP_IMAGE" \
    "$ROOT_DIR/app/"
  "$CONTAINER_CMD" push "$APP_IMAGE"
  info "Pushed $APP_IMAGE"
}

build_db_bootstrap_image() {
  log "Building DB bootstrap Lambda image"
  # shellcheck disable=SC2046  # intentional word-split of Lambda manifest flags
  "$CONTAINER_CMD" build --platform linux/amd64 $(lambda_manifest_flags) \
    -t "$DB_BOOTSTRAP_IMAGE" \
    "$ROOT_DIR/app/infra/db-bootstrap-lambda/"
  "$CONTAINER_CMD" push "$DB_BOOTSTRAP_IMAGE"
  info "Pushed $DB_BOOTSTRAP_IMAGE"
}

build_xtable_image() {
  log "Building XTable Lambda image"
  # shellcheck disable=SC2046  # intentional word-split of Lambda manifest flags
  "$CONTAINER_CMD" build --platform linux/amd64 $(lambda_manifest_flags) \
    --build-arg DEPS_REFRESH="$(date +%s)" \
    -t "$XTABLE_IMAGE" \
    "$ROOT_DIR/engine/infra/xtable-lambda/"
  "$CONTAINER_CMD" push "$XTABLE_IMAGE"
  info "Pushed $XTABLE_IMAGE"
}

build_all_images() {
  build_engine_image
  build_app_image
  build_db_bootstrap_image
  # XTable is opt-in (see BUILD_XTABLE / --with-xtable): the Java/Maven build is
  # slow, and deploy_engine reuses the existing ECR image when we skip it.
  if [ "$BUILD_XTABLE" = "yes" ]; then
    build_xtable_image
  else
    info "Skipping XTable image build (pass --with-xtable to include it)"
  fi
}

# ─── Deploy Engine Stack ─────────────────────────────────────────────

deploy_engine() {
  log "Deploying engine stack: $ENV_NAME-engine"
  
  # Only include XTable image if it's been built and pushed. The leading
  # underscore marks this as an intentionally word-split expansion (it holds two
  # space-separated CFN params), which the shell linter ignores by convention.
  local _xtable_param=""
  if aws ecr describe-images --repository-name ima-xtable-converter --image-ids imageTag=latest --region "$AWS_REGION" &>/dev/null; then
    _xtable_param="XTableImageUri=$XTABLE_IMAGE XTableMemorySize=4096"
  else
    info "XTable image not found in ECR — deploying without Hudi/Delta morph support"
  fi

  ensure_cfn_bucket
  aws cloudformation deploy \
    --template-file "$ROOT_DIR/engine/infra/engine-stack.yaml" \
    --stack-name "$ENV_NAME-engine" \
    --s3-bucket "$CFN_ARTIFACTS_BUCKET" \
    --parameter-overrides \
      EnvironmentName="$ENV_NAME" \
      EngineImageUri="$ENGINE_IMAGE" \
      $_xtable_param \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$AWS_REGION" \
    --no-fail-on-empty-changeset
  info "Engine stack deployed"
}

# ─── Upload Spark Scripts ────────────────────────────────────────────

upload_spark_scripts() {
  log "Uploading Spark scripts to S3"
  local bucket
  bucket=$(get_stack_output "$ENV_NAME-engine" "ScriptsBucket")
  if [ -z "$bucket" ]; then
    err "Could not get ScriptsBucket from engine stack outputs"
  fi
  aws s3 sync "$ROOT_DIR/engine/src/spark/" "s3://$bucket/scripts/" --region "$AWS_REGION"
  info "Scripts uploaded to s3://$bucket/scripts/"
}

# ─── Deploy App Stack ────────────────────────────────────────────────

deploy_app() {
  log "Deploying app stack: $ENV_NAME-app"

  # Auto-extract networking outputs from engine stack
  local vpc_id private_a private_b namespace_arn engine_sg engine_token_arn engine_secrets_key_arn
  vpc_id=$(get_stack_output "$ENV_NAME-engine" "VpcId")
  private_a=$(get_stack_output "$ENV_NAME-engine" "PrivateSubnetA")
  private_b=$(get_stack_output "$ENV_NAME-engine" "PrivateSubnetB")
  namespace_arn=$(get_stack_output "$ENV_NAME-engine" "ServiceConnectNamespaceArn")
  engine_sg=$(get_stack_output "$ENV_NAME-engine" "SecurityGroupId")
  engine_token_arn=$(get_stack_output "$ENV_NAME-engine" "EngineApiTokenSecretArn")
  engine_secrets_key_arn=$(get_stack_output "$ENV_NAME-engine" "EngineSecretsKeyArn")

  if [ -z "$vpc_id" ] || [ -z "$private_a" ] || [ -z "$private_b" ] || [ -z "$engine_sg" ] || [ -z "$engine_token_arn" ] || [ -z "$engine_secrets_key_arn" ]; then
    err "Could not get networking outputs from engine stack. Deploy engine first."
  fi

  ensure_cfn_bucket
  aws cloudformation deploy \
    --template-file "$ROOT_DIR/app/infra/app-stack.yaml" \
    --stack-name "$ENV_NAME-app" \
    --s3-bucket "$CFN_ARTIFACTS_BUCKET" \
    --parameter-overrides \
      EnvironmentName="$ENV_NAME" \
      ImageUri="$APP_IMAGE" \
      VpcId="$vpc_id" \
      PrivateSubnetA="$private_a" \
      PrivateSubnetB="$private_b" \
      EngineUrl="http://engine.local:8001" \
      ServiceConnectNamespaceArn="$namespace_arn" \
      EngineSecurityGroupId="$engine_sg" \
      EngineApiTokenSecretArn="$engine_token_arn" \
      EngineSecretsKeyArn="$engine_secrets_key_arn" \
      NotificationSenderEmail="${NOTIFICATION_SENDER_EMAIL:-}" \
      NotificationEventBusName="${NOTIFICATION_EVENT_BUS:-default}" \
      DbBootstrapImageUri="$DB_BOOTSTRAP_IMAGE" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$AWS_REGION" \
    --no-fail-on-empty-changeset
  info "App stack deployed"
}

# ─── Deploy Agent Stack ──────────────────────────────────────────────

deploy_agent() {
  log "Deploying agent stack: $ENV_NAME-agent"
  ensure_cfn_bucket
  aws cloudformation deploy \
    --template-file "$ROOT_DIR/agent/infra/agent-stack.yaml" \
    --stack-name "$ENV_NAME-agent" \
    --s3-bucket "$CFN_ARTIFACTS_BUCKET" \
    --parameter-overrides EnvironmentName="$ENV_NAME" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$AWS_REGION" \
    --no-fail-on-empty-changeset
  info "Agent IAM stack deployed"

  # Deploy agent runtime via AgentCore CLI
  if command -v agentcore &>/dev/null; then
    local role_arn
    role_arn=$(get_stack_output "$ENV_NAME-agent" "AgentExecutionRoleArn")
    if [ -z "$role_arn" ]; then
      err "Could not get AgentExecutionRoleArn from agent stack"
    fi

    # Generate agentcore/agentcore.json for the @aws/agentcore CLI (schema v1)
    mkdir -p "$ROOT_DIR/agent/agentcore"
    cat > "$ROOT_DIR/agent/agentcore/agentcore.json" <<EOF
{
  "name": "IMA",
  "version": 1,
  "managedBy": "CDK",
  "runtimes": [
    {
      "name": "Agent",
      "build": "CodeZip",
      "entrypoint": "main.py",
      "codeLocation": "src",
      "runtimeVersion": "PYTHON_3_14",
      "networkMode": "PUBLIC",
      "protocol": "HTTP",
      "executionRoleArn": "$role_arn"
    }
  ],
  "memories": [],
  "knowledgeBases": [],
  "credentials": [],
  "evaluators": [],
  "onlineEvalConfigs": [],
  "agentCoreGateways": [],
  "policyEngines": [],
  "configBundles": [],
  "abTests": [],
  "harnesses": [],
  "datasets": [],
  "payments": []
}
EOF

    cat > "$ROOT_DIR/agent/agentcore/aws-targets.json" <<EOF
[
  {
    "name": "IMA",
    "account": "$AWS_ACCOUNT_ID",
    "region": "$AWS_REGION"
  }
]
EOF
    info "Generated agentcore/agentcore.json and aws-targets.json"

    # agentcore/ is gitignored (contains account-specific values), so the CDK
    # app scaffold (agentcore/cdk/) won't exist on a fresh clone or machine.
    # Bootstrap it via a throwaway `agentcore create` if missing.
    if [ ! -f "$ROOT_DIR/agent/agentcore/cdk/bin/cdk.ts" ]; then
      info "agentcore/cdk/ scaffold not found — bootstrapping it..."
      local scaffold_dir
      scaffold_dir=$(mktemp -d)
      agentcore create --defaults --project-name IMA --name Agent \
        --language Python --framework Strands --model-provider Bedrock \
        --memory none --protocol HTTP --type create --build CodeZip \
        --network-mode PUBLIC --skip-git --skip-python-setup --skip-install \
        --output-dir "$scaffold_dir" >/dev/null
      cp -r "$scaffold_dir/IMA/agentcore/cdk" "$ROOT_DIR/agent/agentcore/cdk"
      rm -rf "$scaffold_dir"
      info "Installing agentcore/cdk/ dependencies..."
      (cd "$ROOT_DIR/agent/agentcore/cdk" && npm install)
      if [ ! -f "$ROOT_DIR/agent/agentcore/cdk/node_modules/typescript/lib/tsc.js" ]; then
        err "agentcore/cdk/ npm install produced an incomplete node_modules (missing typescript/lib/tsc.js). Run: rm -rf agent/agentcore/cdk/node_modules agent/agentcore/cdk/package-lock.json && npm install --prefix agent/agentcore/cdk"
      fi
      info "agentcore/cdk/ scaffold ready"
    fi

    info "Deploying agent to AgentCore..."
    (cd "$ROOT_DIR/agent" && agentcore deploy --target IMA --yes)
    info "Agent deployed to AgentCore"
  else
    info "agentcore CLI not found. Install with: npm install -g @aws/agentcore"
    info "Then run: cd agent && agentcore deploy --target IMA --yes"
  fi
}

# ─── Update Images & Redeploy Services ───────────────────────────────

update_images() {
  ecr_login
  build_all_images

  log "Force redeploying ECS services"
  aws ecs update-service --cluster "$ENV_NAME-engine" --service "$ENV_NAME-engine" \
    --force-new-deployment --region "$AWS_REGION" >/dev/null
  info "Engine service redeploying"

  aws ecs update-service --cluster "$ENV_NAME-app" --service "$ENV_NAME-app" \
    --force-new-deployment --region "$AWS_REGION" >/dev/null
  info "App service redeploying"

  # Update XTable Lambda only when we actually rebuilt the XTable image;
  # otherwise there is no new image to point it at.
  if [ "$BUILD_XTABLE" = "yes" ]; then
    aws lambda update-function-code --function-name "$ENV_NAME-xtable-converter-$AWS_REGION" \
      --image-uri "$XTABLE_IMAGE" --region "$AWS_REGION" >/dev/null 2>&1 && \
      info "XTable Lambda updated" || info "XTable Lambda not found (skipped)"
  else
    info "Skipping XTable Lambda update (pass --with-xtable to rebuild & update it)"
  fi
}

# ─── Print Outputs ───────────────────────────────────────────────────

print_outputs() {
  log "Deployment complete!"
  echo ""
  echo "  Engine cluster:  $ENV_NAME-engine"
  echo "  App cluster:     $ENV_NAME-app"
  echo "  Region:          $AWS_REGION"
  echo ""

  local db_endpoint
  db_endpoint=$(get_stack_output "$ENV_NAME-app" "DatabaseEndpoint" 2>/dev/null || echo "")
  if [ -n "$db_endpoint" ]; then
    echo "  Database:        $db_endpoint"
  fi

  local xtable_arn
  xtable_arn=$(get_stack_output "$ENV_NAME-engine" "XTableLambdaArn" 2>/dev/null || echo "")
  if [ -n "$xtable_arn" ]; then
    echo "  XTable Lambda:   $xtable_arn"
  fi

  local jumphost_id
  jumphost_id=$(get_stack_output "$ENV_NAME-app" "JumphostInstanceId" 2>/dev/null || echo "")
  if [ -n "$jumphost_id" ]; then
    echo "  Jumphost:        $jumphost_id"
    echo ""
    echo "  Access the UI via SSM port-forward:"
    echo "    aws ssm start-session --target $jumphost_id \\"
    echo "      --document-name AWS-StartPortForwardingSessionToRemoteHost \\"
    echo "      --parameters '{\"host\":[\"ima-app.local\"],\"portNumber\":[\"8000\"],\"localPortNumber\":[\"8000\"]}' \\"
    echo "      --region $AWS_REGION"
    echo ""
    echo "    Then open: http://localhost:8000"
  fi

  echo ""
  echo "  Next steps:"
  echo "    1. Configure settings in the app UI (see docs/deployment-guide.md § 10)"
  echo "    2. Set AGENT_URL to the AgentCore runtime ARN"
  echo ""
}

# ─── Teardown ────────────────────────────────────────────────────────

teardown() {
  log "Tearing down all stacks"
  echo ""
  echo "  This will DELETE:"
  echo "    - $ENV_NAME-agent"
  echo "    - $ENV_NAME-app (including Aurora database)"
  echo "    - $ENV_NAME-engine (including DynamoDB tables)"
  echo ""
  read -rp "  Are you sure? (yes/no): " confirm
  if [ "$confirm" != "yes" ]; then
    echo "  Cancelled."
    exit 0
  fi

  info "Deleting $ENV_NAME-agent..."
  aws cloudformation delete-stack --stack-name "$ENV_NAME-agent" --region "$AWS_REGION" 2>/dev/null || true

  info "Deleting $ENV_NAME-app..."
  aws cloudformation delete-stack --stack-name "$ENV_NAME-app" --region "$AWS_REGION" 2>/dev/null || true

  info "Waiting for app stack deletion..."
  aws cloudformation wait stack-delete-complete --stack-name "$ENV_NAME-app" --region "$AWS_REGION" 2>/dev/null || true

  info "Deleting $ENV_NAME-engine..."
  aws cloudformation delete-stack --stack-name "$ENV_NAME-engine" --region "$AWS_REGION" 2>/dev/null || true

  info "Waiting for engine stack deletion..."
  aws cloudformation wait stack-delete-complete --stack-name "$ENV_NAME-engine" --region "$AWS_REGION" 2>/dev/null || true

  log "Teardown complete"
}

# ─── Main ────────────────────────────────────────────────────────────

usage() {
  echo "Usage: $0 [--engine-only|--app-only|--agent-only|--update-images|--teardown] [--with-xtable]"
}

main() {
  # Parse args in any order: extract the --with-xtable flag (sets BUILD_XTABLE)
  # and keep the single build-mode positional. More than one mode is an error.
  local mode=""
  for arg in "$@"; do
    case "$arg" in
      --with-xtable)
        BUILD_XTABLE="yes"
        ;;
      --engine-only|--app-only|--agent-only|--update-images|--teardown|full)
        if [ -n "$mode" ]; then
          echo "  ✗ ERROR: specify only one build mode (got '$mode' and '$arg')" >&2
          usage; exit 1
        fi
        mode="$arg"
        ;;
      *)
        usage
        exit 1
        ;;
    esac
  done
  mode="${mode:-full}"

  if [ "$BUILD_XTABLE" = "yes" ]; then
    info "XTable image build: ENABLED (--with-xtable)"
  fi

  # Fail fast on missing prerequisites for this mode.
  preflight "$mode"

  case "$mode" in
    --engine-only)
      # XTable is NOT rebuilt unless --with-xtable is given; deploy_engine
      # reuses whatever XTable image is already in ECR (so Hudi/Delta morph
      # support is preserved as long as a prior build pushed it).
      ecr_login
      create_ecr_repos
      build_engine_image
      if [ "$BUILD_XTABLE" = "yes" ]; then
        build_xtable_image
      else
        info "Skipping XTable image build (pass --with-xtable to include it)"
      fi
      deploy_engine
      upload_spark_scripts
      ;;
    --app-only)
      ecr_login
      create_ecr_repos
      build_app_image
      build_db_bootstrap_image
      deploy_app
      ;;
    --agent-only)
      deploy_agent
      ;;
    --update-images)
      update_images
      ;;
    --teardown)
      teardown
      ;;
    full|"")
      ecr_login
      create_ecr_repos
      build_all_images
      deploy_engine
      upload_spark_scripts
      deploy_app
      deploy_agent
      print_outputs
      ;;
    *)
      # Unreachable: the arg loop already validated the mode. Kept as a guard.
      usage
      exit 1
      ;;
  esac
}

main "$@"
