#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# ═══════════════════════════════════════════════════════════════════════
# Migration Assistant — print the values to enter under Manage > Settings in the app
#
# Reads the deployed CloudFormation stacks (read-only) and prints each setting
# that has to be filled in after a deployment, grouped the way the Settings
# page groups them. Everything not listed keeps its default.
#
# Usage:
#   ./show-ui-settings.sh                       # uses ENV_NAME / AWS_REGION
#   ./show-ui-settings.sh --env ima-beta --region us-east-2
#   ./show-ui-settings.sh --plain               # KEY=VALUE lines only
#   ./show-ui-settings.sh --agent-arn <arn>     # skip agent auto-detection
#
# Environment (same names as deploy.sh):
#   ENV_NAME    stack prefix                       (default: ima)
#   AWS_REGION  region the stacks are deployed to  (default: us-east-1)
#   AGENT_ARN   AgentCore runtime ARN, if known    (optional)
#
# Exit status: 0 when every required value was found, 1 otherwise.
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

ENV_NAME="${ENV_NAME:-ima}"
AWS_REGION="${AWS_REGION:-us-east-1}"
AGENT_ARN="${AGENT_ARN:-}"
PLAIN=no
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
MISSING=0

fail() { echo "ERROR: $*" >&2; exit 2; }

usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --env)       [ $# -ge 2 ] || fail "--env needs a value"; ENV_NAME="$2"; shift 2 ;;
    --region)    [ $# -ge 2 ] || fail "--region needs a value"; AWS_REGION="$2"; shift 2 ;;
    --agent-arn) [ $# -ge 2 ] || fail "--agent-arn needs a value"; AGENT_ARN="$2"; shift 2 ;;
    --plain)     PLAIN=yes; shift ;;
    -h|--help)   usage; exit 0 ;;
    *)           fail "unknown option: $1 (try --help)" ;;
  esac
done

command -v aws >/dev/null 2>&1 || fail "AWS CLI not found"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" \
  || fail "AWS credentials are missing or expired"

# ─── Stack outputs ───────────────────────────────────────────────────
ENGINE_STACK="$ENV_NAME-engine"
APP_STACK="$ENV_NAME-app"

ENGINE_OUT="$(aws cloudformation describe-stacks --stack-name "$ENGINE_STACK" --region "$AWS_REGION" \
  --query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output text 2>&1)" \
  || fail "could not read stack $ENGINE_STACK in $AWS_REGION: $ENGINE_OUT"

# engine_out <OutputKey> → value, or empty when the stack has no such output.
engine_out() {
  printf '%s\n' "$ENGINE_OUT" | awk -F'\t' -v k="$1" '$1 == k { print $2; exit }'
}

# app_param <ParameterKey> → value from the app stack, or empty.
app_param() {
  local v
  v="$(aws cloudformation describe-stacks --stack-name "$APP_STACK" --region "$AWS_REGION" \
    --query "Stacks[0].Parameters[?ParameterKey=='$1'].ParameterValue" --output text 2>/dev/null || true)"
  [ "$v" = "None" ] && v=""
  printf '%s' "$v"
}

# Bucket outputs are plain names already; strip an ARN or s3:// prefix if one
# ever appears so the Settings field always gets the bare name.
bucket_name() {
  local b="$1"
  b="${b#arn:aws:s3:::}"; b="${b#s3://}"; b="${b%%/*}"
  printf '%s' "$b"
}

# ─── AgentCore runtime ARN (not a CloudFormation output) ─────────────
# Order: --agent-arn / AGENT_ARN, then the local agentcore deploy state, then
# the AgentCore control plane. Only an ARN in this account AND region counts,
# so a stale state file from another deployment is never picked up.
AGENT_FINDER='
import json, sys
path, region, account = sys.argv[1:4]
arns = set()
def walk(o):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == "runtimeArn" and isinstance(v, str):
                arns.add(v)
            else:
                walk(v)
    elif isinstance(o, list):
        for v in o:
            walk(v)
walk(json.load(open(path)))
needle = ":%s:%s:runtime/" % (region, account)
print("\n".join(sorted(a for a in arns if needle in a)))
'

resolve_agent_arn() {
  local found="" state="$ROOT_DIR/agent/agentcore/.cli/deployed-state.json"
  if [ -n "$AGENT_ARN" ]; then printf '%s' "$AGENT_ARN"; return; fi
  if [ -f "$state" ] && command -v python3 >/dev/null 2>&1; then
    found="$(python3 -c "$AGENT_FINDER" "$state" "$AWS_REGION" "$ACCOUNT_ID" 2>/dev/null || true)"
  fi
  if [ -z "$found" ]; then
    found="$(aws bedrock-agentcore-control list-agent-runtimes --region "$AWS_REGION" \
      --query "agentRuntimes[?contains(agentRuntimeName, 'IMA')].agentRuntimeArn" \
      --output text 2>/dev/null | tr '\t' '\n' || true)"
    [ "$found" = "None" ] && found=""
  fi
  # Exactly one candidate, or we do not guess.
  if [ -n "$found" ] && [ "$(printf '%s\n' "$found" | wc -l | tr -d ' ')" = "1" ]; then
    printf '%s' "$found"
  fi
}

# ─── Output helpers ──────────────────────────────────────────────────
section() { [ "$PLAIN" = yes ] || printf '\n%s\n' "$1"; }

# row <KEY> <VALUE> <hint shown when VALUE is empty>   (a required setting)
row() {
  local key="$1" value="$2" hint="$3"
  if [ -z "$value" ]; then
    MISSING=$((MISSING + 1))
    if [ "$PLAIN" = yes ]; then printf '# %s: not found (%s)\n' "$key" "$hint"
    else printf '  %-44s <not found: %s>\n' "$key" "$hint"; fi
  else
    if [ "$PLAIN" = yes ]; then printf '%s=%s\n' "$key" "$value"
    else printf '  %-44s %s\n' "$key" "$value"; fi
  fi
}

# optional_row <KEY> <VALUE> <note when VALUE is empty>
optional_row() {
  local key="$1" value="$2" note="$3"
  if [ -n "$value" ]; then row "$key" "$value" ""
  elif [ "$PLAIN" = yes ]; then printf '# %s: %s\n' "$key" "$note"
  else printf '  %-44s (%s)\n' "$key" "$note"; fi
}

# ─── Collect ─────────────────────────────────────────────────────────
LOGS_BUCKET="$(bucket_name "$(engine_out LogsBucket)")"
SCRIPTS_BUCKET="$(bucket_name "$(engine_out ScriptsBucket)")"
SCRIPTS_URI=""; [ -n "$SCRIPTS_BUCKET" ] && SCRIPTS_URI="s3://$SCRIPTS_BUCKET/scripts"
ATHENA_WG="$(engine_out AthenaWorkgroup)"
EMR_APP="$(engine_out EmrApplicationId)"
EMR_ROLE="$(engine_out EmrExecutionRoleArn)"
TRACK_ROLE="$(engine_out TrackingLambdaRoleArn)"
XTABLE_ARN="$(engine_out XTableLambdaArn)"
S3T_ROLE="$(engine_out S3TablesMigrationRoleArn)"
AGENT="$(resolve_agent_arn)"

# ─── Print ───────────────────────────────────────────────────────────
if [ "$PLAIN" != yes ]; then
  echo "Migration Assistant UI settings — env: $ENV_NAME, region: $AWS_REGION, account: $ACCOUNT_ID"
  echo "Enter these under Manage > Settings. Anything not listed keeps its default."
fi

section "General"
row "APP_REGION"       "$AWS_REGION" ""
row "ENGINE_LOGS_BUCKET"  "$LOGS_BUCKET" "engine stack output LogsBucket"

section "Engine"
row "AGENT_URL"             "$AGENT" "run: cd agent && agentcore status, or pass --agent-arn"

section "Athena"
row "ENGINE_ATHENA_WORKGROUP"        "$ATHENA_WG" "engine stack output AthenaWorkgroup"

section "EMR Serverless"
row "ENGINE_EMR_APPLICATION_ID"      "$EMR_APP"    "engine stack output EmrApplicationId"
row "ENGINE_EMR_EXECUTION_ROLE_ARN"  "$EMR_ROLE"   "engine stack output EmrExecutionRoleArn"
row "ENGINE_EMR_SCRIPTS_URI"         "$SCRIPTS_URI" "engine stack output ScriptsBucket"

section "Tracking"
row "ENGINE_TRACKING_LAMBDA_ROLE_ARN" "$TRACK_ROLE" "engine stack output TrackingLambdaRoleArn"

section "Morph"
optional_row "ENGINE_XTABLE_LAMBDA_ARN" "$XTABLE_ARN" "XTable not deployed: leave empty, Hudi/Delta morph is unavailable"

section "Managed Migration"
row "ENGINE_S3TABLES_MIGRATION_ROLE_ARN" "$S3T_ROLE" "engine stack output S3TablesMigrationRoleArn"

# Everything below is informational and is left out of --plain output.
if [ "$PLAIN" != yes ]; then
  BUS="$(app_param NotificationEventBusName)"
  SENDER="$(app_param NotificationSenderEmail)"
  section "Notifications (optional; off by default)"
  printf '  %-44s %s\n' "APP_NOTIFICATIONS_ENABLED" "true  (to turn them on; needs a verified SES sender)"
  printf '  %-44s %s\n' "APP_NOTIFICATIONS_EVENTBRIDGE_BUS" "${BUS:-default}"
  printf '  %-44s %s\n' "SES sender (app stack parameter)" "${SENDER:-<not set>}"

  section "Only when the engine runs outside the stack (for example locally)"
  printf '  %-44s %s\n' "ENGINE_TRACKING_TABLE"   "$ENV_NAME-object-tracking-$AWS_REGION"
  printf '  %-44s %s\n' "ENGINE_VALIDATION_TABLE" "$ENV_NAME-validation-results-$AWS_REGION"
  echo "  Leave both empty on a deployed stack: the engine task already has them."

  section "Set by the stack, nothing to enter"
  echo "  ENGINE_URL is the engine address, $(engine_out EngineInternalUrl)."
  echo "  The engine API token is injected from the secret $(engine_out EngineApiTokenSecretArn)."
  echo
fi

if [ "$MISSING" -gt 0 ]; then
  echo "WARNING: $MISSING required value(s) not found (see above)." >&2
  exit 1
fi
