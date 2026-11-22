# Deployment Guide

## 1. Prerequisites

- AWS CLI v2 configured
- Finch **or** Docker installed — used to build and push the container images.
- Node.js 18+ (repo is pinned to 24.13.0 via `.mise.toml`; used for frontend build and the `@aws/agentcore` CLI)
- The [`@aws/agentcore` CLI](https://docs.aws.amazon.com/bedrock/latest/userguide/agentcore-toolkit.html) installed globally: `npm install -g @aws/agentcore`
- Your target AWS account/region [CDK-bootstrapped](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html): `cdk bootstrap aws://ACCOUNT_ID/REGION` (the agent deploy step runs a CDK app under the hood)
- AWS account with permissions for IAM, VPC, ECS, DynamoDB, Lambda, ECR, Athena, EMR, Aurora, Bedrock AgentCore

> **Container runtime — Finch requires a one-time VM init.** `./deploy.sh`
> **prefers Finch when both Finch and Docker are installed**: it uses `finch` if
> the binary is on your `PATH`, otherwise falls back to `docker`. If Finch is the
> runtime being used, having the binary installed is *not* sufficient — you must
> initialize its VM first, or the deploy (and the manual `finch build` commands
> below) fail with
> `instance "finch" does not exist, run 'finch vm init' to create a new instance`:
>
> ```bash
> finch vm init     # one time, creates the VM
> finch vm start    # if the VM is stopped (e.g. after a reboot)
> finch vm status   # verify it reports "Running"
> ```
>
> Docker Desktop does not need this — just make sure the daemon is running. If
> you have both installed and want to use Docker, remove Finch from your `PATH`
> (or uninstall it) so the script falls back to Docker.

> **Note on the manual commands below:** the `finch build` / `finch push` /
> `finch login` commands in this guide assume Finch. If you are using Docker,
> substitute `docker` for `finch` in each of them.

> **Note:** `agent/agentcore/` (config + generated CDK project) is gitignored since it contains account-specific ARNs. `./deploy.sh --agent-only` regenerates the config and bootstraps the CDK project automatically on first run for a fresh clone.

## 2. Variables

```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=us-east-2
export ENV_NAME=ima
```

---

## 3. Create ECR Repositories

```bash
aws ecr create-repository --repository-name ima-engine --region $AWS_REGION
aws ecr create-repository --repository-name ima-app --region $AWS_REGION
aws ecr create-repository --repository-name ima-xtable-converter --region $AWS_REGION
aws ecr create-repository --repository-name ima-db-bootstrap --region $AWS_REGION
```

## 4. ECR Login

```bash
aws ecr get-login-password --region $AWS_REGION | \
  finch login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
```

## 5. Build & Push Images

### 5.1 Engine

```bash
# Take about 5 minutes to build the image
finch build --platform linux/amd64 \
  -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-engine:latest \
  engine/

finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-engine:latest
```

> **Managed Migration prerequisite — boto3 version.** The Managed Migration
> strategy (`s3tables_managed`) calls the S3 Tables Migration API, which is
> included in boto3/botocore **1.43.124** and later. The engine pins
> `boto3>=1.43.124`, so the image picks it up with no extra steps.

### 5.2 App (Frontend + Backend)

Build the frontend first — the Dockerfile copies the pre-built `client/dist/` static files, it doesn't build them:

```bash
cd app/client && npm run build && cd ../..
```

```bash
finch build --platform linux/amd64 \
  -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-app:latest \
  app/

finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-app:latest
```

### 5.3 XTable Lambda (optional — Hudi/Delta morph only)

> **Optional and skipped by default.** The XTable image is only needed for the
> Morph strategy on **Hudi/Delta** sources; all other strategies and the
> Parquet morph path work without it. Because the Java/Maven build is slow
> (~15 min) and disk-hungry, `./deploy.sh` does **not** build it unless you pass
> `--with-xtable` (or set `BUILD_XTABLE=yes`) — e.g. `./deploy.sh --with-xtable`
> or `./deploy.sh --engine-only --with-xtable`. When skipped, the engine stack
> reuses whatever XTable image is already in ECR (deploying without morph
> support if none exists), so a deploy never fails on its absence. Build it the
> first time you need Hudi/Delta morph, or whenever the converter changes. If
> you build images manually, run the step below.

```bash
# Takes about 15 minutes to build the image
finch build --platform linux/amd64 \
  -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-xtable-converter:latest \
  engine/infra/xtable-lambda/

finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-xtable-converter:latest
```

> **Hudi version compatibility:** The XTable Lambda (v0.2.0) supports Hudi tables
> written with Hudi 0.14.x or 0.15.x. Hudi 1.x tables use a new timeline layout
> that XTable 0.2.0 cannot read. If your source Hudi tables were written with
> Hudi 1.x, XTable conversions (Morph strategy for Hudi sources) will fail. Upgrade
> to XTable 0.3.0+ when available for Hudi 1.x support.

### 5.4 DB Bootstrap Lambda

Provisions the dedicated IAM-auth application database user.

```bash
finch build --platform linux/amd64 \
  -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-db-bootstrap:latest \
  app/infra/db-bootstrap-lambda/

finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-db-bootstrap:latest
```

---

## 6. Deploy Engine Stack

The engine template exceeds CloudFormation's 51,200-byte inline limit, so it
must be staged in S3 via `--s3-bucket`. Create a one-time artifacts bucket (the
`deploy.sh` script does this automatically):

```bash
CFN_BUCKET=ima-cfn-artifacts-$AWS_ACCOUNT_ID-$AWS_REGION
aws s3api head-bucket --bucket "$CFN_BUCKET" --region $AWS_REGION 2>/dev/null || \
  aws s3api create-bucket --bucket "$CFN_BUCKET" --region $AWS_REGION \
    $( [ "$AWS_REGION" = "us-east-1" ] || echo --create-bucket-configuration "LocationConstraint=$AWS_REGION" )

aws cloudformation deploy \
  --template-file engine/infra/engine-stack.yaml \
  --stack-name $ENV_NAME-engine \
  --s3-bucket "$CFN_BUCKET" \
  --parameter-overrides \
    EnvironmentName=$ENV_NAME \
    EngineImageUri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-engine:latest \
    XTableImageUri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-xtable-converter:latest \
    XTableMemorySize=4096 \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION
```

> **Recommended: generate least-privilege data-plane policies at deploy time.**
> Instead of hand-editing IAM policies after deployment (§10), pass your source
> and target buckets as parameters and the stack generates tightly-scoped
> policies automatically (explicit read/write actions on the named bucket ARNs,
> never `s3:Get*`, with an optional `aws:ResourceAccount` guard):
>
> ```bash
>     SourceBucketNames=my-source-bucket-a,my-source-bucket-b \
>     TargetBucketNames=my-iceberg-target-bucket \
>     SourceAccountIds=111122223333 \
> ```
>
> Add these to the `--parameter-overrides` above. If omitted, the data-plane
> policies ship as an inert `Deny` placeholder (safe default — migrations will
> not work until you grant access per §10). The same `SourceBucketNames` /
> `SourceAccountIds` parameters are also accepted by the **agent** stack (§9.1).


## 7. Upload Spark Scripts

```bash
SCRIPTS_BUCKET=$(aws cloudformation describe-stacks --stack-name $ENV_NAME-engine \
  --query "Stacks[0].Outputs[?OutputKey=='ScriptsBucket'].OutputValue" --output text --region $AWS_REGION)

aws s3 sync engine/src/spark/ s3://$SCRIPTS_BUCKET/scripts/
```

---

## 8. Deploy App Stack

```bash
# Get VPC and networking outputs from engine stack
VPC_ID=$(aws cloudformation describe-stacks --stack-name $ENV_NAME-engine \
  --query "Stacks[0].Outputs[?OutputKey=='VpcId'].OutputValue" --output text --region $AWS_REGION)
PRIVATE_A=$(aws cloudformation describe-stacks --stack-name $ENV_NAME-engine \
  --query "Stacks[0].Outputs[?OutputKey=='PrivateSubnetA'].OutputValue" --output text --region $AWS_REGION)
PRIVATE_B=$(aws cloudformation describe-stacks --stack-name $ENV_NAME-engine \
  --query "Stacks[0].Outputs[?OutputKey=='PrivateSubnetB'].OutputValue" --output text --region $AWS_REGION)
NAMESPACE_ARN=$(aws cloudformation describe-stacks --stack-name $ENV_NAME-engine \
  --query "Stacks[0].Outputs[?OutputKey=='ServiceConnectNamespaceArn'].OutputValue" --output text --region $AWS_REGION)

aws cloudformation deploy \
  --template-file app/infra/app-stack.yaml \
  --stack-name $ENV_NAME-app \
  --s3-bucket "$CFN_BUCKET" \
  --parameter-overrides \
    EnvironmentName=$ENV_NAME \
    ImageUri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-app:latest \
    VpcId=$VPC_ID \
    PrivateSubnetA=$PRIVATE_A \
    PrivateSubnetB=$PRIVATE_B \
    EngineUrl=http://engine.local:8001 \
    ServiceConnectNamespaceArn=$NAMESPACE_ARN \
    DbBootstrapImageUri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-db-bootstrap:latest \
    ServiceConnectNamespaceArn=$NAMESPACE_ARN \
    EngineSecurityGroupId=$ENGINE_SG \
    DbBootstrapImageUri=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-db-bootstrap:latest \
    RequireMfa=false \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION
```

> `RequireMfa` (default `false`) makes TOTP MFA **mandatory** for admin/editor
> users when set to `true` — they must enroll a second factor before using the
> app. Leave it `false` to keep MFA opt-in per user. See [§12](#12-authentication).

> The seeded `admin` account's one-time password is generated into Secrets
> Manager at `$ENV_NAME/app/bootstrap-admin-password`. Retrieve it at first login 

> `NotificationSenderEmail` and `NotificationEventBusName` are optional (default to
> empty and `default`). See [§11 Notifications](#11-notifications-optional) for setup.
> The app stack always creates the notification Lambda + EventBridge rule; they stay
> dormant until notifications are enabled in Settings and a verified SES sender is set.

---

## 9. Deploy Agent

### 9.1 Deploy IAM Role & Artifacts Bucket (CloudFormation)

```bash
aws cloudformation deploy \
  --template-file agent/infra/agent-stack.yaml \
  --stack-name $ENV_NAME-agent \
  --s3-bucket "$CFN_BUCKET" \
  --parameter-overrides EnvironmentName=$ENV_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $AWS_REGION
```

> Like the engine stack, the agent stack accepts optional `SourceBucketNames`
> and `SourceAccountIds` parameters to generate a least-privilege READ-ONLY
> data-access policy for the agent automatically. Omit them to ship the inert
> placeholder (grant access later per §10).

### 9.2 Deploy Agent to AgentCore (CLI)

Install the AgentCore CLI (Node):

```bash
npm install -g @aws/agentcore
agentcore --version   # should print the Node CLI version, e.g. 0.28.0
```

> **Use the Node CLI, not the Python starter toolkit.** Both install an
> `agentcore` command. If `which agentcore` points at the starter toolkit
> (e.g. `~/.local/bin/agentcore`), it reads the legacy
> `agent/.bedrock_agentcore.yaml` instead of `agent/agentcore/*.json` and can
> try to update an old runtime with a deleted execution role. Remove it with
> `uv tool uninstall bedrock-agentcore-starter-toolkit` (or `pip uninstall`),
> or put the Node CLI first on `PATH`.

**Deploy.** `deploy.sh` generates `agent/agentcore/agentcore.json` and
`aws-targets.json` from the 7a stack outputs (execution role, account, region)
and then runs the CLI:

```bash
./deploy.sh --agent-only
```

To re-run only the CLI step once those files exist:

```bash
cd agent
agentcore deploy --target IMA --yes
agentcore status
cd ..
```

---

## 10. Post-Deployment Configuration

### 10.1 Grant Data Access (required)

The stacks deploy with **no access to your source data or target data** by
default. There are two ways to grant it:

**Option 1 (recommended) — generate least-privilege policies at deploy time.**
Pass `SourceBucketNames` / `TargetBucketNames` / `SourceAccountIds` to the
engine stack (§6) and `SourceBucketNames` / `SourceAccountIds` to the agent
stack (§9.1). The templates then generate tightly-scoped data-plane policies
automatically — explicit read/write actions on the named bucket ARNs (never
`s3:Get*`), with an optional `aws:ResourceAccount` guard. If you used this
option, the roles below are already correctly scoped and you can **skip to
"Connect to the App via SSM"** — only cross-account source grants (bucket
policy / Lake Formation in the *owning* account) remain your responsibility.

**Option 2 — hand-author the policies after deployment.** If you deployed
*without* those parameters, each data-plane role is attached to an **inert
`Deny` placeholder** managed policy. Until you replace it, inventory refresh,
migrations, and agent analysis will not function. Replace each with the
least-privilege grants below (names use your `EnvironmentName`, default
`ima`):

| Policy | Role | Ships as | Needs |
|---|---|---|---|
| `${ENV}-app-data-access` | App backend | **Own-account Glue read (already granted)** | — (cross-account catalogs are read via per-source assumed roles) |
| `${ENV}-agent-data-access` | AI agent | placeholder¹ | Glue catalog **read** + S3 source **read** |
| `${ENV}-engine-data-access` | Engine | placeholder¹ | S3 source **read** + `PutBucketNotification` + Glue catalog read/write |
| `${ENV}-tracking-data-access` | S3 tracker Lambda | placeholder¹ | S3 source **read** |
| `${ENV}-emr-data-access` | EMR Serverless | placeholder¹ | S3 source **read** + target **read/write** + Glue + S3 Tables + Lake Formation |
| `${ENV}-xtable-data-access` | XTable Lambda | placeholder¹ | S3 source **read** + target **read/write** + Glue |
| `${ENV}-s3tables-migration-data-access` | S3 Tables Migration role | placeholder¹ | S3 source **read** + Glue source-table **read** (Managed Migration only; see §14) |

> ¹ These ship as a generated least-privilege policy when you use Option 1, or
> as an inert `Deny` placeholder when you don't. The **App** policy is never a
> placeholder — it always grants read-only access to the deployment's own-account
> Glue catalog.
>
> The **S3 Tables Migration role** (`${ENV}-s3tables-migration-data-access`) is
> only used by the Managed Migration strategy (`s3tables_managed`). Its S3
> Tables control/data-plane actions and reports-bucket write are already granted
> in-stack; only the **source-read** grant ships as a placeholder (or is
> generated from `SourceBucketNames`). If you are not using Managed Migration
> you can ignore it. See [§14](#14-managed-migration-s3-tables) for details.

Refer to Appendix A for more details on minimal permissions set for each role.


### 10.2 Connect to the App via SSM

The app runs in private subnets with no public endpoint. Use the jumphost deployed in the app stack to port-forward viaWhat SSM:

```bash
JUMPHOST=$(aws cloudformation describe-stacks --stack-name $ENV_NAME-app --region $AWS_REGION --query "Stacks[0].Outputs[?OutputKey=='JumphostInstanceId'].OutputValue" --output text)

TASK_ARN=$(aws ecs list-tasks --cluster $ENV_NAME-app --service-name $ENV_NAME-app --region $AWS_REGION --query "taskArns[0]" --output text)

TASK_IP=$(aws ecs describe-tasks --cluster $ENV_NAME-app --tasks $TASK_ARN --region $AWS_REGION --query "tasks[0].containers[0].networkInterfaces[0].privateIpv4Address" --output text)

PARAMS='{"host":["'$TASK_IP'"],"portNumber":["8000"],"localPortNumber":["8000"]}'

aws ssm start-session --target $JUMPHOST --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters "$PARAMS" --region $AWS_REGION > /dev/null 2>&1 &

```

Then open **http://localhost:8000** in your browser.

#### First login (no default password)

On first deploy the stack generates a random one-time password for the seeded `admin` account and stores it in Secrets Manager. Retrieve it:

```bash
aws secretsmanager get-secret-value \
  --secret-id "$ENV_NAME/app/bootstrap-admin-password" \
  --query SecretString --output text --region $AWS_REGION
# -> {"password":"<one-time password>"}
```

Log in as `admin` with that password. The account is flagged **must change
password**, so you are immediately forced to set a new, policy-compliant
password (min 8 chars incl. upper/lower/digit/special) before you can use the
app. The one-time secret is single-use in practice — once you change the
password it no longer works.

> **Multi-factor authentication (MFA):** MFA is available to every user via
> **Manage > Account > Enable MFA** (TOTP — scan the QR with an authenticator
> app). To *require* MFA for privileged users, deploy the app stack with
> `RequireMfa=true` (see §8); admins and editors are then forced to enroll a
> second factor before they can use the app. See [§12 Authentication](#12-authentication).

#### To terminate the session use the below commands

```bash
aws ssm describe-sessions --state Active --region $AWS_REGION \
  --filters "key=Target,value=$JUMPHOST" \
  --query "Sessions[].SessionId" --output text | \
  xargs -n1 aws ssm terminate-session --region $AWS_REGION --session-id
```

### 10.3 Configure Settings (via App UI)

With the SSM session active and the app open at http://localhost:8000, navigate to **Manage > Settings** page and set:

| Setting | Value | Source |
|---------|-------|--------|
| `APP_REGION` | The AWS region you deployed to (e.g. `us-east-2`). Set once; it drives Athena, EMR, tracking, source discovery, notifications, S3 Tables and XTable and is pushed to the engine. Defaults to the app's own region. | Your deploy region |
| `ENGINE_XTABLE_LAMBDA_ARN` | XTable Lambda function ARN | Engine stack |
| `ENGINE_TRACKING_LAMBDA_ROLE_ARN` | Tracking Lambda role ARN | Engine stack |
| `ENGINE_TRACKING_TABLE` | Only when the engine runs outside the stack (for example locally): the stack's object tracking table, `<env>-object-tracking-<region>`. Leave empty otherwise | Engine stack |
| `ENGINE_VALIDATION_TABLE` | Only when the engine runs outside the stack (for example locally): the stack's validation results table, `<env>-validation-results-<region>`. Leave empty otherwise | Engine stack |
| `ENGINE_EMR_APPLICATION_ID` | EMR Serverless app ID | Engine stack |
| `ENGINE_EMR_EXECUTION_ROLE_ARN` | EMR execution role ARN | Engine stack |
| `ENGINE_LOGS_BUCKET` | `<LogsBucket>` (bucket name only). One bucket for Athena results (`athena-query-results/`), EMR logs (`emr-logs/`) and migration reports (`migration-reports/`) — set once; the app derives the rest and pushes it to the engine | Engine stack (`LogsBucket` output) |
| `ENGINE_EMR_SCRIPTS_URI` | `s3://<ScriptsBucket>/scripts` | Engine stack |
| `ENGINE_ATHENA_WORKGROUP` | Athena workgroup name | Engine stack |
| `AGENT_URL` | AgentCore runtime ARN | `agentcore status` |

Get the engine stack values:

```bash
aws cloudformation describe-stacks --stack-name $ENV_NAME-engine --region $AWS_REGION \
  --query "Stacks[0].Outputs[?contains('XTableLambdaArn|TrackingLambdaRoleArn|ObjectTrackingTable|EmrApplicationId|EmrExecutionRoleArn|LogsBucket|ScriptsBucket|AthenaWorkgroup', OutputKey)].{Key:OutputKey,Value:OutputValue}" \
  --output table
```

Get the agent ARN (set as `AGENT_URL` in Settings):

```bash
cd agent && agentcore status && cd ..
# Look for "Agent ARN:" in the output — use the full ARN value
# Example: arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/IMA_Agent-xxxxx
```

> **Important:** The `AGENT_URL` setting accepts the AgentCore **runtime ARN** (not an HTTP URL).
> The app detects the ARN format and invokes the agent via the boto3 `bedrock-agentcore` SDK
> with automatic SigV4 signing. For local development, use `http://localhost:8080/invocations`.

------------------------------------------------------------------------------------

## 11. Notifications (optional)

The app can emit pipeline status events to Amazon EventBridge, which are delivered
as per-recipient emails via a Lambda → SES pipeline. This is fully optional and
disabled by default.

### 11.1 Setup

1. **Verify the SES sender identity** (required before any email is sent):

   ```bash
   aws ses verify-email-identity --email-address notifications@yourdomain.com --region $AWS_REGION
   # While SES is in sandbox, ALSO verify each recipient address:
   aws ses verify-email-identity --email-address user@example.com --region $AWS_REGION
   # Confirm status = Success (click the links in each inbox first):
   aws ses get-identity-verification-attributes \
     --identities notifications@yourdomain.com user@example.com --region $AWS_REGION
   ```

   For production, verify the sending **domain** and request removal from the SES sandbox.

2. **Set the sender on the app stack** (if not passed at deploy time):

   ```bash
   export NOTIFICATION_SENDER_EMAIL=notifications@yourdomain.com
   ./deploy.sh --app-only
   ```

3. **Enable notifications in the App UI** — under **Manage > Settings**, set:

   | Setting | Value |
   |---------|-------|
   | `APP_NOTIFICATIONS_ENABLED` | `true` |
   | `APP_NOTIFICATIONS_EVENTBRIDGE_BUS` | `default` (match `NotificationEventBusName`) |
   | `APP_NOTIFICATIONS_EVENTS` | `pipeline_completed,pipeline_failed,sync_out_of_sync,plan_completed` |

   The notifications region follows the global `APP_REGION` setting — there is
   no separate notifications region to set.


## 12. Authentication

The Migration Assistant uses username/password login with opaque server-side sessions
(HttpOnly, Secure, `SameSite=strict` cookies), bcrypt password hashing, login
rate-limiting with account lockout, and role-based access control
(admin/editor/viewer) plus group-based plan ownership.

### 12.1 Initial admin account

There is **no default password**. On first deploy the stack generates a random
one-time password for the seeded `admin` user into Secrets Manager at
`$ENV_NAME/app/bootstrap-admin-password`. Retrieve it, log in as `admin`, and
you are immediately forced to set a new password (see
[§10.2 First login](#first-login-no-default-password)). Password policy: minimum
8 characters including upper- and lower-case letters, a digit, and a special
character; the new password may not equal the old one.

Admin-initiated password resets (**Manage > Users > Reset password**) likewise
issue a temporary password and force the target user to change it at next login.

### 12.2 Multi-factor authentication (TOTP)

Any user can enable MFA from **Manage > Account > Enable MFA**: the app shows a
QR code (an `otpauth://` URI) to scan with an authenticator app (Google
Authenticator, Authy, 1Password, etc.), then confirms with a 6-digit code.
Once enabled, that user must supply a current TOTP code at every login.

To make MFA **mandatory** for privileged users, deploy the app stack with
`RequireMfa=true` (§8). Admins and editors without an enrolled factor are then
blocked from the app (HTTP 403 `MFA_ENROLLMENT_REQUIRED`) until they enroll;
viewers are not forced. This is off by default so existing deployments are not
locked out on upgrade.

If a user loses their authenticator device, an admin can clear their MFA via
**Manage > Users** (revoking the user's sessions), after which the user can
re-enroll.

### 12.3 Network exposure & CORS

The two services have deliberately different exposure:

- **App backend** is the only browser-facing service. It enforces CORS with an
  explicit origin allowlist from `CORS_ALLOWED_ORIGINS` (app-stack parameter
  `CorsAllowedOrigins`, injected as the env var). Leave it empty only for local
  dev (falls back to localhost origins); in production set it to your real
  frontend origin(s), comma-separated — e.g.
  `CorsAllowedOrigins=https://ima.example.com`.
- **Engine API** (port 8001) is an **internal, server-to-server** service. It is
  reached only by the app backend over AWS Service Connect
  (`http://engine.local:8001`), its security group allows ingress on 8001 only
  from the VPC CIDR, and the app↔engine path is a scoped SG-to-SG rule. It runs
  in private subnets with no public IP. Every engine route except `/api/health`
  also requires a bearer token (`ENGINE_API_TOKEN`). The engine stack generates
  it in Secrets Manager (output `EngineApiTokenSecretArn`) and injects it into
  both the engine and the app tasks; `deploy.sh` wires this automatically. If the
  token is unset the engine rejects all protected requests. Accordingly, the engine API has **no CORS
  middleware** — no browser ever calls it cross-origin, so a CORS allowlist would
  have no legitimate consumer. Do not add a wildcard CORS policy to the engine.


## 13. Engine Scaling Tunables

Environment variables on the **engine** service for scaling to 100s of
pipelines. Defaults are safe; raise with DB/quota headroom in mind.

| Env var | Default | Purpose |
|---|---|---|
| `ENGINE_RECONCILE_WORKERS` | `50` | Reconcile thread-pool size (concurrent pipeline polls) |
| `ENGINE_RECONCILE_INTERVAL_SECONDS` | `30` | Reconcile sweep interval |
| `ENGINE_MAX_RUNNING_PIPELINES` | `200` | Concurrency cap; excess pipelines wait as `queued`. Keep below EMR/Athena quotas. Also settable in the app under **Settings > Reconciler** (applies without a restart) |
| `ENGINE_MORPH_MAX_CONCURRENT` | `6` | Max concurrent in-process morph loads |
| `ENGINE_ACTION_LOCK_WAIT_SECONDS` | `5` | How long a user action (pause, cancel, complete, ...) waits for an in-flight reconcile of the same pipeline before proceeding. Keep it below the app's request timeouts (10–15s) |
| `ENGINE_MORPH_WORKERS` | `12` | Footer-read threads per morph load (host ceiling = this × MAX_CONCURRENT) |
| `ENGINE_DB_POOL_MAX` | `60` | Postgres pool max; keep ≥ reconcile workers and < Aurora `max_connections` |
| `ENGINE_DB_POOL_MIN` | `1` | Postgres pool min |
| `ENGINE_AWS_CONNECT_TIMEOUT` | `5` | boto3 connect timeout (s) |
| `ENGINE_AWS_READ_TIMEOUT` | `15` | boto3 read timeout (s) |
| `ENGINE_AWS_MAX_ATTEMPTS` | `3` | boto3 adaptive retry attempts |
| `ENGINE_AWS_MAX_POOL` | `50` | boto3 max connections per client |

> Keep `ENGINE_DB_POOL_MAX` ≥ `ENGINE_RECONCILE_WORKERS` and both under
> Aurora's `max_connections`. Keep `ENGINE_MAX_RUNNING_PIPELINES` below your
> EMR Serverless concurrent-job and Athena workgroup limits.

### 13.1 Additional engine env vars

These are read by the engine but **not** injected by the engine stack's task
definition. Defaults are production-safe; set them (via `/api/settings` or the
task environment) only to override. Grouped by purpose.

| Env var | Default | Purpose |
|---|---|---|
| `ENGINE_VALIDATION_WORKERS` | `5` | Max Athena validation pipelines run concurrently (also a stack param, `ValidationWorkers`) |
| `ENGINE_VALIDATION_POLL_INTERVAL` | `3` | Seconds between validation query status polls |
| `ENGINE_VALIDATION_POLL_MAX_ATTEMPTS` | `200` | Max polls before a validation query is abandoned |
| `ENGINE_INCREMENTAL_INTERVAL_SECONDS` | `3600` | Default sync cadence for incremental / add_files / managed-migration syncs (per-pipeline overridable) |
| `ENGINE_S3TABLES_REPORTS_PREFIX` | `migration-reports` | Prefix under the logs bucket for Managed Migration reports. The stack's `S3TablesMigrationRole` write grant is scoped to `migration-reports/*`, so change both together |
| `ENGINE_S3TABLES_REGION` | *(set from the global `APP_REGION` by the app)* | Region for S3 Tables calls other than start/sync (those are pinned to the table bucket's region) |
| `ENGINE_AWS_ACCOUNT_ID` | *(derived from ARNs)* | Fallback caller account id for the Managed Migration Glue source `catalogId` |
| `ENGINE_ATHENA_PRICE_PER_TB` | `5.0` | Athena cost estimate ($/TB scanned) |
| `ENGINE_EMR_VCPU_PRICE_PER_HOUR` | `0.052624` | EMR Serverless cost estimate (vCPU-hour) |
| `ENGINE_EMR_MEMORY_PRICE_PER_GB_HOUR` | `0.0057785` | EMR Serverless cost estimate (GB-hour) |
| `ENGINE_EMR_STORAGE_PRICE_PER_GB_HOUR` | `0.000111` | EMR Serverless cost estimate (storage GB-hour) |
| `ENGINE_DYNAMODB_ENDPOINT` | *(empty)* | DynamoDB endpoint override (local/testing) |
| `ENGINE_LOG_FILE` | *(empty)* | If set, also write engine logs to this file |

> Cost-estimate prices default to `us-east-1` on-demand rates; override them to
> match your region/commitments so the UI's cost figures stay accurate.


## 14. Managed Migration (S3 Tables)

The **Managed Migration** strategy (`s3tables_managed`) migrates Hive-Parquet
sources into S3 Tables using the AWS **S3 Tables Migration API** instead of an
Athena/EMR data-movement job. It only applies to pipelines whose source is
`parquet` and whose target is `s3_tables`.

**What the engine stack already wires for you** (no Settings entry needed):

- An IAM role (`S3TablesMigrationRole`, output `S3TablesMigrationRoleArn`) that
  the migration service assumes. Its ARN is injected into the engine as
  `ENGINE_S3TABLES_MIGRATION_ROLE_ARN`.
- The **reports location** — compatibility and migration reports are written to
  the common `LogsBucket` under the `migration-reports/<pipeline-id>/` prefix
  (alongside `athena-query-results/` and `emr-logs/`), one sub-prefix per report:
  `table-compatibility/`, `object-compatibility/` and `object-migration/`. The
  engine gets the bucket as
  `ENGINE_LOGS_BUCKET` / `ENGINE_S3TABLES_REPORTS_BUCKET` automatically, and the
  role already has write access to that prefix.

**What you must do:**

1. **Grant the migration role source-read access — S3 *and* Glue.** Like the
   other data-plane roles (§10), `S3TablesMigrationRole` ships with an inert `Deny`
   placeholder for source data unless you passed `SourceBucketNames` at
   engine-deploy time. Supply `SourceBucketNames` (and `SourceAccountIds` for
   cross-account) to generate the grant, or hand-author
   `${ENV}-s3tables-migration-data-access` with S3 read
   (`s3:GetObject`/`s3:ListBucket`/`s3:GetBucketLocation`) on your source
   bucket(s) **and** Glue read (`glue:GetDatabase`/`GetTable`/`GetPartitions`) on
   the source table (Appendix A). The migration's source is a Glue table; a role
   with S3 read but no Glue read fails planning with `sourcePermissions`. For a
   Lake Formation-governed database, also add Lake Formation grants for the role.
   If you deployed with a template that predates the Glue statement, update the
   engine stack.
2. **Build the engine image with boto3 >= 1.43.124** (the engine's pinned
   minimum; see [§5.1 Engine](#51-engine)). Older botocore
   releases lack the Migration API.

**Operator interaction — plan review/approval.** Managed migrations start with
`reviewMigrationPlan=true`: the service first produces a compatibility plan and
**pauses**. The pipeline surfaces as `awaiting_review` in the app. An operator
reviews the compatibility report (written to the reports bucket) and then
**approves** to resume — approval issues `update_table_migration(queued)`.
Ongoing syncs run unattended (no review pause). The engine's reconcile loop
drives status, and cutover is a `complete_table_migration` call.

> **Region note:** for `start`/`sync` the engine pins the S3 Tables Migration
> client to the **target table-bucket's region** (parsed from
> `target_bucket_arn`), because the API requires the caller region to match the
> table bucket. `ENGINE_S3TABLES_REGION` only affects other S3 Tables calls.


## 15. Data Validation

The Migration Assistant can validate a migrated table against its source. Validation runs
**on-demand** from the app and compares source vs target. Two tiers:

- **Basic:** V1 row-count parity, V2 schema conformance, V3 partition count
  (partitioned sources), V4 null-count parity on key columns, V5
  non-emptiness + bounds, V6 morph-manifest integrity (morph pipelines only).
- **Advanced:** everything in Basic plus V7 per-column aggregates
  (sum/min/max/distinct), and V8 per-partition reconciliation (partitioned sources).

**Float sums.** Sums over `float`/`double` columns depend on the order the
engine adds the values, so identical data can differ in the last digits
(`3230692.64` vs `3230692.6400000006`). V7 compares those sums with a relative
tolerance of `1e-9` and notes it in the result detail. On the Athena path it is
set with `ENGINE_VALIDATION_FLOAT_REL_TOL`; the EMR job uses the same default.
Integer and decimal sums, and all min/max/distinct counts, are always compared
exactly.

**Execution paths.** Each pipeline validates on its own `execution_engine`:

- **Athena pipelines:** the engine runs the check SQL via Athena (honouring the
  app's Athena region/workgroup/output-location settings), fanning out up to
  `ENGINE_VALIDATION_WORKERS` pipelines concurrently. V7 additionally
  calls Glue `GetTable` to resolve the (numeric) column list — so the engine
  role needs Glue read on the validated tables; if resolution fails that
  check is **skipped** (not failed). Schema/manifest checks (V2/V6) are not
  run on the Athena path.
- **EMR pipelines:** a single Spark job validates all EMR pipelines in a run and
  writes results to the validation DynamoDB table itself. It builds its client
  from the plan's `strategy.emr` block (so the EMR application id / execution
  role from Settings must be correct), and the EMR execution role needs
  DynamoDB write on the validation-results table.

Results are stored in the `ENGINE_VALIDATION_TABLE` DynamoDB table
(`ima-validation-results` by default; created by the engine stack). Tune
concurrency/polling via the validation env vars in the table above.


## 16. Update Deployments

### 16.1 Update Engine Image

```bash
finch build --platform linux/amd64 -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-engine:latest engine/
finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-engine:latest
aws ecs update-service --cluster $ENV_NAME-engine --service $ENV_NAME-engine --force-new-deployment --region $AWS_REGION
```

### 16.2 Update App Image

```bash
cd app/client && npm run build && cd ../..
finch build --platform linux/amd64 -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-app:latest app/
finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-app:latest
aws ecs update-service --cluster $ENV_NAME-app --service $ENV_NAME-app --force-new-deployment --region $AWS_REGION
```

### 16.3 Update XTable Lambda

Via `deploy.sh` (rebuilds + pushes the image and updates the Lambda — the
`--with-xtable` flag is required, since image builds skip XTable by default):

```bash
./deploy.sh --update-images --with-xtable
```

Or manually:

```bash
finch build --platform linux/amd64 -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-xtable-converter:latest engine/infra/xtable-lambda/
finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-xtable-converter:latest
aws lambda update-function-code --function-name ima-xtable-converter-$AWS_REGION \
  --image-uri $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-xtable-converter:latest --region $AWS_REGION
```

### 16.4 Update DB Bootstrap Lambda

```bash
finch build --platform linux/amd64 -t $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-db-bootstrap:latest app/infra/db-bootstrap-lambda/
finch push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-db-bootstrap:latest
aws lambda update-function-code --function-name $ENV_NAME-db-bootstrap \
  --image-uri $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/ima-db-bootstrap:latest --region $AWS_REGION
```

---

## 17. Teardown

Before deleting the CloudFormation stacks, disable deletion protection on the Aurora cluster, and empty the Scripts and logs S3 buckets created during setup.

```bash
aws cloudformation delete-stack --stack-name $ENV_NAME-agent --region $AWS_REGION
aws cloudformation delete-stack --stack-name $ENV_NAME-app --region $AWS_REGION
aws cloudformation delete-stack --stack-name $ENV_NAME-engine --region $AWS_REGION
```


## 18. Appendix

#### [A] Minimal permission sets per role

Replace `<SOURCE_BUCKET>`, `<TARGET_BUCKET>`, `<TABLE_BUCKET_ARN>`, `<REGION>`,
`<ACCOUNT>` and the Glue database ARNs with your own resources.

Each managed policy below is attached to exactly one IAM role. The roles are
created with CloudFormation-generated names (no fixed `RoleName`), so they are
identified here by the CloudFormation logical ID and the stack that creates
them:

| Policy (`ManagedPolicyName`) | Attached to role (logical ID) | Stack |
|---|---|---|
| `${ENV}-agent-data-access` | `AgentExecutionRole` | agent stack |
| `${ENV}-engine-data-access` | `EngineTaskRole` | engine stack |
| `${ENV}-tracking-data-access` | `TrackingLambdaRole` | engine stack |
| `${ENV}-emr-data-access` | `EmrExecutionRole` | engine stack |
| `${ENV}-xtable-data-access` | `XTableLambdaRole` | engine stack (XTable only) |


> The **App backend** policy (`${ENV}-app-data-access`) is not listed here — it
> already grants read-only own-account Glue catalog access out of the box, so
> there is nothing to hand-author for it. The examples below are only for the
> roles that ship as placeholders under Option 2.

**AI agent** (`${ENV}-agent-data-access`, attached to `AgentExecutionRole`) — read only, catalog + data:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueCatalogRead",
      "Effect": "Allow",
      "Action": ["glue:GetDatabases","glue:GetDatabase","glue:GetTables","glue:GetTable","glue:GetPartitions","glue:GetPartition","glue:GetColumnStatisticsForTable"],
      "Resource": ["arn:aws:glue:<REGION>:<ACCOUNT>:catalog","arn:aws:glue:<REGION>:<ACCOUNT>:database/<SOURCE_DB>","arn:aws:glue:<REGION>:<ACCOUNT>:table/<SOURCE_DB>/*"]
    },
    {
      "Sid": "SourceDataRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::<SOURCE_BUCKET>","arn:aws:s3:::<SOURCE_BUCKET>/*"]
    }
  ]
}
```

**Engine** (`${ENV}-engine-data-access`, attached to `EngineTaskRole`) — orchestration:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SourceRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:ListBucket","s3:GetBucketLocation","s3:HeadObject"],
      "Resource": ["arn:aws:s3:::<SOURCE_BUCKET>","arn:aws:s3:::<SOURCE_BUCKET>/*"]
    },
    {
      "Sid": "TrackerNotificationConfig",
      "Effect": "Allow",
      "Action": ["s3:GetBucketNotification","s3:PutBucketNotification"],
      "Resource": "arn:aws:s3:::<SOURCE_BUCKET>"
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": ["glue:GetDatabases","glue:GetDatabase","glue:GetTables","glue:GetTable","glue:GetPartitions","glue:CreateTable","glue:UpdateTable","glue:DeleteTable","glue:CreateDatabase"],
      "Resource": ["arn:aws:glue:<REGION>:<ACCOUNT>:catalog","arn:aws:glue:<REGION>:<ACCOUNT>:database/*","arn:aws:glue:<REGION>:<ACCOUNT>:table/*"]
    }
  ]
}
```

**S3 tracker Lambda** (`${ENV}-tracking-data-access`, attached to `TrackingLambdaRole`) — source read only:
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "SourceRead",
    "Effect": "Allow",
    "Action": ["s3:GetObject","s3:ListBucket"],
    "Resource": ["arn:aws:s3:::<SOURCE_BUCKET>","arn:aws:s3:::<SOURCE_BUCKET>/*"]
  }]
}
```

**EMR Serverless** (`${ENV}-emr-data-access`, attached to `EmrExecutionRole`) — data movement:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SourceRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::<SOURCE_BUCKET>","arn:aws:s3:::<SOURCE_BUCKET>/*"]
    },
    {
      "Sid": "TargetWrite",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
      "Resource": ["arn:aws:s3:::<TARGET_BUCKET>","arn:aws:s3:::<TARGET_BUCKET>/*"]
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": ["glue:GetDatabase","glue:GetDatabases","glue:GetTable","glue:GetTables","glue:CreateTable","glue:UpdateTable","glue:DeleteTable","glue:GetPartitions"],
      "Resource": ["arn:aws:glue:<REGION>:<ACCOUNT>:catalog","arn:aws:glue:<REGION>:<ACCOUNT>:database/*","arn:aws:glue:<REGION>:<ACCOUNT>:table/*"]
    },
    {
      "Sid": "S3Tables",
      "Effect": "Allow",
      "Action": ["s3tables:CreateTable","s3tables:GetTable","s3tables:UpdateTableMetadataLocation","s3tables:GetTableMetadataLocation","s3tables:DeleteTable","s3tables:ListTables","s3tables:CreateNamespace","s3tables:GetNamespace","s3tables:ListNamespaces","s3tables:GetTableBucket","s3tables:ListTableBuckets","s3tables:PutTableData","s3tables:GetTableData","s3tables:DeleteTableData"],
      "Resource": "<TABLE_BUCKET_ARN>*"
    },
    {
      "Sid": "LakeFormation",
      "Effect": "Allow",
      "Action": ["lakeformation:GetDataAccess"],
      "Resource": "*"
    }
  ]
}
```

**XTable Lambda** (`${ENV}-xtable-data-access`, attached to `XTableLambdaRole`,
only if XTable deployed) — same as EMR minus S3 Tables/Lake Formation:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SourceRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:ListBucket","s3:HeadObject"],
      "Resource": ["arn:aws:s3:::<SOURCE_BUCKET>","arn:aws:s3:::<SOURCE_BUCKET>/*"]
    },
    {
      "Sid": "TargetWrite",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
      "Resource": ["arn:aws:s3:::<TARGET_BUCKET>","arn:aws:s3:::<TARGET_BUCKET>/*"]
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": ["glue:GetDatabase","glue:GetTable","glue:GetTables","glue:CreateTable","glue:UpdateTable","glue:DeleteTable","glue:GetPartitions"],
      "Resource": ["arn:aws:glue:<REGION>:<ACCOUNT>:catalog","arn:aws:glue:<REGION>:<ACCOUNT>:database/*","arn:aws:glue:<REGION>:<ACCOUNT>:table/*"]
    }
  ]
}
```

**S3 Tables Migration role** (`${ENV}-s3tables-migration-data-access`, attached
to `S3TablesMigrationRole`, Managed Migration only) — **source read: S3 data
and the Glue source table**. The S3 Tables control/data-plane actions and the
reports-bucket write are already granted in-stack
(`S3TablesMigrationSolutionAccess` + `ReportsWrite`); this policy supplies the
source grants. Both halves are required: the migration's source is a Glue table,
so without the Glue statement the service fails planning with
`failureReason.code = sourcePermissions` even when the S3 read is correct. When
you supply `SourceBucketNames` the stack generates exactly this policy.
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SourceDataRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:GetObjectVersion","s3:ListBucket","s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::<SOURCE_BUCKET>","arn:aws:s3:::<SOURCE_BUCKET>/*"]
    },
    {
      "Sid": "GlueSourceRead",
      "Effect": "Allow",
      "Action": ["glue:GetDatabase","glue:GetDatabases","glue:GetTable","glue:GetTables","glue:GetPartition","glue:GetPartitions","glue:BatchGetPartition"],
      "Resource": ["arn:aws:glue:<REGION>:<ACCOUNT>:catalog","arn:aws:glue:<REGION>:<ACCOUNT>:database/<SOURCE_DB>","arn:aws:glue:<REGION>:<ACCOUNT>:table/<SOURCE_DB>/*"]
    },
    {
      "Sid": "LakeFormationDataAccess",
      "Effect": "Allow",
      "Action": "lakeformation:GetDataAccess",
      "Resource": "*"
    }
  ]
}
```
If the source database is governed by **Lake Formation**, an IAM policy is not
enough: also grant the migration role `DESCRIBE` on the database, `SELECT` and
`DESCRIBE` on the source table, and `DATA_LOCATION_ACCESS` on the source S3
location (see the user guide, §2.3).
