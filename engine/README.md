# Migration Assistant Engine

Standalone engine for converting tables to Apache Iceberg. Runs as a microservice with a background reconcile loop that drives migrations to completion.

## Migration Strategies

| Strategy | Data Movement | Compute Engine | Best For |
|----------|--------------|----------------|----------|
| **Managed Migration** | AWS-managed (into S3 Tables) | AWS S3 Tables Migration API | Parquet → S3 Tables (primary path) |
| **Rewrite** | Full copy (CTAS) | Athena or EMR Serverless | Any format, any size |
| **Incremental** | Batched partitions | Athena or EMR Serverless | Large partitioned tables |
| **Add Files** | None (metadata only) | EMR Serverless (`add_files`) | Partitioned Parquet → Iceberg on S3 |
| **Morph** | None (metadata only) | In-process (pyiceberg) | Parquet tables, zero downtime |
| **Morph (XTable)** | None (metadata only) | Lambda (Apache XTable) | Hudi/Delta → Iceberg |

---

## Deploy to AWS

### Prerequisites

Before deploying, you need:

1. **AWS CLI** configured with credentials that have permissions to create IAM roles, VPC, ECS, DynamoDB, Athena, EMR, and Lambda resources.

2. **Docker** installed locally to build container images.

3. **ECR repositories** created and images pushed:

   ```bash
   # Engine image (required)
   aws ecr create-repository --repository-name ima-engine --region us-east-1
   docker build -t ima-engine .
   docker tag ima-engine:latest <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-engine:latest
   aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com
   docker push <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-engine:latest
   ```

   ```bash
   # XTable Lambda image (optional — only for Hudi/Delta morph)
   # NOTE: Uses Apache XTable main branch. Build takes ~10min (Maven from source).
   # The bundled jar is ~1.1GB — requires 4096MB Lambda memory.
   aws ecr create-repository --repository-name ima-xtable --region us-east-1
   docker build --platform linux/amd64 -t ima-xtable infra/xtable-lambda/
   docker tag ima-xtable:latest <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-xtable:latest
   docker push <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-xtable:latest
   ```

   > **Finch users** (macOS without Docker Desktop):
   > ```bash
   > finch build --platform linux/amd64 -t <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-xtable:latest infra/xtable-lambda/
   > aws ecr get-login-password --region <REGION> | finch login --username AWS --password-stdin <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com
   > finch push <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-xtable:latest
   > ```

4. **Spark scripts uploaded to S3** (only if using EMR Serverless engine):

   ```bash
   # The scripts bucket is created by the stack — upload after first deploy
   aws s3 sync src/spark/ s3://ima-scripts-<ACCOUNT>/scripts/
   ```

### Deploy the Stack

```bash
aws cloudformation deploy \
    --template-file infra/engine-stack.yaml \
    --stack-name ima \
    --parameter-overrides \
        EngineImageUri=<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-engine:latest \
    --capabilities CAPABILITY_NAMED_IAM \
    --region <REGION>
```

With XTable Lambda for Hudi/Delta support:

```bash
aws cloudformation deploy \
    --template-file infra/engine-stack.yaml \
    --stack-name ima \
    --parameter-overrides \
        EngineImageUri=<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-engine:latest \
        XTableImageUri=<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-xtable:latest \
    --capabilities CAPABILITY_NAMED_IAM \
    --region <REGION>
```

### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `EnvironmentName` | No | `ima` | Prefix for all resource names |
| `EngineImageUri` | **Yes** | — | ECR URI for the engine container |
| `EngineCpu` | No | `1024` | Fargate task CPU (512, 1024, 2048, 4096) |
| `EngineMemory` | No | `2048` | Fargate task memory in MB |
| `ReconcileIntervalSeconds` | No | `30` | How often the reconcile loop checks pipelines |
| `LogLevel` | No | `INFO` | Engine log verbosity (DEBUG, INFO, WARNING, ERROR) |
| `VpcCIDR` | No | `10.0.0.0/16` | VPC address range |
| `XTableImageUri` | No | *(empty)* | ECR URI for XTable Lambda — deploys only if set |
| `XTableMemorySize` | No | `4096` | XTable Lambda memory in MB (needs 4GB+ for the bundled jar) |
| `ValidationWorkers` | No | `5` | Parallel validation workers |
| `SourceBucketNames` | No | *(empty)* | Source S3 buckets to grant read access. Empty ships an inert deny-only placeholder |
| `TargetBucketNames` | No | *(empty)* | Target S3 buckets to grant read/write access |
| `SourceAccountIds` | No | *(empty)* | Account IDs owning the source data, used as an `aws:ResourceAccount` guard |

### What Gets Created

- **Networking**: VPC, public/private subnets, NAT gateway, S3 + DynamoDB VPC endpoints
- **Compute**: ECS Fargate cluster + service, EMR Serverless application, Athena workgroup
- **Storage**: DynamoDB tables (pipelines, batches, progress, tracking, snapshots, costs), S3 buckets (scripts, logs)
- **IAM**: Engine task role, EMR execution role, Lambda tracking role
- **Optional**: XTable Lambda function + role (if `XTableImageUri` provided)

### After Deployment

1. Get stack outputs:
   ```bash
   aws cloudformation describe-stacks --stack-name ima --query "Stacks[0].Outputs"
   ```

2. Set `ENGINE_URL` in the backend to the internal URL:
   ```
   ENGINE_URL=http://engine.local:8001
   ```

3. Upload Spark scripts (the scripts bucket name is in the outputs):
   ```bash
   aws s3 sync src/spark/ s3://<ScriptsBucket>/scripts/
   ```

4. Verify health:
   ```bash
   curl http://engine.local:8001/api/health
   ```

5. **(If XTable deployed)** Set the Lambda ARN in the app's Settings page:
   ```
   ENGINE_XTABLE_LAMBDA_ARN = arn:aws:lambda:<REGION>:<ACCOUNT>:function:ima-xtable-converter
   ```
   Or get it from stack outputs:
   ```bash
   aws cloudformation describe-stacks --stack-name ima \
       --query "Stacks[0].Outputs[?OutputKey=='XTableLambdaArn'].OutputValue" --output text
   ```

---

## API

Every route except `/api/health` requires `Authorization: Bearer <token>`, where the token equals the engine's `ENGINE_API_TOKEN`. If `ENGINE_API_TOKEN` is unset, the engine rejects all protected requests (HTTP 503). The CloudFormation stack generates the token in Secrets Manager and injects it into both the engine and the app backend.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| POST | `/api/pipeline/submit` | Create a pipeline from spec |
| GET | `/api/pipeline/{id}` | Pipeline details |
| POST | `/api/pipeline/{id}/start` | Start pipeline |
| POST | `/api/pipeline/{id}/pause` | Pause |
| POST | `/api/pipeline/{id}/resume` | Resume |
| POST | `/api/pipeline/{id}/cancel` | Cancel |
| POST | `/api/pipeline/{id}/retry` | Retry failed pipeline (preserves history) |
| GET | `/api/pipeline/{id}/batches` | Batch progress |
| GET | `/api/pipeline/{id}/costs` | Compute cost metrics |
| POST | `/api/pipeline/costs/plan` | Plan-level cost aggregation |

---

## Pipeline Lifecycle

```
pending → running → completed
            ↕
          paused
            ↓
        cancelled / failed
```

The background reconcile loop polls active pipelines every N seconds (configurable) and advances their state through a unified state machine. The state machine handles job polling, batch advancement, interval checks, and sync status transitions — strategies only implement hooks for their specific work (submit jobs, detect changes, execute sync).

---

## Configuration

All settings can be provided as environment variables. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENGINE_API_TOKEN` | *(required)* | Bearer token callers must present to the API |
| `ENGINE_DB_BACKEND` | `sql` | `sql` (SQLite/Postgres) or `dynamodb` |
| `ENGINE_RECONCILE_INTERVAL_SECONDS` | `30` | Reconcile loop frequency |
| `ENGINE_ATHENA_WORKGROUP` | `primary` | Athena workgroup |
| `ENGINE_ATHENA_REGION` | `us-east-1` | Athena region |
| `ENGINE_EMR_APPLICATION_ID` | — | EMR Serverless app ID |
| `ENGINE_EMR_EXECUTION_ROLE_ARN` | — | IAM role for EMR jobs |
| `ENGINE_TRACKING_LAMBDA_ROLE_ARN` | — | IAM role for S3 event tracker Lambdas |
| `ENGINE_XTABLE_LAMBDA_ARN` | — | XTable Lambda for Hudi/Delta morph |
| `ENGINE_S3TABLES_MIGRATION_ROLE_ARN` | — | IAM role assumed by the S3 Tables Migration API (Managed Migration) |
| `ENGINE_S3TABLES_REPORTS_BUCKET` | *(logs bucket)* | Bucket for Managed Migration compatibility/migration reports |
| `ENGINE_LOG_LEVEL` | `INFO` | Log verbosity |


---

## Local Development

```bash
# Install dependencies
uv sync

# Run locally (SQLite backend, all defaults)
uv run ima-engine serve --port 8001

# Or use the project dev script (starts all services)
./dev.sh
```

### CLI

```bash
uv run ima-engine init                    # Initialize database
uv run ima-engine validate spec.json      # Validate a spec
uv run ima-engine submit spec.json        # Create a pipeline
uv run ima-engine start <pipeline_id>     # Start pipeline
uv run ima-engine status <pipeline_id>    # Check status
uv run ima-engine list                    # List all pipelines
```

### Local with DynamoDB

```bash
ENGINE_DB_BACKEND=dynamodb \
ENGINE_TRACKING_BACKEND=dynamodb \
AWS_DEFAULT_REGION=us-east-1 \
uv run ima-engine serve
```

API docs: http://localhost:8001/docs

---

## License

Apache-2.0
