# Agent

AI assistant built on Bedrock AgentCore with Strands Agents. Analyzes tables from the Glue Data Catalog and recommends conversion strategies, partition transforms, batch sizes, and cost estimates.

## Local Development

```bash
uv sync
AGENT_PORT=8082 uv run python src/main.py
```

Requires AWS credentials with access to Bedrock (Claude), Glue, S3, and CloudTrail.

## Key env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENT_PORT` | `8080` | Port to listen on |
| `AWS_REGION` | `us-east-1` | AWS region for API calls |


## Deploy to AWS (CloudFormation)

The `infra/agent-stack.yaml` template creates the agent's IAM execution role, an artifacts bucket, a logs bucket, a KMS key, and a log group. The agent runtime itself is deployed with the AgentCore CLI (see below). `deploy.sh --agent-only` runs both steps.

```bash
aws cloudformation deploy \
    --template-file infra/agent-stack.yaml \
    --stack-name ima-agent \
    --parameter-overrides \
        SourceBucketNames=my-source-bucket \
        SourceAccountIds=123456789012 \
    --capabilities CAPABILITY_NAMED_IAM \
    --region us-east-1
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `EnvironmentName` | `ima` | Prefix for resource names |
| `SourceBucketNames` | *(empty)* | Comma-separated source buckets the agent may read. Empty ships an inert deny-only placeholder |
| `SourceAccountIds` | *(empty)* | Comma-separated account IDs owning the source data, used as an `aws:ResourceAccount` guard |

Stack outputs include the execution role ARN, used when deploying the runtime.

## Deploy to Bedrock AgentCore (CLI)

Prerequisites: install the [AgentCore CLI](https://docs.aws.amazon.com/bedrock/latest/userguide/agentcore-toolkit.html) (`npm install -g @aws/agentcore`) and configure AWS credentials.

The easiest method is via the deploy script which generates all config automatically:

```bash
# From the project root
./deploy.sh --agent-only
```

Or manually:

```bash
cd agent

# Deploy (agentcore/agentcore.json and aws-targets.json must exist,
# and agentcore/cdk/ must have its npm dependencies installed:
# npm install --prefix agentcore/cdk)
agentcore deploy --target IMA --yes

# Verify
agentcore status
```

> **Note:** `deploy.sh --agent-only` generates `agentcore/agentcore.json` and `agentcore/aws-targets.json` from CloudFormation stack outputs. These files are gitignored.

Once deployed, invoke it with:

```bash
agentcore invoke "Analyze the table analytics.user_events for migration to Iceberg"
```

The deployed agent exposes an HTTP endpoint at `/invocations` (POST). Set the app's `AGENT_URL` setting to the AgentCore runtime ARN (from `agentcore status`).


## License

Apache-2.0 — see [LICENSE](../LICENSE).
