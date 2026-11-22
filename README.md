# Migration Assistant for Apache Iceberg

The Migration Assistant converts existing data lake tables into Amazon S3 Tables — primarily
through **Managed Migration**, which runs the Amazon S3 Tables Migration API to
move Parquet tables into S3 Tables for you. It also converts Parquet, Hudi, and
other formats to [Apache Iceberg](https://iceberg.apache.org/) on Amazon S3
general-purpose storage if you want to build a self-managed Iceberg lakehouse instead.

With the Migration Assistant you can plan, analyze, execute, and monitor conversions from a single web portal — backed by an AI agent that recommends a migration strategy per table and a standalone conversion engine that carries it out on AWS.

> **This is an open-source example solution, not a managed product.** It's meant
> as a reference implementation you can deploy and adapt to your
> own data, formats, and operational requirements.

## Architecture

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────────┐
│ Frontend │────►│ Backend  │────►│  Agent   │     │    Engine    │
│ (React)  │     │ (FastAPI)│     │ (Bedrock │     │   Engine     │
│ :5173    │     │ :8000    │     │  AgentCore)    │   :8001      │
└──────────┘     └────┬─────┘     │ :8082    │     └──────┬───────┘
                      │           └──────────┘            │
                      │                              EMR Serverless
                      │                              (Spark jobs)
                      └──────────HTTP──────────────►──────┘
```

The project has four independent components:

| Component | Path | Description |
|-----------|------|-------------|
| App (Frontend) | `app/client/` | React + TypeScript portal for managing plans, inventory, and monitoring conversions |
| App (Backend) | `app/server/` | FastAPI service — users, groups, plans, table inventory, AI assistant orchestration |
| Agent | `agent/` | Bedrock AgentCore AI agent — analyzes tables, recommends strategies, estimates costs |
| Engine | `engine/` | Standalone conversion engine — accepts specs, runs pipelines via EMR Serverless, Amazon Athena or Morph-based strategies |

A shared spec format (`spec/`) defines the contract between all components.

## How it works

1. Users discover tables through the inventory (sourced from AWS Glue Data Catalog)
2. Tables are grouped into plans with a target — **S3 Tables** or Iceberg on S3
3. The AI agent analyzes each table — recommends a strategy, estimates cost/time, suggests partition transforms and sort orders
4. On execute, the backend submits a spec per table to the engine API
5. The engine creates a pipeline and carries out the migration — for Managed Migration it drives the AWS S3 Tables Migration API (with a plan-review/approval step); for the other strategies it submits Spark jobs to EMR Serverless or queries to Athena
6. A background reconcile loop in the engine polls migration/job status and advances pipelines automatically, including keeping the pipelines in sync based on source table changes.

## Conversion Strategies

| Strategy | Data Movement | How it works |
|----------|--------------|--------------|
| **Managed Migration** | AWS-managed (into S3 Tables) | Runs the AWS S3 Tables Migration API — submit, review the compatibility plan, approve, sync, and cut over. Parquet → S3 Tables. |
| Rewrite | Full copy | Single Spark CTAS job on EMR Serverless or Athena |
| Incremental | Batched copy | Initial load in batches, then periodic partition rewrites based on S3 events |
| Add Files | None (metadata only) | Registers existing Parquet files into Iceberg via the `add_files` procedure on EMR Serverless |
| Morph (Parquet) | None (metadata only) | Scans Parquet files, generates Iceberg manifests in-process via pyiceberg |
| Morph (Hudi/Delta) | None (metadata only) | Apache XTable Lambda translates Hudi/Delta metadata to Iceberg |


## Project Structure

```
├── app/               Application (frontend + backend)
│   ├── client/        React + Vite + TanStack Router
│   ├── server/        FastAPI — portal API, SQLite/Aurora
│   └── infra/         CloudFormation template
├── agent/             Bedrock AgentCore — AI analysis
│   └── infra/         CloudFormation template
├── engine/            Conversion engine — pipelines, EMR Serverless, pyiceberg
│   └── infra/         CloudFormation template
├── spec/              Shared spec format (JSON schema + docs)
├── docs/              Deployment guide, user guide, design docs, support matrix
├── deploy.sh          Deploy all stacks to AWS
└── dev.sh             Start all services locally
```

## Deploy to AWS

### Prerequisites

- AWS CLI v2 configured
- Finch **or** Docker installed (used to build the container images). The deploy
  script **prefers Finch when both are installed** — it picks `finch` over
  `docker` if it finds `finch` on your `PATH`.
  - **If you use Finch:** run `finch vm init` once to create its VM, then
    `finch vm start` if the VM is stopped. Unlike Docker Desktop, having the
    `finch` binary on `PATH` is not enough — without an initialized, running VM
    the deploy fails at image-build time with
    `instance "finch" does not exist, run 'finch vm init' to create a new instance`.
  - **If you use Docker:** no extra step — just make sure the Docker daemon is
    running.
- Node.js 18+ (repo is pinned to 24.13.0 via `.mise.toml`; used for frontend build and the `@aws/agentcore` CLI)
- The [`@aws/agentcore` CLI](https://docs.aws.amazon.com/bedrock/latest/userguide/agentcore-toolkit.html) installed globally: `npm install -g @aws/agentcore`
- Your target AWS account/region [CDK-bootstrapped](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html): `cdk bootstrap aws://ACCOUNT_ID/REGION` (the agent deploy step runs a CDK app under the hood)
- AWS account with permissions for IAM, VPC, ECS, DynamoDB, Lambda, ECR, Athena, EMR, Aurora, Bedrock AgentCore


### Deployment script

A single script handles the full deployment:

```bash
export AWS_REGION=us-east-1
export ENV_NAME=ima
```

```bash
./deploy.sh                  # Full deploy — everything from scratch
```

You can also selectively deploy different components of the stack:

```
./deploy.sh --engine-only    # Just the engine stack
./deploy.sh --app-only       # Just the app stack
./deploy.sh --agent-only     # Just the agent
./deploy.sh --update-images  # Rebuild, push, force-redeploy ECS services
./deploy.sh --teardown       # Delete all stacks (with confirmation)

# Optional flag (combine with any build mode above):
./deploy.sh --with-xtable              # also build & push the XTable image
./deploy.sh --engine-only --with-xtable
```

> **XTable image is not built by default.** The Apache XTable (Java/Maven)
> image — which powers morph on Hudi/Delta sources — is slow and disk-hungry to
> build, so build modes skip it unless you pass `--with-xtable` (or set
> `BUILD_XTABLE=yes`). When skipped, the engine reuses whatever XTable image is
> already in ECR, so a deploy never breaks; it just avoids the expensive
> rebuild. Pass `--with-xtable` the first time, or whenever you change the
> XTable converter.

The overall deployment process takes about 1-2 hours. 

The script auto-detects finch/docker, creates ECR repos idempotently, builds all images, deploys stacks in order, and wires cross-stack outputs automatically. See [docs/deployment-guide.md](docs/deployment-guide.md) for the full manual walkthrough.


### Post-deployment steps (required)

> **Deployment does not finish when the stacks complete.** The solution ships
> locked down: the roles have **no access to your data** and the app has **no
> runtime configuration** until you complete the steps below. Skipping them is
> the most common reason a fresh deployment appears to "do nothing" — inventory
> refresh, migrations, and agent analysis will result in errors. Follow through the 
> below steps before starting your migration planning.

1. **Grant data access.** By default every data-plane role is attached to an
   inert `Deny` placeholder policy and cannot read your source or write your
   target buckets. Either pass `SourceBucketNames` / `TargetBucketNames` /
   `SourceAccountIds` at deploy time so least-privilege policies are generated
   for you, or replace each placeholder policy (`${ENV}-agent-data-access`,
   `${ENV}-engine-data-access`, `${ENV}-tracking-data-access`,
   `${ENV}-emr-data-access`, `${ENV}-xtable-data-access`) with the grants listed
   in the guide. Cross-account sources also need a bucket policy / Lake Formation
   grant in the *owning* account.

2. **Set the runtime configuration values.** In the app go to
   **Manage > Settings** and fill in the required values (EMR, Athena, tracking,
   XTable, and `AGENT_URL`). Most come from the **engine stack outputs**.
   The app will not orchestrate anything until these are set.

Each step — including the exact commands, the full policy/permission tables, and
the complete list of settings — is documented in
[Section 8 (Post-Deployment Configuration) of docs/deployment-guide.md](docs/deployment-guide.md#8-post-deployment-configuration).
Follow it end to end before using the solution.


## Quick Start (Local dev)

> **Run these from the repo root** (`IMA_HOME` — the directory that
> contains `agent/`, `engine/`, `app/`, and `dev.sh`). The paths below are
> relative to it, so the `cd` commands will fail if you run them from anywhere
> else. If it helps, set it once and use it: `export IMA_HOME=$(pwd)` from
> the repo root.

```bash
# Run from IMA_HOME (the repo root)
# Install dependencies
(cd app/server && uv sync)
(cd agent && uv sync)
(cd engine && uv sync)
(cd app/client && npm install)

# Start everything
./dev.sh
```

This starts:
- Frontend on http://localhost:5173
- Backend on http://localhost:8000 (API docs at /docs)
- Agent on http://localhost:8082
- Press `r` to restart all, `q` to quit

To run the engine separately (with its own SQLite DB):

```bash
cd engine
./dev.sh
```


## Estimated Costs

Cost estimates for running the Migration Assistant in production (`us-east-1`). Excludes execution-based compute costs (EMR Spark jobs, Athena queries, XTable Lambda invocations) and Agent Invocations.

---

### Infrastructure Costs (Monthly, Steady-State)

| Resource | Spec | Estimated Cost |
|----------|------|---------------|
| ECS Fargate — Engine | 1 vCPU, 2 GB, 24/7 | ~$36 |
| ECS Fargate — App | 0.5 vCPU, 1 GB, 24/7 | ~$18 |
| Aurora PostgreSQL Serverless v2 | 0.5–4 ACU (scales to near-zero) | ~$44 min (0.5 ACU × $0.12/hr) |
| NAT Gateway | 1 gateway, minimal data | ~$32 (hourly) + data |
| DynamoDB (6 tables) | PAY_PER_REQUEST, light traffic | ~$1–5 |
| EC2 Jumphost | t3.micro, 24/7 | ~$8 |
| CloudWatch Logs | ~5 GB/month | ~$3 |
| S3 (scripts + logs) | < 1 GB | ~$1 |
| EMR Serverless App | Idle (auto-stop after 15min) | $0 when idle |
| Athena Workgroup | No charge until queries run | $0 when idle |
| ECR | 3 images (~2 GB total) | ~$0.20 |

**Total baseline: ~$140–150/month**

### Cost Reduction Options

- Scale ECS desired count to 0 when not in use (~saves $54/month)
- Remove NAT Gateway and use VPC endpoints only (~saves $32/month)
- Stop the jumphost when not needed (~saves $8/month)


## Releases

Versions follow [Semantic Versioning](https://semver.org). The current version is in [`VERSION`](VERSION), and every package manifest must match it: `python3 scripts/sync_version.py --check` verifies this (CI runs it), and `python3 scripts/sync_version.py` fixes any drift.

Changes are recorded in [`CHANGELOG.md`](CHANGELOG.md), in [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format. Each pull request with a user-visible change adds an entry under `Unreleased`.

Each release is a [GitHub Release](../../releases) with an annotated `vX.Y.Z` tag on `main`. Its notes are the version's section of `CHANGELOG.md`.

To cut a release:

1. On a `release/vX.Y.Z` branch, edit `VERSION` and run `python3 scripts/sync_version.py`. In `CHANGELOG.md`, rename `Unreleased` to `[X.Y.Z] - YYYY-MM-DD` and add a new empty `Unreleased` section above it.
2. Open a pull request titled `chore(release): vX.Y.Z` and merge it once the checks pass.
3. Tag the merge commit: `git tag -a vX.Y.Z -m "vX.Y.Z" <commit>` and push the tag.
4. Create a GitHub Release from the tag, with the version's `CHANGELOG.md` section as the notes, and publish it.

## Contributing

We are not accepting pull requests at this time. Please use [GitHub issues](../../issues) to report bugs or suggest features. See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues as described in [SECURITY.md](SECURITY.md), not through public issues.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
