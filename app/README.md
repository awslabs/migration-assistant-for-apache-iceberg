# Migration Assistant App

Web application for managing Iceberg migrations. Combines the React frontend and FastAPI backend into a single deployable container.

## Architecture

```
ALB (public, IP-restricted) → ECS Fargate → FastAPI
                                              ├── /api/*    → Backend routes
                                              └── /*        → Frontend static files (React SPA)
```

Single container serves both the API and the built frontend. Access is controlled via security group IP allowlisting on the ALB.

---

## Deploy to AWS

### Prerequisites

1. **Engine deployed** — the app connects to the engine at `ENGINE_URL`. Deploy the engine stack first (see `engine/README.md`).

2. **VPC with public and private subnets** — the engine stack creates these. Use its VPC outputs.

3. **Docker** installed locally to build the container image.

4. **ECR repository** created and image pushed:

   ```bash
   aws ecr create-repository --repository-name ima-app --region us-east-1

   docker build -t ima-app .
   docker tag ima-app:latest <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-app:latest
   aws ecr get-login-password --region <REGION> | docker login --username AWS --password-stdin <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com
   docker push <ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-app:latest
   ```

5. **Your public IP** for the allowlist (find it at https://checkip.amazonaws.com).

### Deploy

```bash
aws cloudformation deploy \
    --template-file infra/app-stack.yaml \
    --stack-name ima-app \
    --parameter-overrides \
        ImageUri=<ACCOUNT>.dkr.ecr.<REGION>.amazonaws.com/ima-app:latest \
        AllowedIPs="<YOUR_IP>/32" \
        VpcId=<VPC_ID> \
        PublicSubnetA=<PUBLIC_SUBNET_A> \
        PublicSubnetB=<PUBLIC_SUBNET_B> \
        PrivateSubnetA=<PRIVATE_SUBNET_A> \
        PrivateSubnetB=<PRIVATE_SUBNET_B> \
        DBMasterPassword=<SECURE_PASSWORD> \
        EngineUrl=http://engine.local:8001 \
    --capabilities CAPABILITY_NAMED_IAM \
    --region <REGION>
```

### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `ImageUri` | **Yes** | — | ECR image URI for the app container |
| `AllowedIPs` | **Yes** | — | Comma-separated CIDRs allowed to access the ALB (e.g., `1.2.3.4/32,5.6.7.0/24`) |
| `VpcId` | **Yes** | — | VPC to deploy into |
| `PublicSubnetA` | **Yes** | — | Public subnet for ALB (AZ 1) |
| `PublicSubnetB` | **Yes** | — | Public subnet for ALB (AZ 2) |
| `PrivateSubnetA` | **Yes** | — | Private subnet for ECS + DB (AZ 1) |
| `PrivateSubnetB` | **Yes** | — | Private subnet for DB (AZ 2) |
| `DBMasterPassword` | **Yes** | — | Aurora PostgreSQL password (min 8 chars) |
| `EnvironmentName` | No | `ima` | Resource name prefix |
| `Cpu` | No | `512` | Fargate task CPU |
| `Memory` | No | `1024` | Fargate task memory (MB) |
| `DBMinCapacity` | No | `0.5` | Aurora min ACU (scales to zero) |
| `DBMaxCapacity` | No | `4` | Aurora max ACU |
| `DBMasterUsername` | No | `imaadmin` | DB username |
| `EngineUrl` | No | `http://engine.local:8001` | Engine URL |
| `AgentUrl` | No | *(empty)* | AI agent endpoint URL |
| `CorsAllowedOrigins` | No | *(empty — dev only)* | Comma-separated CORS origins (e.g., `https://app.example.com`) |

### After Deployment

1. Get the application URL:
   ```bash
   aws cloudformation describe-stacks --stack-name ima-app \
       --query "Stacks[0].Outputs[?OutputKey=='AppUrl'].OutputValue" --output text
   ```

2. Open the URL in your browser. Log in with default credentials: `admin` / `admin`.

3. **Change the default admin password immediately** — the default password is hashed but well-known.

4. To add more allowed IPs, update the stack:
   ```bash
   aws cloudformation deploy \
       --template-file infra/app-stack.yaml \
       --stack-name ima-app \
       --parameter-overrides \
           AllowedIPs="1.2.3.4/32,5.6.7.8/32,9.10.11.0/24" \
           ... \
       --capabilities CAPABILITY_NAMED_IAM
   ```

---

## Security Configuration

### CORS (Cross-Origin Resource Sharing)

By default, the backend only allows requests from local development origins (`localhost:5173`, `localhost:3000`). For production, you **must** set the `CORS_ALLOWED_ORIGINS` environment variable to your actual frontend domain(s).

**Set via environment variable:**

```bash
CORS_ALLOWED_ORIGINS=https://ima.yourcompany.com
```

Multiple origins can be comma-separated:

```bash
CORS_ALLOWED_ORIGINS=https://ima.yourcompany.com,https://app.yourcompany.com
```

**Add to ECS task definition in `app-stack.yaml`:**

Add the following entry to the `Environment` list in the `ContainerDefinitions` section of the `TaskDefinition` resource:

```yaml
- Name: CORS_ALLOWED_ORIGINS
  Value: https://your-production-domain.com
```

Or pass it as a CloudFormation parameter override during deployment.

**Important:** If `CORS_ALLOWED_ORIGINS` is not set, the app falls back to development-only origins and will reject cross-origin requests from your production frontend domain.

---

## Local Development

Run the frontend and backend separately for hot reloading:

```bash
# Terminal 1: Backend
cd server
uv run uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

# Terminal 2: Frontend
cd client
npm run dev
```

Frontend dev server runs at http://localhost:5173 and proxies `/api` to the backend at :8000.

Or use the project-level dev script from the repo root:
```bash
./dev.sh
```

---

## Build

```bash
# Build the combined container locally
docker build -t ima-app .

# Run it
docker run -p 127.0.0.1:8000:8000 \
    -e DB_TYPE=sqlite \
    -e ENGINE_URL=http://host.docker.internal:8001 \
    -e ENGINE_API_TOKEN="$ENGINE_API_TOKEN" \
    ima-app
```

Access at http://localhost:8000.

---

## License

Apache-2.0
