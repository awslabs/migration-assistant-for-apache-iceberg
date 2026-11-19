# Backend

FastAPI service powering the Migration Assistant portal. Manages users, groups, table inventory, plans, and AI assistant orchestration.

## Setup

```bash
uv sync
uv run uvicorn app.main:app --port 8000
```

API docs at http://localhost:8000/docs.

Uses SQLite locally (`app/data/app.db`). Set `DB_TYPE=aurora` with connection env vars for production.

## Key env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_TYPE` | `sqlite` | `sqlite` or `aurora` |
| `AGENT_URL` | `http://localhost:8082/invocations` | AI agent endpoint |
| `ENGINE_URL` | `http://localhost:8001` | Engine endpoint |
| `ENGINE_API_TOKEN` | *(required)* | Bearer token sent to the engine API. Must match the engine's value. Read from the environment only |

## License

Apache-2.0 — see [LICENSE](../../LICENSE).
