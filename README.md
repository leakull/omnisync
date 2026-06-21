# OmniSync

Integration gateway for collecting, normalizing, deduplicating, and serving work events from
multiple sources — GitHub, Telegram, email (IMAP), a Jira task tracker, and S3/MinIO file storage —
with a transactional outbox change feed for downstream Pulse modules and AI agents.

## Architecture

```
┌─────────────┐     ┌─────────────┐
│   GitHub    │     │  Telegram   │
│  (webhook/  │     │  (webhook/  │
│   polling)  │     │   polling)  │
└──────┬──────┘     └──────┬──────┘
       │                   │
       ▼                   ▼
┌──────────────────────────────────┐
│           FastAPI API            │
│  ┌────────────────────────────┐  │
│  │   Normalized Event Service │  │
│  │   (upsert + versioning)    │  │
│  └────────────┬───────────────┘  │
│               │                  │
│  ┌────────────▼───────────────┐  │
│  │       PostgreSQL           │  │
│  │  raw_payloads → events →   │  │
│  │  event_versions → sync_logs│  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
       │
       ▼
┌─────────────┐
│   MinIO/S3  │  (large payloads)
└─────────────┘
```

## Tech Stack

- **Python 3.12**, FastAPI, SQLAlchemy 2.0 (async), asyncpg
- PostgreSQL 16 (timezone-aware timestamps, `ON CONFLICT` upserts), Redis 7, MinIO
- Celery + Redis (task queue, beat scheduler)
- Alembic (migrations)
- Qdrant + sentence-transformers (semantic vector search)
- Transactional outbox + dead-letter queue (DLQ) for reliable delivery & replay
- OpenTelemetry + Jaeger (tracing in API **and** Celery workers)
- Prometheus (metrics)

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env with your tokens

# 2. Start all services
docker compose up -d

# 3. Apply migrations
docker compose exec api alembic upgrade head

# 4. Access
# API:        http://localhost:8000/docs
# Jaeger:     http://localhost:16686
# MinIO:      http://localhost:9001
# Prometheus: http://localhost:8000/metrics
```

## Data Flow

1. **Raw payloads** arrive via webhook or polling (GitHub/Telegram/IMAP)
2. Stored in `raw_payloads` table (small) or MinIO (large, >32KB), deduplicated by content hash
3. **Normalized events** created/updated via idempotent `ON CONFLICT` upsert with versioning
4. Previous versions preserved in `event_versions` for audit trail
5. Sync operations logged in `sync_logs`; exhausted retries land in `failed_events` (DLQ) for replay
6. Each change emits an `outbox_events` row, published asynchronously to the vector index
   (and any future Pulse / AI-agent consumers) — never lost, retried with back-off

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/auth/register` | Register user |
| POST | `/api/v1/auth/login` | Login |
| POST | `/api/v1/auth/refresh` | Refresh tokens |
| POST | `/api/v1/auth/logout` | Logout (revoke refresh) |
| GET | `/api/v1/events` | List events (cursor pagination) |
| GET | `/api/v1/events/{id}` | Get event |
| GET | `/api/v1/events/{id}/history` | Get event version history |
| POST | `/api/v1/github/sync` | Trigger GitHub sync |
| POST | `/api/v1/github/webhooks/github` | GitHub webhook receiver |
| POST | `/api/v1/telegram/sync` | Trigger Telegram sync |
| POST | `/api/v1/telegram/webhook` | Telegram webhook receiver |
| POST | `/api/v1/imap/sync` | Trigger IMAP mailbox sync |
| POST | `/api/v1/imap/send` | Send an email via SMTP |
| GET | `/api/v1/search/events` | Semantic vector search over events |
| GET | `/api/v1/raw-payloads/{id}` | Fetch raw payload (DB or S3) — source traceability |
| GET | `/api/v1/dlq/failed-events` | List dead-lettered sync operations |
| POST | `/api/v1/dlq/failed-events/{id}/replay` | Replay a failed operation |
| GET | `/api/v1/agent/changes` | Incremental change feed (poll by cursor) for AI agents |
| GET | `/api/v1/agent/changes/stream` | SSE stream of the change feed |
| GET | `/health` | Detailed dependency status (observability) |
| GET | `/health/live` | Liveness probe (no external deps) |
| GET | `/health/ready` | Readiness probe (DB + Redis only; 503 if not ready) |
| GET | `/metrics` | Prometheus metrics |

### Sources / connectors

| Source | Type | Incremental cursor |
|--------|------|--------------------|
| `github` | VCS / reviews | event-timestamp watermark |
| `telegram` | messenger | `getUpdates` offset (persisted in `sync_state`) |
| `imap` | email inbox | `SINCE` date (persisted in `sync_state`) |
| `jira` | task tracker | JQL `updated >=` (persisted in `sync_state`) |
| `filestore` | S3/MinIO bucket | object `LastModified` (persisted in `sync_state`) |

Outbound email is sent via SMTP (`POST /api/v1/imap/send`).

### Production notes

- **Embeddings** (`EMBEDDING_BACKEND`): `local` runs sentence-transformers in-process (good for dev);
  in production prefer `openai` (or another remote embedding API) so the heavy model does not load
  into the API process. Indexing itself runs in the outbox worker, not on the request path.
- **Secrets**: when a `SECRETS_DIR` (default `/run/secrets`) exists, every setting may be supplied as a
  file named after the env var (Docker/Kubernetes secrets) instead of being placed in `.env`.

## Adding a New Connector

1. Create `src/myapp/service.py`:
```python
from src.integrations.base import BaseConnector
from src.integrations.registry import register_connector

@register_connector
class MyAppConnector(BaseConnector):
    source = "myapp"

    async def fetch(self, since=None):
        # Fetch data from external API
        ...

    def normalize(self, raw, raw_payload_id=None):
        # Convert to NormalizedEventCreate
        ...
```

2. Create Celery task in `src/myapp/tasks.py`
3. Register in `src/celery_app.py` autodiscover
4. Add router in `src/myapp/router.py`
5. Include in `src/main.py`

## Development

```bash
# Install dependencies
pip install -r requirements/dev.txt

# Run tests
python -m pytest tests/ -v

# Lint
ruff check .

# Type check
mypy src --ignore-missing-imports
```

## License

MIT
