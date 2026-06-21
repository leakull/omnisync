# OmniSync

Integration gateway for collecting, normalizing, and serving work events from GitHub and Telegram.

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
- PostgreSQL 16, Redis 7, MinIO
- Celery + Redis (task queue)
- Alembic (migrations)
- OpenTelemetry + Jaeger (tracing)
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

1. **Raw payloads** arrive via webhook or polling (GitHub/Telegram)
2. Stored in `raw_payloads` table (small) or MinIO (large, >32KB)
3. **Normalized events** created/updated via upsert with versioning
4. Previous versions preserved in `event_versions` for audit trail
5. Sync operations logged in `sync_logs`

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
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |

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
