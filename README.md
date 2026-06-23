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
- Transactional outbox + dead-letter queue (DLQ) with **automatic exponential-backoff retry** & manual replay
- OpenTelemetry + Jaeger (tracing in API **and** Celery workers)
- Prometheus metrics + ready-to-use **alert rules** and a **Grafana dashboard**

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
5. Sync operations logged in `sync_logs`; failed operations land in `failed_events` (DLQ).
   A beat-scheduled retrier (`src.dlq.tasks.retry_failed_events`) automatically re-dispatches them
   with **exponential backoff** until they succeed or exhaust their attempt budget (then they are
   retired to `exhausted` for manual inspection / replay via the DLQ API)
6. Each change emits an `outbox_events` row, published asynchronously to the vector index
   (and any future Pulse / AI-agent consumers) — never lost, retried with back-off.
   Every change carries a `schema_version` so consumers can evolve the content contract safely

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

## Resilience & external-API handling

External systems are slow, rate-limited, and occasionally wrong. The integration layer assumes this:

- **Idempotency** — raw payloads are deduplicated by canonical content hash; normalized events
  upsert on `(source, external_id)`; inbound webhooks are deduplicated by delivery id
  (`X-GitHub-Delivery`, Telegram `update_id`) in `webhook_deliveries`.
- **Webhook authenticity** — GitHub `X-Hub-Signature-256` HMAC and the Telegram secret-token header
  are verified before any processing.
- **Rate limits** — the GitHub client throttles client-side, honors `X-RateLimit-Remaining/Reset`,
  and retries transient failures with `tenacity` exponential backoff.
- **Retries & dead-lettering** — exhausted operations are dead-lettered, then retried automatically
  with exponential backoff (`DLQ_RETRY_*` settings) and a capped attempt budget.
- **Graceful degradation** — if S3/MinIO is unavailable, large payloads transparently fall back to
  Postgres (and the failure is counted in `omnisync_s3_storage_failures_total`).
- **Stable identities** — emails are keyed by RFC 5322 `Message-ID`, falling back to
  `host + folder + UIDVALIDITY + UID` so UIDs never collide across mailboxes or UID resets.
- **Connection reuse** — IMAP and SMTP sessions are cached and health-checked (NOOP) so short-interval
  polling and notification bursts don't pay a TLS+AUTH handshake every time.

## Why Celery (and not Dramatiq / RQ)?

The vacancy lists *Celery / Dramatiq / RQ or analogues* — any is defensible. Celery was chosen because:

- **Beat scheduler** is built in — the project needs periodic polling for every connector plus the
  outbox publisher and the DLQ retrier, all expressed declaratively in `beat_schedule`.
- **Mature operational tooling** — acks-late, visibility timeouts, autoscaling, Flower, and
  first-class OpenTelemetry instrumentation (`opentelemetry-instrumentation-celery`) for worker traces.
- **Redis broker** is already in the stack, so no extra infrastructure.

RQ has no native scheduler (needs `rq-scheduler`) and a thinner feature set; Dramatiq is excellent and
lighter, but its scheduling/ecosystem story is less batteries-included for this mix of periodic +
fan-out workloads. The task layer is thin and isolated (`src/*/tasks.py`), so swapping is low-cost.

## Observability & alerting

```bash
# Start the optional Prometheus + Grafana stack
docker compose --profile monitoring up -d
# Prometheus:  http://localhost:9090   (alert rules in monitoring/alerts.yml)
# Grafana:     http://localhost:3001   (admin/admin; dashboard auto-provisioned)
```

Alert rules (`monitoring/alerts.yml`) cover API availability, DLQ inflow & exhausted retries,
sync error rate, outbox publish failures, S3 fallbacks, and sync p95 latency. The Grafana dashboard
(`monitoring/grafana-dashboard.json` for manual import, or auto-provisioned via the compose profile)
visualizes the same signals. See [docs/data-lineage.md](docs/data-lineage.md) for how a single record
flows from source to AI-agent consumer, including every status and error transition.

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
make install      # install dev dependencies
make hooks        # install pre-commit git hooks
make check        # lint + typecheck + tests (CI parity)

# or individually:
make lint         # ruff check
make format       # ruff format
make typecheck    # mypy src
make cov          # tests with coverage report
make security     # bandit + pip-audit
```

Quality gates enforced in CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)):

- **Lint & format** — ruff (`E,F,I,N,W,B,UP,ASYNC,SIM,C4,RUF` rule sets) + `ruff format --check`.
- **Types** — mypy with `check_untyped_defs`, `warn_return_any` and friends enabled.
- **Tests & coverage** — pytest against real PostgreSQL + Redis services, branch coverage
  reported (`--cov`) with a `--cov-fail-under=70` floor; `coverage.xml` is uploaded as an artifact.
- **Security** — `bandit` static analysis (medium+ severity) and `pip-audit` dependency CVE scan.
- **Migrations** — `alembic upgrade head → downgrade base → upgrade head` round-trip.
- **Build** — Docker image build.

The same checks run locally on every commit via [.pre-commit-config.yaml](.pre-commit-config.yaml)
(`make hooks`).

## License

MIT
