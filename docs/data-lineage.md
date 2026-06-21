# Data Lineage & Traceability

OmniSync feeds AI agents and Pulse modules, so every record must be **traceable**: where it came
from, when it arrived, how it was processed, what failed, and how it changed over time. This document
describes the lifecycle of a single record and the tables that make it auditable.

## Lifecycle of a record

```
        external system (GitHub / Telegram / IMAP / Jira / S3)
                              │
              webhook ────────┤──────── polling (incremental cursor in sync_state)
   verify signature +         │
   dedupe by delivery_id      │
   (webhook_deliveries)       ▼
                    ┌───────────────────────┐
                    │     raw_payloads       │  source, received_at, content_hash,
                    │  (verbatim, dedup'd)   │  correlation_id, storage_url (S3 if large)
                    └───────────┬───────────┘
                                │ normalize()  (per-connector)
                                ▼
                    ┌───────────────────────┐
                    │   normalized_events    │  (source, external_id) unique,
                    │   idempotent upsert     │  version, schema_version, raw_payload_id ──┐
                    └───────────┬───────────┘                                             │
                                │ on content change                                       │ FK back to
                                ▼                                                         │ source payload
                    ┌───────────────────────┐                                            │
                    │    event_versions      │  full history: version, content,           │
                    │   (audit trail)        │  schema_version, changed_by, changed_at  ◀─┘
                    └───────────┬───────────┘
                                │ same transaction
                                ▼
                    ┌───────────────────────┐
                    │     outbox_events      │  append-only change feed,
                    │  (transactional outbox)│  status: pending → published | dead
                    └───────────┬───────────┘
                                │ publish_outbox (Celery beat)
                                ▼
                 vector index (Qdrant) + agent change feed
                 (/api/v1/agent/changes, SSE) — consumers
                 advance a cursor; payload carries schema_version
```

Cross-cutting, every operation is recorded in:

- **`sync_logs`** — one row per sync/webhook run: `correlation_id`, `source`, `status`
  (`started` → `completed` | `failed`), `error_text`. The `correlation_id` threads through
  structured logs (structlog) and OpenTelemetry traces.
- **`failed_events`** (DLQ) — anything that exhausted in-task retries, with the original payload,
  `error_text`, `replay_attempts`, `last_attempt_at`, `next_retry_at`, and
  `status` (`pending` → `retrying` → `resolved` | `exhausted`).

## What an AI-agent consumer can answer

| Question | Source of truth |
|---|---|
| Where did this event come from? | `normalized_events.raw_payload_id` → `raw_payloads` (DB or S3) |
| When was it received? | `raw_payloads.received_at` |
| Was it processed successfully? | `sync_logs.status` (joined by `correlation_id`) |
| Did anything fail, and is it being retried? | `failed_events.status` / `next_retry_at` / `replay_attempts` |
| How has it changed? | `event_versions` (every prior version + `changed_by`/`changed_at`) |
| Which content contract does it use? | `normalized_events.schema_version` (also in the outbox payload) |
| Is it deduplicated? | `raw_payloads.content_hash`, `(source, external_id)` uniqueness, `webhook_deliveries.delivery_id` |

## Status state machines

**Outbox event**

```
pending ──publish ok──▶ published
   │
   └──publish fails (attempts++ )──▶ pending ──(attempts ≥ OUTBOX_MAX_ATTEMPTS)──▶ dead
```

**Dead-letter entry**

```
pending ──auto-retry due──▶ retrying ──(success, re-ingested)──▶ (no new failure)
   │                           │
   │                           └──(replay_attempts ≥ DLQ_MAX_REPLAY_ATTEMPTS)──▶ exhausted
   └──manual replay (DLQ API)──▶ resolved
```

Backoff between automatic retries is `min(DLQ_RETRY_BASE_DELAY · 2^attempts, DLQ_RETRY_MAX_DELAY)`.

## Tracing

Set `OTEL_EXPORTER_OTLP_ENDPOINT` and open Jaeger (`http://localhost:16686`). API requests and Celery
tasks share the same trace context, so a single agent-visible change can be followed from the inbound
webhook/poll through normalization, outbox publication, and vector indexing.
