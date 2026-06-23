"""End-to-end journeys for the remaining connectors.

Each connector is exercised the way it is actually fed in production, then the
ingested data is consumed back through the public API (events list, history,
agent change feed) — proving the full lineage
``raw_payloads → events → event_versions → outbox`` for every source.

* Telegram and IMAP have HTTP ingestion endpoints (webhook / poll-sync), so
  those are driven over the wire.
* Jira and the S3/MinIO file store are background connectors with no HTTP
  ingress; they run through the generic ``run_connector_sync`` driver (the same
  entrypoint the Celery beat schedule calls), pointed at the test database.

External systems (Telegram Bot API, an IMAP server, Jira Cloud, S3) are mocked
at the connector boundary; everything downstream of ``fetch()`` is the real
code path.
"""

from datetime import UTC, datetime

import pytest


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
async def _events_for(client, headers, source):
    resp = await client.get(f"/api/v1/events?source={source}", headers=headers)
    assert resp.status_code == 200
    return resp.json()["items"]


async def _feed_external_ids(client, headers):
    resp = await client.get("/api/v1/agent/changes", headers=headers)
    assert resp.status_code == 200
    return [c["payload"]["external_id"] for c in resp.json()["items"]]


# ---------------------------------------------------------------------------
# Telegram — webhook ingestion (the real-time path)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_telegram_webhook_to_feed(client, auth_headers, monkeypatch):
    from src.telegram import router as tg_router

    # No secret configured → webhook accepts the delivery without a token header.
    monkeypatch.setattr(tg_router.telegram_settings, "TELEGRAM_WEBHOOK_SECRET", "")

    update = {
        "update_id": 5001,
        "message": {
            "message_id": 7,
            "from": {"id": 42, "first_name": "Ada", "username": "ada"},
            "chat": {"id": -100, "type": "supergroup", "title": "Eng"},
            "date": int(datetime(2026, 6, 20, tzinfo=UTC).timestamp()),
            "text": "Deploy is green",
        },
    }

    resp = await client.post("/api/v1/telegram/webhook", json=update)
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "events_created": 1}

    # Operator sees the normalized message.
    items = await _events_for(client, auth_headers, "telegram")
    assert len(items) == 1
    assert items[0]["external_id"] == "7"
    assert items[0]["event_type"] == "message"
    assert "Deploy is green" in items[0]["content"]
    # Sender is parsed from the real Telegram "from" key (aliased to from_user).
    assert items[0]["author_id"] == "42"
    assert items[0]["author_name"] == "Ada"

    # Agent change feed carries it.
    assert "7" in await _feed_external_ids(client, auth_headers)

    # Telegram retries deliver the same update_id → idempotent no-op.
    dup = await client.post("/api/v1/telegram/webhook", json=update)
    assert dup.status_code == 200
    assert dup.json() == {"status": "ok", "duplicate": True}
    assert len(await _events_for(client, auth_headers, "telegram")) == 1


# ---------------------------------------------------------------------------
# Telegram — manual poll-sync (getUpdates)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_telegram_poll_sync(client, auth_headers, monkeypatch):
    from src.telegram import router as tg_router
    from src.telegram.schemas import TelegramChat, TelegramMessage, TelegramUpdate, TelegramUser

    updates = [
        TelegramUpdate(
            update_id=900,
            message=TelegramMessage(
                message_id=11,
                from_user=TelegramUser(id=7, first_name="Grace"),
                chat=TelegramChat(id=1, type="private"),
                date=int(datetime(2026, 6, 21, tzinfo=UTC).timestamp()),
                text="Standup at 10",
            ),
        ),
        # Service messages without text are skipped by the normalizer.
        TelegramUpdate(update_id=901, message=None),
    ]

    async def fake_get_updates():
        return updates

    monkeypatch.setattr(tg_router.telegram_client, "get_updates", fake_get_updates)

    resp = await client.post("/api/v1/telegram/sync", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["events_created"] == 1

    items = await _events_for(client, auth_headers, "telegram")
    assert [i["external_id"] for i in items] == ["11"]
    assert "Standup at 10" in items[0]["content"]


# ---------------------------------------------------------------------------
# IMAP — poll-sync (mailbox → email events)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_imap_sync_to_feed(client, auth_headers, monkeypatch):
    from src.imap import router as imap_router
    from src.imap.service import IMAPConnector

    monkeypatch.setattr(imap_router.imap_settings, "IMAP_HOST", "imap.acme.dev")
    monkeypatch.setattr(imap_router.imap_settings, "IMAP_USERNAME", "bot")
    monkeypatch.setattr(imap_router.imap_settings, "IMAP_PASSWORD", "secret")

    raw_message = {
        "uid": "12",
        "uidvalidity": "9",
        "message_id": "<incident-1@acme.dev>",
        "host": "imap.acme.dev",
        "subject": "Server down",
        "sender": "ops@acme.dev",
        "date": datetime(2026, 6, 22, 8, 30, tzinfo=UTC),
        "body": "Disk full on db-1, paging on-call.",
        "folder": "INBOX",
    }

    async def fake_fetch(self, since=None):
        return [raw_message]

    monkeypatch.setattr(IMAPConnector, "fetch", fake_fetch)

    resp = await client.post("/api/v1/imap/sync", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "messages": 1, "events_created": 1}

    items = await _events_for(client, auth_headers, "imap")
    assert len(items) == 1
    event = items[0]
    # Message-ID is preferred for a stable external id (survives UID churn).
    assert event["external_id"] == "imap-mid-<incident-1@acme.dev>"
    assert event["event_type"] == "email"
    assert "Server down" in event["content"]
    assert "Disk full" in event["content"]

    assert "imap-mid-<incident-1@acme.dev>" in await _feed_external_ids(client, auth_headers)


# ---------------------------------------------------------------------------
# Jira — background connector via run_connector_sync (issue tracker)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_jira_background_sync_versioning(client, auth_headers, monkeypatch):
    import conftest

    from src.integrations import sync as sync_mod
    from src.integrations.sync import run_connector_sync
    from src.jira.service import JiraConnector

    # The background driver opens its own session via ``async_session``; point it
    # at the test database for the duration of the test.
    monkeypatch.setattr(sync_mod, "async_session", conftest.TestSessionLocal)
    monkeypatch.setattr(JiraConnector, "__init__", _jira_init)

    def _issue(summary: str, description_text: str):
        return {
            "key": "ENG-1",
            "fields": {
                "summary": summary,
                "description": {
                    "type": "doc",
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": description_text}],
                        }
                    ],
                },
                "status": {"name": "In Progress"},
                "reporter": {"accountId": "acc-1", "displayName": "Ada Lovelace"},
                "updated": "2026-06-20T10:00:00.000+0000",
                "issuetype": {"name": "Bug"},
            },
        }

    # --- First scheduled sync ingests the issue ---
    async def fetch_v1(self, since=None):
        return [_issue("Login fails on Safari", "Steps to reproduce attached.")]

    monkeypatch.setattr(JiraConnector, "fetch", fetch_v1)
    created = await run_connector_sync("jira")
    assert created == 1

    items = await _events_for(client, auth_headers, "jira")
    assert len(items) == 1
    event = items[0]
    assert event["external_id"] == "jira-ENG-1"
    assert event["event_type"] == "issue"
    # ADF description is flattened to plain text; status/key are in the content.
    assert event["content"].startswith("[In Progress] ENG-1: Login fails on Safari")
    assert "Steps to reproduce attached." in event["content"]
    assert event["version"] == 1
    event_id = event["id"]

    # --- A later sync sees the issue edited → idempotent upsert bumps version ---
    async def fetch_v2(self, since=None):
        return [_issue("Login fails on Safari and Firefox", "Root cause: cookie flag.")]

    monkeypatch.setattr(JiraConnector, "fetch", fetch_v2)
    await run_connector_sync("jira")

    items = await _events_for(client, auth_headers, "jira")
    assert len(items) == 1  # still one issue (dedup by jira-ENG-1)
    assert items[0]["version"] == 2

    history = await client.get(f"/api/v1/events/{event_id}/history", headers=auth_headers)
    assert [v["version"] for v in history.json()] == [1, 2]


def _jira_init(self, *args, **kwargs):
    """A JiraConnector __init__ that needs no live Jira configuration.

    fetch() is mocked in the tests, so the real client is never used.
    """
    self.project = "ENG"
    self.client = None


# ---------------------------------------------------------------------------
# File store (S3/MinIO) — background connector via run_connector_sync
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_filestore_background_sync(client, auth_headers, monkeypatch):
    import conftest

    from src.filestore.service import FileStoreClient, FileStoreConnector
    from src.integrations import sync as sync_mod
    from src.integrations.sync import run_connector_sync
    from src.sync_state.service import get_cursor

    monkeypatch.setattr(sync_mod, "async_session", conftest.TestSessionLocal)

    def _init(self, *args, **kwargs):
        self.prefix = ""
        self.client = FileStoreClient(
            endpoint_url="", access_key="", secret_key="", bucket="work-bucket"
        )

    monkeypatch.setattr(FileStoreConnector, "__init__", _init)

    last_modified = datetime(2026, 6, 22, 12, 0, tzinfo=UTC)

    async def fake_fetch(self, since=None):
        return [
            {
                "key": "reports/q2.pdf",
                "size": 2048,
                "etag": "abc123",
                "last_modified": last_modified,
            }
        ]

    monkeypatch.setattr(FileStoreConnector, "fetch", fake_fetch)

    created = await run_connector_sync("filestore")
    assert created == 1

    items = await _events_for(client, auth_headers, "filestore")
    assert len(items) == 1
    event = items[0]
    assert event["external_id"] == "filestore-work-bucket-reports/q2.pdf"
    assert event["event_type"] == "file"
    assert event["content"] == "File: reports/q2.pdf (2048 bytes)"

    assert "filestore-work-bucket-reports/q2.pdf" in await _feed_external_ids(client, auth_headers)

    # The incremental watermark advanced to the newest object's timestamp, so the
    # next poll only asks for newer files.
    async with conftest.TestSessionLocal() as session:
        cursor = await get_cursor(session, "filestore")
    assert cursor is not None
    assert datetime.fromisoformat(cursor) == last_modified


# ---------------------------------------------------------------------------
# Failure path: a background connector sync error is dead-lettered
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_background_sync_failure_records_dlq(client, auth_headers, monkeypatch):
    import conftest

    from src.dlq import service as dlq_service
    from src.integrations import sync as sync_mod
    from src.integrations.sync import run_connector_sync
    from src.jira.service import JiraConnector

    monkeypatch.setattr(sync_mod, "async_session", conftest.TestSessionLocal)
    # The failure path records the DLQ entry via its own committed session.
    monkeypatch.setattr(dlq_service, "async_session", conftest.TestSessionLocal)
    monkeypatch.setattr(JiraConnector, "__init__", _jira_init)

    async def boom(self, since=None):
        raise RuntimeError("Jira rate limit (HTTP 429)")

    monkeypatch.setattr(JiraConnector, "fetch", boom)

    with pytest.raises(RuntimeError, match="429"):
        await run_connector_sync("jira")

    # The operator finds the failure waiting in the DLQ.
    resp = await client.get("/api/v1/dlq/failed-events", headers=auth_headers)
    assert resp.status_code == 200
    jira_failures = [e for e in resp.json() if e["source"] == "jira"]
    assert len(jira_failures) == 1
    assert "429" in jira_failures[0]["error_text"]
