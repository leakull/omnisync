"""Failure-path coverage: rate limits, retries, dead-letter auto-replay,
S3 fallback and stable external IDs — the 'unhappy paths' that resilient
external integrations must handle (limits, errors, incomplete/unstable data)."""

import time

import pytest

from src.config import settings

# ---------------------------------------------------------------------------
# DLQ exponential backoff + auto-retry
# ---------------------------------------------------------------------------


def test_compute_backoff_doubles_and_caps():
    from src.dlq.service import compute_backoff

    assert compute_backoff(0).total_seconds() == settings.DLQ_RETRY_BASE_DELAY
    assert compute_backoff(1).total_seconds() == settings.DLQ_RETRY_BASE_DELAY * 2
    assert compute_backoff(2).total_seconds() == settings.DLQ_RETRY_BASE_DELAY * 4
    # Never exceeds the configured cap, no matter how many attempts.
    assert compute_backoff(100).total_seconds() == settings.DLQ_RETRY_MAX_DELAY


@pytest.mark.asyncio
async def test_record_failure_dedupes_active_entries(db_session):
    from src.dlq.service import list_failed, record_failure

    id1 = await record_failure(
        db_session, "jira", "src.jira.tasks.sync_jira", {"trigger": "resync"}, "err-1"
    )
    id2 = await record_failure(
        db_session, "jira", "src.jira.tasks.sync_jira", {"trigger": "resync"}, "err-2"
    )
    await db_session.commit()

    # Repeated failures of the same operation collapse onto one active row.
    assert id1 == id2
    jira_rows = [r for r in await list_failed(db_session) if r.source == "jira"]
    assert len(jira_rows) == 1
    assert jira_rows[0].error_text == "err-2"


@pytest.mark.asyncio
async def test_dlq_auto_retry_dispatches_due_entry(db_session, monkeypatch):
    from src.dlq import tasks as dlq_tasks
    from src.dlq.models import FailedEvent
    from src.dlq.service import record_failure

    sent: list[str] = []
    monkeypatch.setattr(dlq_tasks.celery_app, "send_task", lambda name, *a, **k: sent.append(name))

    fid = await record_failure(
        db_session, "github", "src.github.tasks.sync_github_commits", {}, "boom"
    )
    # Force the entry to be due now (record_failure seeds a backoff delay).
    failed = await db_session.get(FailedEvent, fid)
    failed.next_retry_at = None
    await db_session.flush()

    dispatched = await dlq_tasks.process_due_retries(db_session)

    assert dispatched == 1
    assert sent == ["src.github.tasks.sync_github_commits"]
    failed = await db_session.get(FailedEvent, fid)
    assert failed.replay_attempts == 1
    assert failed.status == "retrying"
    assert failed.next_retry_at is not None  # backoff pushed forward


@pytest.mark.asyncio
async def test_dlq_auto_retry_skips_not_due_entries(db_session, monkeypatch):
    from src.dlq import tasks as dlq_tasks
    from src.dlq.service import record_failure

    monkeypatch.setattr(dlq_tasks.celery_app, "send_task", lambda *a, **k: None)
    # record_failure leaves next_retry_at in the future → not yet due.
    await record_failure(db_session, "telegram", "src.telegram.tasks.x", {}, "boom")
    await db_session.flush()

    assert await dlq_tasks.process_due_retries(db_session) == 0


@pytest.mark.asyncio
async def test_dlq_auto_retry_exhausts_after_max_attempts(db_session, monkeypatch):
    from src.dlq import tasks as dlq_tasks
    from src.dlq.models import FailedEvent
    from src.dlq.service import record_failure

    monkeypatch.setattr(settings, "DLQ_MAX_REPLAY_ATTEMPTS", 2)
    monkeypatch.setattr(dlq_tasks.celery_app, "send_task", lambda *a, **k: None)

    fid = await record_failure(db_session, "imap", "src.imap.tasks.sync_imap_messages", {}, "boom")

    async def _make_due():
        row = await db_session.get(FailedEvent, fid)
        row.next_retry_at = None
        await db_session.flush()

    await _make_due()
    await dlq_tasks.process_due_retries(db_session)
    row = await db_session.get(FailedEvent, fid)
    assert (row.replay_attempts, row.status) == (1, "retrying")

    await _make_due()
    await dlq_tasks.process_due_retries(db_session)
    row = await db_session.get(FailedEvent, fid)
    assert row.status == "exhausted"
    assert row.next_retry_at is None


# ---------------------------------------------------------------------------
# GitHub rate-limit handling
# ---------------------------------------------------------------------------


class _FakeHTTPX:
    def __init__(self, response):
        self._response = response
        self.is_closed = False

    async def get(self, endpoint, params=None):
        return self._response


@pytest.mark.asyncio
async def test_github_sleeps_when_rate_limit_low(monkeypatch):
    from src.github.service import GitHubClient

    slept: list[float] = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr("src.github.service.asyncio.sleep", fake_sleep)

    class FakeResp:
        status_code = 200
        headers = {
            "x-ratelimit-remaining": "1",
            "x-ratelimit-reset": str(int(time.time()) + 50),
        }

        def json(self):
            return []

    client = GitHubClient()
    client._min_interval = 0
    client._client = _FakeHTTPX(FakeResp())

    result = await client._request("/x")
    assert result == []
    # Backed off until the rate-limit window resets (~50s).
    assert any(s >= 40 for s in slept)


@pytest.mark.asyncio
async def test_github_403_classified_as_rate_limit_error(monkeypatch):
    from tenacity import RetryError, stop_after_attempt

    from src.github.exceptions import GitHubRateLimitError
    from src.github.service import GitHubClient

    # Collapse retries to a single attempt so the test doesn't wait on backoff.
    monkeypatch.setattr(GitHubClient._request.retry, "stop", stop_after_attempt(1))

    class FakeResp:
        status_code = 403
        headers: dict = {}

        def json(self):
            return {}

    client = GitHubClient()
    client._min_interval = 0
    client._client = _FakeHTTPX(FakeResp())

    with pytest.raises(RetryError) as exc_info:
        await client._request("/x")
    assert isinstance(exc_info.value.last_attempt.exception(), GitHubRateLimitError)


# ---------------------------------------------------------------------------
# S3/MinIO fallback to DB on storage failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_s3_failure_falls_back_to_db(db_session, monkeypatch):
    pytest.importorskip("aioboto3")
    from src.raw_payloads import service as rp_service
    from src.raw_payloads import storage as rp_storage

    # Force the large-payload path, then make S3 blow up.
    monkeypatch.setattr(settings, "S3_PAYLOAD_THRESHOLD", 10)

    async def boom(*args, **kwargs):
        raise RuntimeError("s3 unavailable")

    monkeypatch.setattr(rp_storage.s3_storage, "save_object", boom)

    payload = {"blob": "x" * 200}
    pid = await rp_service.save_raw_payload(db_session, "github", payload, "corr-1")
    await db_session.commit()

    raw = await rp_service.get_raw_payload(db_session, pid)
    assert raw is not None
    assert raw.storage_url is None  # did not offload to S3
    assert raw.payload == payload  # payload preserved in the DB instead


# ---------------------------------------------------------------------------
# IMAP stable external IDs (no UID collisions across mailboxes/UIDVALIDITY)
# ---------------------------------------------------------------------------


def test_imap_external_id_prefers_message_id():
    from src.imap.service import _build_external_id

    raw = {
        "message_id": "<abc@example.com>",
        "host": "mail.example.com",
        "folder": "INBOX",
        "uidvalidity": "7",
        "uid": "3",
    }
    assert _build_external_id(raw) == "imap-mid-<abc@example.com>"


# ---------------------------------------------------------------------------
# Content schema versioning
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_version_propagates_to_event_and_outbox(db_session):
    from datetime import datetime, timezone

    from src.events.schemas import CONTENT_SCHEMA_VERSION, NormalizedEventCreate
    from src.events.service import NormalizedEventService
    from src.outbox.service import fetch_changes

    ev = NormalizedEventCreate(
        external_id="sv-1",
        source="github",
        author_id="a",
        author_name="A",
        content="c",
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )
    saved = await NormalizedEventService.upsert_event(db_session, ev)
    await db_session.commit()

    assert saved.schema_version == CONTENT_SCHEMA_VERSION
    changes = await fetch_changes(db_session)
    assert changes[-1].payload["schema_version"] == CONTENT_SCHEMA_VERSION


def test_imap_external_id_scopes_uid_by_mailbox_and_uidvalidity():
    from src.imap.service import _build_external_id

    base = {
        "message_id": "",
        "host": "mail.example.com",
        "folder": "INBOX",
        "uidvalidity": "7",
        "uid": "3",
    }
    assert _build_external_id(base) == "imap-mail.example.com-INBOX-7-3"

    # Same UID in another folder must not collide.
    other_folder = {**base, "folder": "Archive"}
    assert _build_external_id(other_folder) != _build_external_id(base)

    # Missing UIDVALIDITY degrades gracefully rather than raising.
    no_validity = {**base, "uidvalidity": None}
    assert _build_external_id(no_validity) == "imap-mail.example.com-INBOX-0-3"


# ---------------------------------------------------------------------------
# Production engine / timeout configuration
# ---------------------------------------------------------------------------


def test_engine_kwargs_apply_pool_only_for_postgres(monkeypatch):
    from src import database

    monkeypatch.setattr(settings, "DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/db")
    pg = database._engine_kwargs()
    assert pg["pool_pre_ping"] is True
    assert pg["pool_size"] == settings.DB_POOL_SIZE
    assert pg["pool_recycle"] == settings.DB_POOL_RECYCLE
    assert pg["connect_args"]["timeout"] == settings.DB_CONNECT_TIMEOUT
    assert "statement_timeout" in pg["connect_args"]["server_settings"]

    # SQLite (tests) must not receive queue-pool / asyncpg-only args.
    monkeypatch.setattr(settings, "DATABASE_URL", "sqlite+aiosqlite:///./x.db")
    lite = database._engine_kwargs()
    assert lite["pool_pre_ping"] is True
    assert "pool_size" not in lite
    assert "connect_args" not in lite


@pytest.mark.asyncio
async def test_imap_sync_returns_503_when_not_configured(client, auth_headers, monkeypatch):
    from src.imap import router as imap_router

    monkeypatch.setattr(imap_router.imap_settings, "IMAP_HOST", "")
    resp = await client.post("/api/v1/imap/sync", headers=auth_headers)
    assert resp.status_code == 503
