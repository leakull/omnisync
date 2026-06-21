import sys
import types
from datetime import datetime, timezone

import pytest

from src.config import settings
from src.events.schemas import NormalizedEventCreate
from src.events.service import NormalizedEventService


# ---------------------------------------------------------------------------
# embeddings
# ---------------------------------------------------------------------------
def test_fake_embedding_is_deterministic_and_normalized():
    from src.search.embeddings import embed_text, get_dimension

    v1 = embed_text("hello world")
    v2 = embed_text("hello world")
    assert v1 == v2  # deterministic
    assert len(v1) == get_dimension()
    norm = sum(x * x for x in v1) ** 0.5
    assert abs(norm - 1.0) < 1e-6  # L2-normalized


def test_fake_embedding_differs_for_different_text():
    from src.search.embeddings import embed_text

    assert embed_text("alpha") != embed_text("beta")


def test_embed_texts_empty():
    from src.search.embeddings import embed_texts

    assert embed_texts([]) == []


# ---------------------------------------------------------------------------
# DLQ
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_dlq_record_and_resolve(db_session):
    from src.dlq.service import get_failed, list_failed, mark_resolved, record_failure

    failed_id = await record_failure(
        db_session,
        source="github",
        operation="src.github.tasks.sync_github_commits",
        payload={"trigger": "resync"},
        error_text="boom",
        correlation_id="abc123",
    )
    await db_session.commit()

    items = await list_failed(db_session, status="pending")
    assert any(i.id == failed_id for i in items)

    failed = await get_failed(db_session, failed_id)
    assert failed is not None
    await mark_resolved(db_session, failed)
    await db_session.commit()

    failed = await get_failed(db_session, failed_id)
    assert failed.status == "resolved"
    assert failed.resolved_at is not None


# ---------------------------------------------------------------------------
# Outbox
# ---------------------------------------------------------------------------
def _event(external_id: str, content: str) -> NormalizedEventCreate:
    return NormalizedEventCreate(
        external_id=external_id,
        source="telegram",
        author_id="u1",
        author_name="U1",
        content=content,
        event_type="message",
        timestamp=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_upsert_enqueues_outbox_change(db_session):
    from src.outbox.service import fetch_changes

    await NormalizedEventService.upsert_event(db_session, _event("m-1", "hi"))
    await db_session.commit()

    changes = await fetch_changes(db_session)
    assert len(changes) == 1
    assert changes[0].aggregate_type == "normalized_event"
    assert changes[0].payload["external_id"] == "m-1"


@pytest.mark.asyncio
async def test_fetch_changes_cursor_pagination(db_session):
    from src.outbox.service import fetch_changes

    for i in range(3):
        await NormalizedEventService.upsert_event(db_session, _event(f"c-{i}", f"v{i}"))
    await db_session.commit()

    first = await fetch_changes(db_session, limit=2)
    assert len(first) == 2
    after = (first[-1].created_at, first[-1].id)
    rest = await fetch_changes(db_session, after=after, limit=10)
    seen = {c.id for c in first} | {c.id for c in rest}
    assert len(seen) == 3  # no overlap, full coverage


@pytest.mark.asyncio
async def test_outbox_mark_failed_goes_dead(db_session):
    from src.outbox.models import OutboxEvent
    from src.outbox.service import mark_failed

    ob = OutboxEvent(
        aggregate_type="normalized_event",
        aggregate_id="x",
        event_type="message",
        payload={},
        attempts=settings.OUTBOX_MAX_ATTEMPTS - 1,
    )
    db_session.add(ob)
    await db_session.flush()

    await mark_failed(db_session, ob, "still failing")
    assert ob.status == "dead"


@pytest.mark.asyncio
async def test_outbox_mark_published(db_session):
    from src.outbox.models import OutboxEvent
    from src.outbox.service import mark_published

    ob = OutboxEvent(
        aggregate_type="normalized_event",
        aggregate_id="y",
        event_type="message",
        payload={},
    )
    db_session.add(ob)
    await db_session.flush()

    await mark_published(db_session, ob)
    assert ob.status == "published"
    assert ob.published_at is not None


# ---------------------------------------------------------------------------
# SMTP
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_smtp_not_configured_raises():
    from src.imap.smtp import SMTPNotConfiguredError, send_email

    original = settings.SMTP_HOST
    settings.SMTP_HOST = ""
    try:
        with pytest.raises(SMTPNotConfiguredError):
            await send_email(["to@example.com"], "subj", "body")
    finally:
        settings.SMTP_HOST = original


@pytest.mark.asyncio
async def test_smtp_sends_with_fake_backend(monkeypatch):
    from src.imap import smtp

    sent = {}

    async def fake_send(message, **kwargs):
        sent["message"] = message
        sent["kwargs"] = kwargs

    fake_module = types.ModuleType("aiosmtplib")
    fake_module.send = fake_send
    monkeypatch.setitem(sys.modules, "aiosmtplib", fake_module)

    monkeypatch.setattr(settings, "SMTP_HOST", "smtp.example.com")
    monkeypatch.setattr(settings, "SMTP_FROM", "from@example.com")

    await smtp.send_email(["to@example.com"], "Hello", "Body text")

    assert sent["kwargs"]["hostname"] == "smtp.example.com"
    assert sent["message"]["To"] == "to@example.com"
    assert sent["message"]["Subject"] == "Hello"


# ---------------------------------------------------------------------------
# Connector registry (file store + task tracker)
# ---------------------------------------------------------------------------
def test_filestore_and_jira_connectors_registered():
    import src.filestore.service  # noqa: F401
    import src.jira.service  # noqa: F401
    from src.integrations.registry import CONNECTORS

    assert "filestore" in CONNECTORS
    assert "jira" in CONNECTORS


# ---------------------------------------------------------------------------
# Agent-facing change feed API
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_agent_changes_endpoint(client, auth_headers):
    resp = await client.get("/api/v1/agent/changes", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert "items" in body
    assert "has_more" in body
