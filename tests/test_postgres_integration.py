"""Integration tests against a real PostgreSQL instance.

These exercise PostgreSQL-specific behaviour that SQLite cannot reproduce:
the ``INSERT ... ON CONFLICT DO UPDATE`` upsert, JSONB columns, timezone-aware
timestamps and the unique-constraint driven idempotency/dedup.

Skipped automatically when Docker / testcontainers are unavailable.
"""

import os
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

testcontainers_postgres = pytest.importorskip(
    "testcontainers.postgres", reason="testcontainers not installed"
)

from src.events.schemas import NormalizedEventCreate  # noqa: E402
from src.events.service import NormalizedEventService  # noqa: E402
from src.raw_payloads.service import save_raw_payload  # noqa: E402


@pytest.fixture(scope="module")
def pg_container():
    try:
        container = testcontainers_postgres.PostgresContainer("postgres:16-alpine")
        container.start()
    except Exception as e:  # Docker not available in this environment
        pytest.skip(f"PostgreSQL container unavailable: {e}")
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def migrated_urls(pg_container):
    sync_url = pg_container.get_connection_url()  # postgresql+psycopg2://...
    async_url = sync_url.replace("postgresql+psycopg2", "postgresql+asyncpg")

    from alembic.config import Config

    from alembic import command

    os.environ["SYNC_DATABASE_URL"] = sync_url
    cfg = Config("alembic.ini")
    command.upgrade(cfg, "head")
    return sync_url, async_url


@pytest_asyncio.fixture
async def pg_session(migrated_urls) -> AsyncSession:
    _, async_url = migrated_urls
    engine = create_async_engine(async_url)
    maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with maker() as session:
        yield session
    await engine.dispose()


def _event(external_id: str, content: str) -> NormalizedEventCreate:
    return NormalizedEventCreate(
        external_id=external_id,
        source="github",
        author_id="a1",
        author_name="Author One",
        content=content,
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_upsert_is_idempotent(pg_session):
    e = _event("sha-1", "initial")

    first = await NormalizedEventService.upsert_event(pg_session, e)
    await pg_session.commit()
    second = await NormalizedEventService.upsert_event(pg_session, _event("sha-1", "initial"))
    await pg_session.commit()

    assert first.id == second.id
    assert second.version == 1  # unchanged content does not bump the version


@pytest.mark.asyncio
async def test_upsert_versions_on_change(pg_session):
    await NormalizedEventService.upsert_event(pg_session, _event("sha-2", "v1"))
    await pg_session.commit()
    updated = await NormalizedEventService.upsert_event(pg_session, _event("sha-2", "v2"))
    await pg_session.commit()

    assert updated.version == 2
    history = await NormalizedEventService.get_event_history(pg_session, updated.id)
    assert [h.version for h in history] == [1, 2]


@pytest.mark.asyncio
async def test_bulk_upsert_handles_duplicate_keys(pg_session):
    events = [_event("sha-3", "a"), _event("sha-3", "b"), _event("sha-4", "c")]
    results = await NormalizedEventService.upsert_events_bulk(pg_session, events)
    await pg_session.commit()
    # Duplicate (source, external_id) within the batch must not raise.
    assert len({r.external_id for r in results}) == 2


@pytest.mark.asyncio
async def test_raw_payload_dedup(pg_session):
    payload = {"hello": "world", "n": 1}
    id1 = await save_raw_payload(pg_session, "github_webhook", payload, "corr-1")
    await pg_session.commit()
    id2 = await save_raw_payload(pg_session, "github_webhook", dict(payload), "corr-2")
    await pg_session.commit()
    assert id1 == id2
