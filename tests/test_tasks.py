"""Coverage for Celery task bodies.

The task entrypoints wrap their work in ``asyncio.run`` (which can't be called
from the already-running test loop), so the async ``_sync_*`` coroutines are
awaited directly with module-level ``async_session`` repointed at the test DB
and the external clients mocked. The thin synchronous wrappers (Jira/filestore)
and the DLQ-decision helper are exercised on their own.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from src.events.models import NormalizedEvent
from src.events.schemas import NormalizedEventCreate
from src.events.service import NormalizedEventService
from src.github.schemas import GitHubCommitData, GitHubPRData


def _patch_session(monkeypatch, module):
    import conftest

    monkeypatch.setattr(module, "async_session", conftest.TestSessionLocal)


async def _events(source):
    import conftest

    async with conftest.TestSessionLocal() as session:
        rows = (
            (await session.execute(select(NormalizedEvent).where(NormalizedEvent.source == source)))
            .scalars()
            .all()
        )
        return list(rows)


# ---------------------------------------------------------------------------
# Outbox publisher
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_publish_one_raises_when_index_fails(monkeypatch):
    from src.outbox import tasks as outbox_tasks
    from src.outbox.models import OutboxEvent

    ob = OutboxEvent(
        aggregate_type="normalized_event",
        aggregate_id="x",
        event_type="commit",
        payload={"event_id": "x", "content": "c", "source": "github", "event_type": "commit"},
    )
    monkeypatch.setattr(outbox_tasks, "index_event", lambda **k: True)
    outbox_tasks._publish_one(ob)  # no raise

    monkeypatch.setattr(outbox_tasks, "index_event", lambda **k: False)
    with pytest.raises(RuntimeError, match="vector index publication failed"):
        outbox_tasks._publish_one(ob)


@pytest.mark.asyncio
async def test_publish_outbox_marks_published(monkeypatch):
    import conftest

    from src.outbox import tasks as outbox_tasks
    from src.outbox.models import OutboxEvent

    _patch_session(monkeypatch, outbox_tasks)
    monkeypatch.setattr(outbox_tasks, "index_event", lambda **k: True)

    async with conftest.TestSessionLocal() as session:
        await NormalizedEventService.upsert_event(session, _event("o-1", "hello"))
        await session.commit()

    published = await outbox_tasks._publish_outbox()
    assert published == 1

    async with conftest.TestSessionLocal() as session:
        ob = (await session.execute(select(OutboxEvent))).scalars().one()
        assert ob.status == "published"
        assert ob.published_at is not None


@pytest.mark.asyncio
async def test_publish_outbox_marks_failed(monkeypatch):
    import conftest

    from src.outbox import tasks as outbox_tasks
    from src.outbox.models import OutboxEvent

    _patch_session(monkeypatch, outbox_tasks)
    monkeypatch.setattr(outbox_tasks, "index_event", lambda **k: False)

    async with conftest.TestSessionLocal() as session:
        await NormalizedEventService.upsert_event(session, _event("o-2", "hello"))
        await session.commit()

    published = await outbox_tasks._publish_outbox()
    assert published == 0

    async with conftest.TestSessionLocal() as session:
        ob = (await session.execute(select(OutboxEvent))).scalars().one()
        assert ob.status == "pending"  # below max attempts
        assert ob.attempts == 1
        assert ob.last_error


def _event(external_id: str, content: str) -> NormalizedEventCreate:
    return NormalizedEventCreate(
        external_id=external_id,
        source="github",
        author_id="u1",
        author_name="U1",
        content=content,
        event_type="commit",
        timestamp=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# GitHub sync tasks
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sync_github_commits_happy(monkeypatch):
    from src.github import tasks as gh_tasks

    monkeypatch.setenv("GITHUB_SYNC_OWNER", "acme")
    monkeypatch.setenv("GITHUB_SYNC_REPO", "widgets")
    _patch_session(monkeypatch, gh_tasks)

    commit = GitHubCommitFixture.commit("c1", "Fix bug")

    async def fake_get_commits(owner, repo, since=None):
        return [commit]

    monkeypatch.setattr(gh_tasks.github_client, "get_commits", fake_get_commits)

    await gh_tasks._sync_github_commits()
    events = await _events("github")
    assert [e.external_id for e in events] == ["c1"]
    assert events[0].event_type == "commit"


@pytest.mark.asyncio
async def test_sync_github_commits_empty(monkeypatch):
    from src.github import tasks as gh_tasks

    monkeypatch.setenv("GITHUB_SYNC_OWNER", "acme")
    monkeypatch.setenv("GITHUB_SYNC_REPO", "widgets")
    _patch_session(monkeypatch, gh_tasks)

    async def fake_get_commits(owner, repo, since=None):
        return []

    monkeypatch.setattr(gh_tasks.github_client, "get_commits", fake_get_commits)

    await gh_tasks._sync_github_commits()
    assert await _events("github") == []


@pytest.mark.asyncio
async def test_sync_github_commits_skips_without_config(monkeypatch):
    from src.github import tasks as gh_tasks

    monkeypatch.delenv("GITHUB_SYNC_OWNER", raising=False)
    monkeypatch.delenv("GITHUB_SYNC_REPO", raising=False)

    called = []
    monkeypatch.setattr(
        gh_tasks.github_client,
        "get_commits",
        lambda *a, **k: called.append(1),
    )
    await gh_tasks._sync_github_commits()  # returns early, no client call
    assert called == []


@pytest.mark.asyncio
async def test_sync_github_pull_requests_happy(monkeypatch):
    from src.github import tasks as gh_tasks

    monkeypatch.setenv("GITHUB_SYNC_OWNER", "acme")
    monkeypatch.setenv("GITHUB_SYNC_REPO", "widgets")
    _patch_session(monkeypatch, gh_tasks)

    pr = GitHubCommitFixture.pr(99, 3, "Add docs")

    async def fake_get_prs(owner, repo, since=None):
        return [pr]

    monkeypatch.setattr(gh_tasks.github_client, "get_pull_requests", fake_get_prs)

    await gh_tasks._sync_github_pull_requests()
    events = await _events("github")
    assert [e.external_id for e in events] == ["99"]
    assert events[0].event_type == "pull_request"


def test_to_dlq_if_done_records_non_retryable(monkeypatch):
    from src.github import tasks as gh_tasks

    recorded = {}

    async def fake_record(**kwargs):
        recorded.update(kwargs)

    monkeypatch.setattr(gh_tasks, "record_failure_standalone", fake_record)

    fake_self = _FakeTask(retries=0, max_retries=3)
    gh_tasks._to_dlq_if_done(fake_self, "op", "github", ValueError("nope"))
    assert recorded["source"] == "github"
    assert recorded["operation"] == "op"


def test_to_dlq_if_done_skips_retryable_not_exhausted(monkeypatch):
    import httpx

    from src.github import tasks as gh_tasks

    calls = []

    async def fake_record(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(gh_tasks, "record_failure_standalone", fake_record)

    fake_self = _FakeTask(retries=0, max_retries=3)
    gh_tasks._to_dlq_if_done(fake_self, "op", "github", httpx.ConnectError("down"))
    assert calls == []  # retryable + retries remain → let Celery retry


def test_to_dlq_if_done_records_retryable_when_exhausted(monkeypatch):
    import httpx

    from src.github import tasks as gh_tasks

    calls = []

    async def fake_record(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(gh_tasks, "record_failure_standalone", fake_record)

    fake_self = _FakeTask(retries=3, max_retries=3)
    gh_tasks._to_dlq_if_done(fake_self, "op", "github", httpx.ConnectError("down"))
    assert len(calls) == 1


class _FakeTask:
    def __init__(self, retries, max_retries):
        self.request = type("R", (), {"retries": retries})()
        self.max_retries = max_retries


# ---------------------------------------------------------------------------
# Telegram sync task
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sync_telegram_messages_happy_advances_cursor(monkeypatch):
    import conftest

    from src.sync_state.service import get_cursor
    from src.telegram import tasks as tg_tasks
    from src.telegram.schemas import TelegramChat, TelegramMessage, TelegramUpdate, TelegramUser

    _patch_session(monkeypatch, tg_tasks)

    updates = [
        TelegramUpdate(
            update_id=500,
            message=TelegramMessage(
                message_id=3,
                from_user=TelegramUser(id=1, first_name="Ada"),
                chat=TelegramChat(id=9, type="private"),
                date=int(datetime(2026, 6, 21, tzinfo=UTC).timestamp()),
                text="hi",
            ),
        )
    ]

    async def fake_get_updates(offset=None):
        return updates

    monkeypatch.setattr(tg_tasks.telegram_client, "get_updates", fake_get_updates)

    await tg_tasks._sync_telegram_messages()

    events = await _events("telegram")
    assert [e.external_id for e in events] == ["3"]

    async with conftest.TestSessionLocal() as session:
        cursor = await get_cursor(session, "telegram")
    assert cursor == "501"  # max_update_id + 1


# ---------------------------------------------------------------------------
# IMAP sync task
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_sync_imap_messages_skips_when_unconfigured(monkeypatch):
    from src.imap import tasks as imap_tasks
    from src.imap.config import imap_settings

    monkeypatch.setattr(imap_settings, "IMAP_HOST", "")
    await imap_tasks._sync_imap_messages()  # early return, no DB work
    assert await _events("imap") == []


@pytest.mark.asyncio
async def test_sync_imap_messages_happy_advances_cursor(monkeypatch):
    import conftest

    from src.imap import tasks as imap_tasks
    from src.imap.config import imap_settings
    from src.imap.service import IMAPConnector
    from src.sync_state.service import get_cursor

    monkeypatch.setattr(imap_settings, "IMAP_HOST", "imap.acme.dev")
    monkeypatch.setattr(imap_settings, "IMAP_USERNAME", "bot")
    monkeypatch.setattr(imap_settings, "IMAP_PASSWORD", "pw")
    _patch_session(monkeypatch, imap_tasks)

    msg_date = datetime(2026, 6, 22, 8, 0, tzinfo=UTC)

    async def fake_fetch(self, since=None):
        return [
            {
                "uid": "1",
                "uidvalidity": "9",
                "message_id": "<m@x>",
                "host": "imap.acme.dev",
                "subject": "Hi",
                "sender": "a@x.dev",
                "date": msg_date,
                "body": "body",
                "folder": "INBOX",
            }
        ]

    monkeypatch.setattr(IMAPConnector, "fetch", fake_fetch)

    await imap_tasks._sync_imap_messages()
    events = await _events("imap")
    assert [e.external_id for e in events] == ["imap-mid-<m@x>"]

    async with conftest.TestSessionLocal() as session:
        cursor = await get_cursor(session, "imap")
    assert datetime.fromisoformat(cursor) == msg_date


# ---------------------------------------------------------------------------
# Jira / filestore synchronous wrappers
# ---------------------------------------------------------------------------
def test_sync_jira_skips_without_base_url(monkeypatch):
    from src.jira import tasks as jira_tasks

    monkeypatch.setattr(jira_tasks.jira_settings, "JIRA_BASE_URL", "")
    assert jira_tasks.sync_jira() == 0


def test_sync_jira_dispatches(monkeypatch):
    from src.jira import tasks as jira_tasks

    monkeypatch.setattr(jira_tasks.jira_settings, "JIRA_BASE_URL", "https://x.atlassian.net")

    async def fake_run(source):
        assert source == "jira"
        return 4

    monkeypatch.setattr(jira_tasks, "run_connector_sync", fake_run)
    assert jira_tasks.sync_jira() == 4


def test_sync_filestore_skips_without_bucket(monkeypatch):
    from src.filestore import tasks as fs_tasks

    monkeypatch.setattr(fs_tasks.filestore_settings, "FILESTORE_BUCKET", "")
    assert fs_tasks.sync_filestore() == 0


def test_sync_filestore_dispatches(monkeypatch):
    from src.filestore import tasks as fs_tasks

    monkeypatch.setattr(fs_tasks.filestore_settings, "FILESTORE_BUCKET", "bucket")

    async def fake_run(source):
        assert source == "filestore"
        return 2

    monkeypatch.setattr(fs_tasks, "run_connector_sync", fake_run)
    assert fs_tasks.sync_filestore() == 2


# ---------------------------------------------------------------------------
# Raw payload retention cleanup
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cleanup_old_payloads_deletes_expired(monkeypatch):
    import conftest

    from src.config import settings
    from src.raw_payloads import tasks as rp_tasks
    from src.raw_payloads.models import RawPayload

    _patch_session(monkeypatch, rp_tasks)

    ttl = settings.RAW_PAYLOAD_TTL_DAYS
    old_at = datetime.now(UTC) - timedelta(days=ttl + 5)
    fresh_at = datetime.now(UTC)

    async with conftest.TestSessionLocal() as session:
        session.add(
            RawPayload(
                source="github",
                payload={},
                content_hash="h1",
                correlation_id="c",
                received_at=old_at,
            )
        )
        session.add(
            RawPayload(
                source="github",
                payload={},
                content_hash="h2",
                correlation_id="c",
                received_at=fresh_at,
            )
        )
        await session.commit()

    await rp_tasks._cleanup_old_payloads()

    async with conftest.TestSessionLocal() as session:
        remaining = (await session.execute(select(RawPayload.content_hash))).scalars().all()
    assert set(remaining) == {"h2"}


# ---------------------------------------------------------------------------
# Fixtures for GitHub schema objects
# ---------------------------------------------------------------------------
class GitHubCommitFixture:
    @staticmethod
    def commit(sha, message):
        return GitHubCommitData.model_validate(
            {
                "sha": sha,
                "commit": {
                    "id": sha,
                    "message": message,
                    "author": {
                        "name": "Ada",
                        "email": "ada@x.dev",
                        "date": "2026-06-20T10:00:00Z",
                    },
                },
            }
        )

    @staticmethod
    def pr(pr_id, number, title):
        return GitHubPRData.model_validate(
            {
                "id": pr_id,
                "number": number,
                "title": title,
                "state": "open",
                "user": {"login": "octocat", "id": 1},
                "created_at": "2026-06-19T09:00:00Z",
            }
        )
