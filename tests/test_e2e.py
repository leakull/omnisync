"""End-to-end journeys that exercise OmniSync the way it is actually used.

Unlike the focused unit/integration tests, these drive the running ASGI app
through its public HTTP API and follow complete real-world scenarios:

* an operator onboarding (register → authenticated calls),
* GitHub delivering webhooks (external ingestion → normalize → dedup → version
  → transactional outbox),
* a downstream AI agent consuming the incremental change feed (cursor polling
  and the SSE live stream),
* an operator triggering a manual poll-sync and searching the corpus,
* a background sync failing and being recovered through the DLQ API,
* unauthenticated access being rejected.

The whole lineage `raw_payloads → events → event_versions → outbox` is verified
end to end, against the same SQLite-backed app the other tests use.
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from src.events.models import NormalizedEvent


# ---------------------------------------------------------------------------
# Helpers — build realistic external payloads
# ---------------------------------------------------------------------------
def _push_webhook_body(sha: str, message: str, when: datetime, *, ref: str = "refs/heads/main"):
    """A GitHub `push` webhook payload carrying a single commit."""
    return {
        "ref": ref,
        "repository": {"full_name": "acme/widgets"},
        "sender": {"login": "octocat"},
        "commits": [
            {
                "sha": sha,
                "commit": {
                    "id": sha,
                    "message": message,
                    "author": {
                        "name": "Grace Hopper",
                        "email": "grace@acme.dev",
                        "date": when.isoformat(),
                    },
                },
                "html_url": f"https://github.com/acme/widgets/commit/{sha}",
            }
        ],
    }


async def _post_webhook(client: AsyncClient, body: dict, *, delivery_id: str, monkeypatch):
    # Default deployment has no shared secret, so signature verification is a
    # no-op — pin that here so the journey doesn't depend on the ambient env.
    from src.github import router as gh_router

    monkeypatch.setattr(gh_router.github_settings, "GITHUB_WEBHOOK_SECRET", "")
    return await client.post(
        "/api/v1/github/webhooks/github",
        content=json.dumps(body),
        headers={
            "X-GitHub-Event": "push",
            "X-GitHub-Delivery": delivery_id,
            "Content-Type": "application/json",
        },
    )


# ---------------------------------------------------------------------------
# Journey 1: GitHub webhook → events → versioning → agent change feed
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_github_webhook_to_agent_feed(client, db_session, monkeypatch):
    # --- An operator onboards through the public API ---
    reg = await client.post(
        "/api/v1/auth/register",
        json={"username": "operator", "password": "s3cret-pass"},
    )
    assert reg.status_code == 200
    operator = {"Authorization": f"Bearer {reg.json()['access_token']}"}

    # Nothing ingested yet.
    empty = await client.get("/api/v1/events", headers=operator)
    assert empty.status_code == 200
    assert empty.json()["items"] == []

    # --- An AI agent attaches to the change feed before anything happens ---
    feed0 = await client.get("/api/v1/agent/changes", headers=operator)
    assert feed0.status_code == 200
    assert feed0.json()["items"] == []
    cursor = feed0.json()["next_cursor"]  # None on an empty feed

    # --- GitHub delivers a push webhook (external ingestion) ---
    t0 = datetime.now(UTC) - timedelta(minutes=5)
    resp = await _post_webhook(
        client,
        _push_webhook_body("c0ffee01", "Fix race in upsert", t0),
        delivery_id="delivery-1",
        monkeypatch=monkeypatch,
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "events_created": 1}

    # --- Operator sees the normalized event via the REST API ---
    listing = await client.get("/api/v1/events?source=github", headers=operator)
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert len(items) == 1
    event = items[0]
    assert event["external_id"] == "c0ffee01"
    assert event["event_type"] == "commit"
    assert event["content"] == "Fix race in upsert"
    assert event["version"] == 1
    event_id = event["id"]

    # Detail + history (v1 only, so far).
    detail = await client.get(f"/api/v1/events/{event_id}", headers=operator)
    assert detail.status_code == 200
    assert detail.json()["author_name"] == "Grace Hopper"

    history = await client.get(f"/api/v1/events/{event_id}/history", headers=operator)
    assert history.status_code == 200
    assert [v["version"] for v in history.json()] == [1]

    # --- Raw payload is retained for traceability back to source data ---
    row = (
        await db_session.execute(
            select(NormalizedEvent).where(NormalizedEvent.external_id == "c0ffee01")
        )
    ).scalar_one()
    assert row.raw_payload_id is not None
    raw = await client.get(f"/api/v1/raw-payloads/{row.raw_payload_id}", headers=operator)
    assert raw.status_code == 200
    raw_body = raw.json()
    assert raw_body["source"] == "github_webhook"
    assert raw_body["payload"]["commits"][0]["sha"] == "c0ffee01"

    # --- Agent polls the feed and receives exactly the new change ---
    feed1 = await client.get("/api/v1/agent/changes", headers=operator)
    assert feed1.status_code == 200
    changes = feed1.json()["items"]
    assert len(changes) == 1
    assert changes[0]["aggregate_type"] == "normalized_event"
    assert changes[0]["payload"]["external_id"] == "c0ffee01"
    assert changes[0]["payload"]["version"] == 1
    cursor = feed1.json()["next_cursor"]
    assert cursor is not None

    # --- GitHub re-delivers the SAME commit with an edited message ---
    # Same (source, external_id) → idempotent upsert that bumps the version.
    t1 = datetime.now(UTC)
    resp2 = await _post_webhook(
        client,
        _push_webhook_body("c0ffee01", "Fix race in upsert (amended)", t1),
        delivery_id="delivery-2",
        monkeypatch=monkeypatch,
    )
    assert resp2.status_code == 200

    # Still a single event row (dedup by natural key), now at v2.
    listing2 = await client.get("/api/v1/events?source=github", headers=operator)
    assert len(listing2.json()["items"]) == 1
    assert listing2.json()["items"][0]["version"] == 2

    # Complete history is retained: [v1, v2].
    history2 = await client.get(f"/api/v1/events/{event_id}/history", headers=operator)
    assert [(v["version"], v["content"]) for v in history2.json()] == [
        (1, "Fix race in upsert"),
        (2, "Fix race in upsert (amended)"),
    ]

    # --- Agent resumes from its cursor and sees ONLY the new change ---
    feed2 = await client.get(f"/api/v1/agent/changes?cursor={cursor}", headers=operator)
    assert feed2.status_code == 200
    incr = feed2.json()["items"]
    assert len(incr) == 1
    assert incr[0]["payload"]["version"] == 2

    # --- Redelivery of an already-seen webhook is a no-op (idempotency) ---
    dup = await _post_webhook(
        client,
        _push_webhook_body("c0ffee01", "ignored body", t1),
        delivery_id="delivery-1",  # reused delivery id
        monkeypatch=monkeypatch,
    )
    assert dup.status_code == 200
    assert dup.json() == {"status": "ok", "duplicate": True}


# ---------------------------------------------------------------------------
# Journey 2: agent subscribes to the live SSE stream
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_agent_sse_stream_delivers_changes(client, monkeypatch):
    # The SSE endpoint streams forever, which httpx's ASGITransport can't consume
    # incrementally (it buffers the whole body). So we drive the real endpoint
    # generator directly: seed a change over HTTP, then read the StreamingResponse
    # the handler produces, faking a client disconnect after the first batch.
    import conftest

    import src.agent.router as agent_router

    monkeypatch.setattr(agent_router, "async_session", conftest.TestSessionLocal)
    # Bypass the per-IP rate limiter when invoking the handler outside a request.
    monkeypatch.setattr(agent_router.limiter, "enabled", False)

    resp = await _post_webhook(
        client,
        _push_webhook_body("feed5ee0", "Streamed commit", datetime.now(UTC)),
        delivery_id="stream-seed",
        monkeypatch=monkeypatch,
    )
    assert resp.status_code == 200

    class FakeRequest:
        """Disconnects after one poll pass so the generator terminates."""

        def __init__(self) -> None:
            self._passes = 0

        async def is_disconnected(self) -> bool:
            self._passes += 1
            return self._passes > 1

    stream = await agent_router.stream_changes(
        FakeRequest(), cursor=None, poll_interval=0.5, current_user=None
    )

    async def _collect() -> list[str]:
        chunks = []
        async for chunk in stream.body_iterator:
            chunks.append(chunk if isinstance(chunk, str) else chunk.decode())
        return chunks

    chunks = await asyncio.wait_for(_collect(), timeout=10)

    # Find the SSE `data:` frame and confirm it carries our seeded change.
    data_frames = [
        line[len("data:") :].strip()
        for blob in chunks
        for line in blob.splitlines()
        if line.startswith("data:")
    ]
    assert data_frames, "expected at least one SSE change frame"
    payloads = [json.loads(f) for f in data_frames]
    assert any(p["payload"]["external_id"] == "feed5ee0" for p in payloads)


# ---------------------------------------------------------------------------
# Journey 3: operator-triggered manual poll-sync + semantic search
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_manual_sync_then_search(client, auth_headers, monkeypatch):
    from src.github import router as gh_router
    from src.github.schemas import GitHubCommitData

    commit = GitHubCommitData.model_validate(
        {
            "sha": "deadbeef",
            "commit": {
                "id": "deadbeef",
                "message": "Add Qdrant-backed search",
                "author": {
                    "name": "Ada Lovelace",
                    "email": "ada@acme.dev",
                    "date": datetime.now(UTC).isoformat(),
                },
            },
        }
    )

    async def fake_commits(owner, repo, since=None):
        return [commit]

    async def fake_prs(owner, repo, state="all", since=None):
        return []

    monkeypatch.setattr(gh_router.github_client, "get_commits", fake_commits)
    monkeypatch.setattr(gh_router.github_client, "get_pull_requests", fake_prs)

    sync = await client.post("/api/v1/github/sync?owner=acme&repo=widgets", headers=auth_headers)
    assert sync.status_code == 200
    assert sync.json()["events_created"] == 1

    listing = await client.get("/api/v1/events?source=github", headers=auth_headers)
    contents = [i["content"] for i in listing.json()["items"]]
    assert "Add Qdrant-backed search" in contents

    # Qdrant is an external service; in a real deployment search hits it. Here we
    # stand in for the vector store so the API contract is still exercised.
    def fake_search(query, source=None, event_type=None, limit=10):
        return [
            {
                "event_id": "deadbeef",
                "source": "github",
                "event_type": "commit",
                "content": "Add Qdrant-backed search",
                "score": 0.92,
            }
        ]

    from src.search import router as search_router

    monkeypatch.setattr(search_router, "search_events", fake_search)

    found = await client.get("/api/v1/search/events?q=search", headers=auth_headers)
    assert found.status_code == 200
    body = found.json()
    assert body["query"] == "search"
    assert body["count"] == 1
    assert body["results"][0]["content"] == "Add Qdrant-backed search"


# ---------------------------------------------------------------------------
# Journey 4: a failed background sync is recovered through the DLQ API
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_dlq_failure_listing_and_replay(client, auth_headers, db_session, monkeypatch):
    from src.dlq import router as dlq_router
    from src.dlq.service import record_failure

    # A background sync blew up and was recorded to the dead-letter queue.
    failed_id = await record_failure(
        db_session,
        source="github",
        operation="src.github.tasks.sync_github_commits",
        payload={"trigger": "resync"},
        error_text="HTTP 502: upstream unavailable",
        correlation_id="corr-e2e",
    )
    await db_session.commit()

    # Operator inspects the DLQ via the API.
    listing = await client.get("/api/v1/dlq/failed-events?status=pending", headers=auth_headers)
    assert listing.status_code == 200
    entries = listing.json()
    entry = next(e for e in entries if e["id"] == str(failed_id))
    assert entry["source"] == "github"
    assert entry["operation"] == "src.github.tasks.sync_github_commits"
    assert "502" in entry["error_text"]

    # Operator manually replays it. The Celery broker is external, so capture
    # the dispatch instead of actually enqueueing.
    class _Result:
        id = "task-xyz"

    sent: list[str] = []

    def fake_send_task(name, *args, **kwargs):
        sent.append(name)
        return _Result()

    monkeypatch.setattr(dlq_router.celery_app, "send_task", fake_send_task)

    replay = await client.post(
        f"/api/v1/dlq/failed-events/{failed_id}/replay", headers=auth_headers
    )
    assert replay.status_code == 200
    assert replay.json()["status"] == "replayed"
    assert replay.json()["task_id"] == "task-xyz"
    assert sent == ["src.github.tasks.sync_github_commits"]

    # The entry is cleared from the active (pending) queue.
    after = await client.get("/api/v1/dlq/failed-events?status=pending", headers=auth_headers)
    assert all(e["id"] != str(failed_id) for e in after.json())

    # Replaying a non-existent entry is a clean 404.
    missing = await client.post(
        "/api/v1/dlq/failed-events/00000000-0000-0000-0000-000000000000/replay",
        headers=auth_headers,
    )
    assert missing.status_code == 404


# ---------------------------------------------------------------------------
# Journey 5: unauthenticated access is rejected; health probes are open
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e2e_auth_required_on_protected_endpoints(client):
    for path in (
        "/api/v1/events",
        "/api/v1/agent/changes",
        "/api/v1/dlq/failed-events",
        "/api/v1/search/events?q=anything",
    ):
        resp = await client.get(path)
        # 401/403 (rejected credential) or 422 (required bearer header absent) —
        # all mean an anonymous caller cannot read the data.
        assert resp.status_code in (401, 403, 422), f"{path} should require auth"

    # Liveness/readiness probes never require auth (orchestrator must reach them).
    live = await client.get("/health/live")
    assert live.status_code == 200
    assert live.json()["status"] == "ok"
