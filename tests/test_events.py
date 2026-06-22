from datetime import datetime, timezone

import pytest
from httpx import AsyncClient

from src.events.schemas import NormalizedEventCreate
from src.events.service import NormalizedEventService


@pytest.mark.asyncio
async def test_register_and_login(client: AsyncClient):
    response = await client.post(
        "/api/v1/auth/register",
        json={"username": "newuser", "password": "pass123"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"

    response = await client.post(
        "/api/v1/auth/login",
        json={"username": "newuser", "password": "pass123"},
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data


@pytest.mark.asyncio
async def test_login_wrong_password(client: AsyncClient):
    await client.post(
        "/api/v1/auth/register",
        json={"username": "user2", "password": "pass123"},
    )
    response = await client.post(
        "/api/v1/auth/login",
        json={"username": "user2", "password": "wrongpass"},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_list_events_requires_auth(client: AsyncClient):
    response = await client.get("/api/v1/events")
    assert response.status_code == 422 or response.status_code == 401


@pytest.mark.asyncio
async def test_list_events_empty(client: AsyncClient, auth_headers: dict):
    response = await client.get("/api/v1/events", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["items"] == []
    assert data["has_more"] is False


@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "database" in data


@pytest.mark.asyncio
async def test_upsert_creates_v1(db_session, auth_headers: dict):
    event_data = NormalizedEventCreate(
        external_id="evt-001",
        source="github",
        author_id="user1",
        author_name="User One",
        content="Initial commit",
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )
    event = await NormalizedEventService.upsert_event(db_session, event_data)
    await db_session.commit()

    assert event.version == 1

    history = await NormalizedEventService.get_event_history(db_session, event.id)
    assert len(history) == 1
    assert history[0].version == 1
    assert history[0].content == "Initial commit"


@pytest.mark.asyncio
async def test_upsert_on_change_increments_version(db_session, auth_headers: dict):
    event_data = NormalizedEventCreate(
        external_id="evt-002",
        source="github",
        author_id="user1",
        author_name="User One",
        content="Original content",
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )
    event = await NormalizedEventService.upsert_event(db_session, event_data)
    await db_session.commit()

    updated_data = NormalizedEventCreate(
        external_id="evt-002",
        source="github",
        author_id="user1",
        author_name="User One",
        content="Updated content",
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )
    updated_event = await NormalizedEventService.upsert_event(db_session, updated_data)
    await db_session.commit()

    assert updated_event.version == 2
    assert updated_event.content == "Updated content"

    # event_versions is a complete history: the original v1 and the new v2.
    history = await NormalizedEventService.get_event_history(db_session, event.id)
    assert [(h.version, h.content) for h in history] == [
        (1, "Original content"),
        (2, "Updated content"),
    ]

    latest = await NormalizedEventService.get_event(db_session, event.id)
    assert latest.version == 2
    assert latest.content == "Updated content"


@pytest.mark.asyncio
async def test_upsert_no_change_no_new_version(db_session, auth_headers: dict):
    event_data = NormalizedEventCreate(
        external_id="evt-003",
        source="github",
        author_id="user1",
        author_name="User One",
        content="Same content",
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )
    event = await NormalizedEventService.upsert_event(db_session, event_data)
    await db_session.commit()

    same_data = NormalizedEventCreate(
        external_id="evt-003",
        source="github",
        author_id="user1",
        author_name="User One",
        content="Same content",
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )
    same_event = await NormalizedEventService.upsert_event(db_session, same_data)
    await db_session.commit()

    assert same_event.version == 1

    history = await NormalizedEventService.get_event_history(db_session, event.id)
    assert len(history) == 1
    assert history[0].version == 1


@pytest.mark.asyncio
async def test_event_history_endpoint(client: AsyncClient, auth_headers: dict, db_session):
    event_data = NormalizedEventCreate(
        external_id="evt-004",
        source="github",
        author_id="user1",
        author_name="User One",
        content="First version",
        event_type="commit",
        timestamp=datetime.now(timezone.utc),
    )
    event = await NormalizedEventService.upsert_event(db_session, event_data)
    await db_session.commit()

    response = await client.get(
        f"/api/v1/events/{event.id}/history",
        headers=auth_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["version"] == 1
    assert data[0]["content"] == "First version"


@pytest.mark.asyncio
async def test_upsert_bulk_dedup(db_session, auth_headers: dict):
    events = [
        NormalizedEventCreate(
            external_id="bulk-001",
            source="github",
            author_id="user1",
            author_name="User One",
            content="Bulk event 1",
            event_type="commit",
            timestamp=datetime.now(timezone.utc),
        ),
        NormalizedEventCreate(
            external_id="bulk-002",
            source="github",
            author_id="user1",
            author_name="User One",
            content="Bulk event 2",
            event_type="commit",
            timestamp=datetime.now(timezone.utc),
        ),
        NormalizedEventCreate(
            external_id="bulk-001",
            source="github",
            author_id="user1",
            author_name="User One",
            content="Bulk event 1 duplicate",
            event_type="commit",
            timestamp=datetime.now(timezone.utc),
        ),
    ]
    results = await NormalizedEventService.upsert_events_bulk(db_session, events)
    await db_session.commit()

    assert len(results) == 2

    from sqlalchemy import func, select

    from src.events.models import NormalizedEvent

    count = (
        await db_session.execute(
            select(func.count(NormalizedEvent.id)).where(NormalizedEvent.external_id == "bulk-001")
        )
    ).scalar()
    assert count == 1


@pytest.mark.asyncio
async def test_cursor_pagination(client: AsyncClient, auth_headers: dict, db_session):
    for i in range(5):
        event_data = NormalizedEventCreate(
            external_id=f"page-{i:03d}",
            source="github",
            author_id="user1",
            author_name="User One",
            content=f"Event {i}",
            event_type="commit",
            timestamp=datetime.now(timezone.utc),
        )
        await NormalizedEventService.upsert_event(db_session, event_data)
    await db_session.commit()

    response = await client.get("/api/v1/events?limit=2", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert data["next_cursor"] is not None

    response2 = await client.get(
        f"/api/v1/events?limit=2&cursor={data['next_cursor']}",
        headers=auth_headers,
    )
    assert response2.status_code == 200
    data2 = response2.json()
    assert len(data2["items"]) == 2

    ids1 = {item["id"] for item in data["items"]}
    ids2 = {item["id"] for item in data2["items"]}
    assert ids1.isdisjoint(ids2)


@pytest.mark.asyncio
async def test_refresh_token(client: AsyncClient):
    response = await client.post(
        "/api/v1/auth/register",
        json={"username": "refreshuser", "password": "pass123"},
    )
    assert response.status_code == 200
    refresh_token = response.json()["refresh_token"]

    response = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_refresh_with_access_token_fails(client: AsyncClient):
    response = await client.post(
        "/api/v1/auth/register",
        json={"username": "wrongtokenuser", "password": "pass123"},
    )
    access_token = response.json()["access_token"]

    response = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": access_token},
    )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_logout_revokes_refresh_token(client: AsyncClient):
    response = await client.post(
        "/api/v1/auth/register",
        json={"username": "logoutuser", "password": "pass123"},
    )
    refresh_token = response.json()["refresh_token"]

    response = await client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": refresh_token},
    )
    assert response.status_code == 200

    response = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert response.status_code == 401
