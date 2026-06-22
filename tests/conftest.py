import asyncio
import json
import os
import uuid
from typing import AsyncGenerator
from unittest.mock import patch

# Use the deterministic, dependency-free embedding backend in tests so importing
# the app does not require the sentence-transformers model or a Qdrant server.
os.environ.setdefault("EMBEDDING_BACKEND", "fake")

import fakeredis.aioredis  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import types  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

import src.models  # noqa: F401,E402  (registers all ORM models on Base.metadata)
from src.auth.service import AuthService  # noqa: E402
from src.database import Base, get_db  # noqa: E402
from src.main import app  # noqa: E402

TEST_DATABASE_URL = "sqlite+aiosqlite:///./test.db"

test_engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestSessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

fake_redis = fakeredis.aioredis.FakeRedis()

# Snapshot the pristine (native PostgreSQL) column types right after the models
# are imported, before any test swaps them for SQLite-compatible shims. This is
# the source of truth used to fully restore the shared Base.metadata, so the
# real-Postgres integration suite always sees native UUID/JSONB types.
_PRISTINE_COLUMN_TYPES = {
    column: column.type for table in Base.metadata.tables.values() for column in table.columns
}


def _restore_pristine_column_types() -> None:
    for column, original in _PRISTINE_COLUMN_TYPES.items():
        column.type = original


class UUIDTypeDecorator(types.TypeDecorator):
    impl = types.String
    cache_ok = True

    def __init__(self):
        super().__init__(length=36)

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return str(value)
        return str(uuid.UUID(value))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return uuid.UUID(value)


class JSONTypeDecorator(types.TypeDecorator):
    impl = types.Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return json.loads(value)


async def override_get_db():
    async with TestSessionLocal() as session:
        yield session


async def override_get_redis():
    return fake_redis


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True)
def mock_redis():
    async def _get_redis():
        return fake_redis

    with patch("src.auth.service.get_redis", _get_redis):
        yield


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(autouse=True)
async def setup_db(request):
    # Postgres integration tests run against a real database with native UUID/
    # JSONB columns — they must NOT get the SQLite-compatible type swap. Force the
    # shared Base.metadata back to its pristine PG types so their ORM binds match,
    # regardless of any swap a previous SQLite test may have left behind.
    if request.node.get_closest_marker("postgres"):
        _restore_pristine_column_types()
        yield
        return

    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID

    # Swap PG-specific column types for SQLite-friendly decorators so the schema
    # can be created on SQLite; restored from the pristine snapshot afterwards so
    # the mutation never leaks into later tests (e.g. the Postgres suite).
    for table in Base.metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, PG_UUID):
                column.type = UUIDTypeDecorator()
            elif isinstance(column.type, JSONB):
                column.type = JSONTypeDecorator()

    try:
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield
    finally:
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        _restore_pristine_column_types()


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    async with TestSessionLocal() as session:
        yield session


@pytest_asyncio.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def auth_token(db_session: AsyncSession) -> str:
    from src.auth.schemas import UserCreate

    user = await AuthService.create_user(
        db_session,
        UserCreate(username="testuser", password="testpass123"),
    )
    return AuthService.create_access_token(user.username)


@pytest_asyncio.fixture
async def auth_headers(auth_token: str) -> dict:
    return {"Authorization": f"Bearer {auth_token}"}
