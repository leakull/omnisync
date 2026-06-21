import asyncio
import json
import uuid
from typing import AsyncGenerator
from unittest.mock import patch

import fakeredis.aioredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import types
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.auth.service import AuthService
from src.database import Base, get_db
from src.main import app

TEST_DATABASE_URL = "sqlite+aiosqlite:///./test.db"

test_engine = create_async_engine(TEST_DATABASE_URL, echo=False)
TestSessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)

fake_redis = fakeredis.aioredis.FakeRedis()


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
async def setup_db():
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID

    for table in Base.metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, PG_UUID):
                column.type = UUIDTypeDecorator()
            elif isinstance(column.type, JSONB):
                column.type = JSONTypeDecorator()

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


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
