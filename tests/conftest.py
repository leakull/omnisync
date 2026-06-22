import asyncio
import os
from typing import AsyncGenerator
from unittest.mock import patch

# Use the deterministic, dependency-free embedding backend in tests so importing
# the app does not require the sentence-transformers model or a Qdrant server.
os.environ.setdefault("EMBEDDING_BACKEND", "fake")

import fakeredis.aioredis  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
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
    # Postgres integration tests run against a real database (own engine), so they
    # don't need the SQLite schema. The ORM column types are cross-dialect
    # (src/db_types.py), so no metadata mutation is required for either backend.
    if request.node.get_closest_marker("postgres"):
        yield
        return

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield
    finally:
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
