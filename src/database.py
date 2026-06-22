from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from src.config import settings


def _engine_kwargs() -> dict[str, Any]:
    """Production-grade engine options for PostgreSQL/asyncpg.

    SQLite (used in tests) doesn't support a queue pool or asyncpg connect args,
    so those are only applied to real Postgres URLs.
    """
    kwargs: dict[str, Any] = {"echo": False, "pool_pre_ping": True}
    if settings.DATABASE_URL.startswith("postgresql"):
        connect_args: dict[str, Any] = {"timeout": settings.DB_CONNECT_TIMEOUT}
        if settings.DB_STATEMENT_TIMEOUT_MS > 0:
            # Server-side guard: a runaway query is cancelled instead of pinning
            # a worker/connection forever.
            connect_args["server_settings"] = {
                "statement_timeout": str(settings.DB_STATEMENT_TIMEOUT_MS)
            }
        kwargs.update(
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_timeout=settings.DB_POOL_TIMEOUT,
            pool_recycle=settings.DB_POOL_RECYCLE,
            connect_args=connect_args,
        )
    return kwargs


engine = create_async_engine(settings.DATABASE_URL, **_engine_kwargs())
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with async_session() as session:
        yield session
