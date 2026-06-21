from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.logging_config import logger
from src.sync_logs.models import SyncLog


@asynccontextmanager
async def _get_session(
    session_or_factory: AsyncSession | async_sessionmaker,
) -> AsyncGenerator[tuple[AsyncSession, bool], None]:
    if isinstance(session_or_factory, AsyncSession):
        yield session_or_factory, False
    else:
        async with session_or_factory() as session:
            yield session, True


async def create_sync_log(
    session_or_factory: AsyncSession | async_sessionmaker,
    correlation_id: str,
    source: str,
    status: str = "started",
) -> UUID:
    async with _get_session(session_or_factory) as (session, owns_session):
        log = SyncLog(
            correlation_id=correlation_id,
            source=source,
            status=status,
        )
        session.add(log)
        await session.flush()
        if owns_session:
            await session.commit()
        logger.info("sync_log_created", correlation_id=correlation_id, source=source, status=status)
        return log.id


async def update_sync_log_status(
    session_or_factory: AsyncSession | async_sessionmaker,
    log_id: UUID,
    status: str,
    error_text: str | None = None,
) -> None:
    async with _get_session(session_or_factory) as (session, owns_session):
        await session.execute(
            update(SyncLog).where(SyncLog.id == log_id).values(status=status, error_text=error_text)
        )
        if owns_session:
            await session.commit()
        else:
            await session.flush()
        logger.info(
            "sync_log_updated",
            log_id=str(log_id),
            status=status,
            has_error=error_text is not None,
        )
