from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import async_session
from src.dlq.models import FailedEvent
from src.logging_config import logger
from src.metrics import dlq_events_total


async def record_failure_standalone(
    source: str,
    operation: str,
    payload: dict,
    error_text: str,
    correlation_id: str | None = None,
) -> None:
    """Record a DLQ entry using a dedicated committed session.

    Used from Celery tasks whose own session has already been rolled back.
    """
    async with async_session() as session:
        await record_failure(session, source, operation, payload, error_text, correlation_id)
        await session.commit()


async def record_failure(
    session: AsyncSession,
    source: str,
    operation: str,
    payload: dict,
    error_text: str,
    correlation_id: str | None = None,
) -> UUID:
    failed = FailedEvent(
        source=source,
        operation=operation,
        correlation_id=correlation_id,
        payload=payload,
        error_text=error_text[:2000],
    )
    session.add(failed)
    await session.flush()
    dlq_events_total.labels(source=source, operation=operation).inc()
    logger.error(
        "dlq_event_recorded",
        failed_event_id=str(failed.id),
        source=source,
        operation=operation,
        correlation_id=correlation_id,
    )
    return failed.id


async def list_failed(
    session: AsyncSession, status: str | None = None, limit: int = 100
) -> list[FailedEvent]:
    query = select(FailedEvent).order_by(FailedEvent.created_at.desc()).limit(limit)
    if status:
        query = query.where(FailedEvent.status == status)
    result = await session.execute(query)
    return list(result.scalars().all())


async def get_failed(session: AsyncSession, failed_event_id: UUID) -> FailedEvent | None:
    result = await session.execute(select(FailedEvent).where(FailedEvent.id == failed_event_id))
    return result.scalar_one_or_none()


async def mark_resolved(session: AsyncSession, failed: FailedEvent) -> None:
    failed.status = "resolved"
    failed.resolved_at = datetime.now(timezone.utc)


async def mark_replay_failed(session: AsyncSession, failed: FailedEvent, error: str) -> None:
    failed.replay_attempts += 1
    failed.error_text = error[:2000]
    failed.status = "failed"
