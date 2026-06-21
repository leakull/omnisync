from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.database import async_session
from src.dlq.models import FailedEvent
from src.logging_config import logger
from src.metrics import dlq_events_total

# Statuses from which an entry can still be (re)tried automatically.
ACTIVE_STATUSES = ("pending", "retrying")


def compute_backoff(attempts: int) -> timedelta:
    """Exponential backoff for the next automatic replay.

    ``attempts`` is the number of replays already performed, so the first
    retry waits ``DLQ_RETRY_BASE_DELAY`` and each subsequent one doubles,
    capped at ``DLQ_RETRY_MAX_DELAY``.
    """
    delay = settings.DLQ_RETRY_BASE_DELAY * (2**attempts)
    delay = min(delay, settings.DLQ_RETRY_MAX_DELAY)
    return timedelta(seconds=delay)


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
    """Persist (or refresh) a dead-letter entry for a failed operation.

    Repeated failures of the same ``(source, operation)`` collapse onto a
    single active row so the background retrier doesn't fan out into an
    ever-growing pile of duplicates — the attempt counter and backoff
    watermark carry across failures instead.
    """
    now = datetime.now(timezone.utc)

    existing = (
        (
            await session.execute(
                select(FailedEvent)
                .where(
                    FailedEvent.source == source,
                    FailedEvent.operation == operation,
                    FailedEvent.status.in_(ACTIVE_STATUSES),
                )
                .order_by(FailedEvent.created_at.desc())
            )
        )
        .scalars()
        .first()
    )

    if existing is not None:
        existing.error_text = error_text[:2000]
        existing.payload = payload
        if correlation_id:
            existing.correlation_id = correlation_id
        # Only seed the backoff watermark if nothing scheduled it yet, so an
        # in-flight retry cadence is preserved.
        if existing.next_retry_at is None:
            existing.next_retry_at = now + compute_backoff(existing.replay_attempts)
        await session.flush()
        dlq_events_total.labels(source=source, operation=operation).inc()
        logger.info(
            "dlq_event_refreshed",
            failed_event_id=str(existing.id),
            source=source,
            operation=operation,
            replay_attempts=existing.replay_attempts,
        )
        return existing.id

    failed = FailedEvent(
        source=source,
        operation=operation,
        correlation_id=correlation_id,
        payload=payload,
        error_text=error_text[:2000],
        next_retry_at=now + compute_backoff(0),
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


async def fetch_due_retries(
    session: AsyncSession, now: datetime | None = None, limit: int | None = None
) -> list[FailedEvent]:
    """Active dead-letter entries whose backoff window has elapsed.

    Locked ``FOR UPDATE SKIP LOCKED`` so multiple workers can drain the queue
    concurrently without re-dispatching the same entry.
    """
    now = now or datetime.now(timezone.utc)
    limit = limit or settings.DLQ_RETRY_BATCH_SIZE
    result = await session.execute(
        select(FailedEvent)
        .where(
            FailedEvent.status.in_(ACTIVE_STATUSES),
            FailedEvent.replay_attempts < settings.DLQ_MAX_REPLAY_ATTEMPTS,
            or_(FailedEvent.next_retry_at.is_(None), FailedEvent.next_retry_at <= now),
        )
        .order_by(FailedEvent.next_retry_at.asc().nullsfirst())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(result.scalars().all())


async def mark_retry_scheduled(session: AsyncSession, failed: FailedEvent) -> None:
    """Account for an automatic replay: bump the attempt counter and push the
    next-retry watermark out by the new backoff (or retire the entry once the
    attempt budget is spent)."""
    now = datetime.now(timezone.utc)
    failed.replay_attempts += 1
    failed.last_attempt_at = now
    if failed.replay_attempts >= settings.DLQ_MAX_REPLAY_ATTEMPTS:
        failed.status = "exhausted"
        failed.next_retry_at = None
        logger.error(
            "dlq_event_exhausted",
            failed_event_id=str(failed.id),
            source=failed.source,
            operation=failed.operation,
            replay_attempts=failed.replay_attempts,
        )
    else:
        failed.status = "retrying"
        failed.next_retry_at = now + compute_backoff(failed.replay_attempts)


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
    failed.next_retry_at = None
    failed.resolved_at = datetime.now(timezone.utc)


async def mark_replay_failed(session: AsyncSession, failed: FailedEvent, error: str) -> None:
    failed.replay_attempts += 1
    failed.error_text = error[:2000]
    failed.status = "failed"
