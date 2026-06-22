from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.events.models import NormalizedEvent
from src.logging_config import logger
from src.outbox.models import OutboxEvent


async def enqueue_event(session: AsyncSession, event: NormalizedEvent) -> None:
    """Append an outbox row for a normalized event. Must run in the same
    transaction as the event upsert so publication can never diverge from
    the committed state.
    """
    session.add(
        OutboxEvent(
            aggregate_type="normalized_event",
            aggregate_id=str(event.id),
            event_type=event.event_type,
            payload={
                "event_id": str(event.id),
                "source": event.source,
                "external_id": event.external_id,
                "event_type": event.event_type,
                "content": event.content,
                "version": event.version,
                "schema_version": event.schema_version,
            },
        )
    )


async def fetch_pending(session: AsyncSession, limit: int | None = None) -> list[OutboxEvent]:
    limit = limit or settings.OUTBOX_BATCH_SIZE
    result = await session.execute(
        select(OutboxEvent)
        .where(OutboxEvent.status == "pending")
        .order_by(OutboxEvent.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return list(result.scalars().all())


async def fetch_changes(
    session: AsyncSession,
    after: tuple[datetime, UUID] | None = None,
    limit: int = 100,
) -> list[OutboxEvent]:
    """Read the append-only change feed ordered by (created_at, id).

    Powers the agent-facing polling/streaming API: downstream consumers
    (Pulse modules, AI agents) advance a cursor instead of re-reading events.
    """
    query = select(OutboxEvent).order_by(OutboxEvent.created_at.asc(), OutboxEvent.id.asc())
    if after is not None:
        after_ts, after_id = after
        query = query.where(
            or_(
                OutboxEvent.created_at > after_ts,
                and_(OutboxEvent.created_at == after_ts, OutboxEvent.id > after_id),
            )
        )
    result = await session.execute(query.limit(limit))
    return list(result.scalars().all())


async def mark_published(session: AsyncSession, outbox: OutboxEvent) -> None:
    outbox.status = "published"
    outbox.published_at = datetime.now(UTC)
    outbox.attempts += 1


async def mark_failed(session: AsyncSession, outbox: OutboxEvent, error: str) -> None:
    outbox.attempts += 1
    outbox.last_error = error[:1000]
    if outbox.attempts >= settings.OUTBOX_MAX_ATTEMPTS:
        outbox.status = "dead"
        logger.error(
            "outbox_event_dead",
            outbox_id=str(outbox.id),
            aggregate_id=outbox.aggregate_id,
            attempts=outbox.attempts,
        )
