import asyncio

from src.celery_app import celery_app
from src.database import async_session
from src.logging_config import logger, set_correlation_id
from src.metrics import outbox_published_total
from src.outbox.models import OutboxEvent
from src.outbox.service import fetch_pending, mark_failed, mark_published
from src.search.vector import index_event


@celery_app.task(name="src.outbox.tasks.publish_outbox")
def publish_outbox():
    return asyncio.run(_publish_outbox())


def _publish_one(outbox: OutboxEvent) -> None:
    """Deliver a single outbox event to downstream consumers.

    Currently fans out to the vector index. Additional consumers (Pulse
    modules, AI agents) can be added here or subscribe to the same outbox.
    """
    payload = outbox.payload
    indexed = index_event(
        event_id=payload["event_id"],
        content=payload.get("content", ""),
        source=payload.get("source", ""),
        event_type=payload.get("event_type", ""),
    )
    if not indexed:
        raise RuntimeError("vector index publication failed")


async def _publish_outbox() -> int:
    set_correlation_id()
    published = 0
    async with async_session() as session, session.begin():
        pending = await fetch_pending(session)
        for outbox in pending:
            try:
                _publish_one(outbox)
                await mark_published(session, outbox)
                outbox_published_total.labels(status="published").inc()
                published += 1
            except Exception as e:
                await mark_failed(session, outbox, str(e))
                outbox_published_total.labels(status="failed").inc()
                logger.warning("outbox_publish_failed", outbox_id=str(outbox.id), error=str(e))
    logger.info("outbox_publish_run_completed", published=published)
    return published
