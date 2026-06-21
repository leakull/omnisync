import asyncio

from sqlalchemy.ext.asyncio import AsyncSession

from src.celery_app import celery_app
from src.config import settings
from src.database import async_session
from src.dlq.service import fetch_due_retries, mark_retry_scheduled
from src.logging_config import logger, set_correlation_id
from src.metrics import dlq_replays_total


@celery_app.task(name="src.dlq.tasks.retry_failed_events")
def retry_failed_events():
    """Beat-scheduled background retrier for the dead-letter queue.

    Scans for dead-lettered operations whose exponential-backoff window has
    elapsed and re-dispatches the original Celery task, advancing each entry's
    attempt counter / next-retry watermark. Entries that exhaust their attempt
    budget are retired to ``exhausted`` and left for manual inspection.
    """
    if not settings.DLQ_RETRY_ENABLED:
        return 0
    return asyncio.run(_retry_failed_events())


async def _retry_failed_events() -> int:
    set_correlation_id()
    async with async_session() as session:
        async with session.begin():
            return await process_due_retries(session)


async def process_due_retries(session: AsyncSession) -> int:
    """Re-dispatch every dead-letter entry whose backoff window has elapsed.

    Bookkeeping is committed regardless of whether the re-dispatched task
    ultimately succeeds — a fresh failure simply lands back in the queue and the
    backoff keeps growing until the attempt budget is exhausted.
    """
    dispatched = 0
    due = await fetch_due_retries(session)
    for failed in due:
        # The operation field stores the Celery task name; re-dispatch it.
        try:
            celery_app.send_task(failed.operation)
        except Exception as e:  # broker hiccup — leave the entry for next scan
            dlq_replays_total.labels(source=failed.source, result="dispatch_error").inc()
            logger.warning(
                "dlq_retry_dispatch_failed",
                failed_event_id=str(failed.id),
                operation=failed.operation,
                error=str(e),
            )
            continue

        await mark_retry_scheduled(session, failed)
        dispatched += 1
        result = "exhausted" if failed.status == "exhausted" else "dispatched"
        dlq_replays_total.labels(source=failed.source, result=result).inc()
        logger.info(
            "dlq_retry_dispatched",
            failed_event_id=str(failed.id),
            operation=failed.operation,
            replay_attempts=failed.replay_attempts,
            status=failed.status,
        )
    logger.info("dlq_retry_run_completed", dispatched=dispatched)
    return dispatched
