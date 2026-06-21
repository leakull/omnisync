import asyncio
import time

import httpx

from src.celery_app import celery_app
from src.database import async_session
from src.events.service import NormalizedEventService
from src.logging_config import logger, set_correlation_id
from src.metrics import events_synced_total, sync_duration_seconds
from src.raw_payloads.service import save_raw_payload
from src.sync_logs.service import create_sync_log, update_sync_log_status
from src.telegram.service import telegram_client
from src.telegram.utils import parse_message_to_event

RETRYABLE_ERRORS = (httpx.ConnectError, httpx.TimeoutException, ConnectionError)


@celery_app.task(
    name="src.telegram.tasks.sync_telegram_messages",
    autoretry_for=RETRYABLE_ERRORS,
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 3},
)
def sync_telegram_messages():
    asyncio.run(_sync_telegram_messages())


async def _sync_telegram_messages():
    correlation_id = set_correlation_id()
    start_time = time.monotonic()
    log_id = await create_sync_log(async_session, correlation_id, "telegram_poll")
    async with async_session() as session:
        try:
            updates = await telegram_client.get_updates()

            event_data_list = []
            for update in updates:
                if not update.message:
                    continue
                raw_payload_id = await save_raw_payload(
                    session, "telegram_poll", update.model_dump(), correlation_id
                )
                event_data = parse_message_to_event(update.message, str(raw_payload_id))
                if event_data:
                    event_data_list.append(event_data)

            if event_data_list:
                results = await NormalizedEventService.upsert_events_bulk(
                    session, event_data_list, changed_by=correlation_id
                )
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                events_synced_total.labels(source="telegram", status="success").inc(len(results))
                logger.info(
                    "telegram_sync_completed",
                    correlation_id=correlation_id,
                    updates=len(updates),
                    events_created=len(results),
                )
            else:
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                logger.info(
                    "telegram_sync_completed",
                    correlation_id=correlation_id,
                    updates=len(updates),
                    events_created=0,
                )
        except Exception as e:
            await session.rollback()
            await update_sync_log_status(async_session, log_id, "failed", str(e))
            events_synced_total.labels(source="telegram", status="error").inc()
            logger.error("telegram_sync_failed", correlation_id=correlation_id, error=str(e))
            raise
        finally:
            sync_duration_seconds.labels(source="telegram").observe(time.monotonic() - start_time)
