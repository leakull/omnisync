import asyncio
import time
from datetime import datetime

from src.celery_app import celery_app
from src.database import async_session
from src.dlq.service import record_failure_standalone
from src.events.service import NormalizedEventService
from src.imap.service import IMAPConnector
from src.logging_config import get_correlation_id, logger, set_correlation_id
from src.metrics import events_synced_total, sync_duration_seconds
from src.raw_payloads.service import save_raw_payload
from src.sync_logs.service import create_sync_log, update_sync_log_status
from src.sync_state.service import get_cursor, set_cursor


@celery_app.task(
    bind=True,
    name="src.imap.tasks.sync_imap_messages",
)
def sync_imap_messages(self):
    try:
        asyncio.run(_sync_imap_messages())
    except Exception as e:
        asyncio.run(
            record_failure_standalone(
                source="imap",
                operation=self.name,
                payload={"trigger": "resync"},
                error_text=str(e),
                correlation_id=get_correlation_id() or None,
            )
        )
        raise


async def _sync_imap_messages():
    from src.imap.config import imap_settings

    if not imap_settings.IMAP_HOST:
        logger.warning("imap_sync_skipped", reason="IMAP_HOST not set")
        return

    correlation_id = set_correlation_id()
    start_time = time.monotonic()
    log_id = await create_sync_log(async_session, correlation_id, "imap_poll")

    connector = IMAPConnector(
        host=imap_settings.IMAP_HOST,
        port=imap_settings.IMAP_PORT,
        username=imap_settings.IMAP_USERNAME,
        password=imap_settings.IMAP_PASSWORD,
        folder=imap_settings.IMAP_FOLDER,
        use_ssl=imap_settings.IMAP_USE_SSL,
    )

    async with async_session() as session:
        try:
            cursor = await get_cursor(session, "imap")
            since = datetime.fromisoformat(cursor) if cursor else None
            raw_items = await connector.fetch(since=since)
            event_data_list = []
            max_date = None
            for raw in raw_items:
                raw_date = raw.get("date")
                if isinstance(raw_date, datetime) and (max_date is None or raw_date > max_date):
                    max_date = raw_date
                raw_payload_id = await save_raw_payload(session, "imap_poll", raw, correlation_id)
                event_data = connector.normalize(raw, raw_payload_id)
                if event_data:
                    event_data_list.append(event_data)

            if max_date is not None:
                await set_cursor(session, "imap", max_date.isoformat())

            if event_data_list:
                results = await NormalizedEventService.upsert_events_bulk(
                    session, event_data_list, changed_by=correlation_id
                )
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                events_synced_total.labels(source="imap", status="success").inc(len(results))
                logger.info(
                    "imap_sync_completed",
                    correlation_id=correlation_id,
                    messages=len(raw_items),
                    events_created=len(results),
                )
            else:
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                logger.info(
                    "imap_sync_completed",
                    correlation_id=correlation_id,
                    messages=len(raw_items),
                    events_created=0,
                )
        except Exception as e:
            await session.rollback()
            await update_sync_log_status(async_session, log_id, "failed", str(e))
            events_synced_total.labels(source="imap", status="error").inc()
            logger.error("imap_sync_failed", correlation_id=correlation_id, error=str(e))
            raise
        finally:
            sync_duration_seconds.labels(source="imap").observe(time.monotonic() - start_time)
