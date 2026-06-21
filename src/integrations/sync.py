import time
from datetime import datetime

from src.database import async_session
from src.dlq.service import record_failure_standalone
from src.events.service import NormalizedEventService
from src.integrations.registry import get_connector
from src.logging_config import get_correlation_id, logger, set_correlation_id
from src.metrics import events_synced_total, sync_duration_seconds
from src.raw_payloads.service import save_raw_payload
from src.sync_logs.service import create_sync_log, update_sync_log_status
from src.sync_state.service import get_cursor, set_cursor


async def run_connector_sync(source: str, operation: str | None = None) -> int:
    """Generic incremental sync driver for registry connectors whose default
    construction is valid (filestore, jira, ...).

    Reads the persisted ``since`` watermark, fetches new items, stores raw
    payloads, upserts normalized events and advances the watermark.
    """
    operation = operation or f"{source}_poll"
    correlation_id = set_correlation_id()
    start_time = time.monotonic()
    log_id = await create_sync_log(async_session, correlation_id, operation)

    async with async_session() as session:
        try:
            connector = get_connector(source)

            cursor = await get_cursor(session, source)
            since = datetime.fromisoformat(cursor) if cursor else None

            raw_items = await connector.fetch(since=since)

            event_data_list = []
            max_ts: datetime | None = None
            for raw in raw_items:
                raw_payload_id = await save_raw_payload(session, operation, raw, correlation_id)
                event_data = connector.normalize(raw, raw_payload_id)
                if event_data:
                    event_data_list.append(event_data)
                    if max_ts is None or event_data.timestamp > max_ts:
                        max_ts = event_data.timestamp

            results = []
            if event_data_list:
                results = await NormalizedEventService.upsert_events_bulk(
                    session, event_data_list, changed_by=correlation_id
                )

            if max_ts is not None:
                await set_cursor(session, source, max_ts.isoformat())

            await session.commit()
            await update_sync_log_status(async_session, log_id, "completed")
            events_synced_total.labels(source=source, status="success").inc(len(results))
            logger.info(
                "connector_sync_completed",
                source=source,
                correlation_id=correlation_id,
                fetched=len(raw_items),
                events_created=len(results),
            )
            return len(results)
        except Exception as e:
            await session.rollback()
            await update_sync_log_status(async_session, log_id, "failed", str(e))
            events_synced_total.labels(source=source, status="error").inc()
            logger.error("connector_sync_failed", source=source, error=str(e))
            await record_failure_standalone(
                source=source,
                operation=operation,
                payload={"trigger": "resync"},
                error_text=str(e),
                correlation_id=get_correlation_id() or None,
            )
            raise
        finally:
            sync_duration_seconds.labels(source=source).observe(time.monotonic() - start_time)
