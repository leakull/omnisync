import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete

from src.celery_app import celery_app
from src.config import settings
from src.database import async_session
from src.logging_config import logger
from src.raw_payloads.models import RawPayload


@celery_app.task(
    name="src.raw_payloads.tasks.cleanup_old_payloads",
)
def cleanup_old_payloads():
    asyncio.run(_cleanup_old_payloads())


async def _cleanup_old_payloads():
    cutoff = datetime.now(timezone.utc) - timedelta(days=settings.RAW_PAYLOAD_TTL_DAYS)
    async with async_session() as session:
        result = await session.execute(
            delete(RawPayload).where(RawPayload.received_at < cutoff)
        )
        await session.commit()
        deleted = result.rowcount
        logger.info(
            "raw_payloads_cleanup_completed",
            deleted=deleted,
            cutoff_days=settings.RAW_PAYLOAD_TTL_DAYS,
        )
