import json
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.logging_config import logger
from src.raw_payloads.models import RawPayload


async def save_raw_payload(
    session: AsyncSession,
    source: str,
    payload: dict,
    correlation_id: str,
) -> UUID:
    payload_size = len(json.dumps(payload, default=str).encode())
    storage_url = None

    if payload_size > settings.S3_PAYLOAD_THRESHOLD:
        try:
            from datetime import datetime, timezone

            from src.raw_payloads.storage import s3_storage

            key = f"{source}/{correlation_id}/{datetime.now(timezone.utc).isoformat()}.json"
            storage_url = await s3_storage.save_object(key, payload)
            payload = None
            logger.info("payload_stored_in_s3", source=source, size=payload_size, key=key)
        except Exception as e:
            logger.warning("s3_storage_failed_fallback_to_db", error=str(e), source=source)
            storage_url = None

    raw = RawPayload(
        source=source,
        payload=payload,
        correlation_id=correlation_id,
        storage_url=storage_url,
    )
    session.add(raw)
    await session.flush()
    logger.info(
        "raw_payload_saved",
        source=source,
        correlation_id=correlation_id,
        stored_in_s3=storage_url is not None,
    )
    return raw.id
