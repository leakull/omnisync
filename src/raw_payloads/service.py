import hashlib
import json
from datetime import UTC
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.logging_config import logger
from src.metrics import s3_storage_failures_total
from src.raw_payloads.models import RawPayload


def _canonical_hash(payload: dict) -> str:
    serialized = json.dumps(payload, default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


async def save_raw_payload(
    session: AsyncSession,
    source: str,
    payload: dict,
    correlation_id: str,
) -> UUID:
    content_hash = _canonical_hash(payload)

    # Deduplicate identical payloads from the same source (idempotent ingestion).
    existing = await session.execute(
        select(RawPayload.id).where(
            RawPayload.source == source,
            RawPayload.content_hash == content_hash,
        )
    )
    existing_id = existing.scalars().first()
    if existing_id is not None:
        logger.info("raw_payload_deduplicated", source=source, content_hash=content_hash)
        return existing_id

    # Coerce to a JSON-safe structure (datetime, etc. → str) before it reaches
    # the JSON/JSONB column. The column serializer has no ``default=`` hook, so a
    # raw payload carrying datetimes (IMAP ``date``, file-store ``last_modified``)
    # would otherwise fail to persist. This mirrors the hash computed above.
    payload = json.loads(json.dumps(payload, default=str))

    payload_size = len(json.dumps(payload).encode())
    storage_url = None
    stored_payload: dict | None = payload

    if payload_size > settings.S3_PAYLOAD_THRESHOLD:
        try:
            from datetime import datetime

            from src.raw_payloads.storage import s3_storage

            key = f"{source}/{correlation_id}/{datetime.now(UTC).isoformat()}.json"
            storage_url = await s3_storage.save_object(key, payload)
            stored_payload = None
            logger.info("payload_stored_in_s3", source=source, size=payload_size, key=key)
        except Exception as e:
            s3_storage_failures_total.labels(operation="put").inc()
            logger.warning("s3_storage_failed_fallback_to_db", error=str(e), source=source)
            storage_url = None
            stored_payload = payload

    raw = RawPayload(
        source=source,
        payload=stored_payload,
        content_hash=content_hash,
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


async def get_raw_payload(session: AsyncSession, payload_id: UUID) -> RawPayload | None:
    result = await session.execute(select(RawPayload).where(RawPayload.id == payload_id))
    return result.scalar_one_or_none()


async def load_payload_content(raw: RawPayload) -> dict | None:
    """Return the actual payload, transparently fetching from S3/MinIO when the
    body was offloaded there. Completes the traceability chain back to source data.
    """
    if raw.storage_url:
        try:
            from src.raw_payloads.storage import s3_storage

            return await s3_storage.get_object(raw.storage_url)
        except Exception as e:
            s3_storage_failures_total.labels(operation="get").inc()
            logger.error("s3_payload_fetch_failed", payload_id=str(raw.id), error=str(e))
            return None
    return raw.payload
