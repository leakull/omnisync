from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.database import get_db
from src.raw_payloads.schemas import RawPayloadDetailResponse
from src.raw_payloads.service import get_raw_payload, load_payload_content

router = APIRouter(prefix="/raw-payloads", tags=["raw-payloads"])


@router.get("/{payload_id}", response_model=RawPayloadDetailResponse)
async def read_raw_payload(
    payload_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    raw = await get_raw_payload(db, payload_id)
    if raw is None:
        raise HTTPException(status_code=404, detail="Raw payload not found")

    payload = await load_payload_content(raw)
    return RawPayloadDetailResponse(
        id=raw.id,
        source=raw.source,
        correlation_id=raw.correlation_id,
        received_at=raw.received_at,
        stored_in_s3=raw.storage_url is not None,
        content_hash=raw.content_hash,
        payload=payload,
    )
