from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class RawPayloadResponse(BaseModel):
    id: UUID
    source: str
    correlation_id: str
    received_at: datetime
    stored_in_s3: bool = False

    model_config = {"from_attributes": True}


class RawPayloadDetailResponse(RawPayloadResponse):
    content_hash: str | None = None
    payload: dict | None = None
