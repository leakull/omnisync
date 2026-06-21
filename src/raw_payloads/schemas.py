from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class RawPayloadResponse(BaseModel):
    id: UUID
    source: str
    correlation_id: str
    received_at: datetime

    model_config = {"from_attributes": True}
