from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class SyncLogResponse(BaseModel):
    id: UUID
    correlation_id: str
    source: str
    status: str
    error_text: str | None
    created_at: datetime

    model_config = {"from_attributes": True}
