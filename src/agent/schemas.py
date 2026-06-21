from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class ChangeItem(BaseModel):
    id: UUID
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: dict
    created_at: datetime

    model_config = {"from_attributes": True}


class ChangeFeedResponse(BaseModel):
    items: list[ChangeItem]
    next_cursor: str | None = None
    has_more: bool
