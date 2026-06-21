from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class EventResponse(BaseModel):
    id: UUID
    external_id: str
    source: str
    author_id: str
    author_name: str
    content: str
    event_type: str
    timestamp: datetime
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = {"from_attributes": True}


class NormalizedEventCreate(BaseModel):
    external_id: str
    source: str
    author_id: str
    author_name: str
    content: str
    event_type: str
    timestamp: datetime
    raw_payload_id: UUID | None = None


class EventListParams(BaseModel):
    source: str | None = None
    author_id: str | None = None
    event_type: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    offset: int = 0
    limit: int = 20


class EventVersionResponse(BaseModel):
    id: UUID
    event_id: UUID
    version: int
    content: str
    raw_payload_id: UUID | None
    changed_by: str | None
    changed_at: datetime

    model_config = {"from_attributes": True}
