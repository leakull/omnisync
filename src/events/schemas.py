from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

# Version of the normalized-event *content* contract. Bump when the shape or
# semantics of a connector's normalized output changes so downstream consumers
# (Pulse modules, AI agents) can branch on `schema_version` and migrate safely.
CONTENT_SCHEMA_VERSION = 1


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
    schema_version: int

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
    schema_version: int = CONTENT_SCHEMA_VERSION


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
    schema_version: int
    raw_payload_id: UUID | None
    changed_by: str | None
    changed_at: datetime

    model_config = {"from_attributes": True}
