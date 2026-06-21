from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class FailedEventResponse(BaseModel):
    id: UUID
    source: str
    operation: str
    correlation_id: str | None
    error_text: str
    status: str
    replay_attempts: int
    created_at: datetime
    last_attempt_at: datetime | None = None
    next_retry_at: datetime | None = None
    resolved_at: datetime | None

    model_config = {"from_attributes": True}


class ReplayResponse(BaseModel):
    status: str
    failed_event_id: UUID
    task_id: str | None = None
