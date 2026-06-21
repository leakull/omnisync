from datetime import datetime

from pydantic import BaseModel


class EventFilterParams(BaseModel):
    source: str | None = None
    author_id: str | None = None
    event_type: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    cursor: str | None = None
