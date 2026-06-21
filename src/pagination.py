import base64
import json
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class PaginatedResponse(BaseModel):
    items: list[Any]
    has_more: bool
    next_cursor: str | None = None
    limit: int


def encode_cursor(timestamp: datetime, event_id: UUID) -> str:
    data = {"ts": timestamp.isoformat(), "id": str(event_id)}
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    data = json.loads(base64.urlsafe_b64decode(cursor.encode()))
    return datetime.fromisoformat(data["ts"]), UUID(data["id"])
