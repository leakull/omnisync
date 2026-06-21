from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.database import get_db
from src.events.dependencies import EventFilterParams
from src.events.schemas import EventResponse, EventVersionResponse
from src.events.service import NormalizedEventService
from src.pagination import PaginatedResponse

router = APIRouter(prefix="/events", tags=["events"])


@router.get(
    "",
    response_model=PaginatedResponse,
    responses={
        401: {"description": "Not authenticated"},
    },
)
async def list_events(
    source: str | None = Query(None),
    author_id: str | None = Query(None),
    event_type: str | None = Query(None),
    date_from: datetime | None = Query(None),
    date_to: datetime | None = Query(None),
    cursor: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    filters = EventFilterParams(
        source=source,
        author_id=author_id,
        event_type=event_type,
        date_from=date_from,
        date_to=date_to,
        cursor=cursor,
    )
    return await NormalizedEventService.list_events(db, filters, limit)


@router.get(
    "/{event_id}",
    response_model=EventResponse,
    responses={
        401: {"description": "Not authenticated"},
        404: {"description": "Event not found"},
    },
)
async def get_event(
    event_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return await NormalizedEventService.get_event(db, event_id)


@router.get(
    "/{event_id}/history",
    response_model=list[EventVersionResponse],
    responses={
        401: {"description": "Not authenticated"},
        404: {"description": "Event not found"},
    },
)
async def get_event_history(
    event_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return await NormalizedEventService.get_event_history(db, event_id)
