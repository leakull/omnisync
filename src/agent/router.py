import asyncio

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.ext.asyncio import AsyncSession

from src.agent.schemas import ChangeFeedResponse, ChangeItem
from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.database import async_session, get_db
from src.outbox.service import fetch_changes
from src.pagination import decode_cursor, encode_cursor

router = APIRouter(prefix="/agent", tags=["agent"])
limiter = Limiter(key_func=get_remote_address)


@router.get(
    "/changes",
    response_model=ChangeFeedResponse,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "items": [
                            {
                                "id": "8f2b1c2e-0e3a-4f1a-9b6d-1a2b3c4d5e6f",
                                "aggregate_type": "normalized_event",
                                "aggregate_id": "3c1e...",
                                "event_type": "commit",
                                "payload": {
                                    "event_id": "3c1e...",
                                    "source": "github",
                                    "external_id": "abc123",
                                    "event_type": "commit",
                                    "content": "Fix race in upsert",
                                    "version": 2,
                                    "schema_version": 1,
                                },
                                "created_at": "2026-06-21T12:00:00Z",
                            }
                        ],
                        "next_cursor": "MjAyNi0wNi0yMVQxMjowMDowMHw4ZjJi...",
                        "has_more": False,
                    }
                }
            }
        }
    },
)
@limiter.limit("120/minute")
async def poll_changes(
    request: Request,
    cursor: str | None = Query(None, description="Opaque cursor from a previous response"),
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Incremental change feed for downstream consumers (Pulse / AI agents).

    Poll with the returned ``next_cursor`` to receive only new events. Each item
    carries ``schema_version`` so consumers can branch on the content contract.
    """
    after = decode_cursor(cursor) if cursor else None
    rows = await fetch_changes(db, after=after, limit=limit + 1)
    has_more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = encode_cursor(rows[-1].created_at, rows[-1].id) if rows else cursor
    return ChangeFeedResponse(
        items=[ChangeItem.model_validate(r) for r in rows],
        next_cursor=next_cursor,
        has_more=has_more,
    )


@router.get("/changes/stream")
@limiter.limit("30/minute")
async def stream_changes(
    request: Request,
    cursor: str | None = Query(None),
    poll_interval: float = Query(2.0, ge=0.5, le=30.0),
    current_user: User = Depends(get_current_user),
):
    """Server-Sent Events stream of the change feed. Each event is a JSON
    ``ChangeItem``; clients reconnect with the last ``id`` they received.
    """

    async def event_generator():
        after = decode_cursor(cursor) if cursor else None
        while True:
            if await request.is_disconnected():
                break
            async with async_session() as session:
                rows = await fetch_changes(session, after=after, limit=100)
            for row in rows:
                item = ChangeItem.model_validate(row)
                after = (row.created_at, row.id)
                yield f"id: {row.id}\nevent: change\ndata: {item.model_dump_json()}\n\n"
            if not rows:
                # keepalive comment to hold the connection open
                yield ": keepalive\n\n"
            await asyncio.sleep(poll_interval)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
