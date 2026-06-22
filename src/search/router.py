from fastapi import APIRouter, Depends, Query
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.database import get_db
from src.search.vector import search_events

router = APIRouter(prefix="/search", tags=["search"])


@router.get("/events")
async def search(
    q: str = Query(..., min_length=1, description="Search query"),
    source: str | None = Query(None, description="Filter by source"),
    event_type: str | None = Query(None, description="Filter by event type"),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # search_events is synchronous and CPU/IO-blocking (embedding + Qdrant call);
    # run it off the event loop so it doesn't stall other requests.
    results = await run_in_threadpool(
        search_events, query=q, source=source, event_type=event_type, limit=limit
    )
    return {"query": q, "results": results, "count": len(results)}
