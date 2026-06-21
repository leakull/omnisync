from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.celery_app import celery_app
from src.database import get_db
from src.dlq.schemas import FailedEventResponse, ReplayResponse
from src.dlq.service import get_failed, list_failed, mark_resolved
from src.logging_config import logger

router = APIRouter(prefix="/dlq", tags=["dlq"])


@router.get("/failed-events", response_model=list[FailedEventResponse])
async def list_failed_events(
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return await list_failed(db, status=status, limit=limit)


@router.post("/failed-events/{failed_event_id}/replay", response_model=ReplayResponse)
async def replay_failed_event(
    failed_event_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    failed = await get_failed(db, failed_event_id)
    if failed is None:
        raise HTTPException(status_code=404, detail="Failed event not found")

    # The operation field stores the Celery task name; re-dispatch it.
    result = celery_app.send_task(failed.operation)
    failed.replay_attempts += 1
    await mark_resolved(db, failed)
    await db.commit()
    logger.info(
        "dlq_event_replayed",
        failed_event_id=str(failed_event_id),
        operation=failed.operation,
        task_id=result.id,
    )
    return ReplayResponse(status="replayed", failed_event_id=failed_event_id, task_id=result.id)
