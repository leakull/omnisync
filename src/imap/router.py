from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.database import get_db
from src.events.service import NormalizedEventService
from src.imap.config import imap_settings
from src.imap.schemas import SendEmailRequest
from src.imap.service import IMAPConnector
from src.imap.smtp import SMTPNotConfiguredError, send_email
from src.logging_config import logger, set_correlation_id
from src.raw_payloads.service import save_raw_payload
from src.sync_logs.service import create_sync_log, update_sync_log_status

router = APIRouter(prefix="/imap", tags=["imap"])
limiter = Limiter(key_func=get_remote_address)


@router.post("/send")
@limiter.limit("20/minute")
async def send_mail(
    request: Request,
    payload: SendEmailRequest,
    current_user: User = Depends(get_current_user),
):
    try:
        await send_email(
            to=[str(addr) for addr in payload.to],
            subject=payload.subject,
            body=payload.body,
            from_addr=str(payload.from_addr) if payload.from_addr else None,
        )
    except SMTPNotConfiguredError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"status": "sent", "recipients": len(payload.to)}


@router.post("/sync")
@limiter.limit("5/minute")
async def imap_sync(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not imap_settings.IMAP_HOST:
        raise HTTPException(status_code=503, detail="IMAP not configured")

    correlation_id = set_correlation_id()
    log_id = await create_sync_log(db, correlation_id, "imap_poll")

    connector = IMAPConnector(
        host=imap_settings.IMAP_HOST,
        port=imap_settings.IMAP_PORT,
        username=imap_settings.IMAP_USERNAME,
        password=imap_settings.IMAP_PASSWORD,
        folder=imap_settings.IMAP_FOLDER,
        use_ssl=imap_settings.IMAP_USE_SSL,
    )

    try:
        raw_items = await connector.fetch()
        event_data_list = []
        for raw in raw_items:
            raw_payload_id = await save_raw_payload(db, "imap_poll", raw, correlation_id)
            event_data = connector.normalize(raw, raw_payload_id)
            if event_data:
                event_data_list.append(event_data)

        if event_data_list:
            results = await NormalizedEventService.upsert_events_bulk(
                db, event_data_list, changed_by=correlation_id
            )
        else:
            results = []

        await db.commit()
        await update_sync_log_status(db, log_id, "completed")
        logger.info(
            "imap_sync_completed",
            correlation_id=correlation_id,
            messages=len(raw_items),
            events_created=len(results),
        )
        return {
            "status": "ok",
            "messages": len(raw_items),
            "events_created": len(results),
        }

    except Exception as e:
        await db.rollback()
        await update_sync_log_status(db, log_id, "failed", str(e))
        logger.error("imap_sync_failed", correlation_id=correlation_id, error=str(e))
        raise
