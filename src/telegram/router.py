import hmac

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select
from sqlalchemy import update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.database import get_db
from src.events.service import NormalizedEventService
from src.logging_config import logger, set_correlation_id
from src.raw_payloads.service import save_raw_payload
from src.sync_logs.service import create_sync_log, update_sync_log_status
from src.telegram.config import telegram_settings
from src.telegram.service import telegram_client
from src.telegram.utils import parse_message_to_event
from src.webhooks.models import WebhookDelivery

router = APIRouter(prefix="/telegram", tags=["telegram"])
limiter = Limiter(key_func=get_remote_address)


@router.post(
    "/sync",
    responses={
        401: {"description": "Not authenticated"},
        502: {"description": "Telegram API error"},
    },
)
@limiter.limit("10/minute")
async def telegram_sync(
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    correlation_id = set_correlation_id()
    log_id = await create_sync_log(db, correlation_id, "telegram_poll")

    try:
        updates = await telegram_client.get_updates()

        events_created = 0
        for update in updates:
            if not update.message:
                continue

            raw_payload_id = await save_raw_payload(
                db, "telegram_poll", update.model_dump(), correlation_id
            )
            event_data = parse_message_to_event(update.message, str(raw_payload_id))
            if event_data:
                await NormalizedEventService.upsert_event(db, event_data)
                events_created += 1

        await db.commit()
        await update_sync_log_status(db, log_id, "completed")
        logger.info(
            "telegram_sync_completed",
            correlation_id=correlation_id,
            updates=len(updates),
            events_created=events_created,
        )
        return {
            "status": "ok",
            "updates": len(updates),
            "events_created": events_created,
        }

    except Exception as e:
        await db.rollback()
        await update_sync_log_status(db, log_id, "failed", str(e))
        logger.error("telegram_sync_failed", correlation_id=correlation_id, error=str(e))
        raise


@router.post(
    "/webhook",
    responses={
        401: {"description": "Missing secret token"},
        403: {"description": "Invalid secret token"},
    },
)
async def telegram_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
    x_telegram_bot_api_secret_token: str | None = Header(None),
):
    secret = telegram_settings.TELEGRAM_WEBHOOK_SECRET
    if secret:
        if not x_telegram_bot_api_secret_token:
            raise HTTPException(status_code=401, detail="Missing secret token")
        if not hmac.compare_digest(x_telegram_bot_api_secret_token, secret):
            raise HTTPException(status_code=403, detail="Invalid secret token")

    body = await request.json()

    update_id = body.get("update_id")
    if update_id:
        delivery_id = str(update_id)
        existing = (
            await db.execute(
                select(WebhookDelivery).where(
                    WebhookDelivery.source == "telegram",
                    WebhookDelivery.delivery_id == delivery_id,
                )
            )
        ).scalar_one_or_none()
        if existing:
            logger.info("telegram_webhook_duplicate", delivery_id=delivery_id)
            return {"status": "ok", "duplicate": True}

        db.add(WebhookDelivery(source="telegram", delivery_id=delivery_id, status="processing"))
        await db.flush()

    correlation_id = set_correlation_id()
    log_id = await create_sync_log(db, correlation_id, "telegram_webhook")

    try:
        from src.telegram.schemas import TelegramUpdate

        update = TelegramUpdate.model_validate(body)

        if not update.message:
            await db.commit()
            await update_sync_log_status(db, log_id, "completed")
            if update_id:
                await db.execute(
                    sql_update(WebhookDelivery)
                    .where(
                        WebhookDelivery.source == "telegram",
                        WebhookDelivery.delivery_id == str(update_id),
                    )
                    .values(status="completed")
                )
            return {"status": "ok", "events_created": 0}

        raw_payload_id = await save_raw_payload(db, "telegram_webhook", body, correlation_id)
        event_data = parse_message_to_event(update.message, str(raw_payload_id))

        events_created = 0
        if event_data:
            await NormalizedEventService.upsert_event(db, event_data)
            events_created = 1

        await db.commit()
        await update_sync_log_status(db, log_id, "completed")

        if update_id:
            await db.execute(
                sql_update(WebhookDelivery)
                .where(
                    WebhookDelivery.source == "telegram",
                    WebhookDelivery.delivery_id == str(update_id),
                )
                .values(status="completed")
            )

        logger.info(
            "telegram_webhook_completed",
            correlation_id=correlation_id,
            events_created=events_created,
        )
        return {"status": "ok", "events_created": events_created}

    except Exception as e:
        await db.rollback()
        await update_sync_log_status(db, log_id, "failed", str(e))

        if update_id:
            await db.execute(
                sql_update(WebhookDelivery)
                .where(
                    WebhookDelivery.source == "telegram",
                    WebhookDelivery.delivery_id == str(update_id),
                )
                .values(status="failed", error_text=str(e)[:500])
            )

        logger.error("telegram_webhook_failed", correlation_id=correlation_id, error=str(e))
        raise
