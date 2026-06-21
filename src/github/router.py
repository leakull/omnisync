import hashlib
import hmac

from fastapi import APIRouter, Depends, Header, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.models import User
from src.database import get_db
from src.events.service import NormalizedEventService
from src.github.config import github_settings
from src.github.exceptions import GitHubWebhookError
from src.github.schemas import GitHubWebhookPayload
from src.github.service import github_client
from src.github.utils import parse_commit_to_event, parse_pr_to_event
from src.logging_config import logger, set_correlation_id
from src.raw_payloads.service import save_raw_payload
from src.sync_logs.service import create_sync_log, update_sync_log_status
from src.webhooks.models import WebhookDelivery

router = APIRouter(prefix="/github", tags=["github"])
limiter = Limiter(key_func=get_remote_address)


def verify_webhook_signature(payload_body: bytes, signature: str | None) -> bool:
    if not github_settings.GITHUB_WEBHOOK_SECRET:
        return True
    if not signature:
        return False
    expected = (
        "sha256="
        + hmac.new(
            github_settings.GITHUB_WEBHOOK_SECRET.encode(),
            payload_body,
            hashlib.sha256,
        ).hexdigest()
    )
    return hmac.compare_digest(expected, signature)


@router.post(
    "/webhooks/github",
    responses={
        403: {"description": "Invalid webhook signature"},
        502: {"description": "GitHub API error"},
    },
)
async def github_webhook(
    request: Request,
    x_hub_signature_256: str | None = Header(None),
    x_github_event: str | None = Header(None),
    x_github_delivery: str | None = Header(None),
    db: AsyncSession = Depends(get_db),
):
    body = await request.body()
    if not verify_webhook_signature(body, x_hub_signature_256):
        raise GitHubWebhookError("Invalid webhook signature")

    delivery_id = x_github_delivery or ""
    if delivery_id:
        existing = (
            await db.execute(
                select(WebhookDelivery).where(
                    WebhookDelivery.source == "github",
                    WebhookDelivery.delivery_id == delivery_id,
                )
            )
        ).scalar_one_or_none()
        if existing:
            logger.info("github_webhook_duplicate", delivery_id=delivery_id)
            return {"status": "ok", "duplicate": True}

        db.add(WebhookDelivery(source="github", delivery_id=delivery_id, status="processing"))
        await db.flush()

    correlation_id = set_correlation_id()
    log_id = await create_sync_log(db, correlation_id, "github_webhook")

    try:
        payload = GitHubWebhookPayload.model_validate_json(body)
        raw_payload_id = await save_raw_payload(
            db, "github_webhook", payload.model_dump(), correlation_id
        )

        event_data_list = []
        if payload.commits:
            for commit in payload.commits:
                event_data_list.append(parse_commit_to_event(commit, "", str(raw_payload_id)))

        if payload.pull_request:
            event_data_list.append(parse_pr_to_event(payload.pull_request, "", str(raw_payload_id)))

        results = await NormalizedEventService.upsert_events_bulk(
            db, event_data_list, changed_by=correlation_id
        )

        await db.commit()
        await update_sync_log_status(db, log_id, "completed")

        if delivery_id:
            await db.execute(
                WebhookDelivery.__table__.update()
                .where(
                    WebhookDelivery.source == "github",
                    WebhookDelivery.delivery_id == delivery_id,
                )
                .values(status="completed")
            )

        logger.info(
            "github_webhook_processed",
            correlation_id=correlation_id,
            github_event=x_github_event,
            events_created=len(results),
        )
        return {"status": "ok", "events_created": len(results)}

    except Exception as e:
        await db.rollback()
        await update_sync_log_status(db, log_id, "failed", str(e))

        if delivery_id:
            await db.execute(
                WebhookDelivery.__table__.update()
                .where(
                    WebhookDelivery.source == "github",
                    WebhookDelivery.delivery_id == delivery_id,
                )
                .values(status="failed", error_text=str(e)[:500])
            )

        logger.error("github_webhook_failed", correlation_id=correlation_id, error=str(e))
        raise


@router.post(
    "/sync",
    responses={
        401: {"description": "Not authenticated"},
        502: {"description": "GitHub API error"},
    },
)
@limiter.limit("10/minute")
async def github_sync(
    request: Request,
    owner: str,
    repo: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    correlation_id = set_correlation_id()
    log_id = await create_sync_log(db, correlation_id, "github_poll")

    try:
        commits = await github_client.get_commits(owner, repo)
        prs = await github_client.get_pull_requests(owner, repo)

        event_data_list = []

        if commits:
            raw_payload_id = await save_raw_payload(
                db, "github_poll", {"type": "commits", "count": len(commits)}, correlation_id
            )
            for commit in commits:
                event_data_list.append(
                    parse_commit_to_event(commit, f"{owner}/{repo}", str(raw_payload_id))
                )

        if prs:
            raw_payload_id = await save_raw_payload(
                db, "github_poll", {"type": "pull_requests", "count": len(prs)}, correlation_id
            )
            for pr in prs:
                event_data_list.append(
                    parse_pr_to_event(pr, f"{owner}/{repo}", str(raw_payload_id))
                )

        results = await NormalizedEventService.upsert_events_bulk(
            db, event_data_list, changed_by=correlation_id
        )

        await db.commit()
        await update_sync_log_status(db, log_id, "completed")
        logger.info(
            "github_sync_completed",
            correlation_id=correlation_id,
            commits=len(commits),
            prs=len(prs),
            events_created=len(results),
        )
        return {
            "status": "ok",
            "commits": len(commits),
            "pull_requests": len(prs),
            "events_created": len(results),
        }

    except Exception as e:
        await db.rollback()
        await update_sync_log_status(db, log_id, "failed", str(e))
        logger.error("github_sync_failed", correlation_id=correlation_id, error=str(e))
        raise
