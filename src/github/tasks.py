import asyncio
import os
import time

import httpx

from src.celery_app import celery_app
from src.database import async_session
from src.events.service import NormalizedEventService
from src.github.service import github_client
from src.github.utils import parse_commit_to_event, parse_pr_to_event
from src.logging_config import logger, set_correlation_id
from src.metrics import events_synced_total, sync_duration_seconds
from src.raw_payloads.service import save_raw_payload
from src.sync_logs.service import create_sync_log, update_sync_log_status

RETRYABLE_ERRORS = (httpx.ConnectError, httpx.TimeoutException, ConnectionError)


@celery_app.task(
    name="src.github.tasks.sync_github_commits",
    autoretry_for=RETRYABLE_ERRORS,
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 3},
)
def sync_github_commits():
    asyncio.run(_sync_github_commits())


async def _sync_github_commits():
    owner = os.getenv("GITHUB_SYNC_OWNER", "")
    repo = os.getenv("GITHUB_SYNC_REPO", "")
    if not owner or not repo:
        logger.warning("github_sync_skipped", reason="GITHUB_SYNC_OWNER/GITHUB_SYNC_REPO not set")
        return

    correlation_id = set_correlation_id()
    start_time = time.monotonic()
    log_id = await create_sync_log(async_session, correlation_id, "github_poll_commits")
    async with async_session() as session:
        try:
            commits = await github_client.get_commits(owner, repo)

            if commits:
                raw_payload_id = await save_raw_payload(
                    session,
                    "github_poll",
                    {"type": "commits", "count": len(commits)},
                    correlation_id,
                )
                event_data_list = [
                    parse_commit_to_event(c, f"{owner}/{repo}", str(raw_payload_id))
                    for c in commits
                ]
                results = await NormalizedEventService.upsert_events_bulk(
                    session, event_data_list, changed_by=correlation_id
                )
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                events_synced_total.labels(source="github", status="success").inc(len(results))
                logger.info(
                    "github_commits_sync_completed",
                    correlation_id=correlation_id,
                    commits=len(commits),
                    events_created=len(results),
                )
            else:
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                logger.info(
                    "github_commits_sync_completed",
                    correlation_id=correlation_id,
                    commits=0,
                    events_created=0,
                )
        except Exception as e:
            await session.rollback()
            await update_sync_log_status(async_session, log_id, "failed", str(e))
            events_synced_total.labels(source="github", status="error").inc()
            logger.error("github_commits_sync_failed", correlation_id=correlation_id, error=str(e))
            raise
        finally:
            sync_duration_seconds.labels(source="github").observe(time.monotonic() - start_time)


@celery_app.task(
    name="src.github.tasks.sync_github_pull_requests",
    autoretry_for=RETRYABLE_ERRORS,
    retry_backoff=True,
    retry_backoff_max=600,
    retry_kwargs={"max_retries": 3},
)
def sync_github_pull_requests():
    asyncio.run(_sync_github_pull_requests())


async def _sync_github_pull_requests():
    owner = os.getenv("GITHUB_SYNC_OWNER", "")
    repo = os.getenv("GITHUB_SYNC_REPO", "")
    if not owner or not repo:
        logger.warning("github_sync_skipped", reason="GITHUB_SYNC_OWNER/GITHUB_SYNC_REPO not set")
        return

    correlation_id = set_correlation_id()
    start_time = time.monotonic()
    log_id = await create_sync_log(async_session, correlation_id, "github_poll_prs")
    async with async_session() as session:
        try:
            prs = await github_client.get_pull_requests(owner, repo)

            if prs:
                raw_payload_id = await save_raw_payload(
                    session,
                    "github_poll",
                    {"type": "pull_requests", "count": len(prs)},
                    correlation_id,
                )
                event_data_list = [
                    parse_pr_to_event(pr, f"{owner}/{repo}", str(raw_payload_id)) for pr in prs
                ]
                results = await NormalizedEventService.upsert_events_bulk(
                    session, event_data_list, changed_by=correlation_id
                )
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                events_synced_total.labels(source="github", status="success").inc(len(results))
                logger.info(
                    "github_prs_sync_completed",
                    correlation_id=correlation_id,
                    prs=len(prs),
                    events_created=len(results),
                )
            else:
                await session.commit()
                await update_sync_log_status(async_session, log_id, "completed")
                logger.info(
                    "github_prs_sync_completed",
                    correlation_id=correlation_id,
                    prs=0,
                    events_created=0,
                )
        except Exception as e:
            await session.rollback()
            await update_sync_log_status(async_session, log_id, "failed", str(e))
            events_synced_total.labels(source="github", status="error").inc()
            logger.error("github_prs_sync_failed", correlation_id=correlation_id, error=str(e))
            raise
        finally:
            sync_duration_seconds.labels(source="github").observe(time.monotonic() - start_time)
