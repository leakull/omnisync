from celery import Celery
from celery.signals import worker_process_init

from src.config import settings

celery_app = Celery(
    "omnisync",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.REDIS_URL,
)

celery_app.autodiscover_tasks(
    [
        "src.github",
        "src.telegram",
        "src.raw_payloads",
        "src.imap",
        "src.outbox",
        "src.filestore",
        "src.jira",
        "src.dlq",
    ]
)


@worker_process_init.connect
def _init_worker_tracing(**_kwargs):
    """Initialize OpenTelemetry inside each worker process so background-task
    traces are exported, and propagate context through Celery messages."""
    from src.otel import init_otel, instrument_celery

    init_otel(service_name="omnisync-worker")
    instrument_celery()


celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        "sync-github-commits": {
            "task": "src.github.tasks.sync_github_commits",
            "schedule": settings.GITHUB_SYNC_INTERVAL,
        },
        "sync-github-pull-requests": {
            "task": "src.github.tasks.sync_github_pull_requests",
            "schedule": settings.GITHUB_SYNC_INTERVAL,
        },
        "sync-telegram-messages": {
            "task": "src.telegram.tasks.sync_telegram_messages",
            "schedule": settings.TELEGRAM_SYNC_INTERVAL,
        },
        "cleanup-old-payloads": {
            "task": "src.raw_payloads.tasks.cleanup_old_payloads",
            "schedule": 86400,
        },
        "sync-imap-messages": {
            "task": "src.imap.tasks.sync_imap_messages",
            "schedule": settings.GITHUB_SYNC_INTERVAL,
        },
        "publish-outbox": {
            "task": "src.outbox.tasks.publish_outbox",
            "schedule": 30,
        },
        "retry-failed-events": {
            "task": "src.dlq.tasks.retry_failed_events",
            "schedule": settings.DLQ_RETRY_INTERVAL,
        },
        "sync-filestore": {
            "task": "src.filestore.tasks.sync_filestore",
            "schedule": settings.GITHUB_SYNC_INTERVAL,
        },
        "sync-jira": {
            "task": "src.jira.tasks.sync_jira",
            "schedule": settings.GITHUB_SYNC_INTERVAL,
        },
    },
)
