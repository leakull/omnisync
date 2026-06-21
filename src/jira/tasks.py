import asyncio

import src.jira.service  # noqa: F401  (registers the connector)
from src.celery_app import celery_app
from src.integrations.sync import run_connector_sync
from src.jira.config import jira_settings
from src.logging_config import logger


@celery_app.task(name="src.jira.tasks.sync_jira")
def sync_jira():
    if not jira_settings.JIRA_BASE_URL:
        logger.warning("jira_sync_skipped", reason="JIRA_BASE_URL not set")
        return 0
    return asyncio.run(run_connector_sync("jira"))
