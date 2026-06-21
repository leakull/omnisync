import asyncio

import src.filestore.service  # noqa: F401  (registers the connector)
from src.celery_app import celery_app
from src.filestore.config import filestore_settings
from src.integrations.sync import run_connector_sync
from src.logging_config import logger


@celery_app.task(name="src.filestore.tasks.sync_filestore")
def sync_filestore():
    if not filestore_settings.FILESTORE_BUCKET:
        logger.warning("filestore_sync_skipped", reason="FILESTORE_BUCKET not set")
        return 0
    return asyncio.run(run_connector_sync("filestore"))
