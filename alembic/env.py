import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool

from alembic import context

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.auth.models import User  # noqa: F401
from src.database import Base
from src.dlq.models import FailedEvent  # noqa: F401
from src.events.audit_models import EventVersion  # noqa: F401
from src.events.models import NormalizedEvent  # noqa: F401
from src.outbox.models import OutboxEvent  # noqa: F401
from src.raw_payloads.models import RawPayload  # noqa: F401
from src.sync_logs.models import SyncLog  # noqa: F401
from src.sync_state.models import SyncState  # noqa: F401
from src.webhooks.models import WebhookDelivery  # noqa: F401

config = context.config

db_url = os.getenv("SYNC_DATABASE_URL")
if db_url:
    config.set_main_option("sqlalchemy.url", db_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
