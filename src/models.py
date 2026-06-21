"""Aggregate import of all ORM models so ``Base.metadata`` is fully populated
wherever this module is imported (tests, migrations, schema creation)."""

from src.auth.models import User
from src.database import Base
from src.dlq.models import FailedEvent
from src.events.audit_models import EventVersion
from src.events.models import NormalizedEvent
from src.outbox.models import OutboxEvent
from src.raw_payloads.models import RawPayload
from src.sync_logs.models import SyncLog
from src.sync_state.models import SyncState
from src.webhooks.models import WebhookDelivery

__all__ = [
    "Base",
    "User",
    "RawPayload",
    "SyncLog",
    "SyncState",
    "NormalizedEvent",
    "EventVersion",
    "WebhookDelivery",
    "OutboxEvent",
    "FailedEvent",
]
