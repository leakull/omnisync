from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base
from src.db_types import GUID, JSONBType


class FailedEvent(Base):
    """Dead-letter store for sync operations that exhausted their retries.

    Keeps the original source payload so a failed ingestion can be replayed
    once the underlying issue is resolved.
    """

    __tablename__ = "failed_events"
    __table_args__ = (Index("ix_failed_events_status", "status", "created_at"),)

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    operation: Mapped[str] = mapped_column(String(50), nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    payload: Mapped[dict] = mapped_column(JSONBType, nullable=False)
    error_text: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    replay_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    # Background-retry bookkeeping: when the last automatic replay ran and the
    # earliest time the next one is allowed (exponential backoff watermark).
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
