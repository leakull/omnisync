from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base
from src.db_types import GUID, JSONBType


class OutboxEvent(Base):
    """Transactional outbox: rows written in the same transaction as the
    domain change, then asynchronously published to downstream consumers
    (vector index, Pulse modules, AI agents) by a background worker.
    """

    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_outbox_unpublished", "status", "created_at"),)

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONBType, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
