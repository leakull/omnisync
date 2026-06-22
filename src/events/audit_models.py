from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base
from src.db_types import GUID


class EventVersion(Base):
    __tablename__ = "event_versions"
    __table_args__ = (UniqueConstraint("event_id", "version", name="uq_event_version"),)

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(
        GUID(),
        ForeignKey("normalized_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version: Mapped[int] = mapped_column(nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[int] = mapped_column(nullable=False, default=1, server_default="1")
    raw_payload_id: Mapped[UUID | None] = mapped_column(
        GUID(), ForeignKey("raw_payloads.id"), nullable=True
    )
    changed_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False
    )
