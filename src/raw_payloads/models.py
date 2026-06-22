from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base
from src.db_types import GUID, JSONBType


class RawPayload(Base):
    __tablename__ = "raw_payloads"
    __table_args__ = (Index("ix_raw_payloads_source_hash", "source", "content_hash"),)

    id: Mapped[UUID] = mapped_column(GUID(), primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(50), index=True, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONBType, nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    correlation_id: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), nullable=False
    )
    storage_url: Mapped[str | None] = mapped_column(String(512), nullable=True)
