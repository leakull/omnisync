from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base


class SyncState(Base):
    """Persistent incremental-sync watermark per (source, stream).

    Lets pollers resume from where they left off instead of re-fetching the
    full history every run — e.g. a Telegram ``update_id`` offset or an IMAP
    ``since`` date. ``stream`` distinguishes multiple cursors for one source.
    """

    __tablename__ = "sync_state"
    __table_args__ = (UniqueConstraint("source", "stream", name="uq_sync_state_source_stream"),)

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    stream: Mapped[str] = mapped_column(String(50), nullable=False, default="default")
    cursor: Mapped[str | None] = mapped_column(String(512), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
