"""initial schema

Revision ID: 001_initial
Revises:
Create Date: 2026-06-19

"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # users
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(255), nullable=False, unique=True),
        sa.Column("hashed_password", sa.String(255), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_users_username", "users", ["username"], unique=True)

    # raw_payloads
    op.create_table(
        "raw_payloads",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("correlation_id", sa.String(32), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("storage_url", sa.String(512), nullable=True),
    )
    op.create_index("ix_raw_payloads_source", "raw_payloads", ["source"])
    op.create_index("ix_raw_payloads_correlation_id", "raw_payloads", ["correlation_id"])
    op.create_index("ix_raw_payloads_source_hash", "raw_payloads", ["source", "content_hash"])

    # sync_logs
    op.create_table(
        "sync_logs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("correlation_id", sa.String(32), nullable=False),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_sync_logs_correlation_id", "sync_logs", ["correlation_id"])

    # normalized_events
    op.create_table(
        "normalized_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("external_id", sa.String(255), nullable=False),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("author_id", sa.String(255), nullable=False),
        sa.Column("author_name", sa.String(255), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "raw_payload_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("raw_payloads.id"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.UniqueConstraint("source", "external_id", name="uq_source_external_id"),
    )
    op.create_index("ix_events_source_author", "normalized_events", ["source", "author_id"])
    op.create_index("ix_events_timestamp", "normalized_events", ["timestamp"])
    op.create_index("ix_events_updated_at", "normalized_events", ["updated_at"])
    op.create_index("ix_events_cursor", "normalized_events", ["timestamp", "id"])

    # event_versions
    op.create_table(
        "event_versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("normalized_events.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "raw_payload_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("raw_payloads.id"),
            nullable=True,
        ),
        sa.Column("changed_by", sa.String(255), nullable=True),
        sa.Column(
            "changed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("event_id", "version", name="uq_event_version"),
    )
    op.create_index("ix_event_versions_event_id", "event_versions", ["event_id"])

    # webhook_deliveries
    op.create_table(
        "webhook_deliveries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("delivery_id", sa.String(255), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="processing"),
        sa.Column("error_text", sa.Text(), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("source", "delivery_id", name="uq_webhook_delivery"),
    )
    op.create_index("ix_webhook_deliveries_source", "webhook_deliveries", ["source"])

    # outbox_events
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("aggregate_type", sa.String(50), nullable=False),
        sa.Column("aggregate_id", sa.String(255), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_outbox_events_status", "outbox_events", ["status"])
    op.create_index("ix_outbox_unpublished", "outbox_events", ["status", "created_at"])

    # failed_events (dead-letter queue)
    op.create_table(
        "failed_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("operation", sa.String(50), nullable=False),
        sa.Column("correlation_id", sa.String(32), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("error_text", sa.Text(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("replay_attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_failed_events_source", "failed_events", ["source"])
    op.create_index("ix_failed_events_correlation_id", "failed_events", ["correlation_id"])
    op.create_index("ix_failed_events_status", "failed_events", ["status", "created_at"])

    # sync_state (incremental sync cursors)
    op.create_table(
        "sync_state",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("stream", sa.String(50), nullable=False, server_default="default"),
        sa.Column("cursor", sa.String(512), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("source", "stream", name="uq_sync_state_source_stream"),
    )
    op.create_index("ix_sync_state_source", "sync_state", ["source"])


def downgrade() -> None:
    op.drop_table("sync_state")
    op.drop_table("failed_events")
    op.drop_table("outbox_events")
    op.drop_table("webhook_deliveries")
    op.drop_table("event_versions")
    op.drop_table("normalized_events")
    op.drop_table("sync_logs")
    op.drop_table("raw_payloads")
    op.drop_table("users")
