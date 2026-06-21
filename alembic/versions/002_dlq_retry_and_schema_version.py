"""dlq auto-retry bookkeeping + content schema_version

Revision ID: 002_dlq_retry_schema_version
Revises: 001_initial
Create Date: 2026-06-21

"""

import sqlalchemy as sa

from alembic import op

revision = "002_dlq_retry_schema_version"
down_revision = "001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Background-retry bookkeeping for the dead-letter queue.
    op.add_column(
        "failed_events",
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "failed_events",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_failed_events_due_retry",
        "failed_events",
        ["status", "next_retry_at"],
    )

    # Content-contract version on normalized events and their version snapshots.
    op.add_column(
        "normalized_events",
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    op.add_column(
        "event_versions",
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )


def downgrade() -> None:
    op.drop_column("event_versions", "schema_version")
    op.drop_column("normalized_events", "schema_version")
    op.drop_index("ix_failed_events_due_retry", table_name="failed_events")
    op.drop_column("failed_events", "next_retry_at")
    op.drop_column("failed_events", "last_attempt_at")
