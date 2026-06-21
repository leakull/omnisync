from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.events.audit_models import EventVersion
from src.events.dependencies import EventFilterParams
from src.events.exceptions import EventNotFoundError
from src.events.models import NormalizedEvent
from src.events.schemas import EventResponse, EventVersionResponse, NormalizedEventCreate
from src.outbox.service import enqueue_event
from src.pagination import PaginatedResponse, decode_cursor, encode_cursor


def _insert_stmt(session: AsyncSession):
    """Return the dialect-specific ``insert`` construct that supports
    ``on_conflict_do_update`` (PostgreSQL in production, SQLite in tests).
    """
    bind = session.get_bind()
    dialect_obj = getattr(bind, "dialect", None)
    if dialect_obj is None and hasattr(bind, "sync_engine"):
        dialect_obj = bind.sync_engine.dialect
    dialect = getattr(dialect_obj, "name", "")
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        return pg_insert
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    return sqlite_insert


class NormalizedEventService:
    @staticmethod
    async def upsert_event(
        session: AsyncSession,
        event_data: NormalizedEventCreate,
        changed_by: str | None = None,
    ) -> NormalizedEvent:
        results = await NormalizedEventService.upsert_events_bulk(
            session, [event_data], changed_by=changed_by
        )
        return results[0]

    @staticmethod
    async def upsert_events_bulk(
        session: AsyncSession,
        events: list[NormalizedEventCreate],
        changed_by: str | None = None,
    ) -> list[NormalizedEvent]:
        if not events:
            return []

        now = datetime.now(timezone.utc)
        insert = _insert_stmt(session)

        # Deduplicate input by (source, external_id), keeping the first occurrence.
        # Required because a single INSERT cannot target the same conflict key twice.
        deduped: list[NormalizedEventCreate] = []
        seen: set[tuple[str, str]] = set()
        for e in events:
            key = (e.source, e.external_id)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(e)

        conditions = [
            (NormalizedEvent.source == e.source) & (NormalizedEvent.external_id == e.external_id)
            for e in deduped
        ]
        existing_result = await session.execute(select(NormalizedEvent).where(or_(*conditions)))
        existing_map = {(e.source, e.external_id): e for e in existing_result.scalars().all()}

        rows: list[dict] = []
        new_keys: list[tuple[str, str]] = []
        for e in deduped:
            key = (e.source, e.external_id)
            existing = existing_map.get(key)

            if existing:
                content_changed = existing.content != e.content
                target_version = existing.version + 1 if content_changed else existing.version
                if content_changed:
                    # Snapshot the previous version before it is overwritten.
                    dup = await session.execute(
                        select(EventVersion.id).where(
                            EventVersion.event_id == existing.id,
                            EventVersion.version == existing.version,
                        )
                    )
                    if dup.scalar_one_or_none() is None:
                        session.add(
                            EventVersion(
                                event_id=existing.id,
                                version=existing.version,
                                content=existing.content,
                                schema_version=existing.schema_version,
                                raw_payload_id=existing.raw_payload_id,
                                changed_by=changed_by,
                            )
                        )
            else:
                target_version = 1
                new_keys.append(key)

            rows.append(
                {
                    "id": uuid4(),
                    "external_id": e.external_id,
                    "source": e.source,
                    "author_id": e.author_id,
                    "author_name": e.author_name,
                    "content": e.content,
                    "event_type": e.event_type,
                    "timestamp": e.timestamp,
                    "raw_payload_id": e.raw_payload_id,
                    "schema_version": e.schema_version,
                    "version": target_version,
                    "created_at": now,
                    "updated_at": now,
                }
            )

        # Persist version snapshots before the upsert.
        await session.flush()

        stmt = insert(NormalizedEvent).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["source", "external_id"],
            set_={
                "author_id": stmt.excluded.author_id,
                "author_name": stmt.excluded.author_name,
                "content": stmt.excluded.content,
                "event_type": stmt.excluded.event_type,
                "timestamp": stmt.excluded.timestamp,
                "raw_payload_id": stmt.excluded.raw_payload_id,
                "schema_version": stmt.excluded.schema_version,
                "version": stmt.excluded.version,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        await session.execute(stmt)
        await session.flush()

        # Reload affected rows by their natural key to get real ids/versions
        # (a concurrent writer may have won the INSERT race for new keys).
        # populate_existing forces a refresh of identity-map objects whose
        # columns were changed out-of-band by the Core ON CONFLICT statement.
        final_result = await session.execute(
            select(NormalizedEvent)
            .where(or_(*conditions))
            .execution_options(populate_existing=True)
        )
        final_map = {(e.source, e.external_id): e for e in final_result.scalars().all()}

        # Initial version row for brand-new events.
        for key in new_keys:
            ev = final_map.get(key)
            if ev is None:
                continue
            dup = await session.execute(
                select(EventVersion.id).where(
                    EventVersion.event_id == ev.id,
                    EventVersion.version == 1,
                )
            )
            if dup.scalar_one_or_none() is None:
                session.add(
                    EventVersion(
                        event_id=ev.id,
                        version=1,
                        content=ev.content,
                        schema_version=ev.schema_version,
                        raw_payload_id=ev.raw_payload_id,
                        changed_by=changed_by,
                    )
                )

        # Transactional outbox entry for downstream consumers.
        for e in deduped:
            ev = final_map.get((e.source, e.external_id))
            if ev is not None:
                await enqueue_event(session, ev)

        await session.flush()

        return [
            final_map[(e.source, e.external_id)]
            for e in deduped
            if (e.source, e.external_id) in final_map
        ]

    @staticmethod
    async def list_events(
        session: AsyncSession,
        filters: EventFilterParams,
        limit: int = 20,
    ) -> PaginatedResponse:
        query = select(NormalizedEvent)

        if filters.source:
            query = query.where(NormalizedEvent.source == filters.source)
        if filters.author_id:
            query = query.where(NormalizedEvent.author_id == filters.author_id)
        if filters.event_type:
            query = query.where(NormalizedEvent.event_type == filters.event_type)
        if filters.date_from:
            query = query.where(NormalizedEvent.timestamp >= filters.date_from)
        if filters.date_to:
            query = query.where(NormalizedEvent.timestamp <= filters.date_to)

        if filters.cursor:
            cursor_ts, cursor_id = decode_cursor(filters.cursor)
            query = query.where(
                (NormalizedEvent.timestamp < cursor_ts)
                | ((NormalizedEvent.timestamp == cursor_ts) & (NormalizedEvent.id < cursor_id))
            )

        result = await session.execute(
            query.order_by(NormalizedEvent.timestamp.desc(), NormalizedEvent.id.desc()).limit(
                limit + 1
            )
        )
        events = result.scalars().all()

        has_more = len(events) > limit
        events = events[:limit]

        next_cursor = None
        if has_more and events:
            last = events[-1]
            next_cursor = encode_cursor(last.timestamp, last.id)

        return PaginatedResponse(
            items=[EventResponse.model_validate(e) for e in events],
            has_more=has_more,
            next_cursor=next_cursor,
            limit=limit,
        )

    @staticmethod
    async def get_watermark(
        session: AsyncSession, source: str, event_type: str | None = None
    ) -> datetime | None:
        """Latest known event timestamp for a source — used as the ``since``
        watermark to drive incremental syncs."""
        query = select(func.max(NormalizedEvent.timestamp)).where(NormalizedEvent.source == source)
        if event_type:
            query = query.where(NormalizedEvent.event_type == event_type)
        result = await session.execute(query)
        return result.scalar_one_or_none()

    @staticmethod
    async def get_event(session: AsyncSession, event_id: UUID) -> EventResponse:
        result = await session.execute(
            select(NormalizedEvent).where(NormalizedEvent.id == event_id)
        )
        event = result.scalar_one_or_none()
        if not event:
            raise EventNotFoundError()
        return EventResponse.model_validate(event)

    @staticmethod
    async def get_event_history(
        session: AsyncSession, event_id: UUID
    ) -> list[EventVersionResponse]:
        result = await session.execute(
            select(EventVersion)
            .where(EventVersion.event_id == event_id)
            .order_by(EventVersion.version.asc())
        )
        versions = result.scalars().all()
        return [EventVersionResponse.model_validate(v) for v in versions]
