from uuid import UUID

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.events.audit_models import EventVersion
from src.events.dependencies import EventFilterParams
from src.events.exceptions import EventNotFoundError
from src.events.models import NormalizedEvent
from src.events.schemas import EventResponse, EventVersionResponse, NormalizedEventCreate
from src.pagination import PaginatedResponse, decode_cursor, encode_cursor


class NormalizedEventService:
    @staticmethod
    async def upsert_event(
        session: AsyncSession,
        event_data: NormalizedEventCreate,
        changed_by: str | None = None,
    ) -> NormalizedEvent:
        with session.no_autoflush:
            existing = await session.execute(
                select(NormalizedEvent).where(
                    NormalizedEvent.source == event_data.source,
                    NormalizedEvent.external_id == event_data.external_id,
                )
            )
            existing_event = existing.scalar_one_or_none()

            content_changed = False
            if existing_event:
                content_changed = existing_event.content != event_data.content
                if content_changed:
                    existing_version = await session.execute(
                        select(EventVersion).where(
                            EventVersion.event_id == existing_event.id,
                            EventVersion.version == existing_event.version,
                        )
                    )
                    if not existing_version.scalar_one_or_none():
                        old_version = EventVersion(
                            event_id=existing_event.id,
                            version=existing_event.version,
                            content=existing_event.content,
                            raw_payload_id=existing_event.raw_payload_id,
                            changed_by=changed_by,
                        )
                        session.add(old_version)

            if existing_event:
                new_version = (
                    existing_event.version + 1 if content_changed else existing_event.version
                )
                await session.execute(
                    update(NormalizedEvent)
                    .where(NormalizedEvent.id == existing_event.id)
                    .values(
                        author_id=event_data.author_id,
                        author_name=event_data.author_name,
                        content=event_data.content,
                        event_type=event_data.event_type,
                        timestamp=event_data.timestamp,
                        raw_payload_id=event_data.raw_payload_id,
                        version=new_version,
                    )
                )
                await session.flush()
                refreshed = await session.execute(
                    select(NormalizedEvent).where(NormalizedEvent.id == existing_event.id)
                )
                event = refreshed.scalar_one()
            else:
                event = NormalizedEvent(
                    external_id=event_data.external_id,
                    source=event_data.source,
                    author_id=event_data.author_id,
                    author_name=event_data.author_name,
                    content=event_data.content,
                    event_type=event_data.event_type,
                    timestamp=event_data.timestamp,
                    raw_payload_id=event_data.raw_payload_id,
                    version=1,
                )
                session.add(event)
                await session.flush()

                initial_version = EventVersion(
                    event_id=event.id,
                    version=1,
                    content=event_data.content,
                    raw_payload_id=event_data.raw_payload_id,
                    changed_by=changed_by,
                )
                session.add(initial_version)

            await session.flush()
            return event

    @staticmethod
    async def upsert_events_bulk(
        session: AsyncSession,
        events: list[NormalizedEventCreate],
        changed_by: str | None = None,
    ) -> list[NormalizedEvent]:
        if not events:
            return []

        conditions = [
            (NormalizedEvent.source == e.source) & (NormalizedEvent.external_id == e.external_id)
            for e in events
        ]
        existing_result = await session.execute(select(NormalizedEvent).where(or_(*conditions)))
        existing_map = {(e.source, e.external_id): e for e in existing_result.scalars().all()}

        results = []
        seen_keys = set()
        for event_data in events:
            key = (event_data.source, event_data.external_id)
            if key in seen_keys:
                existing_event = existing_map.get(key)
                if existing_event:
                    results.append(existing_event)
                continue
            seen_keys.add(key)

            existing_event = existing_map.get(key)

            if existing_event:
                content_changed = existing_event.content != event_data.content
                new_version = (
                    existing_event.version + 1 if content_changed else existing_event.version
                )

                if content_changed:
                    existing_version = await session.execute(
                        select(EventVersion).where(
                            EventVersion.event_id == existing_event.id,
                            EventVersion.version == existing_event.version,
                        )
                    )
                    if not existing_version.scalar_one_or_none():
                        session.add(
                            EventVersion(
                                event_id=existing_event.id,
                                version=existing_event.version,
                                content=existing_event.content,
                                raw_payload_id=existing_event.raw_payload_id,
                                changed_by=changed_by,
                            )
                        )

                await session.execute(
                    update(NormalizedEvent)
                    .where(NormalizedEvent.id == existing_event.id)
                    .values(
                        author_id=event_data.author_id,
                        author_name=event_data.author_name,
                        content=event_data.content,
                        event_type=event_data.event_type,
                        timestamp=event_data.timestamp,
                        raw_payload_id=event_data.raw_payload_id,
                        version=new_version,
                    )
                )
                refreshed = await session.execute(
                    select(NormalizedEvent).where(NormalizedEvent.id == existing_event.id)
                )
                results.append(refreshed.scalar_one())
            else:
                event = NormalizedEvent(
                    external_id=event_data.external_id,
                    source=event_data.source,
                    author_id=event_data.author_id,
                    author_name=event_data.author_name,
                    content=event_data.content,
                    event_type=event_data.event_type,
                    timestamp=event_data.timestamp,
                    raw_payload_id=event_data.raw_payload_id,
                    version=1,
                )
                session.add(event)
                await session.flush()

                session.add(
                    EventVersion(
                        event_id=event.id,
                        version=1,
                        content=event_data.content,
                        raw_payload_id=event_data.raw_payload_id,
                        changed_by=changed_by,
                    )
                )
                results.append(event)

        await session.flush()
        return results

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
