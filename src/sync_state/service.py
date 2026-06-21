from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.sync_state.models import SyncState


async def get_cursor(session: AsyncSession, source: str, stream: str = "default") -> str | None:
    result = await session.execute(
        select(SyncState.cursor).where(SyncState.source == source, SyncState.stream == stream)
    )
    return result.scalars().first()


async def set_cursor(
    session: AsyncSession, source: str, cursor: str | None, stream: str = "default"
) -> None:
    result = await session.execute(
        select(SyncState).where(SyncState.source == source, SyncState.stream == stream)
    )
    state = result.scalar_one_or_none()
    if state is None:
        session.add(SyncState(source=source, stream=stream, cursor=cursor))
    else:
        state.cursor = cursor
    await session.flush()
