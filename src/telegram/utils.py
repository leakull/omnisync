import contextlib
from datetime import UTC, datetime
from uuid import UUID

from src.events.schemas import NormalizedEventCreate
from src.telegram.schemas import TelegramMessage


def parse_message_to_event(
    message: TelegramMessage,
    raw_payload_id: str | UUID | None = None,
) -> NormalizedEventCreate | None:
    if not message.text:
        return None

    rid = UUID(raw_payload_id) if isinstance(raw_payload_id, str) else raw_payload_id

    author_id = ""
    author_name = ""
    if message.from_user:
        author_id = str(message.from_user.id)
        name_parts = []
        if message.from_user.first_name:
            name_parts.append(message.from_user.first_name)
        if message.from_user.last_name:
            name_parts.append(message.from_user.last_name)
        author_name = (
            " ".join(name_parts)
            if name_parts
            else (message.from_user.username or str(message.from_user.id))
        )

    timestamp = datetime.now(UTC)
    if message.date:
        with contextlib.suppress(ValueError, TypeError):
            timestamp = datetime.fromtimestamp(message.date, tz=UTC)

    chat_title = ""
    if message.chat and message.chat.title:
        chat_title = f"[{message.chat.title}] "
    elif message.chat:
        chat_title = f"[chat:{message.chat.id}] "

    return NormalizedEventCreate(
        external_id=str(message.message_id),
        source="telegram",
        author_id=author_id,
        author_name=author_name,
        content=f"{chat_title}{message.text}",
        event_type="message",
        timestamp=timestamp,
        raw_payload_id=rid,
    )
