from datetime import datetime, timezone

from src.events.schemas import NormalizedEventCreate
from src.telegram.schemas import TelegramMessage


def parse_message_to_event(
    message: TelegramMessage,
    raw_payload_id: str | None = None,
) -> NormalizedEventCreate | None:
    if not message.text:
        return None

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

    timestamp = datetime.now(timezone.utc)
    if message.date:
        try:
            timestamp = datetime.fromtimestamp(message.date, tz=timezone.utc)
        except (ValueError, TypeError):
            pass

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
        raw_payload_id=raw_payload_id,
    )
