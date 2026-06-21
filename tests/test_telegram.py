import pytest
from httpx import AsyncClient

from src.telegram.schemas import TelegramChat, TelegramMessage, TelegramUser
from src.telegram.utils import parse_message_to_event


@pytest.mark.asyncio
async def test_telegram_sync_requires_auth(client: AsyncClient):
    response = await client.post("/api/v1/telegram/sync")
    assert response.status_code == 422 or response.status_code == 401


def test_parse_message_to_event():
    message = TelegramMessage(
        message_id=1,
        from_user=TelegramUser(id=123, first_name="Test", username="testuser"),
        chat=TelegramChat(id=456, type="group", title="Test Chat"),
        date=1717257600,
        text="Hello world",
    )
    event = parse_message_to_event(message)
    assert event is not None
    assert event.external_id == "1"
    assert event.source == "telegram"
    assert event.event_type == "message"
    assert "Hello world" in event.content
    assert event.author_name == "Test"


def test_parse_message_to_event_no_text():
    message = TelegramMessage(
        message_id=2,
        from_user=TelegramUser(id=123, first_name="Test"),
        chat=TelegramChat(id=456),
        date=1717257600,
        text=None,
    )
    event = parse_message_to_event(message)
    assert event is None
