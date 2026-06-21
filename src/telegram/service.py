from datetime import datetime
from uuid import UUID

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from src.events.schemas import NormalizedEventCreate
from src.integrations.base import BaseConnector
from src.integrations.registry import register_connector
from src.logging_config import logger
from src.otel import get_tracer
from src.telegram.config import telegram_settings
from src.telegram.constants import (
    LONG_POLL_TIMEOUT,
    REQUEST_TIMEOUT,
    RETRY_ATTEMPTS,
    RETRY_MAX_WAIT,
    RETRY_MIN_WAIT,
)
from src.telegram.exceptions import TelegramAPIError
from src.telegram.schemas import TelegramUpdate
from src.telegram.utils import parse_message_to_event

tracer = get_tracer("omnisync.telegram")


class TelegramClient:
    def __init__(self):
        self.base_url = telegram_settings.TELEGRAM_API_BASE
        self.token = telegram_settings.TELEGRAM_BOT_TOKEN

    def _get_url(self, method: str) -> str:
        return f"{self.base_url}/bot{self.token}/{method}"

    @retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
    )
    async def get_updates(self, offset: int | None = None) -> list[TelegramUpdate]:
        with tracer.start_as_current_span("telegram.get_updates") as span:
            params: dict = {"timeout": LONG_POLL_TIMEOUT}
            if offset is not None:
                params["offset"] = offset

            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                response = await client.get(self._get_url("getUpdates"), params=params)

                if response.status_code != 200:
                    raise TelegramAPIError(f"HTTP {response.status_code}: {response.text[:200]}")

                data = response.json()
                if not data.get("ok"):
                    raise TelegramAPIError(f"API error: {data.get('description', 'Unknown')}")

                updates = []
                for item in data.get("result", []):
                    try:
                        updates.append(TelegramUpdate.model_validate(item))
                    except Exception as e:
                        logger.warning("telegram_update_parse_error", error=str(e))

                span.set_attribute("telegram.updates_count", len(updates))
                return updates

    async def get_me(self) -> dict:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.get(self._get_url("getMe"))
            if response.status_code != 200:
                raise TelegramAPIError(f"HTTP {response.status_code}")
            return response.json()


telegram_client = TelegramClient()


@register_connector
class TelegramConnector(BaseConnector):
    source = "telegram"

    def __init__(self):
        self.client = telegram_client

    async def fetch(self, since: datetime | None = None) -> list[TelegramUpdate]:
        return await self.client.get_updates()

    def normalize(
        self, raw: TelegramUpdate, raw_payload_id: UUID | None = None
    ) -> NormalizedEventCreate | None:
        if not raw.message:
            return None
        return parse_message_to_event(raw.message, str(raw_payload_id) if raw_payload_id else None)
