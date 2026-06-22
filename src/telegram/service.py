from datetime import datetime
from typing import Any, cast
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
    def __init__(self) -> None:
        self.base_url = telegram_settings.TELEGRAM_API_BASE
        self.token = telegram_settings.TELEGRAM_BOT_TOKEN
        self._client: httpx.AsyncClient | None = None

    def _get_url(self, method: str) -> str:
        return f"{self.base_url}/bot{self.token}/{method}"

    def _get_client(self) -> httpx.AsyncClient:
        # Reuse one client (connection pool) across calls; the long-poll read
        # may exceed the per-request socket timeout, so allow a generous read.
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(REQUEST_TIMEOUT, read=LONG_POLL_TIMEOUT + REQUEST_TIMEOUT)
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    @retry(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
    )
    async def get_updates(self, offset: int | None = None) -> list[TelegramUpdate]:
        with tracer.start_as_current_span("telegram.get_updates") as span:
            params: dict[str, Any] = {"timeout": LONG_POLL_TIMEOUT}
            if offset is not None:
                params["offset"] = offset

            response = await self._get_client().get(self._get_url("getUpdates"), params=params)

            if response.status_code != 200:
                raise TelegramAPIError(f"HTTP {response.status_code}: {response.text[:200]}")

            data = response.json()
            if not data.get("ok"):
                raise TelegramAPIError(f"API error: {data.get('description', 'Unknown')}")

            updates: list[TelegramUpdate] = []
            for item in data.get("result", []):
                try:
                    updates.append(TelegramUpdate.model_validate(item))
                except Exception as e:
                    logger.warning("telegram_update_parse_error", error=str(e))

            span.set_attribute("telegram.updates_count", len(updates))
            return updates

    async def get_me(self) -> dict[str, Any]:
        response = await self._get_client().get(self._get_url("getMe"))
        if response.status_code != 200:
            raise TelegramAPIError(f"HTTP {response.status_code}")
        return cast(dict[str, Any], response.json())


telegram_client = TelegramClient()


@register_connector
class TelegramConnector(BaseConnector):
    source = "telegram"

    def __init__(self) -> None:
        self.client = telegram_client

    async def fetch(self, since: datetime | None = None) -> list[TelegramUpdate]:
        return await self.client.get_updates()

    def normalize(
        self, raw: TelegramUpdate, raw_payload_id: UUID | None = None
    ) -> NormalizedEventCreate | None:
        if not raw.message:
            return None
        return parse_message_to_event(raw.message, str(raw_payload_id) if raw_payload_id else None)
