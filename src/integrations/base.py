from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from uuid import UUID

from src.events.schemas import NormalizedEventCreate


class BaseConnector(ABC):
    source: str

    @abstractmethod
    async def fetch(self, since: datetime | None = None) -> list[Any]:
        pass

    @abstractmethod
    def normalize(
        self, raw: Any, raw_payload_id: UUID | None = None
    ) -> NormalizedEventCreate | None:
        pass

    def normalize_all(
        self, raw_items: list[Any], raw_payload_id: UUID | None = None
    ) -> list[NormalizedEventCreate]:
        results = []
        for item in raw_items:
            normalized = self.normalize(item, raw_payload_id)
            if normalized:
                results.append(normalized)
        return results
