from datetime import UTC, datetime

import pytest

from src.events.schemas import NormalizedEventCreate
from src.integrations.base import BaseConnector
from src.integrations.registry import CONNECTORS, get_connector


class StubConnector(BaseConnector):
    source = "stub"

    async def fetch(self, since=None):
        return [{"id": "1", "text": "test"}]

    def normalize(self, raw, raw_payload_id=None):
        return NormalizedEventCreate(
            external_id=raw["id"],
            source="stub",
            author_id="stub-user",
            author_name="Stub User",
            content=raw["text"],
            event_type="message",
            timestamp=datetime.now(UTC),
            raw_payload_id=raw_payload_id,
        )


def test_connector_registry():
    assert "github" in CONNECTORS
    assert "telegram" in CONNECTORS


def test_get_connector():
    github = get_connector("github")
    assert github.source == "github"

    telegram = get_connector("telegram")
    assert telegram.source == "telegram"


def test_get_connector_unknown():
    with pytest.raises(ValueError, match="No connector registered"):
        get_connector("unknown_source")


def test_base_connector_normalize_all():
    connector = StubConnector()
    raw_items = [
        {"id": "1", "text": "hello"},
        {"id": "2", "text": "world"},
    ]
    results = connector.normalize_all(raw_items)
    assert len(results) == 2
    assert results[0].content == "hello"
    assert results[1].content == "world"


def test_base_connector_normalize_none_filtered():
    class FilterConnector(BaseConnector):
        source = "filter"

        async def fetch(self, since=None):
            return []

        def normalize(self, raw, raw_payload_id=None):
            if raw.get("skip"):
                return None
            return NormalizedEventCreate(
                external_id=raw["id"],
                source="filter",
                author_id="u1",
                author_name="U1",
                content=raw["text"],
                event_type="msg",
                timestamp=datetime.now(UTC),
            )

    connector = FilterConnector()
    raw = [{"id": "1", "text": "keep"}, {"id": "2", "text": "skip", "skip": True}]
    results = connector.normalize_all(raw)
    assert len(results) == 1
    assert results[0].content == "keep"
