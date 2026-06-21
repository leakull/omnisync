from src.integrations.base import BaseConnector

CONNECTORS: dict[str, type[BaseConnector]] = {}


def register_connector(cls: type[BaseConnector]) -> type[BaseConnector]:
    CONNECTORS[cls.source] = cls
    return cls


def get_connector(source: str) -> BaseConnector:
    cls = CONNECTORS.get(source)
    if not cls:
        raise ValueError(f"No connector registered for source: {source}")
    return cls()
