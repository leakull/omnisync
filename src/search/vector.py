import uuid
from typing import Any

from src.config import settings
from src.logging_config import logger
from src.otel import get_tracer
from src.search.embeddings import embed_text, get_dimension

tracer = get_tracer("omnisync.search")

COLLECTION_NAME = "omnisync_events"

# Stable namespace so the same event_id always maps to the same Qdrant point id,
# making (re)indexing idempotent instead of creating duplicate points.
_POINT_NAMESPACE = uuid.UUID("6f1d3c2a-0000-4000-8000-000000000001")


def _get_client() -> Any | None:
    try:
        from qdrant_client import QdrantClient

        return QdrantClient(url=settings.QDRANT_URL, timeout=settings.QDRANT_TIMEOUT)
    except Exception as e:
        logger.warning("qdrant_connection_failed", error=str(e))
        return None


def _ensure_collection(client: Any) -> None:
    from qdrant_client.models import Distance, VectorParams

    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in collections:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=get_dimension(), distance=Distance.COSINE),
        )
        logger.info("qdrant_collection_created", collection=COLLECTION_NAME)


def _point_id(event_id: str) -> str:
    return str(uuid.uuid5(_POINT_NAMESPACE, event_id))


def index_event(event_id: str, content: str, source: str, event_type: str) -> bool:
    client = _get_client()
    if not client:
        return False

    from qdrant_client.models import PointStruct

    with tracer.start_as_current_span("qdrant.index_event") as span:
        span.set_attribute("event_id", event_id)
        span.set_attribute("source", source)

        try:
            _ensure_collection(client)
            vector = embed_text(content)
            client.upsert(
                collection_name=COLLECTION_NAME,
                points=[
                    PointStruct(
                        id=_point_id(event_id),
                        vector=vector,
                        payload={
                            "event_id": event_id,
                            "source": source,
                            "event_type": event_type,
                            "content": content[:1000],
                        },
                    )
                ],
            )
            return True
        except Exception as e:
            logger.error("qdrant_index_failed", event_id=event_id, error=str(e))
            return False


def search_events(
    query: str,
    source: str | None = None,
    event_type: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    client = _get_client()
    if not client:
        return []

    with tracer.start_as_current_span("qdrant.search_events") as span:
        span.set_attribute("query", query[:100])
        span.set_attribute("limit", limit)

        try:
            _ensure_collection(client)
            vector = embed_text(query)

            must_filters = []
            if source:
                must_filters.append({"key": "source", "match": {"value": source}})
            if event_type:
                must_filters.append({"key": "event_type", "match": {"value": event_type}})

            query_filter = {"must": must_filters} if must_filters else None

            results = client.search(
                collection_name=COLLECTION_NAME,
                query_vector=vector,
                query_filter=query_filter,
                limit=limit,
            )

            return [
                {
                    "event_id": hit.payload.get("event_id"),
                    "source": hit.payload.get("source"),
                    "event_type": hit.payload.get("event_type"),
                    "content": hit.payload.get("content"),
                    "score": hit.score,
                }
                for hit in results
            ]
        except Exception as e:
            logger.error("qdrant_search_failed", query=query[:50], error=str(e))
            return []
