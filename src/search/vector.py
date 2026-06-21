import hashlib
from uuid import uuid4

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from src.config import settings
from src.logging_config import logger
from src.otel import get_tracer

tracer = get_tracer("omnisync.search")

COLLECTION_NAME = "omnisync_events"
VECTOR_SIZE = 384


def _get_client() -> QdrantClient | None:
    try:
        return QdrantClient(url=settings.QDRANT_URL)
    except Exception as e:
        logger.warning("qdrant_connection_failed", error=str(e))
        return None


def _ensure_collection(client: QdrantClient) -> None:
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME not in collections:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
        )
        logger.info("qdrant_collection_created", collection=COLLECTION_NAME)


def _text_to_vector(text: str) -> list[float]:
    h = hashlib.sha256(text.encode()).digest()
    vec = []
    for i in range(0, min(len(h), VECTOR_SIZE), 1):
        vec.append((h[i % len(h)] / 255.0) * 2 - 1)
    while len(vec) < VECTOR_SIZE:
        vec.append(0.0)
    return vec[:VECTOR_SIZE]


def index_event(event_id: str, content: str, source: str, event_type: str) -> bool:
    client = _get_client()
    if not client:
        return False

    with tracer.start_as_current_span("qdrant.index_event") as span:
        span.set_attribute("event_id", event_id)
        span.set_attribute("source", source)

        try:
            _ensure_collection(client)
            vector = _text_to_vector(content)
            client.upsert(
                collection_name=COLLECTION_NAME,
                points=[
                    PointStruct(
                        id=str(uuid4()),
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
) -> list[dict]:
    client = _get_client()
    if not client:
        return []

    with tracer.start_as_current_span("qdrant.search_events") as span:
        span.set_attribute("query", query[:100])
        span.set_attribute("limit", limit)

        try:
            _ensure_collection(client)
            vector = _text_to_vector(query)

            must_filters = []
            if source:
                must_filters.append(
                    {"key": "source", "match": {"value": source}}
                )
            if event_type:
                must_filters.append(
                    {"key": "event_type", "match": {"value": event_type}}
                )

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
