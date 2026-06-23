"""Coverage for the vector-search module and embedding backends.

Qdrant is an external service (and the client package isn't installed in the
test env), so a fake client + a stubbed ``qdrant_client.models`` module stand in
for it; everything else (collection bootstrap, point id derivation, payload
mapping, error handling) is the real code path.
"""

import sys
import types

import pytest

from src.search import vector
from src.search.embeddings import embed_text, embed_texts, get_dimension


class _FakeCollections:
    def __init__(self, names):
        self.collections = [types.SimpleNamespace(name=n) for n in names]


class _FakeHit:
    def __init__(self, payload, score):
        self.payload = payload
        self.score = score


class FakeQdrant:
    def __init__(self, *, fail_on=None, existing=()):
        self.points = []
        self.created = []
        self._collections = list(existing)
        self._fail_on = fail_on  # method name that should raise

    def _maybe_fail(self, method):
        if self._fail_on == method:
            raise RuntimeError(f"qdrant {method} boom")

    def get_collections(self):
        self._maybe_fail("get_collections")
        return _FakeCollections(self._collections)

    def create_collection(self, collection_name, vectors_config):
        self.created.append((collection_name, vectors_config))
        self._collections.append(collection_name)

    def upsert(self, collection_name, points):
        self._maybe_fail("upsert")
        self.points.extend(points)

    def search(self, collection_name, query_vector, query_filter=None, limit=10):
        self._maybe_fail("search")
        self.last_filter = query_filter
        return [
            _FakeHit(
                {
                    "event_id": "evt-1",
                    "source": "github",
                    "event_type": "commit",
                    "content": "hello",
                },
                0.87,
            )
        ]


def _install_fake_qdrant(monkeypatch, fake_client):
    models = types.ModuleType("qdrant_client.models")

    class PointStruct:
        def __init__(self, id, vector, payload):
            self.id = id
            self.vector = vector
            self.payload = payload

    class Distance:
        COSINE = "Cosine"

    class VectorParams:
        def __init__(self, size, distance):
            self.size = size
            self.distance = distance

    models.PointStruct = PointStruct
    models.Distance = Distance
    models.VectorParams = VectorParams
    monkeypatch.setitem(sys.modules, "qdrant_client", types.ModuleType("qdrant_client"))
    monkeypatch.setitem(sys.modules, "qdrant_client.models", models)
    monkeypatch.setattr(vector, "_get_client", lambda: fake_client)


# ---------------------------------------------------------------------------
# index_event
# ---------------------------------------------------------------------------
def test_index_event_creates_collection_and_upserts(monkeypatch):
    client = FakeQdrant(existing=[])
    _install_fake_qdrant(monkeypatch, client)

    ok = vector.index_event("evt-1", "Fix the bug", "github", "commit")
    assert ok is True
    # Collection bootstrapped on first use.
    assert client.created and client.created[0][0] == vector.COLLECTION_NAME
    # One point upserted with the deterministic, idempotent point id.
    assert len(client.points) == 1
    assert client.points[0].id == vector._point_id("evt-1")
    assert client.points[0].payload["event_id"] == "evt-1"


def test_index_event_reuses_existing_collection(monkeypatch):
    client = FakeQdrant(existing=[vector.COLLECTION_NAME])
    _install_fake_qdrant(monkeypatch, client)

    assert vector.index_event("evt-2", "content", "telegram", "message") is True
    assert client.created == []  # not recreated


def test_index_event_no_client_returns_false(monkeypatch):
    monkeypatch.setattr(vector, "_get_client", lambda: None)
    assert vector.index_event("evt-3", "c", "github", "commit") is False


def test_index_event_swallows_errors(monkeypatch):
    client = FakeQdrant(fail_on="upsert")
    _install_fake_qdrant(monkeypatch, client)
    assert vector.index_event("evt-4", "c", "github", "commit") is False


def test_point_id_is_stable_and_deterministic():
    assert vector._point_id("abc") == vector._point_id("abc")
    assert vector._point_id("abc") != vector._point_id("xyz")


# ---------------------------------------------------------------------------
# search_events
# ---------------------------------------------------------------------------
def test_search_events_maps_hits(monkeypatch):
    client = FakeQdrant(existing=[vector.COLLECTION_NAME])
    _install_fake_qdrant(monkeypatch, client)

    results = vector.search_events("hello", limit=5)
    assert results == [
        {
            "event_id": "evt-1",
            "source": "github",
            "event_type": "commit",
            "content": "hello",
            "score": 0.87,
        }
    ]
    assert client.last_filter is None  # no filters → no query_filter


def test_search_events_builds_filters(monkeypatch):
    client = FakeQdrant(existing=[vector.COLLECTION_NAME])
    _install_fake_qdrant(monkeypatch, client)

    vector.search_events("q", source="github", event_type="commit")
    assert client.last_filter == {
        "must": [
            {"key": "source", "match": {"value": "github"}},
            {"key": "event_type", "match": {"value": "commit"}},
        ]
    }


def test_search_events_no_client_returns_empty(monkeypatch):
    monkeypatch.setattr(vector, "_get_client", lambda: None)
    assert vector.search_events("q") == []


def test_search_events_swallows_errors(monkeypatch):
    client = FakeQdrant(fail_on="search", existing=[vector.COLLECTION_NAME])
    _install_fake_qdrant(monkeypatch, client)
    assert vector.search_events("q") == []


# ---------------------------------------------------------------------------
# embeddings (non-model backends)
# ---------------------------------------------------------------------------
def test_embed_text_fake_backend_dim_and_determinism():
    v = embed_text("hello")
    assert len(v) == get_dimension()
    assert embed_text("hello") == v


def test_embed_texts_unknown_backend_raises(monkeypatch):
    from src.search import embeddings

    monkeypatch.setattr(embeddings.settings, "EMBEDDING_BACKEND", "bogus")
    with pytest.raises(ValueError, match="Unknown EMBEDDING_BACKEND"):
        embed_texts(["x"])


def test_embed_openai_requires_api_key(monkeypatch):
    from src.search import embeddings

    monkeypatch.setattr(embeddings.settings, "EMBEDDING_BACKEND", "openai")
    monkeypatch.setattr(embeddings.settings, "OPENAI_API_KEY", "")
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        embed_texts(["x"])


def test_embed_openai_happy_path(monkeypatch):
    from src.search import embeddings

    monkeypatch.setattr(embeddings.settings, "EMBEDDING_BACKEND", "openai")
    monkeypatch.setattr(embeddings.settings, "OPENAI_API_KEY", "sk-test")

    class FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            # Returned out of order to exercise the index-sort.
            return {"data": [{"index": 1, "embedding": [0.3]}, {"index": 0, "embedding": [0.1]}]}

    monkeypatch.setattr(embeddings.httpx, "post", lambda *a, **k: FakeResp())

    out = embed_texts(["a", "b"])
    assert out == [[0.1], [0.3]]
