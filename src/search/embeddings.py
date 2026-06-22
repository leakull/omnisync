"""Pluggable text embedding providers.

Backends (configured via ``EMBEDDING_BACKEND``):
- ``local``  – sentence-transformers model running in-process (default).
- ``openai`` – OpenAI-compatible HTTP embedding API.
- ``fake``   – deterministic hash-based pseudo-vectors. NOT semantic;
               only meant for tests / environments without a model.

Model loading is lazy and cached so importing this module stays cheap and the
heavy ML dependency is only pulled in when embeddings are actually requested.
"""

from __future__ import annotations

import hashlib
import threading
from functools import lru_cache

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import settings
from src.logging_config import logger

_model_lock = threading.Lock()
_model = None


def get_dimension() -> int:
    return settings.EMBEDDING_DIM


def _load_local_model():
    global _model
    if _model is not None:
        return _model
    with _model_lock:
        if _model is None:
            from sentence_transformers import SentenceTransformer

            logger.info("embedding_model_loading", model=settings.EMBEDDING_MODEL)
            _model = SentenceTransformer(settings.EMBEDDING_MODEL)
            dim = _model.get_sentence_embedding_dimension()
            if dim != settings.EMBEDDING_DIM:
                logger.warning(
                    "embedding_dim_mismatch",
                    configured=settings.EMBEDDING_DIM,
                    actual=dim,
                )
            logger.info("embedding_model_loaded", model=settings.EMBEDDING_MODEL, dim=dim)
    return _model


def _embed_local(texts: list[str]) -> list[list[float]]:
    model = _load_local_model()
    vectors = model.encode(texts, normalize_embeddings=True)
    return [v.tolist() for v in vectors]


def _embed_openai(texts: list[str]) -> list[list[float]]:
    if not settings.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured for the openai embedding backend")

    @retry(
        stop=stop_after_attempt(settings.OPENAI_MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
        reraise=True,
    )
    def _call() -> list[list[float]]:
        resp = httpx.post(
            f"{settings.OPENAI_BASE_URL}/embeddings",
            headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}"},
            json={"model": settings.OPENAI_EMBEDDING_MODEL, "input": texts},
            timeout=settings.OPENAI_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        return [item["embedding"] for item in sorted(data, key=lambda d: d["index"])]

    return _call()


@lru_cache(maxsize=4096)
def _embed_fake_one(text: str) -> tuple[float, ...]:
    """Deterministic, L2-normalized pseudo-embedding derived from a hash.

    Not semantic. Used only when ``EMBEDDING_BACKEND=fake``.
    """
    dim = settings.EMBEDDING_DIM
    raw = bytearray()
    counter = 0
    while len(raw) < dim * 4:
        raw.extend(hashlib.sha256(f"{text}:{counter}".encode()).digest())
        counter += 1
    vec = [(raw[i] / 255.0) * 2 - 1 for i in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5 or 1.0
    return tuple(x / norm for x in vec)


def _embed_fake(texts: list[str]) -> list[list[float]]:
    return [list(_embed_fake_one(t)) for t in texts]


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts using the configured backend."""
    if not texts:
        return []
    backend = settings.EMBEDDING_BACKEND.lower()
    if backend == "local":
        return _embed_local(texts)
    if backend == "openai":
        return _embed_openai(texts)
    if backend == "fake":
        return _embed_fake(texts)
    raise ValueError(f"Unknown EMBEDDING_BACKEND: {settings.EMBEDDING_BACKEND}")


def embed_text(text: str) -> list[float]:
    return embed_texts([text])[0]
