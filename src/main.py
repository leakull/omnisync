from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.agent.router import router as agent_router
from src.auth.router import router as auth_router
from src.config import settings
from src.database import engine, get_db
from src.dlq.router import router as dlq_router
from src.events.router import router as events_router
from src.github.router import router as github_router
from src.imap.router import router as imap_router
from src.logging_config import logger, setup_logging
from src.raw_payloads.router import router as raw_payloads_router
from src.search.router import router as search_router
from src.telegram.router import router as telegram_router

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("omnisync_starting", version="0.1.0")
    from src.otel import init_otel, instrument_fastapi, instrument_sqlalchemy, shutdown_otel

    try:
        init_otel(service_name="omnisync-api")
        instrument_fastapi(app)
        instrument_sqlalchemy(engine)
    except Exception as e:
        logger.warning("otel_init_failed", error=str(e))
    yield
    # Release pooled HTTP connections held by the long-lived API clients.
    for closer in (_close_github_client, _close_telegram_client):
        try:
            await closer()
        except Exception as e:
            logger.warning("client_close_failed", error=str(e))
    shutdown_otel()
    await engine.dispose()
    logger.info("omnisync_shutdown")


async def _close_github_client() -> None:
    from src.github.service import github_client

    await github_client.aclose()


async def _close_telegram_client() -> None:
    from src.telegram.service import telegram_client

    await telegram_client.aclose()


app = FastAPI(
    title="OmniSync",
    description="Integration gateway for work events from GitHub and Telegram",
    version="0.1.0",
    lifespan=lifespan,
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

allowed_origins = settings.ALLOWED_ORIGINS.split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/api/v1")
app.include_router(github_router, prefix="/api/v1")
app.include_router(telegram_router, prefix="/api/v1")
app.include_router(imap_router, prefix="/api/v1")
app.include_router(search_router, prefix="/api/v1")
app.include_router(events_router, prefix="/api/v1")
app.include_router(raw_payloads_router, prefix="/api/v1")
app.include_router(dlq_router, prefix="/api/v1")
app.include_router(agent_router, prefix="/api/v1")

instrumentator = Instrumentator(
    should_group_status_codes=False,
    should_ignore_untemplated=True,
    excluded_handlers=["/health", "/health/ready", "/health/live", "/metrics"],
)
instrumentator.instrument(app)
instrumentator.expose(app, endpoint="/metrics")


class DependencyStatus(BaseModel):
    status: str
    latency_ms: float | None = None
    error: str | None = None


class HealthResponse(BaseModel):
    status: str
    database: bool
    redis: bool
    minio: bool
    jaeger: bool
    dependencies: dict[str, DependencyStatus] | None = None


async def _check_db(session: AsyncSession) -> bool:
    try:
        await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _check_redis() -> tuple[bool, float | None, str | None]:
    try:
        import time

        start = time.monotonic()
        client = aioredis.from_url(settings.REDIS_URL, socket_connect_timeout=2)
        await client.ping()
        latency = (time.monotonic() - start) * 1000
        await client.aclose()
        return True, latency, None
    except Exception as e:
        return False, None, str(e)[:200]


async def _check_minio() -> tuple[bool, float | None, str | None]:
    try:
        import time

        start = time.monotonic()
        async with httpx.AsyncClient(timeout=2) as client:
            resp = await client.get(f"{settings.S3_ENDPOINT_URL}/minio/health/live")
            latency = (time.monotonic() - start) * 1000
            return resp.status_code == 200, latency, None
    except Exception as e:
        return False, None, str(e)[:200]


async def _check_jaeger() -> tuple[bool, float | None, str | None]:
    try:
        import time

        start = time.monotonic()
        async with httpx.AsyncClient(timeout=2) as client:
            resp = await client.get(settings.JAEGER_HEALTH_URL)
            latency = (time.monotonic() - start) * 1000
            return resp.status_code == 200, latency, None
    except Exception as e:
        return False, None, str(e)[:200]


@app.get("/health", response_model=HealthResponse)
async def health(db: AsyncSession = Depends(get_db)):
    db_ok = await _check_db(db)
    redis_ok, redis_lat, redis_err = await _check_redis()
    minio_ok, minio_lat, minio_err = await _check_minio()
    jaeger_ok, jaeger_lat, jaeger_err = await _check_jaeger()

    all_ok = db_ok and redis_ok
    health_status = "ok" if all_ok else "degraded"

    return HealthResponse(
        status=health_status,
        database=db_ok,
        redis=redis_ok,
        minio=minio_ok,
        jaeger=jaeger_ok,
        dependencies={
            "database": DependencyStatus(status="ok" if db_ok else "error"),
            "redis": DependencyStatus(
                status="ok" if redis_ok else "error",
                latency_ms=redis_lat,
                error=redis_err,
            ),
            "minio": DependencyStatus(
                status="ok" if minio_ok else "error",
                latency_ms=minio_lat,
                error=minio_err,
            ),
            "jaeger": DependencyStatus(
                status="ok" if jaeger_ok else "error",
                latency_ms=jaeger_lat,
                error=jaeger_err,
            ),
        },
    )


@app.get("/health/live")
async def liveness():
    """Liveness probe: only confirms the process is up. Must not depend on
    external services, otherwise a transient dependency outage would cause
    the orchestrator to kill an otherwise-healthy process."""
    return {"status": "ok"}


@app.get("/health/ready")
async def readiness(response: Response, db: AsyncSession = Depends(get_db)):
    """Readiness probe: depends only on the *required* backing services
    (database, Redis). Optional services (MinIO, Jaeger) do not gate traffic."""
    db_ok = await _check_db(db)
    redis_ok, _, redis_err = await _check_redis()
    ready = db_ok and redis_ok
    if not ready:
        response.status_code = 503
    return {
        "status": "ready" if ready else "not_ready",
        "database": db_ok,
        "redis": redis_ok,
        "redis_error": redis_err,
    }
