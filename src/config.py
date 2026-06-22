import os

from pydantic import model_validator
from pydantic_settings import BaseSettings


def resolve_secrets_dir() -> str | None:
    """Directory of file-based secrets (Docker/K8s ``/run/secrets`` style).

    Each field can be supplied as a file named after the env var, letting a
    secret manager inject credentials without putting them in ``.env`` or the
    process environment. Returns None when the directory is absent so local
    development keeps using ``.env``.
    """
    candidate = os.getenv("SECRETS_DIR", "/run/secrets")
    return candidate if os.path.isdir(candidate) else None


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/omnisync"
    SYNC_DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/omnisync"
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"

    # Database connection pool / timeouts (asyncpg). Tuned for a long-lived API
    # process; ignored for SQLite (tests).
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30  # max wait (s) to check out a connection from the pool
    DB_POOL_RECYCLE: int = 1800  # recycle connections older than this (s) to dodge stale TCP
    DB_CONNECT_TIMEOUT: int = 10  # TCP/handshake timeout (s)
    DB_STATEMENT_TIMEOUT_MS: int = 30000  # server-side statement timeout (ms); 0 disables

    # Redis client socket timeouts (used by the auth/blacklist client).
    REDIS_SOCKET_TIMEOUT: int = 5
    REDIS_SOCKET_CONNECT_TIMEOUT: int = 5
    REDIS_HEALTH_CHECK_INTERVAL: int = 30

    GITHUB_TOKEN: str = ""
    GITHUB_API_BASE: str = "https://api.github.com"
    GITHUB_WEBHOOK_SECRET: str = ""

    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_API_BASE: str = "https://api.telegram.org"

    JWT_SECRET: str = "changeme-min-32-chars-placeholder"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 1440

    GITHUB_SYNC_INTERVAL: int = 3600
    TELEGRAM_SYNC_INTERVAL: int = 300

    ALLOWED_ORIGINS: str = "http://localhost:3000,http://localhost:8000"
    ENV: str = "development"

    S3_ENDPOINT_URL: str = "http://localhost:9000"
    S3_ACCESS_KEY: str = "minioadmin"
    S3_SECRET_KEY: str = "minioadmin"
    S3_BUCKET: str = "omnisync-raw-payloads"
    S3_PAYLOAD_THRESHOLD: int = 32768
    # S3/MinIO client timeouts & retries (botocore). Prevents a hung object
    # store from blocking a worker indefinitely.
    S3_CONNECT_TIMEOUT: int = 5
    S3_READ_TIMEOUT: int = 30
    S3_MAX_ATTEMPTS: int = 3

    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4317"
    JAEGER_HEALTH_URL: str = "http://jaeger:16686/"

    RAW_PAYLOAD_TTL_DAYS: int = 90

    QDRANT_URL: str = "http://localhost:6333"
    QDRANT_TIMEOUT: int = 10  # seconds for Qdrant client operations

    # Embeddings: backend = local (sentence-transformers) | openai | fake
    EMBEDDING_BACKEND: str = "local"
    EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"
    EMBEDDING_DIM: int = 384
    OPENAI_API_KEY: str = ""
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    OPENAI_EMBEDDING_MODEL: str = "text-embedding-3-small"
    OPENAI_TIMEOUT: int = 30  # seconds for OpenAI embedding HTTP calls
    OPENAI_MAX_RETRIES: int = 3

    # Outbox publisher
    OUTBOX_BATCH_SIZE: int = 100
    OUTBOX_MAX_ATTEMPTS: int = 10

    # Dead-letter queue
    DLQ_MAX_REPLAY_ATTEMPTS: int = 5
    # Background auto-retry of dead-lettered operations (exponential backoff)
    DLQ_RETRY_ENABLED: bool = True
    DLQ_RETRY_INTERVAL: int = 60  # how often the beat task scans for due retries (s)
    DLQ_RETRY_BASE_DELAY: int = 60  # first retry delay (s); doubles each attempt
    DLQ_RETRY_MAX_DELAY: int = 3600  # cap on the computed backoff (s)
    DLQ_RETRY_BATCH_SIZE: int = 50

    # Outbound email (SMTP)
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = ""
    SMTP_PASSWORD: str = ""
    SMTP_USE_TLS: bool = True
    SMTP_FROM: str = ""

    model_config = {
        "env_file": ".env",
        "secrets_dir": resolve_secrets_dir(),
        "extra": "ignore",
    }

    @model_validator(mode="after")
    def _validate_jwt_secret(self) -> "Settings":
        if self.ENV == "production" and "changeme" in self.JWT_SECRET:
            raise ValueError(
                "JWT_SECRET must be changed from default in production. "
                "Set a strong, unique secret in your environment."
            )
        return self


settings = Settings()
