from pydantic import model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/omnisync"
    SYNC_DATABASE_URL: str = "postgresql://postgres:postgres@localhost:5432/omnisync"
    REDIS_URL: str = "redis://localhost:6379/0"
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"

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

    OTEL_EXPORTER_OTLP_ENDPOINT: str = "http://jaeger:4317"

    RAW_PAYLOAD_TTL_DAYS: int = 90

    QDRANT_URL: str = "http://localhost:6333"

    model_config = {"env_file": ".env"}

    @model_validator(mode="after")
    def _validate_jwt_secret(self) -> "Settings":
        if self.ENV == "production" and "changeme" in self.JWT_SECRET:
            raise ValueError(
                "JWT_SECRET must be changed from default in production. "
                "Set a strong, unique secret in your environment."
            )
        return self


settings = Settings()
