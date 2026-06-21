from pydantic_settings import BaseSettings


class AuthSettings(BaseSettings):
    JWT_SECRET: str = "changeme-min-32-chars-placeholder"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 1440
    JWT_REFRESH_EXPIRE_MINUTES: int = 10080  # 7 days
    REDIS_URL: str = "redis://localhost:6379/0"

    model_config = {"env_file": ".env"}


auth_settings = AuthSettings()
