from pydantic_settings import BaseSettings


class TelegramSettings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_API_BASE: str = "https://api.telegram.org"
    TELEGRAM_WEBHOOK_SECRET: str = ""

    model_config = {"env_file": ".env"}


telegram_settings = TelegramSettings()
