from src.exceptions import ExternalAPIError


class TelegramAPIError(ExternalAPIError):
    def __init__(self, detail: str = "Telegram API error"):
        super().__init__(detail=detail)
