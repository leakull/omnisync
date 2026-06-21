from src.exceptions import AppException


class EventNotFoundError(AppException):
    def __init__(self, detail: str = "Event not found"):
        super().__init__(status_code=404, detail=detail)
