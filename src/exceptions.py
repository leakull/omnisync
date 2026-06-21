from fastapi import HTTPException


class AppException(HTTPException):
    pass


class NotFoundException(AppException):
    def __init__(self, detail: str = "Not found"):
        super().__init__(status_code=404, detail=detail)


class ExternalAPIError(AppException):
    def __init__(self, detail: str = "External API error"):
        super().__init__(status_code=502, detail=detail)


class DuplicateEventError(AppException):
    def __init__(self, detail: str = "Event already exists"):
        super().__init__(status_code=409, detail=detail)
