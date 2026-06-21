from src.exceptions import AppException


class InvalidTokenError(AppException):
    def __init__(self, detail: str = "Invalid token"):
        super().__init__(status_code=401, detail=detail)


class TokenExpiredError(AppException):
    def __init__(self, detail: str = "Token has expired"):
        super().__init__(status_code=401, detail=detail)


class UnauthorizedError(AppException):
    def __init__(self, detail: str = "Unauthorized"):
        super().__init__(status_code=401, detail=detail)


class UserAlreadyExistsError(AppException):
    def __init__(self, detail: str = "User already exists"):
        super().__init__(status_code=409, detail=detail)
