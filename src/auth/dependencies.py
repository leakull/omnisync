from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.exceptions import UnauthorizedError
from src.auth.models import User
from src.auth.service import AuthService
from src.database import get_db


async def get_current_user(
    authorization: str = Header(..., description="Bearer <token>"),
    db: AsyncSession = Depends(get_db),
) -> User:
    if not authorization.startswith("Bearer "):
        raise UnauthorizedError("Invalid authorization header format")

    token = authorization[7:]

    if await AuthService.is_token_blacklisted(token):
        raise UnauthorizedError("Token has been revoked")

    token_payload = AuthService.verify_token(token, expected_type="access")

    user = await AuthService.get_user_by_username(db, token_payload.sub)
    if not user:
        raise UnauthorizedError("User not found")
    if not user.is_active:
        raise UnauthorizedError("User account is disabled")

    return user
