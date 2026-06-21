from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.exceptions import UnauthorizedError
from src.auth.schemas import RefreshTokenRequest, TokenResponse, UserCreate, UserLogin
from src.auth.service import AuthService
from src.database import get_db

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=TokenResponse)
async def register(data: UserCreate, db: AsyncSession = Depends(get_db)):
    user = await AuthService.create_user(db, data)
    access_token = AuthService.create_access_token(user.username)
    refresh_token = AuthService.create_refresh_token(user.username)
    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@router.post("/login", response_model=TokenResponse)
async def login(data: UserLogin, db: AsyncSession = Depends(get_db)):
    user = await AuthService.authenticate_user(db, data)
    access_token = AuthService.create_access_token(user.username)
    refresh_token = AuthService.create_refresh_token(user.username)
    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@router.post("/refresh", response_model=TokenResponse)
async def refresh(data: RefreshTokenRequest, db: AsyncSession = Depends(get_db)):
    if await AuthService.is_token_blacklisted(data.refresh_token):
        raise UnauthorizedError("Refresh token has been revoked")

    token_payload = AuthService.verify_token(data.refresh_token, expected_type="refresh")

    user = await AuthService.get_user_by_username(db, token_payload.sub)
    if not user:
        raise UnauthorizedError("User not found")
    if not user.is_active:
        raise UnauthorizedError("User account is disabled")

    access_token = AuthService.create_access_token(user.username)
    refresh_token = AuthService.create_refresh_token(user.username)
    return TokenResponse(access_token=access_token, refresh_token=refresh_token)


@router.post("/logout")
async def logout(data: RefreshTokenRequest):
    await AuthService.blacklist_token(data.refresh_token)
    return {"detail": "Successfully logged out"}
