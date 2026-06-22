from datetime import UTC, datetime, timedelta

import redis.asyncio as aioredis
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.config import auth_settings
from src.auth.exceptions import InvalidTokenError, TokenExpiredError, UserAlreadyExistsError
from src.auth.models import User
from src.auth.schemas import TokenPayload, UserCreate, UserLogin
from src.config import settings
from src.logging_config import logger

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

_redis_client: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        # Bounded socket timeouts + periodic health checks so a hung Redis can't
        # block request handling; retry transient timeouts once transparently.
        _redis_client = aioredis.from_url(
            auth_settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=settings.REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=settings.REDIS_SOCKET_CONNECT_TIMEOUT,
            retry_on_timeout=True,
            health_check_interval=settings.REDIS_HEALTH_CHECK_INTERVAL,
        )
    return _redis_client


class AuthService:
    @staticmethod
    def hash_password(password: str) -> str:
        return str(pwd_context.hash(password))

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        return bool(pwd_context.verify(plain_password, hashed_password))

    @staticmethod
    def create_access_token(username: str) -> str:
        now = datetime.now(UTC)
        expire = now + timedelta(minutes=auth_settings.JWT_EXPIRE_MINUTES)
        payload = {
            "sub": username,
            "exp": expire,
            "iat": now,
            "token_type": "access",
        }
        return str(
            jwt.encode(payload, auth_settings.JWT_SECRET, algorithm=auth_settings.JWT_ALGORITHM)
        )

    @staticmethod
    def create_refresh_token(username: str) -> str:
        now = datetime.now(UTC)
        expire = now + timedelta(minutes=auth_settings.JWT_REFRESH_EXPIRE_MINUTES)
        payload = {
            "sub": username,
            "exp": expire,
            "iat": now,
            "token_type": "refresh",
        }
        return str(
            jwt.encode(payload, auth_settings.JWT_SECRET, algorithm=auth_settings.JWT_ALGORITHM)
        )

    @staticmethod
    def verify_token(token: str, expected_type: str = "access") -> TokenPayload:
        try:
            payload = jwt.decode(
                token,
                auth_settings.JWT_SECRET,
                algorithms=[auth_settings.JWT_ALGORITHM],
            )
            token_type = payload.get("token_type", "access")
            if token_type != expected_type:
                raise InvalidTokenError(f"Expected {expected_type} token, got {token_type}")
            return TokenPayload(
                sub=payload["sub"],
                exp=payload["exp"],
                iat=payload["iat"],
                token_type=token_type,
            )
        except JWTError as e:
            if "expired" in str(e).lower():
                raise TokenExpiredError() from e
            raise InvalidTokenError() from e

    @staticmethod
    async def blacklist_token(token: str) -> None:
        redis = await get_redis()
        try:
            payload = jwt.decode(
                token,
                auth_settings.JWT_SECRET,
                algorithms=[auth_settings.JWT_ALGORITHM],
                options={"verify_exp": False},
            )
            exp = payload.get("exp")
            if exp:
                now = datetime.now(UTC).timestamp()
                ttl = max(int(exp - now), 1)
                await redis.setex(f"blacklist:{token}", ttl, "1")
        except JWTError:
            pass

    @staticmethod
    async def is_token_blacklisted(token: str) -> bool:
        redis = await get_redis()
        return bool(await redis.exists(f"blacklist:{token}") > 0)

    @staticmethod
    async def create_user(session: AsyncSession, data: UserCreate) -> User:
        result = await session.execute(select(User).where(User.username == data.username))
        existing = result.scalar_one_or_none()
        if existing:
            raise UserAlreadyExistsError()

        user = User(
            username=data.username,
            hashed_password=AuthService.hash_password(data.password),
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        logger.info("user_created", username=user.username)
        return user

    @staticmethod
    async def authenticate_user(session: AsyncSession, data: UserLogin) -> User:
        result = await session.execute(select(User).where(User.username == data.username))
        user = result.scalar_one_or_none()
        if not user or not AuthService.verify_password(data.password, user.hashed_password):
            from src.auth.exceptions import UnauthorizedError

            raise UnauthorizedError("Invalid username or password")
        if not user.is_active:
            from src.auth.exceptions import UnauthorizedError

            raise UnauthorizedError("User account is disabled")
        return user

    @staticmethod
    async def get_user_by_username(session: AsyncSession, username: str) -> User | None:
        result = await session.execute(select(User).where(User.username == username))
        return result.scalar_one_or_none()
