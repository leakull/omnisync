import json
from typing import Any, cast

import aioboto3
from botocore.config import Config as BotoConfig

from src.config import settings
from src.logging_config import logger


def _boto_config() -> BotoConfig:
    """Bounded timeouts + retries so a slow/unreachable object store fails fast
    instead of hanging a worker indefinitely."""
    return BotoConfig(
        connect_timeout=settings.S3_CONNECT_TIMEOUT,
        read_timeout=settings.S3_READ_TIMEOUT,
        retries={"max_attempts": settings.S3_MAX_ATTEMPTS, "mode": "standard"},
    )


class S3Storage:
    def __init__(self) -> None:
        self.endpoint_url = settings.S3_ENDPOINT_URL
        self.access_key = settings.S3_ACCESS_KEY
        self.secret_key = settings.S3_SECRET_KEY
        self.bucket = settings.S3_BUCKET

    async def save_object(self, key: str, data: dict) -> str:
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=_boto_config(),
        ) as client:
            body = json.dumps(data, default=str).encode()
            await client.put_object(
                Bucket=self.bucket, Key=key, Body=body, ContentType="application/json"
            )
            url = f"{self.endpoint_url}/{self.bucket}/{key}"
            logger.info("s3_object_saved", key=key, bucket=self.bucket, size=len(body))
            return url

    async def get_object(self, url: str) -> dict:
        parts = url.replace(f"{self.endpoint_url}/", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]

        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=_boto_config(),
        ) as client:
            response = await client.get_object(Bucket=bucket, Key=key)
            body = await response["Body"].read()
            return cast(dict[Any, Any], json.loads(body))


s3_storage = S3Storage()
