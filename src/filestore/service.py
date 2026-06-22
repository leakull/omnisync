from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from src.config import settings
from src.events.schemas import NormalizedEventCreate
from src.filestore.config import filestore_settings
from src.integrations.base import BaseConnector
from src.integrations.registry import register_connector
from src.logging_config import logger
from src.otel import get_tracer

tracer = get_tracer("omnisync.filestore")


class FileStoreClient:
    """Lists objects from an S3-compatible bucket (AWS S3, MinIO, Ceph, ...)
    so a file store can act as a *source* of work events.
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "us-east-1",
    ) -> None:
        self.endpoint_url = endpoint_url or None
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.region = region

    async def list_objects(
        self, prefix: str = "", since: datetime | None = None
    ) -> list[dict[str, Any]]:
        import aioboto3
        from botocore.config import Config as BotoConfig

        objects: list[dict[str, Any]] = []
        session = aioboto3.Session()
        async with session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=BotoConfig(
                connect_timeout=settings.S3_CONNECT_TIMEOUT,
                read_timeout=settings.S3_READ_TIMEOUT,
                retries={"max_attempts": settings.S3_MAX_ATTEMPTS, "mode": "standard"},
            ),
        ) as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    last_modified = obj.get("LastModified")
                    if since and last_modified and last_modified <= since:
                        continue
                    objects.append(
                        {
                            "key": obj["Key"],
                            "size": obj.get("Size", 0),
                            "etag": (obj.get("ETag") or "").strip('"'),
                            "last_modified": last_modified,
                        }
                    )
        return objects


@register_connector
class FileStoreConnector(BaseConnector):
    source = "filestore"

    def __init__(
        self,
        endpoint_url: str = "",
        access_key: str = "",
        secret_key: str = "",
        bucket: str = "",
        prefix: str = "",
        region: str = "us-east-1",
    ) -> None:
        bucket = bucket or filestore_settings.FILESTORE_BUCKET
        if not bucket:
            raise ValueError("File store bucket must be configured (FILESTORE_BUCKET)")
        self.prefix = prefix or filestore_settings.FILESTORE_PREFIX
        self.client = FileStoreClient(
            endpoint_url=endpoint_url or filestore_settings.FILESTORE_ENDPOINT_URL,
            access_key=access_key or filestore_settings.FILESTORE_ACCESS_KEY,
            secret_key=secret_key or filestore_settings.FILESTORE_SECRET_KEY,
            bucket=bucket,
            region=region or filestore_settings.FILESTORE_REGION,
        )

    async def fetch(self, since: datetime | None = None) -> list[dict[str, Any]]:
        with tracer.start_as_current_span("filestore.fetch") as span:
            span.set_attribute("filestore.bucket", self.client.bucket)
            objects = await self.client.list_objects(self.prefix, since)
            span.set_attribute("filestore.object_count", len(objects))
            logger.info("filestore_fetched", count=len(objects), bucket=self.client.bucket)
            return objects

    def normalize(
        self, raw: dict[str, Any], raw_payload_id: UUID | None = None
    ) -> NormalizedEventCreate | None:
        key = raw.get("key")
        if not key:
            return None
        last_modified = raw.get("last_modified") or datetime.now(timezone.utc)
        size = raw.get("size", 0)
        return NormalizedEventCreate(
            external_id=f"filestore-{self.client.bucket}-{key}",
            source="filestore",
            author_id=self.client.bucket,
            author_name=self.client.bucket,
            content=f"File: {key} ({size} bytes)",
            event_type="file",
            timestamp=last_modified,
            raw_payload_id=raw_payload_id,
        )
