import asyncio
import email
import imaplib
from datetime import datetime, timezone
from email.header import decode_header
from typing import Any
from uuid import UUID

from src.events.schemas import NormalizedEventCreate
from src.integrations.base import BaseConnector
from src.integrations.registry import register_connector
from src.logging_config import logger
from src.otel import get_tracer

tracer = get_tracer("omnisync.imap")


class IMAPClient:
    def __init__(self, host: str, port: int, username: str, password: str, use_ssl: bool = True):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl

    def _connect(self) -> imaplib.IMAP4_SSL | imaplib.IMAP4:
        if self.use_ssl:
            return imaplib.IMAP4_SSL(self.host, self.port)
        return imaplib.IMAP4(self.host, self.port)

    def fetch_messages(
        self, folder: str = "INBOX", since: datetime | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        messages = []
        with tracer.start_as_current_span("imap.fetch_messages") as span:
            span.set_attribute("imap.folder", folder)
            span.set_attribute("imap.host", self.host)

            conn = self._connect()
            try:
                conn.login(self.username, self.password)
                conn.select(folder)

                criteria = "ALL"
                if since:
                    date_str = since.strftime("%d-%b-%Y")
                    criteria = f'(SINCE "{date_str}")'

                status, data = conn.search(None, criteria)
                if status != "OK":
                    return messages

                msg_ids = data[0].split()
                if limit:
                    msg_ids = msg_ids[-limit:]

                span.set_attribute("imap.message_count", len(msg_ids))

                for msg_id in msg_ids:
                    status, msg_data = conn.fetch(msg_id, "(RFC822)")
                    if status != "OK":
                        continue

                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)

                    subject = self._decode_header(msg.get("Subject", ""))
                    sender = self._decode_header(msg.get("From", ""))
                    date_str = msg.get("Date", "")

                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == "text/plain":
                                body = part.get_payload(decode=True).decode(
                                    part.get_content_charset() or "utf-8", errors="replace"
                                )
                                break
                    else:
                        body = msg.get_payload(decode=True).decode(
                            msg.get_content_charset() or "utf-8", errors="replace"
                        )

                    date = datetime.now(timezone.utc)
                    if date_str:
                        try:
                            from email.utils import parsedate_to_datetime

                            date = parsedate_to_datetime(date_str)
                        except Exception:
                            pass

                    messages.append(
                        {
                            "uid": msg_id.decode(),
                            "subject": subject,
                            "sender": sender,
                            "date": date,
                            "body": body[:5000],
                            "folder": folder,
                        }
                    )
            finally:
                try:
                    conn.logout()
                except Exception:
                    pass

            logger.info("imap_messages_fetched", count=len(messages), folder=folder)
            return messages

    @staticmethod
    def _decode_header(header: str) -> str:
        if not header:
            return ""
        decoded_parts = decode_header(header)
        result = []
        for part, charset in decoded_parts:
            if isinstance(part, bytes):
                result.append(part.decode(charset or "utf-8", errors="replace"))
            else:
                result.append(part)
        return " ".join(result)


@register_connector
class IMAPConnector(BaseConnector):
    source = "imap"

    def __init__(
        self,
        host: str = "",
        port: int = 993,
        username: str = "",
        password: str = "",
        folder: str = "INBOX",
    ):
        self.client = IMAPClient(
            host=host or None,
            port=port,
            username=username or None,
            password=password or None,
        )
        self.folder = folder

    async def fetch(self, since: datetime | None = None) -> list[dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.client.fetch_messages(self.folder, since)
        )

    def normalize(
        self, raw: dict[str, Any], raw_payload_id: UUID | None = None
    ) -> NormalizedEventCreate | None:
        if not raw.get("subject") and not raw.get("body"):
            return None

        return NormalizedEventCreate(
            external_id=f"imap-{raw['uid']}",
            source="imap",
            author_id=raw.get("sender", ""),
            author_name=raw.get("sender", ""),
            content=f"Subject: {raw['subject']}\n\n{raw['body'][:2000]}",
            event_type="email",
            timestamp=raw["date"],
            raw_payload_id=raw_payload_id,
        )
