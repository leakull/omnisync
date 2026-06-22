import asyncio
import email
import email.message
import imaplib
import threading
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
    """Thin IMAP wrapper that keeps a single authenticated connection alive and
    reuses it across polls.

    A new TLS handshake + LOGIN per sync is expensive when polling on a short
    interval, so the connection is cached and only re-established when a
    lightweight ``NOOP`` health-check fails. ``imaplib`` connections are not
    thread-safe, so all access is serialized behind a lock (Celery prefork runs
    one task at a time per process, but the lock keeps reuse safe regardless).
    """

    def __init__(self, host: str, port: int, username: str, password: str, use_ssl: bool = True):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self._conn: imaplib.IMAP4 | None = None
        self._lock = threading.Lock()

    def _new_connection(self) -> imaplib.IMAP4:
        conn: imaplib.IMAP4 = (
            imaplib.IMAP4_SSL(self.host, self.port)
            if self.use_ssl
            else imaplib.IMAP4(self.host, self.port)
        )
        conn.login(self.username, self.password)
        return conn

    def _ensure_connection(self) -> imaplib.IMAP4:
        """Return a live, authenticated connection, reconnecting if the cached
        one has gone stale."""
        if self._conn is not None:
            try:
                status, _ = self._conn.noop()
                if status == "OK":
                    return self._conn
            except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError):
                pass
            self._safe_logout()
        self._conn = self._new_connection()
        logger.info("imap_connection_established", host=self.host)
        return self._conn

    def _safe_logout(self) -> None:
        if self._conn is not None:
            try:
                self._conn.logout()
            except Exception:
                pass
            self._conn = None

    def close(self) -> None:
        with self._lock:
            self._safe_logout()

    def fetch_messages(
        self, folder: str = "INBOX", since: datetime | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        messages: list[dict[str, Any]] = []
        with tracer.start_as_current_span("imap.fetch_messages") as span:
            span.set_attribute("imap.folder", folder)
            span.set_attribute("imap.host", self.host)

            with self._lock:
                try:
                    conn = self._ensure_connection()
                    conn.select(folder)
                    uidvalidity = self._read_uidvalidity(conn)

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

                        if not msg_data or not isinstance(msg_data[0], tuple):
                            continue
                        raw_email = msg_data[0][1]
                        if not isinstance(raw_email, (bytes, bytearray)):
                            continue
                        msg = email.message_from_bytes(raw_email)
                        messages.append(self._parse_message(msg, msg_id, folder, uidvalidity))
                except (imaplib.IMAP4.abort, imaplib.IMAP4.error, OSError) as e:
                    # Drop the (possibly half-dead) connection so the next poll
                    # re-establishes a clean one.
                    self._safe_logout()
                    logger.warning("imap_fetch_error", host=self.host, error=str(e))
                    raise

            logger.info("imap_messages_fetched", count=len(messages), folder=folder)
            return [m for m in messages if m]

    def _parse_message(
        self,
        msg: email.message.Message,
        msg_id: bytes,
        folder: str,
        uidvalidity: str | None,
    ) -> dict[str, Any]:
        subject = self._decode_header(msg.get("Subject", ""))
        sender = self._decode_header(msg.get("From", ""))
        message_id = (msg.get("Message-ID") or msg.get("Message-Id") or "").strip()
        date_str = msg.get("Date", "")

        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if isinstance(payload, (bytes, bytearray)):
                        body = payload.decode(
                            part.get_content_charset() or "utf-8", errors="replace"
                        )
                    break
        else:
            payload = msg.get_payload(decode=True)
            if isinstance(payload, (bytes, bytearray)):
                body = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")

        date = datetime.now(timezone.utc)
        if date_str:
            try:
                from email.utils import parsedate_to_datetime

                date = parsedate_to_datetime(date_str)
            except Exception:
                pass

        return {
            "uid": msg_id.decode(),
            "uidvalidity": uidvalidity,
            "message_id": message_id,
            "host": self.host,
            "subject": subject,
            "sender": sender,
            "date": date,
            "body": body[:5000],
            "folder": folder,
        }

    @staticmethod
    def _read_uidvalidity(conn: imaplib.IMAP4) -> str | None:
        try:
            _, data = conn.response("UIDVALIDITY")
            if data and data[0]:
                value = data[0]
                return value.decode() if isinstance(value, (bytes, bytearray)) else str(value)
        except Exception:
            pass
        return None

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


# Cache one client per connection identity so the authenticated IMAP session is
# reused across Celery task invocations in the same worker process.
_client_cache: dict[tuple[str, int, str], IMAPClient] = {}
_client_cache_lock = threading.Lock()


def get_imap_client(
    host: str, port: int, username: str, password: str, use_ssl: bool = True
) -> IMAPClient:
    key = (host, port, username)
    with _client_cache_lock:
        client = _client_cache.get(key)
        if client is None:
            client = IMAPClient(host, port, username, password, use_ssl)
            _client_cache[key] = client
        return client


def _build_external_id(raw: dict[str, Any]) -> str:
    """Stable identity for an email.

    Prefer the globally-unique RFC 5322 ``Message-ID``. Fall back to a key
    scoped by host + folder + ``UIDVALIDITY`` so a UID is never confused across
    mailboxes or after the server resets UIDVALIDITY (UIDs are only unique
    within a single UIDVALIDITY epoch).
    """
    message_id = raw.get("message_id")
    if message_id:
        return f"imap-mid-{message_id}"
    host = raw.get("host", "")
    folder = raw.get("folder", "")
    uidvalidity = raw.get("uidvalidity") or "0"
    return f"imap-{host}-{folder}-{uidvalidity}-{raw['uid']}"


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
        use_ssl: bool = True,
    ):
        if not host:
            raise ValueError("IMAP host must be configured (IMAP_HOST)")
        if not username or not password:
            raise ValueError("IMAP username and password must be configured")
        self.client = get_imap_client(host, port, username, password, use_ssl)
        self.folder = folder

    async def fetch(self, since: datetime | None = None) -> list[dict[str, Any]]:
        # imaplib is blocking; run it in a worker thread so it never stalls the
        # event loop. get_running_loop() (not get_event_loop) is the 3.12+ API.
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, lambda: self.client.fetch_messages(self.folder, since)
        )

    def normalize(
        self, raw: dict[str, Any], raw_payload_id: UUID | None = None
    ) -> NormalizedEventCreate | None:
        if not raw.get("subject") and not raw.get("body"):
            return None

        return NormalizedEventCreate(
            external_id=_build_external_id(raw),
            source="imap",
            author_id=raw.get("sender", ""),
            author_name=raw.get("sender", ""),
            content=f"Subject: {raw['subject']}\n\n{raw['body'][:2000]}",
            event_type="email",
            timestamp=raw["date"],
            raw_payload_id=raw_payload_id,
        )
