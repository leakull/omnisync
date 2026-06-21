import asyncio
from email.message import EmailMessage

from src.config import settings
from src.logging_config import logger
from src.otel import get_tracer

tracer = get_tracer("omnisync.smtp")


class SMTPNotConfiguredError(RuntimeError):
    pass


class SMTPConnection:
    """Keeps a single authenticated SMTP session alive and reuses it across
    sends, reconnecting transparently when the relay drops the connection.

    Opening a TCP connection + STARTTLS + AUTH for every message is wasteful
    under bursts (e.g. fan-out notifications); reuse amortizes that cost. Access
    is serialized with an ``asyncio.Lock`` because an SMTP session is stateful
    and cannot interleave commands.
    """

    def __init__(self) -> None:
        self._client = None
        self._lock = asyncio.Lock()

    async def _ensure_client(self):
        import aiosmtplib

        if self._client is not None:
            try:
                await self._client.noop()
                return self._client
            except (aiosmtplib.SMTPException, OSError):
                await self._safe_quit()

        client = aiosmtplib.SMTP(
            hostname=settings.SMTP_HOST,
            port=settings.SMTP_PORT,
            start_tls=settings.SMTP_USE_TLS,
        )
        await client.connect()
        if settings.SMTP_USERNAME:
            await client.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD or "")
        self._client = client
        logger.info("smtp_connection_established", host=settings.SMTP_HOST)
        return client

    async def _safe_quit(self) -> None:
        if self._client is not None:
            try:
                await self._client.quit()
            except Exception:
                pass
            self._client = None

    async def close(self) -> None:
        async with self._lock:
            await self._safe_quit()

    async def send_message(self, message: EmailMessage) -> None:
        import aiosmtplib

        async with self._lock:
            try:
                client = await self._ensure_client()
                await client.send_message(message)
            except (aiosmtplib.SMTPException, OSError) as e:
                # One transparent reconnect+retry before surfacing the failure.
                logger.warning("smtp_send_retry", error=str(e))
                await self._safe_quit()
                client = await self._ensure_client()
                await client.send_message(message)


_smtp_connection = SMTPConnection()


async def send_email(
    to: list[str],
    subject: str,
    body: str,
    from_addr: str | None = None,
) -> None:
    """Send a plain-text email via the configured SMTP relay (reused session)."""
    if not settings.SMTP_HOST:
        raise SMTPNotConfiguredError("SMTP_HOST is not configured")

    sender = from_addr or settings.SMTP_FROM or settings.SMTP_USERNAME
    if not sender:
        raise SMTPNotConfiguredError("No sender address configured (SMTP_FROM)")

    message = EmailMessage()
    message["From"] = sender
    message["To"] = ", ".join(to)
    message["Subject"] = subject
    message.set_content(body)

    with tracer.start_as_current_span("smtp.send_email") as span:
        span.set_attribute("smtp.recipients", len(to))
        await _smtp_connection.send_message(message)
        logger.info("smtp_email_sent", recipients=len(to), subject=subject[:80])
