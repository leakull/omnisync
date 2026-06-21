from email.message import EmailMessage

from src.config import settings
from src.logging_config import logger
from src.otel import get_tracer

tracer = get_tracer("omnisync.smtp")


class SMTPNotConfiguredError(RuntimeError):
    pass


async def send_email(
    to: list[str],
    subject: str,
    body: str,
    from_addr: str | None = None,
) -> None:
    """Send a plain-text email via the configured SMTP relay."""
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

    import aiosmtplib

    with tracer.start_as_current_span("smtp.send_email") as span:
        span.set_attribute("smtp.recipients", len(to))
        await aiosmtplib.send(
            message,
            hostname=settings.SMTP_HOST,
            port=settings.SMTP_PORT,
            username=settings.SMTP_USERNAME or None,
            password=settings.SMTP_PASSWORD or None,
            start_tls=settings.SMTP_USE_TLS,
        )
        logger.info("smtp_email_sent", recipients=len(to), subject=subject[:80])
