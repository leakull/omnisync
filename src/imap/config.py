from pydantic_settings import BaseSettings


class IMAPSettings(BaseSettings):
    IMAP_HOST: str = ""
    IMAP_PORT: int = 993
    IMAP_USERNAME: str = ""
    IMAP_PASSWORD: str = ""
    IMAP_FOLDER: str = "INBOX"
    IMAP_USE_SSL: bool = True

    model_config = {"env_file": ".env"}


imap_settings = IMAPSettings()
