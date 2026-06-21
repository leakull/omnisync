from pydantic import BaseModel


class TelegramUser(BaseModel):
    id: int
    is_bot: bool | None = None
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None


class TelegramChat(BaseModel):
    id: int
    type: str | None = None
    title: str | None = None


class TelegramMessage(BaseModel):
    message_id: int
    from_user: TelegramUser | None = None
    chat: TelegramChat | None = None
    date: int | None = None
    text: str | None = None

    model_config = {"populate_by_name": True}


class TelegramUpdate(BaseModel):
    update_id: int
    message: TelegramMessage | None = None
