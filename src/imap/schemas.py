from datetime import datetime

from pydantic import BaseModel, EmailStr, Field


class IMAPMessageData(BaseModel):
    uid: str
    subject: str
    sender: str
    date: datetime
    body: str
    folder: str


class SendEmailRequest(BaseModel):
    to: list[EmailStr] = Field(..., min_length=1)
    subject: str
    body: str
    from_addr: EmailStr | None = None
