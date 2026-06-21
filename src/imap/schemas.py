from datetime import datetime

from pydantic import BaseModel


class IMAPMessageData(BaseModel):
    uid: str
    subject: str
    sender: str
    date: datetime
    body: str
    folder: str
