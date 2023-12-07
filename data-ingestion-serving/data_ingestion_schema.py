from pydantic import BaseModel
from typing import Optional

class TripMessageModel(BaseModel):
    timestamp_id: str
    message: bytes
