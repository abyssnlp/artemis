from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class MeetingSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")
