# schemas/models.py

from datetime import date
from pydantic import BaseModel, Field, field_validator


class LatestDataRecord(BaseModel):
    """Schema for latest_data table records."""

    dam_id: str = Field(..., max_length=20)
    dam_name: str = Field(..., max_length=255)
    date: date
    storage_volume: float | None = None
    percentage_full: float | None = None
    storage_inflow: float | None = None
    storage_release: float | None = None

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v


class DamResourceRecord(BaseModel):
    """Schema for dam_resources table records."""

    dam_id: str = Field(..., max_length=20)
    date: date
    storage_volume: float | None = None
    percentage_full: float | None = None
    storage_inflow: float | None = None
    storage_release: float | None = None

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v
