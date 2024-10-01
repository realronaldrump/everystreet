from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, ValidationInfo, field_validator


class DateRange(BaseModel):
    start_date: date = Field(..., description="Start date of the range")
    end_date: date = Field(..., description="End date of the range")

    @field_validator("end_date")
    def end_date_must_be_after_start_date(
            cls, v: date, info: ValidationInfo) -> date:
        start_date = info.data.get("start_date")
        if start_date and v < start_date:
            raise ValueError("end_date must be after start_date")
        return v


class HistoricalDataParams(BaseModel):
    date_range: DateRange
    filter_waco: bool = Field(
        False, description="Whether to filter data to Waco area")
    waco_boundary: str = Field(
        "city_limits", description="Type of Waco boundary to use"
    )
    bounds: Optional[list] = Field(
        None, description="Bounding box for filtering data")

    @field_validator("bounds")
    def validate_bounds(
            cls,
            v: Optional[list],
            info: ValidationInfo) -> Optional[list]:
        if v is not None:
            if len(v) != 4:
                raise ValueError("bounds must be a list of 4 float values")
            if not all(isinstance(x, (int, float)) for x in v):
                raise ValueError("all values in bounds must be numbers")
        return v
