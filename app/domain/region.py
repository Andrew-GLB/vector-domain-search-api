import re
from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class RegionDomain(BaseModel):
    """The pure domain representation of a Geographic or Logical Region.

    Represents specific data center locations (e.g., us-east-1, eu-central-1).

    Attributes:
        region_code (str): Technical identifier for the region.
        display_name (str): Human-readable name of the region.
        continent (str): Geographic grouping for high-level reporting.
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data
    region_code: str = Field(..., description="Technical code (e.g., us-east-1)")
    display_name: str = Field(..., description="Friendly name (e.g., N. Virginia)")
    continent: str = Field(..., description="Geographic area (e.g., North America, Europe)")

    # 3. Pipeline Metadata (Must match DimRegion)
    is_active: bool = Field(default=True)
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('region_code', mode='before')
    @classmethod
    def validate_region_code(cls, v: str) -> str:
        """Standardizes region codes to lowercase and validates naming format."""
        if not isinstance(v, str):
            return v

        v = v.strip().lower()
        if not re.match(r"^[a-z0-9\-]+$", v):
            raise ValueError(f"region_code '{v}' must only contain lowercase letters, numbers, and hyphens")
        return v

    model_config = {
        "from_attributes": True, # Allows Pydantic to parse DimRegion SQLModel objects
        "json_schema_extra": {
            "example": {
                "region_code": "us-east-1",
                "display_name": "US East (N. Virginia)",
                "continent": "North America",
                "is_active": True
            }
        }
    }
