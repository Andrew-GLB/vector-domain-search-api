from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class StatusDomain(BaseModel):
    """The pure domain representation of an Operational Status.

    Represents the current state of a cloud resource at the time of
    metric collection.

    Attributes:
        status_name (str): The state (e.g., Active, Stopped, Maintenance).
        is_billable (bool): Whether resources in this state incur compute costs.
        description (str): A brief explanation of the status code.
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data
    status_name: str = Field(..., description="Operational state name", min_length=2)
    is_billable: bool = Field(True, description="Indicates if this state generates usage costs")
    description: str = Field(..., description="Details regarding what this state represents")

    # 3. Pipeline Metadata (Matching DimStatus)
    is_active: bool = Field(default=True)
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('status_name', mode='before')
    @classmethod
    def standardize_status(cls, v: Any) -> Any:
        """Ensures status names are stored in uppercase for consistent joining."""
        if isinstance(v, str):
            return v.strip().upper()
        return v

    model_config = {
        "from_attributes": True, # Crucial for SQLModel -> Pydantic conversion
        "json_schema_extra": {
            "example": {
                "status_name": "ACTIVE",
                "is_billable": True,
                "description": "Resource is running and fully operational.",
                "is_active": True
            }
        }
    }
