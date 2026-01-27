import re
from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class CostCenterDomain(BaseModel):
    """The pure domain representation of a Financial Cost Center.
    
    Attributes:
        center_code (str): Unique budget identifier (Format: CC-XXXX).
        department (str): The department responsible for the spend.
        approver_email (str): The manager responsible for budget approval.
    """
    # Add id so the Domain can hold the database primary key
    id: int | None = None

    center_code: str = Field(..., description="Unique financial code (Format: CC-XXXX)")
    department: str = Field(..., description="Department name")
    budget_limit: float = Field(..., description="Annual budget limit for this center", gt=0)

    # Add these to match your DimTable/Typesense metadata
    is_active: bool = True
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('center_code')
    @classmethod
    def validate_code_format(cls, v: str) -> str:
        """Standardizes cost center code to CC-XXXX format."""
        v = v.strip().upper()
        if not re.match(r"^CC-\d{4}$", v):
            raise ValueError("center_code must follow the format 'CC-1234'")
        return v

    model_config = {
        "from_attributes": True, # Allows Pydantic to read from SQLModel objects
        "json_schema_extra": {
            "example": {
                "center_code": "CC-8800",
                "department": "Data Engineering",
                "budget_limit": 50000.0,
                "is_active": True
            }
        }
    }
