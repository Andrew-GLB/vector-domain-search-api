from datetime import datetime
from typing import Any

from pydantic import BaseModel, EmailStr, Field, field_validator


class TeamDomain(BaseModel):
    """The pure domain representation of an Organizational Team.

    Represents the team responsible for managing and paying for a specific
    cloud resource.

    Attributes:
        team_name (str): Unique name of the team (e.g., DevOps, Data Science).
        department (str): Higher-level organizational unit (e.g., Engineering, Finance).
        lead_email (EmailStr): Primary technical contact for the team.
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data
    team_name: str = Field(..., description="Unique name of the team", min_length=2)
    department: str = Field(..., description="Organizational department")
    lead_email: EmailStr = Field(..., description="Primary contact for technical or billing issues")

    # 3. Pipeline Metadata (Matching DimTeam)
    is_active: bool = Field(default=True)
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('team_name', 'department', mode='before')
    @classmethod
    def clean_team_metadata(cls, v: Any) -> Any:
        """Standardizes team and department names using Title Case."""
        if isinstance(v, str):
            return v.strip().title()
        return v

    model_config = {
        "from_attributes": True, # Crucial for DimTeam -> TeamDomain conversion
        "json_schema_extra": {
            "example": {
                "team_name": "Cloud Operations",
                "department": "Infrastructure",
                "lead_email": "ops-lead@company.com",
                "is_active": True
            }
        }
    }
