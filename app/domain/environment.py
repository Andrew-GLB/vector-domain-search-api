from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class EnvironmentDomain(BaseModel):
    """The pure domain representation of a Deployment Environment.

    Classifies resources based on their role in the software development
    lifecycle (SDLC).

    Attributes:
        env_name (str): The name of the environment (e.g., Production, Staging).
        tier (str): Classification for criticality (Mission Critical, Business, Sandbox).
        is_ephemeral (bool): True if the environment is temporary (e.g., PR previews).
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data (Standardized via Literals)
    env_name: Literal["Production", "Staging", "Development", "UAT", "Sandbox"] = Field(
        ...,
        description="The standardized name of the environment"
    )
    tier: Literal["Mission Critical", "Standard", "Low Impact"] = Field(
        "Standard",
        description="The business criticality tier"
    )
    is_ephemeral: bool = Field(False, description="Whether the environment is long-lived or temporary")

    # 3. Pipeline Metadata (Must match DimEnvironment)
    is_active: bool = True
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('env_name', mode='before')
    @classmethod
    def clean_env_name(cls, v: str) -> str:
        """Ensures the environment name is standardized.

        Args:
            v (str): The raw environment name.

        Returns:
            str: The cleaned environment name.
        """
        if isinstance(v, str):
            cleaned = v.strip().upper()
            # If it's UAT, keep it all caps; otherwise, use Title Case for others
            if cleaned == "UAT":
                return "UAT"
            return cleaned.capitalize() # .title() would also work for single words
        return v

    model_config = {
        "from_attributes": True, # Crucial for SQLModel integration
        "json_schema_extra": {
            "example": {
                "env_name": "Production",
                "tier": "Mission Critical",
                "is_ephemeral": False,
                "is_active": True
            }
        }
    }
