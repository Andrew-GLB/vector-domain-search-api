from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class SecurityTierDomain(BaseModel):
    """The pure domain representation of a Security and Compliance Tier.
    
    Attributes:
        tier_name (str): Classification (e.g., Public, Restricted).
        encryption_required (bool): If data must be encrypted at rest.
        compliance_standard (str): Relevant regulation (SOC2, GDPR, HIPAA).
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data
    tier_name: Literal["Public", "Internal", "Confidential", "Restricted"] = Field(..., description="Data sensitivity level")
    encryption_required: bool = Field(True, description="Indicates if encryption is mandatory")
    compliance_standard: str = Field(..., description="Primary regulation scope")

    # 3. Pipeline Metadata (Matching DimSecurityTier)
    is_active: bool = Field(default=True)
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('tier_name', mode='before')
    @classmethod
    def validate_tier_name(cls, v: Any) -> Any:
        """Ensures the tier name is Title Case to match the Literal allowed list."""
        if isinstance(v, str):
            return v.strip().title()
        return v

    model_config = {
        "from_attributes": True, # Crucial for SQLModel -> Pydantic conversion
        "json_schema_extra": {
            "example": {
                "tier_name": "Confidential",
                "encryption_required": True,
                "compliance_standard": "GDPR",
                "is_active": True
            }
        }
    }
