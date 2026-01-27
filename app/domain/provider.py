from datetime import datetime
from typing import Literal

from pydantic import BaseModel, EmailStr, Field, field_validator


class ProviderDomain(BaseModel):
    """The pure domain representation of a Cloud Infrastructure Provider.

    This entity defines the source platform (e.g., AWS, Azure, GCP)
    where the cloud resources are hosted.

    Attributes:
        provider_name (str): Unique name of the platform.
        provider_type (str): Classification of the provider (Cloud vs. On-Prem).
        support_contact (EmailStr): Support or billing contact for this provider.
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data
    provider_name: str = Field(..., description="Name of the provider (e.g., AWS, AZURE)", min_length=2)
    provider_type: Literal["Public Cloud", "Private Cloud", "On-Premise"] = Field(
        "Public Cloud",
        description="The infrastructure classification"
    )
    support_contact: EmailStr = Field(..., description="Administrative contact email")

    # 3. Pipeline Metadata (Matching DimProvider)
    is_active: bool = Field(default=True)
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('provider_name', mode='before')
    @classmethod
    def standardize_provider_name(cls, v: str) -> str:
        """Ensures provider names are uppercase for consistent joining."""
        if isinstance(v, str):
            return v.strip().upper()
        return v

    @field_validator('provider_type', mode='before')
    @classmethod
    def clean_provider_type(cls, v: str) -> str:
        """Fixes casing issues before the Literal check (e.g., 'public cloud' -> 'Public Cloud')."""
        if isinstance(v, str):
            cleaned = v.strip().title()
            # Handle the specific hyphenation in 'On-Premise' if necessary
            if cleaned == "On-Premise":
                return "On-Premise"
            return cleaned
        return v

    model_config = {
        "from_attributes": True, # Crucial for DimProvider -> ProviderDomain conversion
        "json_schema_extra": {
            "example": {
                "provider_name": "AWS",
                "provider_type": "Public Cloud",
                "support_contact": "ops-team@company.com",
                "is_active": True
            }
        }
    }
