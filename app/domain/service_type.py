from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class ServiceTypeDomain(BaseModel):
    """The pure domain representation of a Cloud Service Classification.

    Defines what the resource actually is (e.g., Virtual Machine,
    Relational Database, Object Storage).

    Attributes:
        service_name (str): Technical name of the service (e.g., EC2, RDS, S3).
        category (str): General classification (Compute, Storage, Networking, Database).
        is_managed (bool): Whether the provider manages the underlying infrastructure.
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data
    service_name: str = Field(..., description="Technical service name", min_length=2)
    category: Literal["Compute", "Storage", "Database", "Networking", "Security", "Other"] = Field(
        ...,
        description="High-level service category"
    )
    is_managed: bool = Field(True, description="True if the service is a PaaS/SaaS offering")

    # 3. Pipeline Metadata (Matching DimServiceType)
    is_active: bool = Field(default=True)
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('service_name', mode='before')
    @classmethod
    def standardize_service_name(cls, v: Any) -> Any:
        """Cleans and uppercases technical service identifiers."""
        if isinstance(v, str):
            return v.strip().upper()
        return v

    @field_validator('category', mode='before')
    @classmethod
    def validate_category(cls, v: Any) -> Any:
        """Standardizes category to Title Case before the Literal check."""
        if isinstance(v, str):
            return v.strip().title()
        return v

    model_config = {
        "from_attributes": True, # Required for SQLModel -> Pydantic conversion
        "json_schema_extra": {
            "example": {
                "service_name": "RDS",
                "category": "Database",
                "is_managed": True,
                "is_active": True
            }
        }
    }
