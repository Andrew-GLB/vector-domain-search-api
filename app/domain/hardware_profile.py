from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class HardwareProfileDomain(BaseModel):
    """The pure domain representation of a Resource Hardware Profile.
    
    Attributes:
        profile_name (str): Technical name (e.g., t3.medium, high-mem-1).
        cpu_count (int): Number of virtual CPUs.
        ram_gb (int): Amount of RAM in Gigabytes.
    """
    # 1. Database Identity
    id: int | None = None

    # 2. Core Data
    profile_name: str = Field(..., description="Technical hardware identifier")
    cpu_count: int = Field(..., description="VCPU count", gt=0)
    ram_gb: int = Field(..., description="RAM in GB", gt=0)

    # 3. Pipeline Metadata (Matching DimHardwareProfile)
    is_active: bool = Field(default=True)
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('profile_name', mode='before')
    @classmethod
    def standardize_name(cls, v: str) -> str:
        """Ensures hardware profiles are stored in lowercase."""
        if isinstance(v, str):
            return v.strip().lower()
        return v

    model_config = {
        "from_attributes": True, # Enables SQLModel compatibility
        "json_schema_extra": {
            "example": {
                "profile_name": "m5.xlarge",
                "cpu_count": 4,
                "ram_gb": 16,
                "is_active": True
            }
        }
    }
