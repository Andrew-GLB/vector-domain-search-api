import re
from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator


class AssetDomain(BaseModel):
    """The pure domain representation of a Cloud Assets Infrastructure.

    This anchor asset represents a specific resource (e.g., a Server, Database,
    or Storage Bucket) within the system. It enforces strict business rules
    for resource identification and metadata formatting to ensure a clean
    'Gold Layer' in the warehouse.

    Attributes:
        resource_name (str): Human-readable name of the cloud resource.
        serial_number (str): Unique business key (Format: RES-XXXX-YYYY).
        description (str): Detailed metadata about the asset's purpose.
        created_at (date): The date the resource was officially provisioned.
    """

    # 1. Add ID to track back to Supabase/Postgres
    id: int | None = None

    # Core Data
    resource_name: str = Field(
        ...,
        description="The human-readable name of the resource",
        min_length=3
    )
    serial_number: str = Field(
        ...,
        description="Unique Business Key (Format: RES-XXXX-YYYY)"
    )
    description: str = Field(
        ...,
        description="Detailed metadata about the resource usage"
    )
    created_at: date = Field(
        ...,
        description="Provisioning date of the asset"
    )

    # 2. Add Pipeline Metadata to match DimAsset
    is_active: bool = True
    source_timestamp: datetime | None = None
    updated_at: datetime | None = None

    @field_validator('serial_number')
    @classmethod
    def validate_serial_format(cls, v: str) -> str:
        """Validates the Resource Serial Number format.

        The serial must follow the 'RES-XXXX-YYYY' pattern where X and Y
        are alphanumeric characters. This is the primary business key
        used for Star Schema joins.

        Args:
            v (str): The raw serial number string.

        Returns:
            str: The cleaned, uppercase serial number.

        Raises:
            ValueError: If the serial does not match the required pattern.
        """
        v = v.strip().upper()
        pattern = r"^RES-[A-Z0-9]{4}-[A-Z0-9]{4}$"
        if not re.match(pattern, v):
            raise ValueError("serial_number must follow format 'RES-XXXX-YYYY'")
        return v

    @field_validator('resource_name')
    @classmethod
    def clean_resource_name(cls, v: str) -> str:
        """Standardizes the resource name by stripping whitespace and title-casing.

        Args:
            v (str): The raw name input.

        Returns:
            str: The cleaned and formatted name.
        """
        return v.strip().title()

    def get_search_summary(self) -> str:
        """Provides a combined string for quick search indexing.

        Returns:
            str: A summary of the asset (e.g., 'Production-DB [RES-1234-5678]').
        """
        return f"{self.resource_name} [{self.serial_number}]"

    # 3. Enable ORM mode for SQLModel compatibility
    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "resource_name": "Primary Production Database",
                "serial_number": "RES-SQLS-2024",
                "description": "Main SQL Server instance.",
                "created_at": "2024-01-15",
                "is_active": True
            }
        }
    }
