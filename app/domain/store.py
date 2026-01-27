import re
from pydantic import BaseModel, Field, field_validator
from typing import Optional

class StoreDomain(BaseModel):
    """
    The pure domain representation of a Store.

    This entity represents a physical location within the retail network. 
    It enforces data quality standards for contact information and 
    addresses to ensure consistent location-based reporting in the Gold Layer.

    Attributes:
        location_name (str): The unique name of the store location.
        address (str): The physical street address of the store.
        phone (str): Contact phone number for the store.
    """

    location_name: str = Field(
        ..., 
        description="The unique name of the store location (e.g., Downtown Flagship)",
        min_length=2
    )
    address: str = Field(
        ..., 
        description="The full physical address of the store"
    )
    phone: str = Field(
        ..., 
        description="Contact phone number in international format"
    )

    @field_validator('location_name', 'address')
    @classmethod
    def clean_strings(cls, v: str) -> str:
        """
        Standardizes string inputs by trimming whitespace and applying title case.

        Args:
            v (str): The raw string input from the API or ETL.

        Returns:
            str: The cleaned and formatted string.

        Raises:
            ValueError: If the string is empty or only contains whitespace.
        """
        stripped = v.strip()
        if not stripped:
            raise ValueError("Field cannot be empty or just whitespace.")
        return stripped.title()

    @field_validator('phone')
    @classmethod
    def validate_phone_format(cls, v: str) -> str:
        """
        Validates the store phone number using a standard regex pattern.

        Accepts digits, spaces, hyphens, and an optional leading plus sign.

        Args:
            v (str): The raw phone number string.

        Returns:
            str: The validated phone number.

        Raises:
            ValueError: If the phone number does not match the required format.
        """
        pattern = r"^\+?[\d\s\-]{7,15}$"
        if not re.match(pattern, v):
            raise ValueError("Invalid phone format. Must be 7-15 digits (spaces/hyphens allowed).")
        return v

    def get_display_label(self) -> str:
        """
        Generates a formatted label for use in UI components or reports.

        Returns:
            str: A string combining the location name and phone (e.g., 'DOWNTOWN [555-0101]').
        """
        return f"{self.location_name.upper()} [{self.phone}]"

    model_config = {
        "json_schema_extra": {
            "example": {
                "location_name": "Downtown Flagship",
                "address": "123 Broadway, New York, NY",
                "phone": "+1-555-0101"
            }
        }
    }