from pydantic import BaseModel, Field, field_validator
from typing import Optional

class CategoryDomain(BaseModel):
    """
    The pure domain representation of a Product Category.

    This entity serves as the high-level definition for data classification 
    within the Star Schema. It ensures that category metadata is cleaned 
    and validated before moving from the Silver to the Gold layer.

    Attributes:
        name (str): The unique name of the category.
        description (Optional[str]): A brief explanation of what the category includes.
    """

    name: str = Field(
        ..., 
        description="The unique name of the category (e.g., Electronics, Home & Garden)",
        min_length=2,
        max_length=50
    )
    description: Optional[str] = Field(
        None, 
        description="A detailed description of the category's scope"
    )

    @field_validator('name')
    @classmethod
    def clean_and_format_name(cls, v: str) -> str:
        """
        Cleans and formats the category name for consistency.

        Trims leading/trailing whitespace and ensures the name is stored 
        in Title Case to prevent duplicate entries in the Gold Layer.

        Args:
            v (str): The raw name string provided via the API or ETL.

        Returns:
            str: The cleaned and title-cased category name.

        Raises:
            ValueError: If the name is empty or contains only whitespace.
        """
        stripped = v.strip()
        if not stripped:
            raise ValueError("Category name cannot be empty or just whitespace.")
        
        return stripped.title()

    def get_slug(self) -> str:
        """
        Generates a URL-friendly slug for the category.

        Returns:
            str: A lowercase string with spaces replaced by hyphens.
        """
        return self.name.lower().replace(" ", "-")

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "Electronics",
                "description": "Devices, gadgets, and electronic hardware components."
            }
        }
    }