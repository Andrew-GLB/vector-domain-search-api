import re
from pydantic import BaseModel, Field, field_validator
from typing import Optional

class ProductDomain(BaseModel):
    """
    The pure domain representation of a Product.

    This entity acts as the primary contract for product data. It enforces 
    strict business rules for SKUs and pricing to ensure that only high-quality 
    data reaches the Gold Layer of the Medallion Architecture.

    Attributes:
        sku (str): Unique stock keeping unit, must follow format PROD-XXXX.
        name (str): The name of the product.
        description (str): Detailed product description.
        price (float): The unit price of the product (must be non-negative).
        category_name (str): The name of the associated category.
    """

    sku: str = Field(..., description="Unique Business Key (Format: PROD-XXXX)")
    name: str = Field(..., description="Name of the product", min_length=2)
    description: str = Field(..., description="Full product description")
    price: float = Field(..., description="Unit price in USD", gt=0)
    category_name: str = Field(..., description="Name of the dimension category")

    @field_validator('sku')
    @classmethod
    def validate_sku_format(cls, v: str) -> str:
        """
        Validates that the SKU follows the required business format.

        The format must be 'PROD-' followed by exactly 4 digits. The 
        input is automatically converted to uppercase.

        Args:
            v (str): The raw SKU string.

        Returns:
            str: The cleaned, uppercase SKU.

        Raises:
            ValueError: If the SKU does not match the regex pattern PROD-XXXX.
        """
        v = v.strip().upper()
        pattern = r"^PROD-\d{4}$"
        if not re.match(pattern, v):
            raise ValueError("SKU must follow the format 'PROD-XXXX' (e.g., PROD-1234)")
        return v

    @field_validator('name', 'category_name')
    @classmethod
    def clean_strings(cls, v: str) -> str:
        """
        Standardizes string inputs by trimming whitespace and applying title case.

        Args:
            v (str): The raw string input.

        Returns:
            str: The cleaned and formatted string.
        """
        return v.strip().title()

    def get_formatted_price(self) -> str:
        """
        Returns the product price as a human-readable currency string.

        Returns:
            str: The price formatted as '$XX.XX'.
        """
        return f"${self.price:,.2f}"

    def apply_discount(self, percent: float) -> float:
        """
        Calculates a discounted price based on a percentage.

        Args:
            percent (float): The discount percentage (e.g., 20.0 for 20%).

        Returns:
            float: The new price after the discount is applied.

        Raises:
            ValueError: If the percentage is not between 0 and 100.
        """
        if not (0 <= percent <= 100):
            raise ValueError("Discount percentage must be between 0 and 100")
        return round(self.price * (1 - (percent / 100)), 2)

    model_config = {
        "json_schema_extra": {
            "example": {
                "sku": "PROD-1001",
                "name": "Wireless Mouse",
                "description": "Ergonomic 2.4GHz wireless optical mouse.",
                "price": 25.99,
                "category_name": "Electronics"
            }
        }
    }