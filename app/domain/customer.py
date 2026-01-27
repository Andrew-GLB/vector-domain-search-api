from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional

class CustomerDomain(BaseModel):
    """
    The pure domain representation of a Customer.

    This entity acts as a contract for customer data across the system layers.
    It includes basic contact information and enforces business rules for 
    data integrity, such as email format validation.

    Attributes:
        name (str): The full name of the customer.
        email (EmailStr): A validated email address.
        phone (Optional[str]): Contact phone number.
        address (Optional[str]): Physical or billing address.
    """
    
    name: str = Field(..., description="The full name of the customer", min_length=2)
    email: EmailStr = Field(..., description="The unique email address of the customer")
    phone: Optional[str] = Field(None, description="Contact phone number in international format")
    address: Optional[str] = Field(None, description="The customer's primary residential or business address")

    @field_validator('name')
    @classmethod
    def name_must_be_capitalized(cls, v: str) -> str:
        """
        Validates that the customer name starts with a capital letter.

        This business rule ensures consistent data formatting in the 
        Gold Layer reporting.

        Args:
            v (str): The name string to be validated.

        Returns:
            str: The validated and potentially modified name string.

        Raises:
            ValueError: If the name is empty or starts with a lowercase letter.
        """
        if not v or not v[0].isupper():
            raise ValueError("Customer name must start with a capital letter")
        return v

    def get_masked_email(self) -> str:
        """
        Provides a masked version of the email for privacy in logs.

        Returns:
            str: The masked email address (e.g., j***@example.com).
        """
        parts = self.email.split("@")
        if len(parts[0]) <= 1:
            return f"*@{parts[1]}"
        return f"{parts[0][0]}***@{parts[1]}"

    model_config = {
        "json_schema_extra": {
            "example": {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "phone": "+1-555-0101",
                "address": "123 Maple St. New York, NY"
            }
        }
    }