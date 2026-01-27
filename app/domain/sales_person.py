from pydantic import BaseModel, EmailStr, Field, field_validator
from datetime import date

class SalesPersonDomain(BaseModel):
    """
    The pure domain representation of a Sales Person.

    This entity represents a member of the sales team within the Medallion 
    Architecture. It enforces data quality standards for contact information 
    and hire dates to ensure reliable reporting in the Gold Layer.

    Attributes:
        first_name (str): The salesperson's given name.
        last_name (str): The salesperson's family name.
        email (EmailStr): Validated work email address.
        hire_date (date): The date the employee joined the company.
    """

    first_name: str = Field(..., description="Given name", min_length=2)
    last_name: str = Field(..., description="Family name", min_length=2)
    email: EmailStr = Field(..., description="Unique work email address")
    hire_date: date = Field(..., description="Date of hire (cannot be in the future)")

    @field_validator('first_name', 'last_name')
    @classmethod
    def clean_and_format_names(cls, v: str) -> str:
        """
        Cleans and standardizes the name strings.

        Trims whitespace and applies Title Case to ensure consistency in 
        database records and analytical reports.

        Args:
            v (str): The raw name string.

        Returns:
            str: The cleaned and title-cased name.

        Raises:
            ValueError: If the name contains non-alphabetic characters.
        """
        stripped = v.strip()
        if not stripped.isalpha():
            # Note: In a real world app, you'd allow hyphens or spaces for composite names
            raise ValueError("Names must only contain alphabetic characters.")
        return stripped.title()

    @field_validator('hire_date')
    @classmethod
    def validate_hire_date(cls, v: date) -> date:
        """
        Validates that the hire date is not in the future.

        Args:
            v (date): The hire date to validate.

        Returns:
            date: The validated date.

        Raises:
            ValueError: If the date is later than the current system date.
        """
        if v > date.today():
            raise ValueError("Hire date cannot be in the future.")
        return v

    def get_full_name(self) -> str:
        """
        Returns the formatted full name of the salesperson.

        Returns:
            str: The combination of First and Last name.
        """
        return f"{self.first_name} {self.last_name}"

    model_config = {
        "json_schema_extra": {
            "example": {
                "first_name": "Michael",
                "last_name": "Scott",
                "email": "m.scott@dundermifflin.com",
                "hire_date": "2023-05-15"
            }
        }
    }