from pydantic import BaseModel, Field, field_validator
from datetime import date
from typing import Optional

class FactSalesDomain(BaseModel):
    """
    The pure domain representation of a Sales Transaction (Fact Table).

    In a Star Schema, this model represents the quantitative metrics (facts) 
    and the foreign keys required to link to all dimensions. It acts as the 
    contract for data entering the 'Gold Layer' of the warehouse.

    Attributes:
        product_id (int): Foreign key to the Product dimension.
        sales_person_id (int): Foreign key to the Sales Person dimension.
        store_id (int): Foreign key to the Store dimension.
        order_id (int): Foreign key to the Order dimension.
        customer_id (int): Foreign key to the Customer dimension (denormalized for performance).
        order_number (str): The human-readable business key for the order.
        sale_date (date): The calendar date the transaction occurred.
        quantity (int): Number of units sold. Must be greater than zero.
        unit_price (float): Price per individual unit.
        tax_amount (float): Tax applied to the transaction.
        discount_amount (float): Total discount applied.
        total_amount (float): The final calculated transaction value.
    """

    # Keys to Dimensions
    product_id: int = Field(..., description="ID of the sold product")
    sales_person_id: int = Field(..., description="ID of the salesperson involved")
    store_id: int = Field(..., description="ID of the store location")
    order_id: int = Field(..., description="ID of the linked order dimension")
    customer_id: int = Field(..., description="ID of the customer who made the purchase")
    
    # Business Keys
    order_number: str = Field(..., description="The business identifier for the order (e.g., ORD-1001)")
    
    # Quantitative Measures (The Facts)
    sale_date: date = Field(..., description="The date of the sale")
    quantity: int = Field(gt=0, description="The number of items sold must be greater than zero")
    unit_price: float = Field(ge=0, description="The price per unit")
    tax_amount: float = Field(default=0.0, ge=0)
    discount_amount: float = Field(default=0.0, ge=0)
    
    # Calculated Field
    total_amount: float = Field(..., description="The final transaction total after taxes and discounts")

    @field_validator('total_amount')
    @classmethod
    def validate_total_calculation(cls, v: float, info) -> float:
        """
        Validates that the total_amount is a positive value.

        Args:
            v (float): The total_amount value to validate.
            info: Contextual information about the validation.

        Returns:
            float: The validated total_amount.

        Raises:
            ValueError: If total_amount is negative.
        """
        if v < 0:
            raise ValueError("total_amount cannot be negative")
        return v

    def calculate_margin(self, cost_price: float) -> float:
        """
        Calculates the profit margin for the transaction.

        Args:
            cost_price (float): The base cost price of the product.

        Returns:
            float: The calculated profit margin (total_amount - (cost_price * quantity)).
        """
        return self.total_amount - (cost_price * self.quantity)

    class Config:
        """Pydantic configuration class."""
        from_attributes = True
        json_schema_extra = {
            "example": {
                "product_id": 1,
                "sales_person_id": 5,
                "store_id": 2,
                "order_id": 10,
                "customer_id": 100,
                "order_number": "ORD-1001",
                "sale_date": "2024-01-20",
                "quantity": 2,
                "unit_price": 50.0,
                "tax_amount": 5.0,
                "discount_amount": 0.0,
                "total_amount": 105.0
            }
        }