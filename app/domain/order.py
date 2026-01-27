import re
from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import date

class OrderDomain(BaseModel):
    """
    Pure Domain representation of a Sales Order.

    This entity acts as the business contract for order metadata, linking 
    customers to specific transactions. It ensures that all order-related 
    attributes are validated and formatted correctly before being persisted 
    in the Silver and Gold layers of the warehouse.

    Attributes:
        customer_id (int): The unique identifier of the customer.
        order_date (date): The date the order was placed.
        order_number (str): The unique business key for the order (e.g., ORD-1001).
        order_status (str): The current state of the order.
        payment_method (str): The method used for transaction payment.
        shipping_priority (Optional[str]): The priority level for fulfillment.
    """
    customer_id: int = Field(..., description="Reference ID to the Customer dimension", gt=0)
    order_date: date = Field(..., description="The date the order was officially placed")
    order_number: str = Field(..., description="Unique Business Key (Format: ORD-XXXX)")
    order_status: str = Field(..., description="Status (Pending, Completed, Shipped, Cancelled)")
    payment_method: str = Field(..., description="Payment type (e.g., Credit Card, PayPal)")
    shipping_priority: Optional[str] = Field(None, description="Shipping level (Low, Medium, High)")

    @field_validator('order_number')
    @classmethod
    def validate_order_number(cls, v: str) -> str:
        """
        Validates the format of the order business key.

        Ensures the order number follows the standardized pattern 'ORD-' 
        followed by numbers and is stored in uppercase.

        Args:
            v (str): The raw order number string.

        Returns:
            str: The cleaned, uppercase order number.

        Raises:
            ValueError: If the order number does not match the required pattern.
        """
        v = v.strip().upper()
        pattern = r"^ORD-\d+$"
        if not re.match(pattern, v):
            raise ValueError("order_number must follow the format 'ORD-1234'")
        return v

    @field_validator('order_status')
    @classmethod
    def format_status(cls, v: str) -> str:
        """
        Standardizes the casing of the order status.

        Args:
            v (str): The raw status string.

        Returns:
            str: The status string converted to Title Case (e.g., 'shipped' -> 'Shipped').
        """
        return v.strip().title()

    def is_finalized(self) -> bool:
        """
        Helper method to determine if an order is in a final state.

        Returns:
            bool: True if the status is 'Completed' or 'Cancelled', False otherwise.
        """
        return self.order_status in ["Completed", "Cancelled"]

    model_config = {
        "json_schema_extra": {
            "example": {
                "customer_id": 1,
                "order_date": "2024-01-20",
                "order_number": "ORD-1001",
                "order_status": "Pending",
                "payment_method": "Credit Card",
                "shipping_priority": "High"
            }
        }
    }