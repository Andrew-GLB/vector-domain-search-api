from typing import Optional, List
from sqlmodel import Field, SQLModel, Relationship
from datetime import date

# --- Dimension Tables ---

class DimCategory(SQLModel, table=True):
    # This line explicitly sets the table name in SQLite
    __tablename__ = "dim_category"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True)

class DimProduct(SQLModel, table=True):
    # This line explicitly sets the table name in SQLite
    __tablename__ = "dim_product"
    id: Optional[int] = Field(default=None, primary_key=True)
    sku: str = Field(unique=True, index=True)
    name: str
    description: str
    price: float
    category_id: int = Field(foreign_key="dim_category.id")

class DimSalesPerson(SQLModel, table=True):
    # This line explicitly sets the table name in SQLite
    __tablename__ = "dim_sales_person"    
    id: Optional[int] = Field(default=None, primary_key=True)
    first_name: str
    last_name: str
    email: str
    hire_date: date

class DimStore(SQLModel, table=True):
    # This line explicitly sets the table name in SQLite
    __tablename__ = "dim_store"        
    id: Optional[int] = Field(default=None, primary_key=True)
    location_name: str
    address: str
    phone: str

class DimCustomer(SQLModel, table=True):
    __tablename__ = "dim_customer"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    email: str = Field(index=True, unique=True)
    phone: Optional[str] = None
    address: Optional[str] = None   

class DimOrder(SQLModel, table=True):
    __tablename__ = "dim_order"
    id: Optional[int] = Field(default=None, primary_key=True)
    order_number: str = Field(index=True, unique=True) # Business Key for ETL
    customer_id: int = Field(foreign_key="dim_customer.id")
    product_id: int = Field(foreign_key="dim_product.id") # Added to link order to product
    store_id: int = Field(foreign_key="dim_store.id")     # Added to link order to store
    sales_person_id: int = Field(foreign_key="dim_sales_person.id") # Added
    order_date: date
    quantity: int = Field(gt=0) 
    unit_price: float           
    order_status: str # e.g., 'Completed', 'Cancelled', 'Pending'
    payment_method: str # e.g., 'Credit Card', 'Cash', 'PayPal'
    shipping_priority: Optional[str] = None

class DimDate(SQLModel, table=True):
    # This line explicitly sets the table name in SQLite
    __tablename__ = "dim_date"       
    id: Optional[int] = Field(default=None, primary_key=True)
    full_date: str = Field(index=True)  # e.g., "2024-01-21"
    year: int
    month: int
    month_name: str                     # e.g., "January"
    day: int
    day_of_week: int                    # 1-7
    day_name: str                       # e.g., "Wednesday"
    quarter: int                        # 1-4
    is_weekend: bool

# --- Fact Table ---
class FactSales(SQLModel):
    """
    Requirement: Fact Table (implemented as a SQL View).
    This model does NOT have table=True because it is a Virtual View.
    """
    order_id: int
    product_id: int
    customer_id: int
    store_id: int
    sales_person_id: int
    date_id: Optional[int]
    order_number: str
    quantity: int
    unit_price: float
    total_amount: float # Calculated in the View (qty * price)