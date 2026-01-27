import os
from sqlmodel import create_engine, SQLModel, Session, text
from typing import Generator

# Configuration: Ensuring the directory exists
DB_PATH = "./data/gold/warehouse.db"
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

DATABASE_URL = f"sqlite:///{DB_PATH}"

# echo=True allows you to see the SQL generated in the terminal (Good for the challenge review)
engine = create_engine(DATABASE_URL, echo=True)

def create_db_and_tables():
    """
    Creates the physical tables and the virtual Fact View.
    
    Requirement: REST API should create necessary schemas if they don't exist.
    This fulfills the 'Gold Layer' virtualization using a SQL View.
    """
    # 1. Create all physical Dimension and Order tables defined in models.py
    SQLModel.metadata.create_all(engine)

    # 2. Define the Virtual Fact Table (Gold Layer View)
    # This logic calculates total_amount and resolves the date_id on the fly.
    view_sql = """
    CREATE VIEW IF NOT EXISTS fact_sales AS
    SELECT 
        o.id AS order_id,
        o.product_id,
        o.customer_id,
        o.store_id,
        o.sales_person_id,
        d.id AS date_id,
        o.order_number,
        o.quantity,
        o.unit_price,
        (o.quantity * o.unit_price) AS total_amount
    FROM dim_order o
    LEFT JOIN dim_date d ON o.order_date = d.full_date;
    """

    # 3. Execute the view creation
    with Session(engine) as session:
        session.exec(text(view_sql))
        session.commit()

def get_session() -> Generator:
    """
    Dependency for FastAPI to provide a database session per request.
    
    Yields:
        Session: The SQLModel session.
    """
    with Session(engine) as session:
        yield session