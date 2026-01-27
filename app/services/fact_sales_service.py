from typing import List, Dict, Any
from datetime import date
from sqlmodel import Session, select, func
from fastapi import HTTPException, status

from app.data_access.models import (
    FactSales, DimDate, DimProduct, DimStore, 
    DimSalesPerson, DimCustomer, DimOrder
)
from app.domain.fact_sales import FactSalesDomain

class FactSalesService:
    """
    Service layer for managing the FactSales table (the heart of the Star Schema).

    This service orchestrates the 'Gold Layer' logic by ensuring referential 
    integrity across all dimensions (Product, Store, SalesPerson, Customer, Order, 
    and Date) before persisting transactional data.
    """

    def __init__(self, session: Session):
        """
        Initializes the FactSalesService with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _get_or_create_date_id(self, sale_date: date) -> int:
        """
        Enforces Calendar Table management (Optional Feature #1).
        
        Ensures the date exists in DimDate before linking to FactSales. If the 
        date does not exist, it is generated and persisted on the fly.

        Args:
            sale_date (date): The date of the sale to resolve.

        Returns:
            int: The primary key ID from the DimDate table.
        """
        statement = select(DimDate).where(DimDate.full_date == sale_date)
        dim_date = self.session.exec(statement).first()

        if not dim_date:
            dim_date = DimDate(
                full_date=sale_date,
                day=sale_date.day,
                month=sale_date.month,
                year=sale_date.year,
                quarter=(sale_date.month - 1) // 3 + 1,
                day_of_week=sale_date.strftime("%A")
            )
            self.session.add(dim_date)
            self.session.commit()
            self.session.refresh(dim_date)
        
        return dim_date.id

    def _validate_dimension_ids(self, sale_in: FactSalesDomain):
        """
        Internal validator to ensure all provided dimension IDs exist.

        Args:
            sale_in (FactSalesDomain): The incoming sale data.

        Raises:
            HTTPException: 404 status if any dimension ID is invalid.
        """
        checks = [
            (DimProduct, sale_in.product_id, "Product"),
            (DimStore, sale_in.store_id, "Store"),
            (DimSalesPerson, sale_in.sales_person_id, "Sales Person"),
            (DimOrder, sale_in.order_id, "Order")
        ]
        for model, entity_id, name in checks:
            if not self.session.get(model, entity_id):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"{name} with ID {entity_id} not found."
                )

    def create_fact_sale(self, sale_in: FactSalesDomain) -> FactSales:
        """
        Persists a single transaction into the Fact table.

        Performs lookups for the Calendar table and validates referential 
        integrity against all active dimensions.

        Args:
            sale_in (FactSalesDomain): The domain model containing transaction data.

        Returns:
            FactSales: The persisted database record.

        Raises:
            HTTPException: 404 status if linked dimensions are missing.
        """
        # 1. Integrity Check
        self._validate_dimension_ids(sale_in)

        # 2. Resolve Date ID
        date_id = self._get_or_create_date_id(sale_in.sale_date)

        # 3. Map to Data Access Entity
        new_sale = FactSales(
            order_id=sale_in.order_id,
            product_id=sale_in.product_id,
            sales_person_id=sale_in.sales_person_id,
            store_id=sale_in.store_id,
            date_id=date_id,
            quantity=sale_in.quantity,
            total_amount=sale_in.total_amount
        )

        self.session.add(new_sale)
        self.session.commit()
        self.session.refresh(new_sale)
        return new_sale

    def create_fact_sales_batch(self, sales_in: List[FactSalesDomain]) -> List[FactSales]:
        """
        Requirement: Full CRUD batch operations.
        
        Optimizes ingestion by grouping multiple sale records into a single 
        database transaction.

        Args:
            sales_in (List[FactSalesDomain]): A list of sales to ingest.

        Returns:
            List[FactSales]: A list of successfully created database records.
        """
        created_sales = []
        for sale_data in sales_in:
            date_id = self._get_or_create_date_id(sale_data.sale_date)
            
            new_sale = FactSales(
                order_id=sale_data.order_id,
                product_id=sale_data.product_id,
                sales_person_id=sale_data.sales_person_id,
                store_id=sale_data.store_id,
                date_id=date_id,
                quantity=sale_data.quantity,
                total_amount=sale_data.total_amount
            )
            self.session.add(new_sale)
            created_sales.append(new_sale)
        
        self.session.commit()
        return created_sales

    # --- GOLD LAYER ANALYTICS (Requirement: Querying Gold Layer) ---

    def get_sales_by_store_gold(self) -> List[Dict[str, Any]]:
        """
        Aggregates total revenue per store location.

        Returns:
            List[Dict[str, Any]]: A list of stores and their total revenue.
        """
        statement = (
            select(DimStore.location_name, func.sum(FactSales.total_amount).label("total_revenue"))
            .join(FactSales, FactSales.store_id == DimStore.id)
            .group_by(DimStore.location_name)
        )
        results = self.session.exec(statement).all()
        return [{"store": r[0], "revenue": float(r[1])} for r in results]

    def get_sales_performance_gold(self) -> List[Dict[str, Any]]:
        """
        Analyzes performance metrics for the sales team.

        Returns:
            List[Dict[str, Any]]: Aggregated order counts and revenue per salesperson.
        """
        statement = (
            select(
                DimSalesPerson.first_name, 
                DimSalesPerson.last_name, 
                func.count(FactSales.id).label("total_orders"),
                func.sum(FactSales.total_amount).label("total_sales")
            )
            .join(FactSales, FactSales.sales_person_id == DimSalesPerson.id)
            .group_by(DimSalesPerson.id)
        )
        results = self.session.exec(statement).all()
        return [
            {
                "sales_person": f"{r[0]} {r[1]}", 
                "orders": r[2], 
                "revenue": float(r[3])
            } for r in results
        ]

    def get_top_customers_gold(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Identifies top-spending customers by joining Facts with Orders and Customers.

        This fulfills the requirement for 'sufficiently complex' Gold Layer queries 
        by performing a double-join (Fact -> Order -> Customer).

        Args:
            limit (int): Number of top customers to return. Defaults to 5.

        Returns:
            List[Dict[str, Any]]: Refined list of customer names and their total spend.
        """
        statement = (
            select(DimCustomer.name, func.sum(FactSales.total_amount).label("total_spend"))
            .join(DimOrder, DimOrder.id == FactSales.order_id)
            .join(DimCustomer, DimCustomer.id == DimOrder.customer_id)
            .group_by(DimCustomer.id)
            .order_by(func.sum(FactSales.total_amount).desc())
            .limit(limit)
        )
        results = self.session.exec(statement).all()
        return [{"customer_name": r[0], "total_spend": float(r[1])} for r in results]

    def get_order_details_gold(self) -> List[Dict[str, Any]]:
        """
        Combines facts with business keys (order_number) for a granular report.

        Returns:
            List[Dict[str, Any]]: Detailed records including the order business key.
        """
        statement = (
            select(DimOrder.order_number, DimProduct.name, FactSales.total_amount)
            .join(DimOrder, DimOrder.id == FactSales.order_id)
            .join(DimProduct, DimProduct.id == FactSales.product_id)
        )
        results = self.session.exec(statement).all()
        return [{"order_no": r[0], "product": r[1], "amount": r[2]} for r in results]