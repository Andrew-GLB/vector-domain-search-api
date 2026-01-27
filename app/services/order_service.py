from typing import List, Optional
from datetime import date
from sqlmodel import Session, select
from fastapi import HTTPException, status

from app.data_access.models import DimOrder, DimCustomer
from app.domain.order import OrderDomain

class OrderService:
    """
    Service layer for managing Order-related business logic.
    
    This service handles the orchestration between the API (Layer 1) and 
    the Data Access (Layer 4) for order dimensions. it ensures data integrity 
    by validating customer relationships and unique business keys.
    """

    def __init__(self, session: Session):
        """
        Initializes the OrderService with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _get_dim_order_or_404(self, order_id: int) -> DimOrder:
        """
        Internal helper to retrieve an order dimension record or raise a 404 error.

        Args:
            order_id (int): The primary key ID of the order to retrieve.

        Returns:
            DimOrder: The retrieved database model instance.

        Raises:
            HTTPException: 404 status if the order record does not exist.
        """
        order = self.session.get(DimOrder, order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order with ID {order_id} not found."
            )
        return order

    def _map_to_domain(self, order: DimOrder) -> OrderDomain:
        """
        Maps a Data Access entity (SQLModel) to a Domain entity (Pydantic).

        Args:
            order (DimOrder): The database record to be transformed.

        Returns:
            OrderDomain: The clean domain representation of the order.
        """
        return OrderDomain(
            customer_id=order.customer_id,
            order_date=order.order_date,
            order_number=order.order_number,
            order_status=order.order_status,
            payment_method=order.payment_method,
            shipping_priority=order.shipping_priority
        )

    def validate_order_constraints(self, order_in: OrderDomain):
        """
        Enforces business rules and referential integrity for orders.

        Validates the order status against a whitelist, ensures the linked 
        customer exists in the warehouse, and checks for duplicate business keys.

        Args:
            order_in (OrderDomain): The domain model containing the proposed order data.

        Raises:
            HTTPException: 400 status if the order status is invalid.
            HTTPException: 404 status if the linked customer_id does not exist.
            HTTPException: 400 status if the order_number already exists (Business Key).
        """
        # 1. Validate status
        allowed_statuses = {"Completed", "Cancelled", "Pending", "Shipped"}
        if order_in.order_status not in allowed_statuses:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status '{order_in.order_status}'. Allowed: {allowed_statuses}"
            )

        # 2. Check if customer exists
        customer = self.session.get(DimCustomer, order_in.customer_id)
        if not customer:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Customer with ID {order_in.customer_id} does not exist."
            )

        # 3. Check for duplicate order_number (Business Key)
        statement = select(DimOrder).where(DimOrder.order_number == order_in.order_number)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Order number '{order_in.order_number}' already exists."
            )

    def create_order(self, order_in: OrderDomain) -> OrderDomain:
        """
        Validates and persists a single new order dimension record.

        This method ensures the domain model is mapped to the data access model 
        after all business constraints are satisfied.

        Args:
            order_in (OrderDomain): The validated OrderDomain object from the API.

        Returns:
            OrderDomain: The created order mapped back to a Domain object.

        Raises:
            HTTPException: 404 if the customer_id does not exist.
            HTTPException: 400 if the order_number is a duplicate or status is invalid.
        """
        self.validate_order_constraints(order_in)

        new_order = DimOrder(
            customer_id=order_in.customer_id,
            order_date=order_in.order_date,
            order_number=order_in.order_number,
            order_status=order_in.order_status,
            payment_method=order_in.payment_method,
            shipping_priority=order_in.shipping_priority
        )
        self.session.add(new_order)
        self.session.commit()
        self.session.refresh(new_order)
        return self._map_to_domain(new_order)

    def create_orders_batch(self, orders_in: List[OrderDomain]) -> List[OrderDomain]:
        """
        Performs a batch CRUD operation for order records.

        Fulfills the mandatory feature requirement for batch processing 
        within the REST API logic.

        Args:
            orders_in (List[OrderDomain]): A list of domain models to ingest.

        Returns:
            List[OrderDomain]: A list of the successfully created records as domain models.
        """
        created_dim_orders = []
        for o_data in orders_in:
            new_order = DimOrder(
                customer_id=o_data.customer_id,
                order_date=o_data.order_date,
                order_number=o_data.order_number,
                order_status=o_data.order_status,
                payment_method=o_data.payment_method,
                shipping_priority=o_data.shipping_priority
            )
            self.session.add(new_order)
            created_dim_orders.append(new_order)
        
        self.session.commit()
        return [self._map_to_domain(o) for o in created_dim_orders]

    def get_all_orders(self) -> List[OrderDomain]:
        """
        Retrieves all order records and maps them to the domain representation.

        Returns:
            List[OrderDomain]: A list of all orders formatted as domain entities.
        """
        orders = self.session.exec(select(DimOrder)).all()
        return [self._map_to_domain(o) for o in orders]

    def get_order_by_id(self, order_id: int) -> OrderDomain:
        """
        Retrieves a single order by its primary key.

        Args:
            order_id (int): The ID of the order to find.

        Returns:
            OrderDomain: The domain representation of the found order.

        Raises:
            HTTPException: 404 if the order is not found.
        """
        db_order = self._get_dim_order_or_404(order_id)
        return self._map_to_domain(db_order)

    def update_order(self, order_id: int, order_in: OrderDomain) -> OrderDomain:
        """
        Updates an existing order record's metadata.

        Args:
            order_id (int): The ID of the order to update.
            order_in (OrderDomain): The new data to be applied.

        Returns:
            OrderDomain: The updated order as a domain model.

        Raises:
            HTTPException: 404 if the target order does not exist.
        """
        db_order = self._get_dim_order_or_404(order_id)
        
        db_order.customer_id = order_in.customer_id
        db_order.order_date = order_in.order_date
        db_order.order_number = order_in.order_number
        db_order.order_status = order_in.order_status
        db_order.payment_method = order_in.payment_method
        db_order.shipping_priority = order_in.shipping_priority

        self.session.add(db_order)
        self.session.commit()
        self.session.refresh(db_order)
        return self._map_to_domain(db_order)

    def delete_order(self, order_id: int) -> dict:
        """
        Removes an order record from the database.

        Args:
            order_id (int): The ID of the order to be deleted.

        Returns:
            dict: A confirmation message of the deletion.

        Raises:
            HTTPException: 404 if the order does not exist.
        """
        db_order = self._get_dim_order_or_404(order_id)
        self.session.delete(db_order)
        self.session.commit()
        return {"detail": f"Order {order_id} deleted."}

    def get_gold_order_analytics(self) -> dict:
        """
        Queries the Gold Layer to generate a refined analytical report of order data.

        Calculates status distributions and total counts to fulfill the 
        'Querying Gold Layer' requirement.

        Returns:
            dict: A refined dictionary containing total counts and status breakdowns.
        """
        orders = self.session.exec(select(DimOrder)).all()
        total = len(orders)
        if total == 0:
            return {"total": 0, "status_breakdown": {}}
            
        status_counts = {}
        for o in orders:
            status_counts[o.order_status] = status_counts.get(o.order_status, 0) + 1
            
        return {
            "total_count": total,
            "status_breakdown": status_counts,
            "report_label": "Order Metadata Summary"
        }