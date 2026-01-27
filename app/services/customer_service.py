from typing import List
from sqlmodel import Session, select
from fastapi import HTTPException, status
from app.data_access.models import DimCustomer
from app.domain.customer import CustomerDomain

class CustomerService:
    """
    Service layer for managing Customer-related business logic and database operations.

    This service acts as the intermediary between the API routes (Layer 1) and 
    the Data Access layer (Layer 4), ensuring that customer data is validated 
    and persisted according to the system's business rules.
    """

    def __init__(self, session: Session):
        """
        Initializes the CustomerService with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _get_dim_customer_or_404(self, customer_id: int) -> DimCustomer:
        """
        Internal helper to retrieve a customer database record or raise a 404 error.

        Args:
            customer_id (int): The primary key ID of the customer to find.

        Returns:
            DimCustomer: The database record found.

        Raises:
            HTTPException: 404 status code if the customer does not exist.
        """
        customer = self.session.get(DimCustomer, customer_id)
        if not customer:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Customer {customer_id} not found"
            )
        return customer

    def create_customer(self, customer_in: CustomerDomain) -> DimCustomer:
        """
        Validates and persists a single new customer to the database.

        Checks for email uniqueness as a business constraint before 
        mapping the Domain model to the Data Access model.

        Args:
            customer_in (CustomerDomain): The Pydantic domain model containing input data.

        Returns:
            DimCustomer: The newly created customer database record.

        Raises:
            HTTPException: 400 status code if the email is already registered.
        """
        statement = select(DimCustomer).where(DimCustomer.email == customer_in.email)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="Email already registered"
            )
        
        db_cust = DimCustomer(**customer_in.model_dump())
        self.session.add(db_cust)
        self.session.commit()
        self.session.refresh(db_cust)
        return db_cust

    def create_customers_batch(self, customers_in: List[CustomerDomain]) -> List[DimCustomer]:
        """
        Fulfills the mandatory feature for Batch CRUD operations.

        Allows for the bulk ingestion of multiple customer records within a 
        single database transaction.

        Args:
            customers_in (List[CustomerDomain]): A list of Pydantic domain models to ingest.

        Returns:
            List[DimCustomer]: A list of the newly created database records.
        """
        created_list = []
        for cust_data in customers_in:
            # Mapping Domain -> Data Access
            db_cust = DimCustomer(**cust_data.model_dump())
            self.session.add(db_cust)
            created_list.append(db_cust)
        
        self.session.commit()
        for c in created_list:
            self.session.refresh(c)
        return created_list

    def get_all_customers(self) -> List[DimCustomer]:
        """
        Retrieves all customer records stored in the dimension table.

        Returns:
            List[DimCustomer]: A list of all customer records in the database.
        """
        return self.session.exec(select(DimCustomer)).all()

    def get_customer_by_id(self, customer_id: int) -> DimCustomer:
        """
        Retrieves a single customer by their database ID.

        Args:
            customer_id (int): The ID of the customer to retrieve.

        Returns:
            DimCustomer: The retrieved customer database record.

        Raises:
            HTTPException: 404 status code if the customer is not found.
        """
        return self._get_dim_customer_or_404(customer_id)

    def update_customer(self, customer_id: int, customer_in: CustomerDomain) -> DimCustomer:
        """
        Updates an existing customer record's information.

        Args:
            customer_id (int): The ID of the customer to update.
            customer_in (CustomerDomain): The new data to apply.

        Returns:
            DimCustomer: The updated database record.

        Raises:
            HTTPException: 404 status code if the customer does not exist.
        """
        db_cust = self._get_dim_customer_or_404(customer_id)
        
        # Update fields dynamically based on the input model
        data = customer_in.model_dump(exclude_unset=True)
        for key, value in data.items():
            setattr(db_cust, key, value)
            
        self.session.add(db_cust)
        self.session.commit()
        self.session.refresh(db_cust)
        return db_cust

    def delete_customer(self, customer_id: int) -> dict:
        """
        Removes a customer record from the database.

        Args:
            customer_id (int): The ID of the customer to delete.

        Returns:
            dict: A confirmation message indicating the ID of the deleted customer.

        Raises:
            HTTPException: 404 status code if the customer does not exist.
        """
        db_cust = self._get_dim_customer_or_404(customer_id)
        self.session.delete(db_cust)
        self.session.commit()
        return {"detail": f"Customer {customer_id} deleted"}