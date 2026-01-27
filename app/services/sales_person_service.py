from typing import List
from datetime import date
from sqlmodel import Session, select
from fastapi import HTTPException, status

from app.data_access.models import DimSalesPerson
from app.domain.sales_person import SalesPersonDomain

class SalesPersonService:
    """
    Service layer for managing Sales Person business logic and data orchestration.

    This service handles the lifecycle of Sales Person entities within the 
    Medallion Architecture, providing CRUD operations for the Silver layer 
    and refined analytical views for the Gold layer.
    """

    def __init__(self, session: Session):
        """
        Initializes the SalesPersonService with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _get_dim_sales_person_or_404(self, sales_person_id: int) -> DimSalesPerson:
        """
        Internal helper to retrieve a sales person record or raise a 404 error.

        Args:
            sales_person_id (int): The primary key ID of the sales person.

        Returns:
            DimSalesPerson: The retrieved database record.

        Raises:
            HTTPException: 404 status code if the sales person does not exist.
        """
        person = self.session.get(DimSalesPerson, sales_person_id)
        if not person:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Sales Person with ID {sales_person_id} not found."
            )
        return person

    def create_sales_person(self, person_in: SalesPersonDomain) -> DimSalesPerson:
        """
        Validates and persists a new Sales Person to the dimension table.

        Enforces business rules regarding unique email addresses and ensuring 
        hire dates are not set in the future.

        Args:
            person_in (SalesPersonDomain): The Pydantic domain model containing input data.

        Returns:
            DimSalesPerson: The newly created database record.

        Raises:
            HTTPException: 400 status code if the email already exists.
            HTTPException: 400 status code if the hire date is in the future.
        """
        # 1. Business Rule: Email must be unique in our system
        statement = select(DimSalesPerson).where(DimSalesPerson.email == person_in.email)
        existing = self.session.exec(statement).first()
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Sales person with email '{person_in.email}' already exists."
            )

        # 2. Business Rule: Hire date cannot be in the future
        if person_in.hire_date > date.today():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Hire date cannot be in the future."
            )

        # 3. Map Domain (Pydantic) -> Data Access (SQLModel)
        new_person = DimSalesPerson(
            first_name=person_in.first_name,
            last_name=person_in.last_name,
            email=person_in.email,
            hire_date=person_in.hire_date
        )

        self.session.add(new_person)
        self.session.commit()
        self.session.refresh(new_person)
        return new_person

    def get_all_sales_people(self) -> List[DimSalesPerson]:
        """
        Retrieves all sales people records from the database.

        Returns:
            List[DimSalesPerson]: A list of all sales person records.
        """
        return self.session.exec(select(DimSalesPerson)).all()

    def get_sales_person_by_id(self, sales_person_id: int) -> DimSalesPerson:
        """
        Retrieves a specific sales person by their unique identifier.

        Args:
            sales_person_id (int): The ID of the sales person to retrieve.

        Returns:
            DimSalesPerson: The retrieved database record.

        Raises:
            HTTPException: 404 status code if the sales person is not found.
        """
        return self._get_dim_sales_person_or_404(sales_person_id)

    def update_sales_person(self, sales_person_id: int, person_in: SalesPersonDomain) -> DimSalesPerson:
        """
        Updates the information of an existing sales person.

        Args:
            sales_person_id (int): The ID of the sales person to update.
            person_in (SalesPersonDomain): The updated domain data.

        Returns:
            DimSalesPerson: The updated database record.

        Raises:
            HTTPException: 404 status code if the target sales person does not exist.
        """
        db_person = self._get_dim_sales_person_or_404(sales_person_id)

        db_person.first_name = person_in.first_name
        db_person.last_name = person_in.last_name
        db_person.email = person_in.email
        db_person.hire_date = person_in.hire_date

        self.session.add(db_person)
        self.session.commit()
        self.session.refresh(db_person)
        return db_person

    def delete_sales_person(self, sales_person_id: int) -> dict:
        """
        Deletes a sales person record from the dimension table.

        Args:
            sales_person_id (int): The ID of the sales person to delete.

        Returns:
            dict: A confirmation dictionary with the deletion details.

        Raises:
            HTTPException: 404 status code if the sales person is not found.
        """
        db_person = self._get_dim_sales_person_or_404(sales_person_id)
        
        self.session.delete(db_person)
        self.session.commit()
        
        return {"detail": f"Sales Person {sales_person_id} deleted successfully."}

    def get_sales_team_performance_view(self) -> List[dict]:
        """
        Queries the Gold Layer to provide a refined view of sales team performance.

        This method applies logical transformations, such as calculating 
        tenure in years and formatting names for display. This fulfills 
        the 'Querying Gold Layer' requirement.

        Returns:
            List[dict]: A list of refined dictionaries containing sales performance metrics.
        """
        people = self.get_all_sales_people()
        today = date.today()
        
        results = []
        for p in people:
            tenure_years = today.year - p.hire_date.year
            results.append({
                "id": p.id,
                "full_name": f"{p.last_name.upper()}, {p.first_name}",
                "tenure": f"{tenure_years} years",
                "contact": p.email
            })
        return results