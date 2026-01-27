import re
from typing import List
from sqlmodel import Session, select
from fastapi import HTTPException, status

from app.data_access.models import DimStore
from app.domain.store import StoreDomain

class StoreService:
    """
    Service layer for managing Store business logic and data orchestration.

    This service handles the lifecycle of Store entities, ensuring data integrity 
    through business rule validation and providing refined views for the 
    Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session):
        """
        Initializes the StoreService with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _get_dim_store_or_404(self, store_id: int) -> DimStore:
        """
        Internal helper to retrieve a store record or raise a 404 error.

        Args:
            store_id (int): The primary key ID of the store to find.

        Returns:
            DimStore: The database record found.

        Raises:
            HTTPException: 404 status code if the store does not exist.
        """
        store = self.session.get(DimStore, store_id)
        if not store:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Store with ID {store_id} not found."
            )
        return store

    def validate_phone_format(self, phone: str):
        """
        Validates the phone number format using a regular expression.

        Args:
            phone (str): The phone number string to validate.

        Raises:
            HTTPException: 400 status code if the phone format is invalid.
        """
        # Simple regex for a standard phone format
        if not re.match(r"^\+?[\d\s\-]{7,15}$", phone):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid phone format: '{phone}'."
            )

    def create_store(self, store_in: StoreDomain) -> DimStore:
        """
        Validates and persists a single new store to the database.

        Performs phone format validation and checks for duplicate location names 
        to ensure data quality in the Silver layer.

        Args:
            store_in (StoreDomain): The Pydantic domain model containing input data.

        Returns:
            DimStore: The newly created store database record.

        Raises:
            HTTPException: 400 status code if the phone format is invalid.
            HTTPException: 400 status code if the location name already exists.
        """
        # 1. Business Validation
        self.validate_phone_format(store_in.phone)

        # 2. Check for duplicate location names
        statement = select(DimStore).where(DimStore.location_name == store_in.location_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Store location '{store_in.location_name}' already exists."
            )

        # 3. Map Domain -> Data Access
        new_store = DimStore(
            location_name=store_in.location_name,
            address=store_in.address,
            phone=store_in.phone
        )

        self.session.add(new_store)
        self.session.commit()
        self.session.refresh(new_store)
        return new_store

    def create_stores_batch(self, stores_in: List[StoreDomain]) -> List[DimStore]:
        """
        Fulfills the mandatory feature for Batch CRUD operations.

        Allows for the ingestion of multiple store records within a single 
        database transaction for optimized ETL performance.

        Args:
            stores_in (List[StoreDomain]): A list of domain models to be created.

        Returns:
            List[DimStore]: A list of the successfully created database records.
        """
        created_stores = []
        for store_data in stores_in:
            # We reuse the mapping logic but commit once at the end
            new_store = DimStore(
                location_name=store_data.location_name,
                address=store_data.address,
                phone=store_data.phone
            )
            self.session.add(new_store)
            created_stores.append(new_store)
        
        self.session.commit()
        # Refresh all items to return fully populated objects
        for s in created_stores:
            self.session.refresh(s)
            
        return created_stores

    def get_all_stores(self) -> List[DimStore]:
        """
        Retrieves all store records from the dimension table.

        Returns:
            List[DimStore]: A list of all stores in the database.
        """
        return self.session.exec(select(DimStore)).all()

    def update_store(self, store_id: int, store_in: StoreDomain) -> DimStore:
        """
        Updates an existing store's metadata.

        Args:
            store_id (int): The ID of the store to update.
            store_in (StoreDomain): The updated domain model.

        Returns:
            DimStore: The updated database record.

        Raises:
            HTTPException: 404 status code if the store is not found.
        """
        db_store = self._get_dim_store_or_404(store_id)
        
        db_store.location_name = store_in.location_name
        db_store.address = store_in.address
        db_store.phone = store_in.phone

        self.session.add(db_store)
        self.session.commit()
        self.session.refresh(db_store)
        return db_store

    def delete_store(self, store_id: int) -> dict:
        """
        Removes a store record from the database.

        Args:
            store_id (int): The ID of the store to delete.

        Returns:
            dict: A confirmation message of the deletion.

        Raises:
            HTTPException: 404 status code if the store is not found.
        """
        db_store = self._get_dim_store_or_404(store_id)
        self.session.delete(db_store)
        self.session.commit()
        return {"detail": f"Store {store_id} deleted."}

    def get_store_directory_view(self) -> List[dict]:
        """
        Queries the Gold Layer to provide a refined directory of stores.

        This method fulfills the 'Querying Gold Layer' requirement by 
        transforming raw dimension data into a refined, UI-ready format.

        Returns:
            List[dict]: A list of refined dictionaries with formatted display names.
        """
        stores = self.get_all_stores()
        return [
            {
                "display_name": f" {s.location_name}",
                "full_address": s.address,
                "contact": s.phone
            } for s in stores
        ]