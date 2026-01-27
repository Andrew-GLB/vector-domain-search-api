import logging
from typing import Any, cast  # Added cast

from fastapi import HTTPException, status
from sqlmodel import Session, col, select  # Added col

# Layer 4: Data Access (Gold Layer)
from app.data_access.models import DimServiceType

# Layer 3: Domain Entities
from app.domain.service_type import ServiceTypeDomain


logger = logging.getLogger(__name__)

class ServiceTypeService:
    """Service layer for managing Cloud Service Type dimensions.

    This service coordinates the classification of cloud resources, ensuring
    technical identifiers are standardized (uppercase) and categorized correctly
    within the Gold Layer Warehouse.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel session.
        """
        self.session = session

    def _map_to_domain(self, db_service: DimServiceType) -> ServiceTypeDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_service (DimServiceType): The database record.

        Returns:
            ServiceTypeDomain: The Pydantic domain representation.
        """
        return ServiceTypeDomain(
            service_name=db_service.service_name,
            # Fix: Use cast to satisfy Mypy strict Literal checking
            category=cast(Any, db_service.category),
            is_managed=db_service.is_managed
        )

    def _get_dim_service_or_404(self, service_id: int) -> DimServiceType:
        """Internal helper to retrieve a service record or raise a 404 error.

        Args:
            service_id (int): The primary key ID.

        Returns:
            DimServiceType: The database record found.
        """
        service_obj = self.session.get(DimServiceType, service_id)
        if not service_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Service Type with ID {service_id} not found."
            )
        return service_obj

    def create_service_type(self, service_in: ServiceTypeDomain) -> ServiceTypeDomain:
        """Validates business rules and persists a new Cloud Service Type.

        Args:
            service_in (ServiceTypeDomain): Input data from the API/ETL.

        Returns:
            ServiceTypeDomain: The created service type.
        """
        # 1. Idempotency Check (Standardized Name)
        statement = select(DimServiceType).where(
            DimServiceType.service_name == service_in.service_name
        )
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Service '{service_in.service_name}' already exists in the catalog."
            )

        # 2. Map and Persist to Gold Layer
        new_db_service = DimServiceType(
            service_name=service_in.service_name,
            category=service_in.category,
            is_managed=service_in.is_managed
        )

        self.session.add(new_db_service)
        self.session.commit()
        self.session.refresh(new_db_service)

        logger.info(f"Registered new service type: {new_db_service.service_name}")
        return self._map_to_domain(new_db_service)

    def create_service_types_batch(self, services_in: list[ServiceTypeDomain]) -> list[ServiceTypeDomain]:
        """Requirement: Batch CRUD operations.
        
        Optimizes ingestion for bulk CSV uploads or initial seeding.
        """
        db_entries = [
            DimServiceType(
                service_name=s.service_name,
                category=s.category,
                is_managed=s.is_managed
            ) for s in services_in
        ]

        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return services_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Service Type load failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process batch service type ingestion."
            )

    def get_all_service_types(self) -> list[ServiceTypeDomain]:
        """Retrieves the full catalog of service classifications."""
        statement = select(DimServiceType)
        results = self.session.exec(statement).all()
        return [self._map_to_domain(s) for s in results]

    def get_service_type(self, service_id: int) -> ServiceTypeDomain:
        """Retrieves a single service classification by ID."""
        db_service = self._get_dim_service_or_404(service_id)
        return self._map_to_domain(db_service)

    def update_service_type(self, service_id: int, service_in: ServiceTypeDomain) -> ServiceTypeDomain:
        """Updates an existing service definition (e.g., changing managed status)."""
        db_service = self._get_dim_service_or_404(service_id)

        db_service.category = service_in.category
        db_service.is_managed = service_in.is_managed

        self.session.add(db_service)
        self.session.commit()
        self.session.refresh(db_service)
        return self._map_to_domain(db_service)

    def delete_service_type(self, id: int) -> None:
        """Removes a service classification record from the dimension table.

        This operation deletes the physical record from the Silver layer.
        Note: In a production environment, this should be preceded by a
        check for existing metrics linked to this service type.

        Args:
            id (int): The primary key ID of the service type to delete.

        Returns:
            None: Returns nothing on successful deletion.

        Raises:
            HTTPException: 404 status code if the service ID does not exist,
                triggered by the internal helper.
        """
        # 1. Retrieve the object or fail immediately with 404
        # Fix: Call existing _get_dim_service_or_404 helper
        db_service = self._get_dim_service_or_404(id)

        # 2. Perform the deletion
        self.session.delete(db_service)

        # 3. Commit the transaction
        self.session.commit()

    def update_service_types_batch(self, services_in: list[ServiceTypeDomain]) -> list[ServiceTypeDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple service types using 'service_name' as the business key.
        
        Ensures atomicity: if one service_name is not found, the entire batch
        transaction is rolled back.

        Args:
            services_in (List[ServiceTypeDomain]): List of updated service type data.

        Returns:
            List[ServiceTypeDomain]: The original input list on success.

        Raises:
            HTTPException: 404 status if a specific service name is not found.
            HTTPException: 500 status if a database error occurs.
        """
        try:
            for s_data in services_in:
                # 1. Lookup by unique business key: service_name
                statement = select(DimServiceType).where(DimServiceType.service_name == s_data.service_name)
                db_service = self.session.exec(statement).first()

                # 2. If not found, abort the whole batch to ensure data integrity
                if not db_service:
                    self.session.rollback()
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Service type '{s_data.service_name}' not found. Batch aborted."
                    )

                # 3. Apply updates to the Silver layer model
                db_service.category = s_data.category
                db_service.is_managed = s_data.is_managed

                self.session.add(db_service)

            # 4. Finalize the transaction
            self.session.commit()
            logger.info(f"Successfully updated batch of {len(services_in)} service types.")
            return services_in

        except HTTPException:
            # Re-raise the 404 we created above
            raise
        except Exception as e:
            # Handle unexpected database or connection errors
            self.session.rollback()
            logger.error(f"Batch Service Type update failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update."
            )

    def delete_service_types_batch(self, ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes multiple service types by their primary key IDs.
        
        Ensures atomicity: If any provided ID is not found in the database,
        the entire transaction is rolled back.

        Args:
            ids (List[int]): List of primary key IDs to be removed.

        Returns:
            None: Returns nothing on successful completion.

        Raises:
            HTTPException: 404 status if one or more IDs are not found.
            HTTPException: 500 status if a database error occurs.
        """
        try:
            # 1. Fetch all matching records in a single query (Efficiency)
            # Fix: Use col() to ensure Mypy recognizes attributes on Optional field
            statement = select(DimServiceType).where(col(DimServiceType.id).in_(ids))
            items_to_delete = self.session.exec(statement).all()

            # 2. Validation: Ensure every ID provided actually exists (Integrity)
            if len(items_to_delete) != len(ids):
                # Calculate which IDs were missing for a better error message
                found_ids = {item.id for item in items_to_delete}
                missing_ids = set(ids) - found_ids

                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch delete aborted. IDs not found: {list(missing_ids)}"
                )

            # 3. Perform deletions
            for item in items_to_delete:
                self.session.delete(item)

            # 4. Commit as a single transaction
            self.session.commit()
            logger.info(f"Successfully deleted batch of {len(items_to_delete)} service types.")

        except HTTPException:
            # Re-raise the 404 from the validation step
            raise
        except Exception as e:
            # Handle unexpected DB errors
            self.session.rollback()
            logger.error(f"Batch Service Type deletion failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deletion."
            )

    # --- GOLD LAYER ANALYTICS ---

    def get_infrastructure_managed_report_gold(self) -> list[dict[str, Any]]:
        """Requirement: Querying Gold Layer.
        
        Refines catalog data to provide insights into operational responsibility
        and infrastructure complexity.
        """
        services = self.get_all_service_types()
        return [
            {
                "technical_id": s.service_name,
                "domain": s.category,
                "responsibility_model": "Provider Managed (PaaS/SaaS)" if s.is_managed else "Customer Managed (IaaS)",
                "operational_complexity": "Low" if s.is_managed else "High"
            } for s in services
        ]
