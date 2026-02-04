import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access (Gold Layer)
from app.data_access.models import DimServiceType

# Layer 3: Domain Entities
from app.domain.service_type import ServiceTypeDomain


logger = logging.getLogger(__name__)

class ServiceTypeService:
    """Service layer for managing Cloud Service Type dimensions.

    This service coordinates the classification of cloud resources, ensuring
    technical identifiers are standardized and categorized correctly
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
        return ServiceTypeDomain.model_validate(db_service)

    def _get_dim_service_or_404(self, id: int) -> DimServiceType:
        """Internal helper to retrieve an active service record or raise a 404 error.

        Args:
            id (int): The primary key ID.

        Returns:
            DimServiceType: The database record found.

        Raises:
            HTTPException: 404 status if not found or inactive.
        """
        # 1. Build statement filtering by ID and active status
        statement = select(DimServiceType).where(
            DimServiceType.id == id,
            col(DimServiceType.is_active)
        )
        service_obj = self.session.exec(statement).first()

        # 2. Raise 404 if record is missing or soft-deleted
        if not service_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Service Type with ID {id} not found."
            )
        return service_obj

    # --- 1. create_service_type ---
    def create_service_type(self, service_in: ServiceTypeDomain) -> ServiceTypeDomain:
        """Full CRUD: Validates business rules and persists a new Cloud Service Type.

        Args:
            service_in (ServiceTypeDomain): Input data from the API/ETL.

        Returns:
            ServiceTypeDomain: The created service type.
        """
        # 1. Idempotency Check (Business Key)
        statement = select(DimServiceType).where(
            DimServiceType.service_name == service_in.service_name
        )
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Service '{service_in.service_name}' already exists in the catalog."
            )

        # 2. Extract data excluding system-managed fields
        service_data = service_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_db_service = DimServiceType(**service_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_db_service.source_timestamp = getattr(service_in, "source_timestamp", None) or now
        new_db_service.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_db_service)
            self.session.commit()
            
            # 5. Refresh to capture generated ID
            self.session.refresh(new_db_service)
            logger.info(f"Registered new service type: {new_db_service.service_name}")
            return self._map_to_domain(new_db_service)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create service type: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during service type creation."
            )

    # --- 2. create_service_types_batch ---
    def create_service_types_batch(self, services_in: list[ServiceTypeDomain]) -> list[ServiceTypeDomain]:
        """Requirement: Batch CRUD. Ingests multiple service types in one transaction.

        Args:
            services_in (list[ServiceTypeDomain]): List of service type objects.

        Returns:
            list[ServiceTypeDomain]: The list of created service types.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_names = [s.service_name for s in services_in]
        statement = select(DimServiceType).where(col(DimServiceType.service_name).in_(input_names))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains service types that already exist in the catalog."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for s_in in services_in:
            entry_data = s_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_service = DimServiceType(**entry_data)
            db_service.source_timestamp = getattr(s_in, "source_timestamp", None) or now
            db_service.updated_at = now
            db_entries.append(db_service)

        try:
            # 3. Batch insert and Atomic Commit
            self.session.add_all(db_entries)
            self.session.commit()
            
            # 4. Refresh all to capture IDs
            for entry in db_entries:
                self.session.refresh(entry)
            
            # 5. Map back to Domain objects
            return [self._map_to_domain(e) for e in db_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Service Type load failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch service type creation."
            )

    # --- 3. get_all_service_types ---
    def get_all_service_types(self, limit: int = 100, offset: int = 0) -> list[ServiceTypeDomain]:
        """Full CRUD: Retrieves the full catalog of active service classifications.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip for pagination.

        Returns:
            list[ServiceTypeDomain]: Paginated list of service types.
        """
        # 1. Build statement with pagination and alphabetical sorting
        statement = (
            select(DimServiceType)
            .where(col(DimServiceType.is_active))
            .order_by(col(DimServiceType.service_name))
            .offset(offset)
            .limit(limit)
        )
        
        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(s) for s in results]

    # --- 4. get_service_type ---
    def get_service_type(self, id: int) -> ServiceTypeDomain:
        """Full CRUD: Retrieves a single service classification by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            ServiceTypeDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_service = self._get_dim_service_or_404(id)
        return self._map_to_domain(db_service)

    # --- 5. update_service_type ---
    def update_service_type(self, id: int, data: ServiceTypeDomain) -> ServiceTypeDomain:
        """Full CRUD: Updates an existing service definition's properties.

        Args:
            id (int): The ID to update.
            data (ServiceTypeDomain): Updated service type data.

        Returns:
            ServiceTypeDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_service = self._get_dim_service_or_404(id)

        # 2. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_service.sqlmodel_update(update_data)
        
        # 3. Refresh update timestamp
        db_service.updated_at = datetime.now(UTC)

        try:
            # 4. Persist and return
            self.session.add(db_service)
            self.session.commit()
            self.session.refresh(db_service)
            return self._map_to_domain(db_service)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update service type {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during service type update."
            )

    # --- 6. update_service_types_batch ---
    def update_service_types_batch(self, services_in: list[ServiceTypeDomain]) -> list[ServiceTypeDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple service types using 'service_name' as the business key.

        Args:
            services_in (list[ServiceTypeDomain]): Updated service type data.

        Returns:
            list[ServiceTypeDomain]: Refreshed list of updated entities.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_names = [s.service_name for s in services_in]
        statement = select(DimServiceType).where(col(DimServiceType.service_name).in_(input_names))
        db_services = self.session.exec(statement).all()
        
        # 2. Create Lookup Map for O(1) access
        db_map = {s.service_name: s for s in db_services}

        # 3. Atomic Validation
        for s_data in services_in:
            if s_data.service_name not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Service type '{s_data.service_name}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for s_data in services_in:
                db_service = db_map[s_data.service_name]
                update_dict = s_data.model_dump(exclude={"id", "updated_at"})
                db_service.sqlmodel_update(update_dict)
                db_service.updated_at = now
                self.session.add(db_service)
                updated_entries.append(db_service)

            # 5. Commit and map results
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch service type update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_service_type ---
    def delete_service_type(self, id: int) -> None:
        """Full CRUD: Marks a service type as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record via helper
        db_service = self._get_dim_service_or_404(id)
        
        # 2. Early return if already inactive
        if not db_service.is_active:
            return

        try:
            # 3. Apply Soft-Delete and update metadata
            db_service.is_active = False
            db_service.updated_at = datetime.now(UTC)
            self.session.add(db_service)
            self.session.commit()
            logger.info(f"Service Type {id} deactivated.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete service type {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during service type deactivation."
            )

    # --- 8. delete_service_types_batch ---
    def delete_service_types_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple service types as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Input Safety Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items in single query
            statement = select(DimServiceType).where(col(DimServiceType.id).in_(ids))
            items = self.session.exec(statement).all()

            # 3. Validation: Ensure all requested IDs exist
            if len(items) != len(ids):
                found_ids = {item.id for item in items if item.id is not None}
                missing_ids = set(ids) - found_ids
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch aborted. IDs not found: {list(missing_ids)}"
                )

            # 4. Apply Soft-Delete to all items
            now = datetime.now(UTC)
            for item in items:
                item.is_active = False
                item.updated_at = now
                self.session.add(item)

            # 5. Atomic Commit
            self.session.commit()
            logger.info(f"Successfully deactivated {len(items)} service types.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch service type deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
