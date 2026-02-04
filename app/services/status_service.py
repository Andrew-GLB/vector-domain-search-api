import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimStatus

# Layer 3: Domain Entities
from app.domain.status import StatusDomain


logger = logging.getLogger(__name__)

class StatusService:
    """Service layer for managing Operational Status dimensions.

    This service coordinates the lifecycle of operational states, ensuring
    standardized status naming and providing refined health reports
    for the Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_status: DimStatus) -> StatusDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_status (DimStatus): The database record.

        Returns:
            StatusDomain: The Pydantic domain representation.
        """
        return StatusDomain.model_validate(db_status)

    def _get_dim_status_or_404(self, id: int) -> DimStatus:
        """Internal helper to retrieve an active status or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimStatus: The database record.

        Raises:
            HTTPException: 404 status if not found or inactive.
        """
        # 1. Fetch record ensuring it is currently active
        statement = select(DimStatus).where(
            DimStatus.id == id,
            col(DimStatus.is_active)
        )
        status_obj = self.session.exec(statement).first()

        # 2. Raise 404 if record is missing or soft-deleted
        if not status_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Status with ID {id} not found."
            )
        return status_obj

    # --- 1. create_status ---
    def create_status(self, status_in: StatusDomain) -> StatusDomain:
        """Full CRUD: Validates business rules and persists a new operational status.

        Args:
            status_in (StatusDomain): Input data from the API.

        Returns:
            StatusDomain: The created status.
        """
        # 1. Unique Name Check (Business Key)
        statement = select(DimStatus).where(DimStatus.status_name == status_in.status_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Status '{status_in.status_name}' is already registered."
            )

        # 2. Extract data excluding system-managed fields
        status_data = status_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_status = DimStatus(**status_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_status.source_timestamp = getattr(status_in, "source_timestamp", None) or now
        new_status.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_status)
            self.session.commit()

            # 5. Refresh to capture generated ID
            self.session.refresh(new_status)
            return self._map_to_domain(new_status)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create status: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during status creation."
            )

    # --- 2. create_statuses_batch ---
    def create_statuses_batch(self, statuses_in: list[StatusDomain]) -> list[StatusDomain]:
        """Requirement: Batch CRUD. Ingests multiple statuses in one transaction.

        Args:
            statuses_in (list[StatusDomain]): List of status objects.

        Returns:
            list[StatusDomain]: The list of created statuses.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_names = [s.status_name for s in statuses_in]
        statement = select(DimStatus).where(col(DimStatus.status_name).in_(input_names))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains status names that already exist."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for s_in in statuses_in:
            entry_data = s_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_status = DimStatus(**entry_data)
            db_status.source_timestamp = getattr(s_in, "source_timestamp", None) or now
            db_status.updated_at = now
            db_entries.append(db_status)

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
            logger.error(f"Batch status creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch status creation."
            )

    # --- 3. get_all_statuses ---
    def get_all_statuses(self, limit: int = 100, offset: int = 0) -> list[StatusDomain]:
        """Full CRUD: Retrieves all active operational statuses with pagination.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip for pagination.

        Returns:
            list[StatusDomain]: Paginated list of statuses.
        """
        # 1. Build statement with pagination and alphabetical sorting
        statement = (
            select(DimStatus)
            .where(col(DimStatus.is_active))
            .order_by(col(DimStatus.status_name))
            .offset(offset)
            .limit(limit)
        )

        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(s) for s in results]

    # --- 4. get_status ---
    def get_status(self, id: int) -> StatusDomain:
        """Full CRUD: Retrieves a single operational status record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            StatusDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_status = self._get_dim_status_or_404(id)
        return self._map_to_domain(db_status)

    # --- 5. update_status ---
    def update_status(self, id: int, data: StatusDomain) -> StatusDomain:
        """Full CRUD: Updates an existing operational status definition.

        Args:
            id (int): The ID to update.
            data (StatusDomain): Updated status data.

        Returns:
            StatusDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_status = self._get_dim_status_or_404(id)

        # 2. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_status.sqlmodel_update(update_data)

        # 3. Refresh update timestamp
        db_status.updated_at = datetime.now(UTC)

        try:
            # 4. Persist and return
            self.session.add(db_status)
            self.session.commit()
            self.session.refresh(db_status)
            return self._map_to_domain(db_status)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update status {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during status update."
            )

    # --- 6. update_statuses_batch ---
    def update_statuses_batch(self, data: list[StatusDomain]) -> list[StatusDomain]:
        """Requirement: Batch CRUD. Updates multiple statuses using 'status_name'.

        Args:
            data (list[StatusDomain]): Updated status data list.

        Returns:
            list[StatusDomain]: Refreshed list of updated entities.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_names = [s.status_name for s in data]
        statement = select(DimStatus).where(col(DimStatus.status_name).in_(input_names))
        db_statuses = self.session.exec(statement).all()

        # 2. Create Lookup Map for O(1) access
        db_map = {s.status_name: s for s in db_statuses}

        # 3. Atomic Validation
        for s_data in data:
            if s_data.status_name not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Status '{s_data.status_name}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for s_data in data:
                db_status = db_map[s_data.status_name]
                update_dict = s_data.model_dump(exclude={"id", "updated_at"})
                db_status.sqlmodel_update(update_dict)
                db_status.updated_at = now
                self.session.add(db_status)
                updated_entries.append(db_status)

            # 5. Commit and map results
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch status update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_status ---
    def delete_status(self, id: int) -> None:
        """Full CRUD: Marks an operational status as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record via helper
        db_status = self._get_dim_status_or_404(id)

        # 2. Early return if already inactive
        if not db_status.is_active:
            return

        try:
            # 3. Apply Soft-Delete and update metadata
            db_status.is_active = False
            db_status.updated_at = datetime.now(UTC)
            self.session.add(db_status)
            self.session.commit()
            logger.info(f"Status {id} deactivated.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete status {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during status deactivation."
            )

    # --- 8. delete_statuses_batch ---
    def delete_statuses_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple statuses as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Input Safety Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items in single query
            statement = select(DimStatus).where(col(DimStatus.id).in_(ids))
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
            logger.info(f"Successfully deactivated {len(items)} statuses.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch status deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
