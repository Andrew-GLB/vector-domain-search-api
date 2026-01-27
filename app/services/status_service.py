import logging
from typing import Any

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
    that status names are standardized and providing refined health reports
    for the Gold Layer.
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
        return StatusDomain(
            status_name=db_status.status_name,
            is_billable=db_status.is_billable,
            description=db_status.description
        )

    def _get_dim_status_or_404(self, status_id: int) -> DimStatus:
        """Internal helper to retrieve a status record or raise a 404 error.

        Args:
            status_id (int): The primary key ID.

        Returns:
            DimStatus: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        status_obj = self.session.get(DimStatus, status_id)
        if not status_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Status with ID {status_id} not found."
            )
        return status_obj

    def create_status(self, status_in: StatusDomain) -> StatusDomain:
        """Validates business rules and persists a new operational status.

        Args:
            status_in (StatusDomain): Input data from the API.

        Returns:
            StatusDomain: The created status.

        Raises:
            HTTPException: 400 status if the status name already exists.
        """
        # 1. Unique name check
        statement = select(DimStatus).where(DimStatus.status_name == status_in.status_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Status '{status_in.status_name}' already exists."
            )

        # 2. Map and Persist
        new_db_status = DimStatus(
            status_name=status_in.status_name,
            is_billable=status_in.is_billable,
            description=status_in.description
        )
        self.session.add(new_db_status)
        self.session.commit()
        self.session.refresh(new_db_status)

        return self._map_to_domain(new_db_status)

    def create_statuses_batch(self, statuses_in: list[StatusDomain]) -> list[StatusDomain]:
        """Requirement: CRUD should allow batch operations.
        
        Optimizes the ingestion of multiple operational statuses in a single transaction.

        Args:
            statuses_in (List[StatusDomain]): A list of status objects.

        Returns:
            List[StatusDomain]: The list of created statuses.
        """
        db_entries = [
            DimStatus(
                status_name=s.status_name,
                is_billable=s.is_billable,
                description=s.description
            ) for s in statuses_in
        ]

        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return statuses_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Status load failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process batch status creation."
            )

    def get_all_statuses(self) -> list[StatusDomain]:
        """Retrieves all registered operational statuses."""
        statement = select(DimStatus)
        results = self.session.exec(statement).all()
        return [self._map_to_domain(s) for s in results]

    def get_status(self, id: int) -> StatusDomain:
        """Requirement: Full CRUD.
        Retrieves a single operational status record from the Silver layer.

        This method coordinates the retrieval of a database model and its
        transformation into a pure Pydantic Domain model for the API layer.

        Args:
            id (int): The primary key identifier of the status to retrieve.

        Returns:
            StatusDomain: The validated domain representation of the status.

        Raises:
            HTTPException: 404 status code if the status record is not found,
                propagated from the internal helper.
        """
        # 1. Fetch from Data Access layer (Silver)
        db_status = self._get_dim_status_or_404(id)

        # 2. Map and return to Domain layer
        return self._map_to_domain(db_status)

    def update_status(self, status_id: int, status_in: StatusDomain) -> StatusDomain:
        """Full CRUD: Updates an existing operational status definition.

        Args:
            status_id (int): The ID to update.
            status_in (StatusDomain): The updated data.

        Returns:
            StatusDomain: The updated entity.
        """
        db_status = self._get_dim_status_or_404(status_id)

        db_status.is_billable = status_in.is_billable
        db_status.description = status_in.description

        self.session.add(db_status)
        self.session.commit()
        self.session.refresh(db_status)
        return self._map_to_domain(db_status)

    def update_statuses_batch(self, statuses_in: list[StatusDomain]) -> list[StatusDomain]:
        """Updates multiple statuses in a single atomic transaction.
        Uses 'status_name' to identify existing records.

        Args:
            statuses_in (List[StatusDomain]): List of updated status data.

        Returns:
            List[StatusDomain]: The confirmed updated data.

        Raises:
            HTTPException: 404 if a status_name does not exist.
            HTTPException: 500 on database error.
        """
        try:
            for s_data in statuses_in:
                # 1. Find the existing record by Business Key
                statement = select(DimStatus).where(DimStatus.status_name == s_data.status_name)
                db_status = self.session.exec(statement).first()

                if not db_status:
                    # Rollback the whole batch if one name is missing
                    self.session.rollback()
                    logger.error(f"Batch update failed: Status '{s_data.status_name}' not found.")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Status '{s_data.status_name}' not found. Batch update aborted."
                    )

                # 2. Update metadata fields
                db_status.is_billable = s_data.is_billable
                db_status.description = s_data.description

                self.session.add(db_status)

            # 3. Commit all changes at once
            self.session.commit()
            logger.info(f"Successfully updated {len(statuses_in)} statuses in batch.")
            return statuses_in

        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Critical error in status batch update: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error during batch update."
            )

    def delete_status(self, id: int) -> None:
        """Requirement: Full CRUD.
        Removes an operational status record from the dimension table.

        Args:
            id (int): The primary key ID of the status to delete.

        Returns:
            None: Returns nothing on successful deletion.

        Raises:
            HTTPException: 404 status code if the status ID does not exist,
                triggered by the internal helper.
        """
        # 1. Retrieve the object or fail immediately with a 404
        db_status = self._get_dim_status_or_404(id)

        # 2. Perform the deletion from the Silver layer
        self.session.delete(db_status)

        # 3. Finalize the transaction
        self.session.commit()

        logger.info(f"Successfully deleted status ID: {id}")

    def delete_statuses_batch(self, ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes multiple statuses by their primary key IDs.
        
        Uses the efficient 'IN' operator and ensures atomicity.
        Satisfies Mypy type-checking by using the col() helper.

        Args:
            ids (List[int]): List of primary key IDs to remove.

        Returns:
            None: Returns nothing on success.

        Raises:
            HTTPException: 404 if one or more IDs do not exist.
            HTTPException: 500 if a database error occurs.
        """
        try:
            # 1. Fetch all matching records in a single query for efficiency
            # We use col() to ensure Mypy recognizes the .in_() attribute
            statement = select(DimStatus).where(col(DimStatus.id).in_(ids))
            items_to_delete = self.session.exec(statement).all()

            # 2. Validation: Ensure every ID provided exists in the database
            if len(items_to_delete) != len(ids):
                # Map found IDs (filtering out None to satisfy Mypy)
                found_ids = {item.id for item in items_to_delete if item.id is not None}

                # Identify exactly which IDs are missing for the error message
                missing_ids = set(ids) - found_ids

                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch delete aborted. Status IDs not found: {list(missing_ids)}"
                )

            # 3. Perform the deletions
            for item in items_to_delete:
                self.session.delete(item)

            # 4. Commit transaction
            self.session.commit()
            logger.info(f"Successfully deleted batch of {len(items_to_delete)} statuses.")

        except HTTPException:
            # Re-raise managed HTTP exceptions
            raise
        except Exception as e:
            # Rollback on unexpected database or system errors
            self.session.rollback()
            logger.error(f"Batch Status deletion failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch status deletion."
            )

    # --- GOLD LAYER ANALYTICS ---

    def get_operational_health_gold(self) -> list[dict[str, Any]]:
        """Requirement: Querying Gold Layer.
        
        Refines status data to provide a high-level summary of operational
        health and cost implications (Billable vs Non-Billable states).

        Returns:
            List[Dict[str, Any]]: A list of statuses with formatted metadata.
        """
        statuses = self.get_all_statuses()
        return [
            {
                "status": s.status_name,
                "billing_impact": "Active Cost" if s.is_billable else "Idle/No Cost",
                "summary": s.description
            } for s in statuses
        ]
