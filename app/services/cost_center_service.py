import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status

# Added col to satisfy Mypy strict typing on Optional fields
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimCostCenter

# Layer 3: Domain Entities
from app.domain.cost_center import CostCenterDomain


logger = logging.getLogger(__name__)

class CostCenterService:
    """Service layer for managing Financial Cost Centers.

    This service coordinates the lifecycle of cost centers, ensuring that
    financial budget codes are unique and providing refined reports for the
    Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_cc: DimCostCenter) -> CostCenterDomain:
        """Maps a Data Access model back to a Domain entity.

        Args:
            db_cc (DimCostCenter): The database record.

        Returns:
            CostCenterDomain: The pure domain representation.
        """
        return CostCenterDomain.model_validate(db_cc)

    def _get_dim_cc_or_404(self, id: int) -> DimCostCenter:
        """Internal helper to retrieve a cost center or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimCostCenter: The database record.

        Raises:
            HTTPException: 404 status if not found.
        """
        statement = select(DimCostCenter).where(DimCostCenter.id == id, col(DimCostCenter.is_active))
        cc = self.session.exec(statement).first()
        if not cc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cost Center with ID {id} not found."
            )
        return cc

    # --- 1. create_cost_center ---
    def create_cost_center(self, cc_in: CostCenterDomain) -> CostCenterDomain:
        """Validates and persists a new cost center.

        Args:
            cc_in (CostCenterDomain): Input data from the API.

        Returns:
            CostCenterDomain: The created cost center.

        Raises:
            HTTPException: 400 status if the center_code already exists.
        """
        # 1. Unique Constraint Check
        statement = select(DimCostCenter).where(DimCostCenter.center_code == cc_in.center_code)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cost Center code '{cc_in.center_code}' is already registered."
            )

        # 2. Convert Pydantic model to dict, excluding fields we want to handle manually
        # We exclude 'id' so the DB autoincrements it.
        # We exclude timestamps to ensure they are set correctly here.
        cc_data = cc_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})

        # 3. Initialize the DB model using the unpacked dictionary
        new_cc = DimCostCenter(**cc_data)

        # 4. Handle Metadata
        # If source_timestamp isn't provided, use current UTC time.
        new_cc.source_timestamp = cc_in.source_timestamp or datetime.now(UTC)
        # On creation, updated_at is always "now"
        new_cc.updated_at = datetime.now(UTC)

        try:
            # 5. Persist to Database
            self.session.add(new_cc)
            self.session.commit()

            # 6. Refresh to get the Autoincremented ID from the DB
            self.session.refresh(new_cc)

            # 7. Return the Domain representation
            return self._map_to_domain(new_cc)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create cost center: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during cost center creation.")

    # --- 2. create_cost_centers_batch ---
    def create_cost_centers_batch(self, ccs_in: list[CostCenterDomain]) -> list[CostCenterDomain]:
        """Requirement: CRUD should allow batch operations.
        
        Optimizes the ingestion of multiple cost centers in a single transaction.

        Args:
            ccs_in (List[CostCenterDomain]): A list of cost center objects.

        Returns:
            List[CostCenterDomain]: The list of created centers.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        # We check if any of the incoming center_codes already exist in the DB.
        input_codes = [cc.center_code for cc in ccs_in]
        statement = select(DimCostCenter).where(col(DimCostCenter.center_code).in_(input_codes))
        existing_ccs = self.session.exec(statement).all()
        
        if existing_ccs:
            existing_codes = [e.center_code for e in existing_ccs]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Batch contains Cost Center codes that already exist: {existing_codes}"
            )
    
        # 2. Prepare DB entries
        db_entries = []
        now = datetime.now(UTC)

        for cc in ccs_in:
            # Use model_dump to handle all fields, excluding metadata and ID
            # This ensures that if you add 'manager' or 'location' to the model later,
            # this code doesn't need to change.
            entry_data = cc.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_cc = DimCostCenter(**entry_data)
            
            # Apply standard metadata
            # (Assuming DimCostCenter follows the same metadata pattern as DimAsset)
            if hasattr(db_cc, "source_timestamp"):
                db_cc.source_timestamp = getattr(cc, "source_timestamp", None) or now
            if hasattr(db_cc, "updated_at"):
                db_cc.updated_at = now
                
            db_entries.append(db_cc)

        try:
            # 3. Add all to session
            self.session.add_all(db_entries)
            
            # 4. Atomic Transaction: All succeed or all fail
            self.session.commit()

            # 5. Refresh to capture Autoincremented IDs
            for entry in db_entries:
                self.session.refresh(entry)

            # 6. Map back to Domain objects (assuming you have a _map_to_domain helper for CCs)
            # If you don't have a helper, you can use: CostCenterDomain.model_validate(e)
            return [CostCenterDomain.model_validate(e) for e in db_entries]

        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Cost Center load failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during cost center batch creation."
            )
    
    # --- 3. get_all_cost_centers ---
    def get_all_cost_centers(self, limit: int = 100, offset: int = 0) -> list[CostCenterDomain]:
        """Full CRUD: Retrieves all active cost centers from the database.

        Provides a paginated list of cost center records registered in the Silver layer.
        Results are sorted alphabetically by center code to ensure UI consistency
        across dropdowns and tables.

        Args:
            limit (int): The maximum number of records to return. Defaults to 100.
            offset (int): The number of records to skip (for pagination). Defaults to 0.

        Returns:
            list[CostCenterDomain]: A list of cost center domain representations.
        """
        # 1. Added order_by (Sorting by center_code is usually best for Cost Centers)
        # 2. Added limit/offset to prevent data overloads
        statement = (
            select(DimCostCenter)
            .where(col(DimCostCenter.is_active))
            .order_by(col(DimCostCenter.center_code))
            .offset(offset)
            .limit(limit)
        )
        
        results = self.session.exec(statement).all()
        
        # 3. Use the mapping helper for consistency
        return [self._map_to_domain(cc) for cc in results]

    # --- 4. get_cost_center ---
    def get_cost_center(self, id: int) -> CostCenterDomain:
        """Full CRUD: Retrieves a single cost center record by ID.

        Args:
            id (int): The primary key ID.

        Returns:
            Optional[CostCenterDomain]: The domain entity if found.
        """
        db_cc = self._get_dim_cc_or_404(id)
        return self._map_to_domain(db_cc)

    # --- 5. update_cost_center ---
    def update_cost_center(self, id: int, data: CostCenterDomain) -> CostCenterDomain:
        """Full CRUD: Updates an existing cost center.

        Args:
            id (int): The ID to update.
            data (CostCenterDomain): The updated data.

        Returns:
            CostCenterDomain: The updated entity.
        """
        # 1. Fetch existing record (raises 404 via helper if missing)
        db_cc = self._get_dim_cc_or_404(id)

        # 2. Convert incoming Pydantic model to a dict
        # We exclude 'id' to prevent accidental Primary Key changes.
        # We exclude 'updated_at' to handle it manually.
        update_data = data.model_dump(exclude={"id", "updated_at"})

        # 3. Use SQLModel's helper to update all fields automatically
        # This covers department, budget_limit, center_code, etc.
        db_cc.sqlmodel_update(update_data)

        # 4. Update Metadata
        # On every update, we refresh the updated_at timestamp
        if hasattr(db_cc, "updated_at"):
            db_cc.updated_at = datetime.now(UTC)

        try:
            # 5. Persist changes
            self.session.add(db_cc)
            self.session.commit()
            
            # 6. Refresh to get the latest state from DB
            self.session.refresh(db_cc)
            
            logger.info(f"Cost Center {id} ({db_cc.center_code}) updated successfully.")
            return self._map_to_domain(db_cc)

        except Exception as e:
            # 7. Rollback on failure to keep the session clean
            self.session.rollback()
            logger.error(f"Failed to update Cost Center {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during cost center update."
            )

    # --- 6. update_cost_centers_batch ---
    def update_cost_centers_batch(self, ccs_in: list[CostCenterDomain]) -> list[CostCenterDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple centers using 'center_code' as the business key.
        
        Ensures atomicity: if one code is not found, the whole transaction is rolled back.

        Args:
            ccs_in (List[CostCenterDomain]): List of updated cost center data.

        Returns:
            List[CostCenterDomain]: The original input list on success.
        """
        # 1. Fetch all existing Cost Centers in ONE query (Performance Optimization)
        input_codes = [cc.center_code for cc in ccs_in]
        statement = select(DimCostCenter).where(col(DimCostCenter.center_code).in_(input_codes))
        db_ccs = self.session.exec(statement).all()

        # 2. Map existing CCs to a dictionary for O(1) fast lookup by center_code
        db_map = {cc.center_code: cc for cc in db_ccs}

        # 3. Pre-validation: Ensure all center_codes in the batch actually exist
        for cc_data in ccs_in:
            if cc_data.center_code not in db_map:
                # We haven't changed data yet, but we rollback to stay safe
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Cost Center code '{cc_data.center_code}' not found. Batch aborted."
                )
            
        now = datetime.now(UTC)

        try:
            updated_entries = []
            for cc_data in ccs_in:
                db_cc = db_map[cc_data.center_code]
                
                # 4. Use model_dump to extract data, excluding protected fields
                # We exclude 'id' to protect the primary key
                update_dict = cc_data.model_dump(exclude={"id", "updated_at"})
                
                # 5. Apply the update automatically using SQLModel's helper
                db_cc.sqlmodel_update(update_dict)
                
                # 6. Apply Metadata logic
                if hasattr(db_cc, "updated_at"):
                    db_cc.updated_at = now
                
                self.session.add(db_cc)
                updated_entries.append(db_cc)

            # 7. Atomic Commit: All records are updated in a single transaction
            self.session.commit()
            
            # 8. Map back to Domain models to return fresh data (including IDs/Timestamps)
            return [self._map_to_domain(e) for e in updated_entries]

        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Cost Center update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_cost_center ---
    def delete_cost_center(self, id: int) -> None:
        """Marks a cost center as inactive instead of removing it."""
        # 1. Fetch the record
        db_cc = self._get_dim_cc_or_404(id)

        # Optional: Logic check - if already inactive, you might want to return early
        if not db_cc.is_active:
            logger.info(f"Cost Center {id} is already inactive.")
            return
            
        try:
            # 2. Update the flag instead of calling .delete()
            db_cc.is_active = False
            db_cc.updated_at = datetime.now(UTC)
            
            # 3. Save the change
            self.session.add(db_cc)
            self.session.commit()
            
            logger.info(f"Cost Center {id} was deactivated (Soft-Deleted).")
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete CC {id}: {e}")
            raise HTTPException(status_code=500, detail="Error during soft-deletion")

    # --- 8. delete_cost_centers_batch (Soft-Delete) ---
    def delete_cost_centers_batch(self, ids: list[int]) -> None:
        """Atomic batch soft-delete for cost centers.
        
        Sets is_active=False for all provided IDs. If any ID is not found,
        the entire transaction is rolled back.
        """
        # 1. Safety check
        if not ids:
            return

        try:
            # 2. Fetch all targeted cost centers in one query
            # Use col() to ensure Mypy handles the Optional ID field correctly
            statement = select(DimCostCenter).where(col(DimCostCenter.id).in_(ids))
            items = self.session.exec(statement).all()

            # 3. Validation: Strict All-or-Nothing
            if len(items) != len(ids):
                found_ids = {item.id for item in items if item.id is not None}
                missing_ids = set(ids) - found_ids
                
                logger.warning(f"Batch soft-delete failed. Missing Cost Center IDs: {missing_ids}")
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch aborted. One or more Cost Center IDs not found: {list(missing_ids)}"
                )

            # 4. Perform the Soft-Delete
            now = datetime.now(UTC)
            for item in items:
                item.is_active = False
                # If your DimCostCenter model has an updated_at field:
                if hasattr(item, "updated_at"):
                    item.updated_at = now
                
                # Add back to session to mark as modified
                self.session.add(item)

            # 5. Atomic Commit: All are deactivated at once
            self.session.commit()
            logger.info(f"Successfully deactivated {len(items)} cost centers. IDs: {ids}")

        except HTTPException:
            # Re-raise the 404 error
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Cost Center soft-delete failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during cost center batch deactivation."
            )
