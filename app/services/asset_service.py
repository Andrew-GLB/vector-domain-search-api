import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimAsset

# Layer 3: Domain Entities
from app.domain.asset import AssetDomain


logger = logging.getLogger(__name__)

class AssetService:
    """Service layer for managing Cloud Asset Infrastructure.

    This service orchestrates the lifecycle of an asset, including business
    validation, SQL persistence (Silver Layer), AI-driven metadata enrichment,
    and Vector DB synchronization for semantic search.
    """
    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_asset: DimAsset) -> AssetDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_asset (DimAsset): The database record.

        Returns:
            AssetDomain: The Pydantic domain representation.
        """
        return AssetDomain.model_validate(db_asset)

    def _get_dim_asset_or_404(self, id: int) -> DimAsset:
        """Internal helper to retrieve an ACTIVE asset or raise a 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimAsset: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        statement = select(DimAsset).where(DimAsset.id == id, col(DimAsset.is_active))
        asset = self.session.exec(statement).first()
        if not asset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Asset with ID {id} not found."
            )
        return asset

    # --- 1. create_asset ---
    def create_asset(self, asset_in: AssetDomain, enrich_with_ai: bool = False) -> AssetDomain:
        """Validates and persists a new cloud asset.

        Args:
            asset_in (AssetDomain): Input data from the API.
            enrich_with_ai (bool): Boolean value to enrich description field using AI tools.

        Returns:
            AssetDomain: The created cloud asset.

        Raises:
            HTTPException: 400 status if the center_code already exists.
        """
        # 1. Check for existing serial number (Business Key)
        statement = select(DimAsset).where(DimAsset.serial_number == asset_in.serial_number)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Asset with serial {asset_in.serial_number} already exists."
            )

        # 2. Convert Pydantic model to dict, excluding fields we want to handle manually
        # We exclude 'id' so the DB autoincrements it.
        # We exclude timestamps to ensure they are set correctly here.
        asset_data = asset_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})

        # 3. Initialize the DB model using the unpacked dictionary
        new_db_asset = DimAsset(**asset_data)

        # 4. Handle Metadata
        # If source_timestamp isn't provided, use current UTC time.
        new_db_asset.source_timestamp = asset_in.source_timestamp or datetime.now(UTC)
        # On creation, updated_at is always "now"
        new_db_asset.updated_at = datetime.now(UTC)

        try:
            #6. Persist to Database
            self.session.add(new_db_asset)
            self.session.commit()

            # 7. Refresh to get the Autoincremented ID from the DB
            self.session.refresh(new_db_asset)

            # 8. Return the Domain representation
            return self._map_to_domain(new_db_asset)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create asset: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during asset creation.")

    # --- 2. create_assets_batch ---
    def create_assets_batch(self, data: list[AssetDomain]) -> list[AssetDomain]:
        """Requirement: CRUD should allow batch operations.
        
        Optimizes the ingestion of multiple assets in a single transaction.

        Args:
            data (list[AssetDomain]): A list of asset objects.

        Returns:
            list[AssetDomain]: The list of created assets.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        # We pull all serial numbers from the input and check the DB once.
        input_serials = [a.serial_number for a in data]
        statement = select(DimAsset).where(col(DimAsset.serial_number).in_(input_serials))
        existing_assets = self.session.exec(statement).all()
        
        if existing_assets:
            existing_serials = [e.serial_number for e in existing_assets]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Batch contains serial numbers that already exist: {existing_serials}"
            )
        
        # 2. Prepare DB entries
        db_entries = []
        now = datetime.now(UTC)
    
        for a in data:
            # Use model_dump to handle all fields except metadata/id (same logic as single create)
            entry_data = a.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_asset = DimAsset(**entry_data)
            
            # Apply metadata
            db_asset.source_timestamp = a.source_timestamp or now
            db_asset.updated_at = now
            
            db_entries.append(db_asset)

        try:
            # 3. Add all entries to the session
            self.session.add_all(db_entries)
            
            # 4. Commit once (Atomic Transaction)
            self.session.commit()

            # 5. Refresh entries to capture the IDs generated by the DB autoincrement
            # We must refresh each one to get the ID back into the object
            for entry in db_entries:
                self.session.refresh(entry)

            # 6. Map back to Domain objects and return
            return [self._map_to_domain(e) for e in db_entries]

        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Asset creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch creation."
            )

    # --- 3. get_all_assets ---
    def get_all_assets(self, limit: int = 100, offset: int = 0) -> list[AssetDomain]:
        """Retrieves all assets from the database with pagination and sorting."""
        # 1. Added order_by to ensure the list doesn't "jump around" in the UI
        # 2. Added offset/limit to protect against huge data transfers
        statement = (
            select(DimAsset)
            .where(col(DimAsset.is_active))
            .order_by(col(DimAsset.serial_number))
            .offset(offset)
            .limit(limit)
        )
        
        results = self.session.exec(statement).all()
        # 3. Map back to Domain objects
        return [self._map_to_domain(a) for a in results]

    # --- 4. get_asset ---
    def get_asset(self, id: int) -> AssetDomain:
        """Full CRUD: Retrieves a single cloud asset record by ID.

        Args:
            id (int): The primary key ID.

        Returns:
            AssetDomain: The domain entity representation.
        """
        # 1. Fetch from DB using the helper (raises 404 if missing or inactive)
        db_asset = self._get_dim_asset_or_404(id)
        
        # 2. Map the ORM object to the Domain Pydantic model
        return self._map_to_domain(db_asset)

    # --- 5. update_asset ---
    def update_asset(self, id: int, data: AssetDomain) -> AssetDomain:
        """Updates an existing asset and re-syncs with Vector DB."""
        # 1. Fetch the existing record (raises 404 if not found)
        db_asset = self._get_dim_asset_or_404(id)

        # 2. Convert incoming Pydantic model to a dictionary
        # We exclude 'id' because we NEVER want to update the Primary Key.
        # We exclude timestamps because we handle them manually.
        update_data = data.model_dump(exclude={"id", "source_timestamp", "updated_at"})

        # 3. Use SQLModel's built-in update helper
        # This automatically assigns all dictionary keys to the DB object
        db_asset.sqlmodel_update(update_data)

        # 4. Update Metadata
        # On an update, we keep the incoming source_timestamp if provided,
        # otherwise we keep the OLD one from the DB.
        db_asset.source_timestamp = data.source_timestamp or db_asset.source_timestamp
        # updated_at should ALWAYS be 'now' on an update
        db_asset.updated_at = datetime.now(UTC)

        try:
            self.session.add(db_asset)
            self.session.commit()
            self.session.refresh(db_asset)
            
            # Optional: Log the update for auditing
            logger.info(f"Asset {id} updated successfully.")
            
            return self._map_to_domain(db_asset)
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update asset {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during asset update."
            )

    # --- 6. update_assets_batch ---
    def update_assets_batch(self, data: list[AssetDomain]) -> list[AssetDomain]:
        """Atomic batch update using serial_number as business key."""
        # 1. Fetch all existing records in ONE query
        input_serials = [a.serial_number for a in data]
        statement = select(DimAsset).where(col(DimAsset.serial_number).in_(input_serials))
        db_assets = self.session.exec(statement).all()

        # 2. Map existing assets to a dictionary for fast lookup by serial
        db_map = {asset.serial_number: asset for asset in db_assets}

        # 3. Validation: Ensure all serials in the batch actually exist
        for a_data in data:
            if a_data.serial_number not in db_map:
                # We rollback just in case, though technically nothing has been added yet
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Asset serial '{a_data.serial_number}' not found. Batch aborted."
                )

        now = datetime.now(UTC)

        try:
            updated_entries = []
            for a_data in data:
                db_asset = db_map[a_data.serial_number]
                
                # 4. Use model_dump to exclude protected fields
                update_dict = a_data.model_dump(exclude={"id", "source_timestamp", "updated_at"})
                
                # 5. Apply the update automatically
                db_asset.sqlmodel_update(update_dict)
                
                # 6. Apply Metadata logic
                db_asset.source_timestamp = a_data.source_timestamp or db_asset.source_timestamp
                db_asset.updated_at = now
                
                self.session.add(db_asset)
                updated_entries.append(db_asset)

            # 7. Atomic Commit
            self.session.commit()
            
            # 8. Return the refreshed list mapped back to Domain models
            return [self._map_to_domain(e) for e in updated_entries]

        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Asset update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update."
            )

    # --- 7. delete_asset ---
    def delete_asset(self, id: int) -> None:
        """Removes a single asset by ID."""
        # 1. Fetch the asset (raises 404 if it doesn't exist)
        db_asset = self._get_dim_asset_or_404(id)

        # Optional: Logic check - if already inactive, you might want to return early
        if not db_asset.is_active:
            logger.info(f"Asset {id} is already inactive.")
            return
            
        try:
            # 2. Perform the "Soft-Delete" by updating flags
            db_asset.is_active = False
            
            # 3. Always update the timestamp so we know WHEN it was deactivated
            db_asset.updated_at = datetime.now(UTC)
            
            # 4. Save the change (Standard Update pattern)
            self.session.add(db_asset)
            self.session.commit()
            
            # 5. Log for audit
            logger.info(f"Asset with ID {id} (Serial: {db_asset.serial_number}) was deactivated (Soft-Deleted).")
                
        except Exception as e:
            # 6. Rollback if the update fails
            self.session.rollback()
            logger.error(f"Failed to soft-delete asset {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during asset deactivation."
            )

    # --- 8. delete_assets_batch (Soft-Delete) ---
    def delete_assets_batch(self, ids: list[int]) -> None:
        """Atomic batch soft-delete using efficient SQL IN operator.
        
        Sets is_active=False for all provided IDs. If any ID is not found,
        the entire transaction is rolled back.
        """
        # 1. Safety check
        if not ids:
            return

        try:
            # 2. Fetch all targeted items in one query
            statement = select(DimAsset).where(col(DimAsset.id).in_(ids))
            items = self.session.exec(statement).all()

            # 3. Validation: Strict All-or-Nothing
            if len(items) != len(ids):
                found_ids = {item.id for item in items}
                missing_ids = set(ids) - found_ids
                
                logger.warning(f"Batch soft-delete failed. Missing IDs: {missing_ids}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch aborted. One or more IDs not found: {missing_ids}"
                )

            # 4. Perform the Soft-Delete
            now = datetime.now(UTC)
            for item in items:
                item.is_active = False
                item.updated_at = now
                # We add back to session to mark them as "dirty" (to be updated)
                self.session.add(item)

            # 5. Atomic Commit: All are marked inactive at once
            self.session.commit()
            logger.info(f"Successfully deactivated {len(items)} assets. IDs: {ids}")

        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Asset soft-delete failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
