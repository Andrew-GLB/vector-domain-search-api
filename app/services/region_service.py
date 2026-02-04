import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimRegion

# Layer 3: Domain Entities
from app.domain.region import RegionDomain


logger = logging.getLogger(__name__)

class RegionService:
    """Service layer for managing geographic and logical cloud regions.

    This service coordinates the lifecycle of cloud regions, ensuring
    standardized geographic identifiers and providing refined reach reports
    for the Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_obj: DimRegion) -> RegionDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_obj (DimRegion): The database record.

        Returns:
            RegionDomain: The Pydantic domain representation.
        """
        return RegionDomain.model_validate(db_obj)

    def _get_dim_region_or_404(self, id: int) -> DimRegion:
        """Internal helper to retrieve an active region or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimRegion: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist or is inactive.
        """
        # 1. Fetch record ensuring it is currently active
        statement = select(DimRegion).where(
            DimRegion.id == id,
            col(DimRegion.is_active)
        )
        region = self.session.exec(statement).first()

        if not region:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Region with ID {id} not found."
            )
        return region

    # --- 1. create_region ---
    def create_region(self, region_in: RegionDomain) -> RegionDomain:
        """Full CRUD: Validates geographic rules and persists a new region.

        Args:
            region_in (RegionDomain): Input data from the API.

        Returns:
            RegionDomain: The created region entity.
        """
        # 1. Unique Code Check (Business Key)
        statement = select(DimRegion).where(DimRegion.region_code == region_in.region_code)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Region code '{region_in.region_code}' is already registered."
            )

        # 2. Extract data excluding system-managed fields
        region_data = region_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_region = DimRegion(**region_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_region.source_timestamp = getattr(region_in, "source_timestamp", None) or now
        new_region.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_region)
            self.session.commit()
            
            # 5. Refresh to capture ID and return Domain model
            self.session.refresh(new_region)
            return self._map_to_domain(new_region)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create region: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during region creation."
            )

    # --- 2. create_regions_batch ---
    def create_regions_batch(self, regions_in: list[RegionDomain]) -> list[RegionDomain]:
        """Requirement: Batch CRUD. Ingests multiple regions in one transaction.

        Args:
            regions_in (list[RegionDomain]): List of region objects.

        Returns:
            list[RegionDomain]: The list of created regions.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_codes = [r.region_code for r in regions_in]
        statement = select(DimRegion).where(col(DimRegion.region_code).in_(input_codes))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains region codes that already exist."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for r_in in regions_in:
            entry_data = r_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_region = DimRegion(**entry_data)
            db_region.source_timestamp = getattr(r_in, "source_timestamp", None) or now
            db_region.updated_at = now
            db_entries.append(db_region)

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
            logger.error(f"Batch region creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch region creation."
            )

    # --- 3. get_all_regions ---
    def get_all_regions(self, limit: int = 100, offset: int = 0) -> list[RegionDomain]:
        """Full CRUD: Retrieves all active regions with pagination and sorting.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip for pagination.

        Returns:
            list[RegionDomain]: Paginated list of regions.
        """
        # 1. Build statement with pagination and alphabetical sorting
        statement = (
            select(DimRegion)
            .where(col(DimRegion.is_active))
            .order_by(col(DimRegion.region_code))
            .offset(offset)
            .limit(limit)
        )
        
        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(r) for r in results]

    # --- 4. get_region ---
    def get_region(self, id: int) -> RegionDomain:
        """Full CRUD: Retrieves a single region record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            RegionDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_region = self._get_dim_region_or_404(id)
        return self._map_to_domain(db_region)

    # --- 5. update_region ---
    def update_region(self, id: int, data: RegionDomain) -> RegionDomain:
        """Full CRUD: Updates an existing region's properties.

        Args:
            id (int): The ID to update.
            data (RegionDomain): Updated region data.

        Returns:
            RegionDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_region = self._get_dim_region_or_404(id)

        # 2. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_region.sqlmodel_update(update_data)
        
        # 3. Refresh update timestamp
        db_region.updated_at = datetime.now(UTC)

        try:
            # 4. Persist and return
            self.session.add(db_region)
            self.session.commit()
            self.session.refresh(db_region)
            return self._map_to_domain(db_region)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update region {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during region update."
            )

    # --- 6. update_regions_batch ---
    def update_regions_batch(self, data: list[RegionDomain]) -> list[RegionDomain]:
        """Requirement: Batch CRUD. Updates multiple regions using 'region_code'.

        Args:
            data (list[RegionDomain]): Updated region data list.

        Returns:
            list[RegionDomain]: Refreshed list of updated entities.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_codes = [r.region_code for r in data]
        statement = select(DimRegion).where(col(DimRegion.region_code).in_(input_codes))
        db_regions = self.session.exec(statement).all()
        
        # 2. Create Lookup Map for O(1) access
        db_map = {r.region_code: r for r in db_regions}

        # 3. Atomic Validation
        for r_data in data:
            if r_data.region_code not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Region code '{r_data.region_code}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for r_data in data:
                db_region = db_map[r_data.region_code]
                update_dict = r_data.model_dump(exclude={"id", "updated_at"})
                db_region.sqlmodel_update(update_dict)
                db_region.updated_at = now
                self.session.add(db_region)
                updated_entries.append(db_region)

            # 5. Commit and map results
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch region update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_region ---
    def delete_region(self, id: int) -> None:
        """Full CRUD: Marks a region as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record via helper
        db_region = self._get_dim_region_or_404(id)
        
        # 2. Early return if already inactive
        if not db_region.is_active:
            return

        try:
            # 3. Apply Soft-Delete and update metadata
            db_region.is_active = False
            db_region.updated_at = datetime.now(UTC)
            self.session.add(db_region)
            self.session.commit()
            logger.info(f"Region {id} deactivated.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete region {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during region deactivation."
            )

    # --- 8. delete_regions_batch ---
    def delete_regions_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple regions as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Input Safety Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items in single query
            statement = select(DimRegion).where(col(DimRegion.id).in_(ids))
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
            logger.info(f"Successfully deactivated {len(items)} regions.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch region deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
