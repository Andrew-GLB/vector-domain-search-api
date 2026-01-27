import logging
from typing import Any

from fastapi import HTTPException, status

# Added 'col' to imports to satisfy Mypy [union-attr] errors
from sqlmodel import Session, col, select

from app.data_access.models import DimRegion
from app.domain.region import RegionDomain


logger = logging.getLogger(__name__)

class RegionService:
    """Service layer for managing geographic and logical cloud regions."""

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_obj: DimRegion) -> RegionDomain:
        return RegionDomain(**db_obj.model_dump())

    def _get_dim_region_or_404(self, region_id: int) -> DimRegion:
        """Internal helper to retrieve a region record or raise a 404 error."""
        region = self.session.get(DimRegion, region_id)
        if not region:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Region with ID {region_id} not found."
            )
        return region

    def create_region(self, region_in: RegionDomain) -> RegionDomain:
        """Validates and persists a new geographic region."""
        statement = select(DimRegion).where(DimRegion.region_code == region_in.region_code)
        if self.session.exec(statement).first():
            raise HTTPException(status_code=400, detail=f"Region {region_in.region_code} already exists.")

        db_region = DimRegion(**region_in.model_dump())
        self.session.add(db_region)
        self.session.commit()
        self.session.refresh(db_region)
        return self._map_to_domain(db_region)

    def get_region(self, region_id: int) -> RegionDomain | None:
        """Full CRUD: Retrieves a single region record by ID.

        Args:
            region_id (int): The primary key ID.

        Returns:
            Optional[RegionDomain]: The domain entity if found.
        """
        db_region = self.session.get(DimRegion, region_id)
        return self._map_to_domain(db_region) if db_region else None

    def update_region(self, region_id: int, region_in: RegionDomain) -> RegionDomain:
        """Full CRUD: Updates an existing region record.

        Args:
            region_id (int): The ID to update.
            region_in (RegionDomain): The updated data.

        Returns:
            RegionDomain: The updated domain entity.
        """
        db_region = self._get_dim_region_or_404(region_id)

        # Update fields
        db_region.region_code = region_in.region_code
        db_region.display_name = region_in.display_name
        db_region.continent = region_in.continent

        self.session.add(db_region)
        self.session.commit()
        self.session.refresh(db_region)
        return self._map_to_domain(db_region)

    def delete_region(self, region_id: int) -> None:
        """Full CRUD: Deletes a single region."""
        db_region = self._get_dim_region_or_404(region_id)
        self.session.delete(db_region)
        self.session.commit()

    def create_regions_batch(self, regions_in: list[RegionDomain]) -> list[RegionDomain]:
        """Requirement: Batch CRUD operation for global scale setup."""
        db_entries = [DimRegion(**r.model_dump()) for r in regions_in]
        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return regions_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Region load failed: {e}")
            raise HTTPException(status_code=500, detail="Batch creation failed.")

    def get_all_regions(self) -> list[RegionDomain]:
        results = self.session.exec(select(DimRegion)).all()
        return [self._map_to_domain(r) for r in results]

    def update_regions_batch(self, regions_in: list[RegionDomain]) -> list[RegionDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple regions using 'region_code' as the business key.
        """
        try:
            for r_data in regions_in:
                statement = select(DimRegion).where(DimRegion.region_code == r_data.region_code)
                db_region = self.session.exec(statement).first()

                if not db_region:
                    self.session.rollback()
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Region code '{r_data.region_code}' not found. Batch aborted."
                    )

                db_region.display_name = r_data.display_name
                db_region.continent = r_data.continent
                self.session.add(db_region)

            self.session.commit()
            return regions_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Region update failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch update.")

    def delete_regions_batch(self, region_ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes regions by their primary key IDs.
        """
        try:
            # Fix: Use col() to ensure Mypy recognizes the .in_() attribute
            statement = select(DimRegion).where(col(DimRegion.id).in_(region_ids))
            regions_to_delete = self.session.exec(statement).all()

            for region in regions_to_delete:
                self.session.delete(region)

            self.session.commit()
            logger.info(f"Batch delete successful. {len(regions_to_delete)} regions removed.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Region deletion failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch delete.")

    def get_geographic_reach_gold(self) -> list[dict[str, Any]]:
        """Requirement: Querying Gold Layer. Refines regions by continent."""
        regions = self.get_all_regions()
        # Fix: Add explicit type annotation for Mypy
        reach: dict[str, int] = {}
        for r in regions:
            reach[r.continent] = reach.get(r.continent, 0) + 1
        return [{"continent": k, "region_count": v} for k, v in reach.items()]
