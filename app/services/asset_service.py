import logging

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
        return AssetDomain(
            resource_name=db_asset.resource_name,
            serial_number=db_asset.serial_number,
            description=db_asset.description,
            created_at=db_asset.created_at
        )

    def _get_dim_asset_or_404(self, id: int) -> DimAsset:
        """Internal helper to retrieve a status record or raise a 404 error.

        Args:
            id (int): The primary key ID.

        Returns:
            DimAsset: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        asset = self.session.get(DimAsset, id)
        if not asset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Asset with ID {id} not found."
            )
        return asset

    # --- 1. create_asset ---
    def create_asset(self, asset_in: AssetDomain, enrich_with_ai: bool = False) -> AssetDomain:
        """Persists a new cloud asset and synchronizes with the Vector DB."""
        statement = select(DimAsset).where(DimAsset.serial_number == asset_in.serial_number)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Asset with serial {asset_in.serial_number} already exists."
            )

        new_db_asset = DimAsset(
            resource_name=asset_in.resource_name,
            serial_number=asset_in.serial_number,
            description=asset_in.description,
            created_at=asset_in.created_at
        )
        self.session.add(new_db_asset)
        self.session.commit()
        self.session.refresh(new_db_asset)
        return self._map_to_domain(new_db_asset)

    # --- 2. create_assets_batch ---
    def create_assets_batch(self, data: list[AssetDomain]) -> list[AssetDomain]:
        """Requirement: Batch CRUD. Ingests multiple assets in one transaction."""
        db_entries = [
            DimAsset(
                resource_name=a.resource_name,
                serial_number=a.serial_number,
                description=a.description,
                created_at=a.created_at
            ) for a in data
        ]
        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return data
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Asset creation failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch creation.")

    # --- 3. get_all_assets ---
    def get_all_assets(self) -> list[AssetDomain]:
        """Retrieves all assets from the database."""
        results = self.session.exec(select(DimAsset)).all()
        return [self._map_to_domain(a) for a in results]

    # --- 4. get_asset ---
    def get_asset(self, id: int) -> AssetDomain:
        """Retrieves a single asset by ID."""
        return self._map_to_domain(self._get_dim_asset_or_404(id))

    # --- 5. update_asset ---
    def update_asset(self, id: int, data: AssetDomain) -> AssetDomain:
        """Updates an existing asset and re-syncs with Vector DB."""
        db_asset = self._get_dim_asset_or_404(id)

        db_asset.resource_name = data.resource_name
        db_asset.description = data.description
        db_asset.created_at = data.created_at

        self.session.add(db_asset)
        self.session.commit()
        self.session.refresh(db_asset)
        return self._map_to_domain(db_asset)

    # --- 6. update_assets_batch ---
    def update_assets_batch(self, data: list[AssetDomain]) -> list[AssetDomain]:
        """Atomic batch update using serial_number as business key."""
        try:
            for a_data in data:
                statement = select(DimAsset).where(DimAsset.serial_number == a_data.serial_number)
                db_asset = self.session.exec(statement).first()

                if not db_asset:
                    self.session.rollback()
                    raise HTTPException(
                        status_code=404,
                        detail=f"Asset serial '{a_data.serial_number}' not found. Batch aborted."
                    )

                db_asset.resource_name = a_data.resource_name
                db_asset.description = a_data.description
                db_asset.created_at = a_data.created_at
                self.session.add(db_asset)

            self.session.commit()
            return data
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Asset update failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch update.")

    # --- 7. delete_asset ---
    def delete_asset(self, id: int) -> None:
        """Removes a single asset by ID."""
        db_asset = self._get_dim_asset_or_404(id)
        self.session.delete(db_asset)
        self.session.commit()

    # --- 8. delete_assets_batch ---
    def delete_assets_batch(self, ids: list[int]) -> None:
        """Atomic batch delete using efficient SQL IN operator."""
        try:
            statement = select(DimAsset).where(col(DimAsset.id).in_(ids))
            items = self.session.exec(statement).all()

            if len(items) != len(ids):
                self.session.rollback()
                raise HTTPException(status_code=404, detail="One or more Asset IDs not found. Batch aborted.")

            for item in items:
                self.session.delete(item)

            self.session.commit()
            logger.info(f"Successfully deleted {len(items)} assets.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Asset deletion failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch delete.")
