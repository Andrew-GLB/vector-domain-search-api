import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimProvider

# Layer 3: Domain Entities
from app.domain.provider import ProviderDomain


logger = logging.getLogger(__name__)

class ProviderService:
    """Service layer for managing Cloud Infrastructure Providers.

    Handles the lifecycle of cloud platforms (AWS, Azure, etc.) and provides
    refined platform classification reports for the Gold Layer.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_prov: DimProvider) -> ProviderDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_prov (DimProvider): The database record.

        Returns:
            ProviderDomain: The Pydantic domain representation.
        """
        return ProviderDomain.model_validate(db_prov)

    def _get_dim_provider_or_404(self, id: int) -> DimProvider:
        """Internal helper to retrieve an active provider or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimProvider: The database record.

        Raises:
            HTTPException: 404 status if not found or inactive.
        """
        # 1. Fetch record ensuring it is currently active
        statement = select(DimProvider).where(
            DimProvider.id == id,
            col(DimProvider.is_active)
        )
        provider = self.session.exec(statement).first()
        
        if not provider:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Provider with ID {id} not found."
            )
        return provider

    # --- 1. create_provider ---
    def create_provider(self, provider_in: ProviderDomain) -> ProviderDomain:
        """Full CRUD: Validates business rules and persists a new provider.

        Args:
            provider_in (ProviderDomain): Input data from the API.

        Returns:
            ProviderDomain: The created provider.
        """
        # 1. Unique Name Check (Business Key)
        statement = select(DimProvider).where(DimProvider.provider_name == provider_in.provider_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Provider '{provider_in.provider_name}' is already registered."
            )

        # 2. Extract data excluding system-managed fields
        provider_data = provider_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_provider = DimProvider(**provider_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_provider.source_timestamp = getattr(provider_in, "source_timestamp", None) or now
        new_provider.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_provider)
            self.session.commit()
            
            # 5. Refresh to capture generated ID
            self.session.refresh(new_provider)
            return self._map_to_domain(new_provider)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create provider: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during provider creation."
            )

    # --- 2. create_providers_batch ---
    def create_providers_batch(self, providers_in: list[ProviderDomain]) -> list[ProviderDomain]:
        """Requirement: Batch CRUD. Ingests multiple providers in one transaction.

        Args:
            providers_in (list[ProviderDomain]): List of provider objects.

        Returns:
            list[ProviderDomain]: The list of created providers.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_names = [p.provider_name for p in providers_in]
        statement = select(DimProvider).where(col(DimProvider.provider_name).in_(input_names))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains provider names that already exist."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for p_in in providers_in:
            entry_data = p_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_provider = DimProvider(**entry_data)
            db_provider.source_timestamp = getattr(p_in, "source_timestamp", None) or now
            db_provider.updated_at = now
            db_entries.append(db_provider)

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
            logger.error(f"Batch provider creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch provider creation."
            )

    # --- 3. get_all_providers ---
    def get_all_providers(self, limit: int = 100, offset: int = 0) -> list[ProviderDomain]:
        """Full CRUD: Retrieves all active providers with pagination and sorting.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip for pagination.

        Returns:
            list[ProviderDomain]: Paginated list of providers.
        """
        # 1. Build statement with pagination and alphabetical sorting
        statement = (
            select(DimProvider)
            .where(col(DimProvider.is_active))
            .order_by(col(DimProvider.provider_name))
            .offset(offset)
            .limit(limit)
        )
        
        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(p) for p in results]

    # --- 4. get_provider ---
    def get_provider(self, id: int) -> ProviderDomain:
        """Full CRUD: Retrieves a single provider record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            ProviderDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_prov = self._get_dim_provider_or_404(id)
        return self._map_to_domain(db_prov)

    # --- 5. update_provider ---
    def update_provider(self, id: int, data: ProviderDomain) -> ProviderDomain:
        """Full CRUD: Updates an existing provider dimension record.

        Args:
            id (int): The ID to update.
            data (ProviderDomain): Updated provider data.

        Returns:
            ProviderDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_prov = self._get_dim_provider_or_404(id)

        # 2. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_prov.sqlmodel_update(update_data)
        
        # 3. Refresh update timestamp
        db_prov.updated_at = datetime.now(UTC)

        try:
            # 4. Persist and return
            self.session.add(db_prov)
            self.session.commit()
            self.session.refresh(db_prov)
            return self._map_to_domain(db_prov)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update provider {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during provider update."
            )

    # --- 6. update_providers_batch ---
    def update_providers_batch(self, data: list[ProviderDomain]) -> list[ProviderDomain]:
        """Requirement: Batch CRUD. Updates multiple providers using 'provider_name'.

        Args:
            data (list[ProviderDomain]): Updated provider data list.

        Returns:
            list[ProviderDomain]: Refreshed list of updated entities.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_names = [p.provider_name for p in data]
        statement = select(DimProvider).where(col(DimProvider.provider_name).in_(input_names))
        db_provs = self.session.exec(statement).all()
        
        # 2. Create Lookup Map for O(1) access
        db_map = {p.provider_name: p for p in db_provs}

        # 3. Atomic Validation
        for p_data in data:
            if p_data.provider_name not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Provider '{p_data.provider_name}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for p_data in data:
                db_prov = db_map[p_data.provider_name]
                update_dict = p_data.model_dump(exclude={"id", "updated_at"})
                db_prov.sqlmodel_update(update_dict)
                db_prov.updated_at = now
                self.session.add(db_prov)
                updated_entries.append(db_prov)

            # 5. Commit and map results
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch provider update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_provider ---
    def delete_provider(self, id: int) -> None:
        """Full CRUD: Marks a provider as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record via helper
        db_prov = self._get_dim_provider_or_404(id)
        
        # 2. Early return if already inactive
        if not db_prov.is_active:
            return

        try:
            # 3. Apply Soft-Delete and update metadata
            db_prov.is_active = False
            db_prov.updated_at = datetime.now(UTC)
            self.session.add(db_prov)
            self.session.commit()
            logger.info(f"Provider {id} deactivated.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete provider {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during provider deactivation."
            )

    # --- 8. delete_providers_batch ---
    def delete_providers_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple providers as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Input Safety Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items in single query
            statement = select(DimProvider).where(col(DimProvider.id).in_(ids))
            items = self.session.exec(statement).all()

            # 3. Validation: Ensure all requested IDs exist and are active
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
            logger.info(f"Successfully deactivated {len(items)} providers.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch provider deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
