import logging
from typing import Any

from fastapi import HTTPException, status
from sqlmodel import Session, select

# Layer 4: Data Access
from app.data_access.models import DimProvider

# Layer 3: Domain Entities
from app.domain.provider import ProviderDomain


logger = logging.getLogger(__name__)

class ProviderService:
    """Service layer for managing Cloud Infrastructure Providers.

    Handles the lifecycle of cloud platforms and provides refined
    platform classification reports for the Gold Layer.
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

    def _get_dim_provider_or_404(self, provider_id: int) -> DimProvider:
        """Internal helper to retrieve a provider or raise 404."""
        provider = self.session.get(DimProvider, provider_id)
        if not provider:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Provider with ID {provider_id} not found."
            )
        return provider

    def create_provider(self, provider_in: ProviderDomain) -> ProviderDomain:
        """Validates and persists a new cloud provider.

        Args:
            provider_in (ProviderDomain): Input data from the API.

        Returns:
            ProviderDomain: The created provider.
        """
        statement = select(DimProvider).where(DimProvider.provider_name == provider_in.provider_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Provider '{provider_in.provider_name}' already exists."
            )

        new_db_prov = DimProvider(**provider_in.model_dump())
        self.session.add(new_db_prov)
        self.session.commit()
        self.session.refresh(new_db_prov)
        return self._map_to_domain(new_db_prov)

    def create_providers_batch(self, providers_in: list[ProviderDomain]) -> list[ProviderDomain]:
        """Requirement: Batch CRUD operations."""
        db_entries = [DimProvider(**p.model_dump()) for p in providers_in]
        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return providers_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Provider load failed: {e}")
            raise HTTPException(status_code=500, detail="Batch creation failed.")

    def get_all_providers(self) -> list[ProviderDomain]:
        """Retrieves all registered providers."""
        results = self.session.exec(select(DimProvider)).all()
        return [self._map_to_domain(p) for p in results]

    def get_provider(self, id: int) -> ProviderDomain | None:
        """Retrieves a single provider by ID."""
        db_prov = self.session.get(DimProvider, id)
        if not db_prov:
            return None
        return self._map_to_domain(db_prov)

    def update_provider(self, id: int, data: ProviderDomain) -> ProviderDomain:
        """Updates an existing provider dimension record."""
        db_prov = self.session.get(DimProvider, id)
        if not db_prov:
            raise HTTPException(status_code=404, detail="Provider not found")

        db_prov.provider_name = data.provider_name
        db_prov.provider_type = data.provider_type
        db_prov.support_contact = data.support_contact

        self.session.add(db_prov)
        self.session.commit()
        self.session.refresh(db_prov)
        return self._map_to_domain(db_prov)

    def update_providers_batch(self, data: list[ProviderDomain]) -> list[ProviderDomain]:
        """Requirement: Batch CRUD. Updates multiple providers by name."""
        updated_list: list[ProviderDomain] = []
        for item in data:
            statement = select(DimProvider).where(DimProvider.provider_name == item.provider_name)
            db_prov = self.session.exec(statement).first()
            if db_prov:
                db_prov.provider_type = item.provider_type
                db_prov.support_contact = item.support_contact
                self.session.add(db_prov)
                updated_list.append(self._map_to_domain(db_prov))

        self.session.commit()
        return updated_list

    def delete_provider(self, id: int) -> None:
        """Removes a provider record from the dimension table.

        Args:
            id (int): The primary key ID of the provider.

        Returns:
            None: Returns nothing on success.
        
        Raises:
            HTTPException: 404 status if the provider does not exist.
        """
        # Use the helper to find the object or raise 404 immediately
        db_prov = self._get_dim_provider_or_404(id)

        self.session.delete(db_prov)
        self.session.commit()


    def delete_providers_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Deletes multiple IDs in one transaction."""
        try:
            for provider_id in ids:
                db_prov = self.session.get(DimProvider, provider_id)
                if db_prov:
                    self.session.delete(db_prov)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to delete batch: {e}")
            raise HTTPException(status_code=500, detail="Batch deletion failed")

    # --- GOLD LAYER ANALYTICS ---

    def get_provider_distribution_gold(self) -> dict[str, Any]:
        """Requirement: Querying Gold Layer.
        
        Refines provider data to show the balance between Public,
        Private, and On-Premise infrastructure.
        """
        providers = self.get_all_providers()
        distribution: dict[str, int] = {}
        for p in providers:
            distribution[p.provider_type] = distribution.get(p.provider_type, 0) + 1

        return {
            "total_providers": len(providers),
            "type_distribution": distribution,
            "report_name": "Infrastructure Source Strategy"
        }
