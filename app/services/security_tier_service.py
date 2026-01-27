import logging
from typing import Any, cast

from fastapi import HTTPException, status

# Added col to satisfy Mypy strict typing on Optional fields
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimSecurityTier

# Layer 3: Domain Entities
from app.domain.security_tier import SecurityTierDomain


logger = logging.getLogger(__name__)

class SecurityTierService:
    """Service layer for managing Security and Compliance Tiers.

    This service coordinates the lifecycle of security classifications, ensuring
    governance standards are standardized and providing refined compliance
    reports for the Gold Layer.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_obj: DimSecurityTier) -> SecurityTierDomain:
        """Maps a Data Access model back to a pure Domain entity.
        Uses typing.cast to satisfy Mypy strict Literal checking.

        Args:
            db_obj (DimSecurityTier): The database record.

        Returns:
            SecurityTierDomain: The Pydantic domain representation.
        """
        return SecurityTierDomain(
            tier_name=cast(Any, db_obj.tier_name),
            encryption_required=db_obj.encryption_required,
            compliance_standard=db_obj.compliance_standard
        )

    def _get_dim_tier_or_404(self, tier_id: int) -> DimSecurityTier:
        """Internal helper to retrieve a security tier or raise a 404 error.

        Args:
            tier_id (int): The primary key ID.

        Returns:
            DimSecurityTier: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        tier = self.session.get(DimSecurityTier, tier_id)
        if not tier:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Security Tier with ID {tier_id} not found."
            )
        return tier

    def create_security_tier(self, tier_in: SecurityTierDomain) -> SecurityTierDomain:
        """Validates business rules and persists a new security tier.

        Args:
            tier_in (SecurityTierDomain): Input data from the API.

        Returns:
            SecurityTierDomain: The created security tier.

        Raises:
            HTTPException: 400 status if the tier name already exists.
        """
        statement = select(DimSecurityTier).where(DimSecurityTier.tier_name == tier_in.tier_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Security Tier '{tier_in.tier_name}' already exists."
            )

        new_db_tier = DimSecurityTier(**tier_in.model_dump())
        self.session.add(new_db_tier)
        self.session.commit()
        self.session.refresh(new_db_tier)
        return self._map_to_domain(new_db_tier)

    def create_security_tiers_batch(self, tiers_in: list[SecurityTierDomain]) -> list[SecurityTierDomain]:
        """Requirement: Batch CRUD operations.
        Optimizes ingestion for bulk compliance tier setup.
        """
        db_entries = [DimSecurityTier(**t.model_dump()) for t in tiers_in]
        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return tiers_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Security Tier load failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process batch security tier ingestion."
            )

    def get_all_security_tiers(self) -> list[SecurityTierDomain]:
        """Retrieves all registered security and compliance tiers."""
        results = self.session.exec(select(DimSecurityTier)).all()
        return [self._map_to_domain(t) for t in results]

    def get_security_tier(self, tier_id: int) -> SecurityTierDomain:
        """Full CRUD: Retrieves a single security tier by ID."""
        db_tier = self._get_dim_tier_or_404(tier_id)
        return self._map_to_domain(db_tier)

    def update_security_tier(self, tier_id: int, tier_in: SecurityTierDomain) -> SecurityTierDomain:
        """Full CRUD: Updates an existing security tier."""
        db_tier = self._get_dim_tier_or_404(tier_id)

        db_tier.encryption_required = tier_in.encryption_required
        db_tier.compliance_standard = tier_in.compliance_standard

        self.session.add(db_tier)
        self.session.commit()
        self.session.refresh(db_tier)
        return self._map_to_domain(db_tier)

    def update_security_tiers_batch(self, tiers_in: list[SecurityTierDomain]) -> list[SecurityTierDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple tiers using 'tier_name' as the business key.
        """
        try:
            for t_data in tiers_in:
                statement = select(DimSecurityTier).where(DimSecurityTier.tier_name == t_data.tier_name)
                db_tier = self.session.exec(statement).first()

                if not db_tier:
                    self.session.rollback()
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Security Tier '{t_data.tier_name}' not found. Batch update aborted."
                    )

                db_tier.encryption_required = t_data.encryption_required
                db_tier.compliance_standard = t_data.compliance_standard
                self.session.add(db_tier)

            self.session.commit()
            return tiers_in
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Security Tier update failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch update.")

    def delete_security_tier(self, tier_id: int) -> None:
        """Full CRUD: Deletes a single security tier record."""
        db_tier = self._get_dim_tier_or_404(tier_id)
        self.session.delete(db_tier)
        self.session.commit()

    def delete_security_tiers_batch(self, ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes multiple tiers by their primary key IDs.
        """
        try:
            # Fix: Use col() to satisfy Mypy for Optional ID fields
            statement = select(DimSecurityTier).where(col(DimSecurityTier.id).in_(ids))
            items_to_delete = self.session.exec(statement).all()

            if len(items_to_delete) != len(ids):
                found_ids = {item.id for item in items_to_delete if item.id is not None}
                missing_ids = set(ids) - found_ids
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch delete aborted. IDs not found: {list(missing_ids)}"
                )

            for item in items_to_delete:
                self.session.delete(item)

            self.session.commit()
            logger.info(f"Successfully deleted batch of {len(items_to_delete)} security tiers.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Security Tier deletion failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch deletion.")

    # --- GOLD LAYER ANALYTICS ---

    def get_compliance_exposure_gold(self) -> list[dict[str, Any]]:
        """Requirement: Querying Gold Layer.
        Refines security data to show compliance standards coverage.
        """
        tiers = self.get_all_security_tiers()
        return [
            {
                "tier": t.tier_name,
                "standard": t.compliance_standard,
                "encryption": "Mandatory" if t.encryption_required else "Optional"
            }
            for t in tiers
        ]
