import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
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
    reports for the Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_obj: DimSecurityTier) -> SecurityTierDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_obj (DimSecurityTier): The database record.

        Returns:
            SecurityTierDomain: The Pydantic domain representation.
        """
        return SecurityTierDomain.model_validate(db_obj)

    def _get_dim_tier_or_404(self, id: int) -> DimSecurityTier:
        """Internal helper to retrieve an active security tier or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimSecurityTier: The database record.

        Raises:
            HTTPException: 404 status if not found or inactive.
        """
        # 1. Build statement filtering by ID and active status
        statement = select(DimSecurityTier).where(
            DimSecurityTier.id == id,
            col(DimSecurityTier.is_active)
        )
        tier = self.session.exec(statement).first()

        # 2. Raise 404 if record is missing or soft-deleted
        if not tier:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Security Tier with ID {id} not found."
            )
        return tier

    # --- 1. create_security_tier ---
    def create_security_tier(self, tier_in: SecurityTierDomain) -> SecurityTierDomain:
        """Full CRUD: Validates governance rules and persists a new security tier.

        Args:
            tier_in (SecurityTierDomain): Input data from the API.

        Returns:
            SecurityTierDomain: The created security tier.
        """
        # 1. Unique Name Check (Business Key)
        statement = select(DimSecurityTier).where(DimSecurityTier.tier_name == tier_in.tier_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Security Tier '{tier_in.tier_name}' is already registered."
            )

        # 2. Extract data excluding system-managed fields
        tier_data = tier_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_tier = DimSecurityTier(**tier_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_tier.source_timestamp = getattr(tier_in, "source_timestamp", None) or now
        new_tier.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_tier)
            self.session.commit()

            # 5. Refresh to capture ID and return Domain model
            self.session.refresh(new_tier)
            return self._map_to_domain(new_tier)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create security tier: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during security tier creation."
            )

    # --- 2. create_security_tiers_batch ---
    def create_security_tiers_batch(self, tiers_in: list[SecurityTierDomain]) -> list[SecurityTierDomain]:
        """Requirement: Batch CRUD. Ingests multiple tiers in one transaction.

        Args:
            tiers_in (list[SecurityTierDomain]): List of security tier objects.

        Returns:
            list[SecurityTierDomain]: The list of created tiers.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_names = [t.tier_name for t in tiers_in]
        statement = select(DimSecurityTier).where(col(DimSecurityTier.tier_name).in_(input_names))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains security tier names that already exist."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for t_in in tiers_in:
            entry_data = t_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_tier = DimSecurityTier(**entry_data)
            db_tier.source_timestamp = getattr(t_in, "source_timestamp", None) or now
            db_tier.updated_at = now
            db_entries.append(db_tier)

        try:
            # 3. Batch insert and Atomic Commit
            self.session.add_all(db_entries)
            self.session.commit()

            # 4. Refresh all to capture generated IDs
            for entry in db_entries:
                self.session.refresh(entry)

            # 5. Map back to Domain objects
            return [self._map_to_domain(e) for e in db_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch security tier creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch security tier creation."
            )

    # --- 3. get_all_security_tiers ---
    def get_all_security_tiers(self, limit: int = 100, offset: int = 0) -> list[SecurityTierDomain]:
        """Full CRUD: Retrieves all active security tiers with pagination.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip for pagination.

        Returns:
            list[SecurityTierDomain]: Paginated list of security tiers.
        """
        # 1. Build statement with pagination and alphabetical sorting
        statement = (
            select(DimSecurityTier)
            .where(col(DimSecurityTier.is_active))
            .order_by(col(DimSecurityTier.tier_name))
            .offset(offset)
            .limit(limit)
        )

        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(t) for t in results]

    # --- 4. get_security_tier ---
    def get_security_tier(self, id: int) -> SecurityTierDomain:
        """Full CRUD: Retrieves a single security tier record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            SecurityTierDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_tier = self._get_dim_tier_or_404(id)
        return self._map_to_domain(db_tier)

    # --- 5. update_security_tier ---
    def update_security_tier(self, id: int, data: SecurityTierDomain) -> SecurityTierDomain:
        """Full CRUD: Updates an existing security tier's properties.

        Args:
            id (int): The ID to update.
            data (SecurityTierDomain): Updated tier data.

        Returns:
            SecurityTierDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_tier = self._get_dim_tier_or_404(id)

        # 2. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_tier.sqlmodel_update(update_data)

        # 3. Refresh update timestamp
        db_tier.updated_at = datetime.now(UTC)

        try:
            # 4. Persist and return
            self.session.add(db_tier)
            self.session.commit()
            self.session.refresh(db_tier)
            return self._map_to_domain(db_tier)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update security tier {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during security tier update."
            )

    # --- 6. update_security_tiers_batch ---
    def update_security_tiers_batch(self, tiers_in: list[SecurityTierDomain]) -> list[SecurityTierDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple tiers using 'tier_name' as business key.

        Args:
            tiers_in (list[SecurityTierDomain]): Updated security tier data.

        Returns:
            list[SecurityTierDomain]: Refreshed list of updated entities.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_names = [t.tier_name for t in tiers_in]
        statement = select(DimSecurityTier).where(col(DimSecurityTier.tier_name).in_(input_names))
        db_tiers = self.session.exec(statement).all()

        # 2. Create Lookup Map for O(1) access
        db_map = {t.tier_name: t for t in db_tiers}

        # 3. Atomic Validation
        for t_data in tiers_in:
            if t_data.tier_name not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Security Tier '{t_data.tier_name}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for t_data in tiers_in:
                db_tier = db_map[t_data.tier_name]
                update_dict = t_data.model_dump(exclude={"id", "updated_at"})
                db_tier.sqlmodel_update(update_dict)
                db_tier.updated_at = now
                self.session.add(db_tier)
                updated_entries.append(db_tier)

            # 5. Commit and map results
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch security tier update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_security_tier ---
    def delete_security_tier(self, id: int) -> None:
        """Full CRUD: Marks a security tier as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record
        db_tier = self._get_dim_tier_or_404(id)

        # 2. Early return if already inactive
        if not db_tier.is_active:
            return

        try:
            # 3. Apply Soft-Delete and update metadata
            db_tier.is_active = False
            db_tier.updated_at = datetime.now(UTC)
            self.session.add(db_tier)
            self.session.commit()
            logger.info(f"Security Tier {id} deactivated.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete security tier {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during security tier deactivation."
            )

    # --- 8. delete_security_tiers_batch ---
    def delete_security_tiers_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple tiers as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Input Safety Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items in single query
            statement = select(DimSecurityTier).where(col(DimSecurityTier.id).in_(ids))
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
            logger.info(f"Successfully deactivated {len(items)} security tiers.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch security tier deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
