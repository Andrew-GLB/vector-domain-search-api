import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimHardwareProfile

# Layer 3: Domain Entities
from app.domain.hardware_profile import HardwareProfileDomain


logger = logging.getLogger(__name__)

class HardwareProfileService:
    """Service layer for managing Resource Hardware Profiles.

    This service coordinates the technical specifications of entities,
    ensuring standardized compute tiers and providing refined hardware
    metrics for the Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_profile: DimHardwareProfile) -> HardwareProfileDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_profile (DimHardwareProfile): The database record.

        Returns:
            HardwareProfileDomain: The Pydantic domain representation.
        """
        return HardwareProfileDomain.model_validate(db_profile)

    def _get_dim_profile_or_404(self, id: int) -> DimHardwareProfile:
        """Internal helper to retrieve an active hardware profile or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimHardwareProfile: The database record.

        Raises:
            HTTPException: 404 status if not found or inactive.
        """
        # 1. Fetch record ensuring it is currently active
        statement = select(DimHardwareProfile).where(
            DimHardwareProfile.id == id,
            col(DimHardwareProfile.is_active)
        )
        profile = self.session.exec(statement).first()
        
        if not profile:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Hardware Profile with ID {id} not found."
            )
        return profile

    # --- 1. create_hardware_profile ---
    def create_hardware_profile(self, profile_in: HardwareProfileDomain) -> HardwareProfileDomain:
        """Full CRUD: Validates technical rules and persists a new profile.

        Args:
            profile_in (HardwareProfileDomain): Input specs from the API.

        Returns:
            HardwareProfileDomain: The created profile.
        """
        # 1. Unique Name Check (Business Key)
        statement = select(DimHardwareProfile).where(
            DimHardwareProfile.profile_name == profile_in.profile_name
        )
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Profile '{profile_in.profile_name}' is already registered."
            )

        # 2. Extract data excluding system-managed fields
        profile_data = profile_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_profile = DimHardwareProfile(**profile_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_profile.source_timestamp = getattr(profile_in, "source_timestamp", None) or now
        new_profile.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_profile)
            self.session.commit()
            
            # 5. Refresh to capture ID and return Domain model
            self.session.refresh(new_profile)
            return self._map_to_domain(new_profile)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create hardware profile: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during hardware profile creation."
            )

    # --- 2. create_hardware_profiles_batch ---
    def create_hardware_profiles_batch(self, profiles_in: list[HardwareProfileDomain]) -> list[HardwareProfileDomain]:
        """Requirement: Batch CRUD. Ingests multiple profiles in one transaction.

        Args:
            profiles_in (list[HardwareProfileDomain]): List of hardware profile objects.

        Returns:
            list[HardwareProfileDomain]: The list of created profiles.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_names = [p.profile_name for p in profiles_in]
        statement = select(DimHardwareProfile).where(col(DimHardwareProfile.profile_name).in_(input_names))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains profile names that already exist."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for p_in in profiles_in:
            entry_data = p_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_profile = DimHardwareProfile(**entry_data)
            db_profile.source_timestamp = getattr(p_in, "source_timestamp", None) or now
            db_profile.updated_at = now
            db_entries.append(db_profile)

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
            logger.error(f"Batch hardware profile creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch profile creation."
            )

    # --- 3. get_all_profiles ---
    def get_all_profiles(self, limit: int = 100, offset: int = 0) -> list[HardwareProfileDomain]:
        """Full CRUD: Retrieves all active hardware profiles from the database.

        Args:
            limit (int): Max records to return. Defaults to 100.
            offset (int): Records to skip for pagination. Defaults to 0.

        Returns:
            list[HardwareProfileDomain]: Paginated list of profiles.
        """
        # 1. Build statement with pagination and alphabetical sorting
        statement = (
            select(DimHardwareProfile)
            .where(col(DimHardwareProfile.is_active))
            .order_by(col(DimHardwareProfile.profile_name))
            .offset(offset)
            .limit(limit)
        )
        
        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(p) for p in results]

    # --- 4. get_hardware_profile ---
    def get_hardware_profile(self, id: int) -> HardwareProfileDomain:
        """Full CRUD: Retrieves a single hardware profile record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            HardwareProfileDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_profile = self._get_dim_profile_or_404(id)
        return self._map_to_domain(db_profile)

    # --- 5. update_hardware_profile ---
    def update_hardware_profile(self, id: int, data: HardwareProfileDomain) -> HardwareProfileDomain:
        """Full CRUD: Updates an existing hardware profile's technical specs.

        Args:
            id (int): The ID to update.
            data (HardwareProfileDomain): Updated specs.

        Returns:
            HardwareProfileDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_profile = self._get_dim_profile_or_404(id)

        # 2. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_profile.sqlmodel_update(update_data)
        
        # 3. Refresh update timestamp
        db_profile.updated_at = datetime.now(UTC)

        try:
            # 4. Persist and return
            self.session.add(db_profile)
            self.session.commit()
            self.session.refresh(db_profile)
            return self._map_to_domain(db_profile)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update hardware profile {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during hardware profile update."
            )

    # --- 6. update_hardware_profiles_batch ---
    def update_hardware_profiles_batch(self, profiles_in: list[HardwareProfileDomain]) -> list[HardwareProfileDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple profiles using 'profile_name' as business key.

        Args:
            profiles_in (list[HardwareProfileDomain]): Updated hardware data.

        Returns:
            list[HardwareProfileDomain]: Refreshed list of updated entities.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_names = [p.profile_name for p in profiles_in]
        statement = select(DimHardwareProfile).where(col(DimHardwareProfile.profile_name).in_(input_names))
        db_profiles = self.session.exec(statement).all()
        
        # 2. Create Lookup Map for O(1) access
        db_map = {p.profile_name: p for p in db_profiles}

        # 3. Atomic Validation
        for p_data in profiles_in:
            if p_data.profile_name not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Profile '{p_data.profile_name}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for p_data in profiles_in:
                db_profile = db_map[p_data.profile_name]
                update_dict = p_data.model_dump(exclude={"id", "updated_at"})
                db_profile.sqlmodel_update(update_dict)
                db_profile.updated_at = now
                self.session.add(db_profile)
                updated_entries.append(db_profile)

            # 5. Commit and map results
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch hardware profile update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_hardware_profile ---
    def delete_hardware_profile(self, id: int) -> None:
        """Full CRUD: Marks a hardware profile as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record
        db_profile = self._get_dim_profile_or_404(id)
        
        # 2. Early return if already inactive
        if not db_profile.is_active:
            return

        try:
            # 3. Apply Soft-Delete and update metadata
            db_profile.is_active = False
            db_profile.updated_at = datetime.now(UTC)
            self.session.add(db_profile)
            self.session.commit()
            logger.info(f"Hardware Profile {id} deactivated.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete hardware profile {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during hardware profile deactivation."
            )

    # --- 8. delete_hardware_profiles_batch ---
    def delete_hardware_profiles_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple profiles as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Input Safety Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items in single query
            statement = select(DimHardwareProfile).where(col(DimHardwareProfile.id).in_(ids))
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
            logger.info(f"Successfully deactivated {len(items)} hardware profiles.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch hardware profile deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
