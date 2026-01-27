import logging
from typing import Any

from fastapi import HTTPException, status

# Added col to satisfy Mypy strict typing on Optional fields
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimHardwareProfile

# Layer 3: Domain Entities
from app.domain.hardware_profile import HardwareProfileDomain


logger = logging.getLogger(__name__)

class HardwareProfileService:
    """Service layer for managing Resource Hardware Profiles.

    This service coordinates the technical specifications of entities,
    ensuring that profile names are unique and providing refined
    compute-density reports for the Gold Layer.
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
        return HardwareProfileDomain(
            profile_name=db_profile.profile_name,
            cpu_count=db_profile.cpu_count,
            ram_gb=db_profile.ram_gb
        )

    def _get_dim_profile_or_404(self, profile_id: int) -> DimHardwareProfile:
        """Internal helper to retrieve a hardware profile or raise a 404 error.

        Args:
            profile_id (int): The primary key ID.

        Returns:
            DimHardwareProfile: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        profile = self.session.get(DimHardwareProfile, profile_id)
        if not profile:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Hardware Profile with ID {profile_id} not found."
            )
        return profile

    def create_hardware_profile(self, profile_in: HardwareProfileDomain) -> HardwareProfileDomain:
        """Validates and persists a new hardware profile.

        Args:
            profile_in (HardwareProfileDomain): Input data from the API.

        Returns:
            HardwareProfileDomain: The created hardware profile.

        Raises:
            HTTPException: 400 status if the profile name already exists.
        """
        # 1. Unique constraint check
        statement = select(DimHardwareProfile).where(
            DimHardwareProfile.profile_name == profile_in.profile_name
        )
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Profile '{profile_in.profile_name}' already exists."
            )

        # 2. Map and Persist
        new_db_profile = DimHardwareProfile(
            profile_name=profile_in.profile_name,
            cpu_count=profile_in.cpu_count,
            ram_gb=profile_in.ram_gb
        )
        self.session.add(new_db_profile)
        self.session.commit()
        self.session.refresh(new_db_profile)

        return self._map_to_domain(new_db_profile)

    def create_hardware_profiles_batch(self, profiles_in: list[HardwareProfileDomain]) -> list[HardwareProfileDomain]:
        """Requirement: CRUD should allow batch operations.
        
        Optimizes the ingestion of multiple hardware profiles in a single transaction.

        Args:
            profiles_in (List[HardwareProfileDomain]): A list of hardware profile objects.

        Returns:
            List[HardwareProfileDomain]: The list of created profiles.
        """
        db_entries = [
            DimHardwareProfile(
                profile_name=p.profile_name,
                cpu_count=p.cpu_count,
                ram_gb=p.ram_gb
            ) for p in profiles_in
        ]

        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return profiles_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Hardware Profile load failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process batch hardware profile creation."
            )

    def get_all_profiles(self) -> list[HardwareProfileDomain]:
        """Retrieves all registered hardware profiles."""
        statement = select(DimHardwareProfile)
        results = self.session.exec(statement).all()
        return [self._map_to_domain(p) for p in results]

    def get_hardware_profile(self, profile_id: int) -> HardwareProfileDomain:
        """Full CRUD: Retrieves a single hardware profile record by ID.

        Args:
            profile_id (int): The primary key ID of the profile.

        Returns:
            HardwareProfileDomain: The domain representation of the profile.
        """
        db_profile = self._get_dim_profile_or_404(profile_id)
        return self._map_to_domain(db_profile)

    def update_hardware_profile(self, profile_id: int, profile_in: HardwareProfileDomain) -> HardwareProfileDomain:
        """Full CRUD: Updates an existing hardware profile's technical specs.

        Args:
            profile_id (int): The ID to update.
            profile_in (HardwareProfileDomain): The updated specs.

        Returns:
            HardwareProfileDomain: The updated entity.
        """
        db_profile = self._get_dim_profile_or_404(profile_id)

        db_profile.cpu_count = profile_in.cpu_count
        db_profile.ram_gb = profile_in.ram_gb

        self.session.add(db_profile)
        self.session.commit()
        self.session.refresh(db_profile)
        return self._map_to_domain(db_profile)

    def update_hardware_profiles_batch(self, profiles_in: list[HardwareProfileDomain]) -> list[HardwareProfileDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple hardware profiles using 'profile_name' as the business key.
        
        Ensures atomicity: if one profile is not found, the entire batch transaction is rolled back.

        Args:
            profiles_in (List[HardwareProfileDomain]): List of updated hardware profile data.

        Returns:
            List[HardwareProfileDomain]: The original input list on success.

        Raises:
            HTTPException: 404 status if a specific profile name is not found.
            HTTPException: 500 status if a database error occurs.
        """
        try:
            for p_data in profiles_in:
                # 1. Lookup by unique business key
                statement = select(DimHardwareProfile).where(
                    DimHardwareProfile.profile_name == p_data.profile_name
                )
                db_profile = self.session.exec(statement).first()

                if not db_profile:
                    self.session.rollback()
                    logger.error(f"Batch update failed: Profile '{p_data.profile_name}' not found.")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Profile '{p_data.profile_name}' not found. Batch update aborted."
                    )

                # 2. Apply updates to the model
                db_profile.cpu_count = p_data.cpu_count
                db_profile.ram_gb = p_data.ram_gb
                self.session.add(db_profile)

            self.session.commit()
            logger.info(f"Successfully updated batch of {len(profiles_in)} hardware profiles.")
            return profiles_in

        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Hardware Profile update failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch hardware profile update."
            )

    def delete_hardware_profile(self, profile_id: int) -> bool:
        """Full CRUD: Deletes a hardware profile record."""
        db_profile = self._get_dim_profile_or_404(profile_id)
        self.session.delete(db_profile)
        self.session.commit()
        return True

    def delete_hardware_profiles_batch(self, ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes multiple hardware profiles by their primary key IDs.
        
        Uses col() to satisfy Mypy strict typing.

        Args:
            ids (List[int]): List of primary key IDs to remove.

        Raises:
            HTTPException: 404 if one or more IDs do not exist.
            HTTPException: 500 if a database error occurs.
        """
        try:
            # 1. Fetch records in single query for efficiency
            statement = select(DimHardwareProfile).where(col(DimHardwareProfile.id).in_(ids))
            items_to_delete = self.session.exec(statement).all()

            # 2. Validation: Ensure all IDs exist
            if len(items_to_delete) != len(ids):
                found_ids = {item.id for item in items_to_delete if item.id is not None}
                missing_ids = set(ids) - found_ids

                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch delete aborted. Profile IDs not found: {list(missing_ids)}"
                )

            # 3. Perform deletions
            for item in items_to_delete:
                self.session.delete(item)

            self.session.commit()
            logger.info(f"Successfully deleted batch of {len(items_to_delete)} hardware profiles.")

        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Hardware Profile deletion failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch hardware profile deletion."
            )

    # --- GOLD LAYER ANALYTICS ---

    def get_compute_density_report_gold(self) -> list[dict[str, Any]]:
        """Requirement: Querying Gold Layer.
        
        Refines hardware data to provide a "Compute Density" score
        (RAM per CPU core) for infrastructure optimization analysis.

        Returns:
            List[Dict[str, Any]]: A list of profiles with calculated density metrics.
        """
        profiles = self.get_all_profiles()
        return [
            {
                "profile": p.profile_name,
                "cpu": p.cpu_count,
                "ram": f"{p.ram_gb}GB",
                "ram_per_cpu": round(p.ram_gb / p.cpu_count, 2),
                "classification": "High Memory" if (p.ram_gb / p.cpu_count) > 4 else "Standard"
            } for p in profiles
        ]
