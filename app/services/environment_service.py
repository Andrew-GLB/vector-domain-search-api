import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimEnvironment

# Layer 3: Domain Entities
from app.domain.environment import EnvironmentDomain


logger = logging.getLogger(__name__)

class EnvironmentService:
    """Service layer for managing Deployment Environments.

    This service coordinates the lifecycle of environments (e.g., Production, Staging),
    ensuring standardized deployment tiers and providing consistent metadata
    for the Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_env: DimEnvironment) -> EnvironmentDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_env (DimEnvironment): The database record.

        Returns:
            EnvironmentDomain: The Pydantic domain representation.
        """
        return EnvironmentDomain.model_validate(db_env)

    def _get_dim_env_or_404(self, id: int) -> DimEnvironment:
        """Internal helper to retrieve an active environment or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimEnvironment: The database record.

        Raises:
            HTTPException: 404 status if not found or inactive.
        """
        statement = select(DimEnvironment).where(
            DimEnvironment.id == id,
            col(DimEnvironment.is_active)
        )
        env = self.session.exec(statement).first()
        if not env:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Environment with ID {id} not found."
            )
        return env

    # --- 1. create_environment ---
    def create_environment(self, env_in: EnvironmentDomain) -> EnvironmentDomain:
        """Full CRUD: Validates business rules and persists a new environment.

        Args:
            env_in (EnvironmentDomain): Input data from the API.

        Returns:
            EnvironmentDomain: The created environment.
        """
        # 1. Unique Name Check (Business Key)
        statement = select(DimEnvironment).where(DimEnvironment.env_name == env_in.env_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Environment '{env_in.env_name}' is already registered."
            )

        # 2. Extract data excluding system-managed fields
        env_data = env_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_env = DimEnvironment(**env_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_env.source_timestamp = getattr(env_in, "source_timestamp", None) or now
        new_env.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_env)
            self.session.commit()
            
            # 5. Refresh to capture ID and return Domain model
            self.session.refresh(new_env)
            return self._map_to_domain(new_env)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create environment: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during environment creation."
            )

    # --- 2. create_environments_batch ---
    def create_environments_batch(self, envs_in: list[EnvironmentDomain]) -> list[EnvironmentDomain]:
        """Requirement: Batch CRUD. Ingests multiple environments in one transaction.

        Args:
            envs_in (list[EnvironmentDomain]): List of environment objects.

        Returns:
            list[EnvironmentDomain]: The list of created environments.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_names = [e.env_name for e in envs_in]
        statement = select(DimEnvironment).where(col(DimEnvironment.env_name).in_(input_names))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains environment names that already exist."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for e_in in envs_in:
            entry_data = e_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_env = DimEnvironment(**entry_data)
            db_env.source_timestamp = getattr(e_in, "source_timestamp", None) or now
            db_env.updated_at = now
            db_entries.append(db_env)

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
            logger.error(f"Batch environment creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch environment creation."
            )

    # --- 3. get_all_environments ---
    def get_all_environments(self, limit: int = 100, offset: int = 0) -> list[EnvironmentDomain]:
        """Full CRUD: Retrieves all active environments from the database.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip.

        Returns:
            list[EnvironmentDomain]: Paginated list of environments.
        """
        # 1. Build statement with pagination and sorting
        statement = (
            select(DimEnvironment)
            .where(col(DimEnvironment.is_active))
            .order_by(col(DimEnvironment.env_name))
            .offset(offset)
            .limit(limit)
        )
        
        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(e) for e in results]

    # --- 4. get_environment ---
    def get_environment(self, id: int) -> EnvironmentDomain:
        """Full CRUD: Retrieves a single environment record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            EnvironmentDomain: The domain entity.
        """
        # 1. Fetch via helper and map
        db_env = self._get_dim_env_or_404(id)
        return self._map_to_domain(db_env)

    # --- 5. update_environment ---
    def update_environment(self, id: int, data: EnvironmentDomain) -> EnvironmentDomain:
        """Full CRUD: Updates an existing environment's properties.

        Args:
            id (int): The ID to update.
            data (EnvironmentDomain): Updated data.

        Returns:
            EnvironmentDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_env = self._get_dim_env_or_404(id)

        # 2. Dump data and update model
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_env.sqlmodel_update(update_data)
        
        # 3. Refresh update timestamp
        db_env.updated_at = datetime.now(UTC)

        try:
            # 4. Persist and return
            self.session.add(db_env)
            self.session.commit()
            self.session.refresh(db_env)
            return self._map_to_domain(db_env)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update environment {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during environment update."
            )

    # --- 6. update_environments_batch ---
    def update_environments_batch(self, envs_in: list[EnvironmentDomain]) -> list[EnvironmentDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple environments using 'env_name' as business key.

        Args:
            envs_in (list[EnvironmentDomain]): Updated environment data.

        Returns:
            list[EnvironmentDomain]: Refreshed list of updated environments.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_names = [e.env_name for e in envs_in]
        statement = select(DimEnvironment).where(col(DimEnvironment.env_name).in_(input_names))
        db_envs = self.session.exec(statement).all()
        
        # 2. Create Lookup Map
        db_map = {e.env_name: e for e in db_envs}

        # 3. Atomic Validation
        for e_data in envs_in:
            if e_data.env_name not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Environment '{e_data.env_name}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for e_data in envs_in:
                db_env = db_map[e_data.env_name]
                update_dict = e_data.model_dump(exclude={"id", "updated_at"})
                db_env.sqlmodel_update(update_dict)
                db_env.updated_at = now
                self.session.add(db_env)
                updated_entries.append(db_env)

            # 5. Commit and map results
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch environment update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_environment ---
    def delete_environment(self, id: int) -> None:
        """Full CRUD: Marks an environment as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record
        db_env = self._get_dim_env_or_404(id)
        
        # 2. Safety check
        if not db_env.is_active:
            return

        try:
            # 3. Apply Soft-Delete metadata
            db_env.is_active = False
            db_env.updated_at = datetime.now(UTC)
            self.session.add(db_env)
            self.session.commit()
            logger.info(f"Environment {id} deactivated.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete environment {id}: {e}")
            raise HTTPException(status_code=500, detail="Error during environment deactivation.")

    # --- 8. delete_environments_batch ---
    def delete_environments_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple environments as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Empty Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items
            statement = select(DimEnvironment).where(col(DimEnvironment.id).in_(ids))
            items = self.session.exec(statement).all()

            # 3. Batch Validation
            if len(items) != len(ids):
                found_ids = {item.id for item in items if item.id is not None}
                missing_ids = set(ids) - found_ids
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch aborted. IDs not found: {list(missing_ids)}"
                )

            # 4. Apply Soft-Delete to all
            now = datetime.now(UTC)
            for item in items:
                item.is_active = False
                item.updated_at = now
                self.session.add(item)

            # 5. Finalize Transaction
            self.session.commit()
            logger.info(f"Successfully deactivated {len(items)} environments.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch environment deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
