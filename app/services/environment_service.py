import logging
from typing import Any, cast

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
    ensuring that deployment stages are standardized and providing refined
    criticality reports for the Gold Layer.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_env: DimEnvironment) -> EnvironmentDomain:
        """Maps a Data Access model back to a pure Domain entity.
        
        Requirement: Domain entities != Data Access entities.
        Uses typing.cast to satisfy Mypy strict Literal checking.

        Args:
            db_env (DimEnvironment): The database record.

        Returns:
            EnvironmentDomain: The Pydantic domain representation.
        """
        return EnvironmentDomain(
            env_name=cast(Any, db_env.env_name),
            tier=cast(Any, db_env.tier),
            is_ephemeral=db_env.is_ephemeral
        )

    def _get_dim_env_or_404(self, env_id: int) -> DimEnvironment:
        """Internal helper to retrieve an environment or raise a 404 error.

        Args:
            env_id (int): The primary key ID.

        Returns:
            DimEnvironment: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        env = self.session.get(DimEnvironment, env_id)
        if not env:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Environment with ID {env_id} not found."
            )
        return env

    def create_environment(self, env_in: EnvironmentDomain) -> EnvironmentDomain:
        """Validates business rules and persists a new environment.

        Args:
            env_in (EnvironmentDomain): Input data from the API.

        Returns:
            EnvironmentDomain: The created environment.

        Raises:
            HTTPException: 400 status if the environment name already exists.
        """
        # 1. Unique name check
        statement = select(DimEnvironment).where(DimEnvironment.env_name == env_in.env_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Environment '{env_in.env_name}' already exists."
            )

        # 2. Map and Persist
        new_db_env = DimEnvironment(
            env_name=env_in.env_name,
            tier=env_in.tier,
            is_ephemeral=env_in.is_ephemeral
        )
        self.session.add(new_db_env)
        self.session.commit()
        self.session.refresh(new_db_env)

        return self._map_to_domain(new_db_env)

    def create_environments_batch(self, envs_in: list[EnvironmentDomain]) -> list[EnvironmentDomain]:
        """Requirement: CRUD should allow batch operations.
        
        Ingests multiple environments in a single transaction for efficiency.

        Args:
            envs_in (List[EnvironmentDomain]): A list of environment objects.

        Returns:
            List[EnvironmentDomain]: The list of created environments.
        """
        db_entries = [
            DimEnvironment(
                env_name=e.env_name,
                tier=e.tier,
                is_ephemeral=e.is_ephemeral
            ) for e in envs_in
        ]

        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return envs_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Environment load failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process batch environment creation."
            )

    def get_all_environments(self) -> list[EnvironmentDomain]:
        """Retrieves all registered environments.

        Returns:
            List[EnvironmentDomain]: All environment records mapped to domain.
        """
        statement = select(DimEnvironment)
        results = self.session.exec(statement).all()
        return [self._map_to_domain(e) for e in results]

    def get_environment(self, env_id: int) -> EnvironmentDomain:
        """Full CRUD: Retrieves a single environment record by ID.

        Args:
            env_id (int): The primary key ID.

        Returns:
            EnvironmentDomain: The domain representation of the environment.
        """
        db_env = self._get_dim_env_or_404(env_id)
        return self._map_to_domain(db_env)

    def update_environment(self, env_id: int, env_in: EnvironmentDomain) -> EnvironmentDomain:
        """Full CRUD: Updates an existing environment's properties.

        Args:
            env_id (int): The ID to update.
            env_in (EnvironmentDomain): The updated data.

        Returns:
            EnvironmentDomain: The updated entity.
        """
        db_env = self._get_dim_env_or_404(env_id)

        db_env.env_name = env_in.env_name
        db_env.tier = env_in.tier
        db_env.is_ephemeral = env_in.is_ephemeral

        self.session.add(db_env)
        self.session.commit()
        self.session.refresh(db_env)
        return self._map_to_domain(db_env)

    def update_environments_batch(self, envs_in: list[EnvironmentDomain]) -> list[EnvironmentDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple environments using 'env_name' as the business key.
        
        Ensures atomicity: if one environment is not found, the entire batch
        transaction is rolled back.

        Args:
            envs_in (List[EnvironmentDomain]): List of updated environment data.

        Returns:
            List[EnvironmentDomain]: The original input list on success.

        Raises:
            HTTPException: 404 status if a specific environment name is not found.
            HTTPException: 500 status if a database error occurs.
        """
        try:
            for e_data in envs_in:
                # 1. Lookup by unique business key
                statement = select(DimEnvironment).where(DimEnvironment.env_name == e_data.env_name)
                db_env = self.session.exec(statement).first()

                if not db_env:
                    self.session.rollback()
                    logger.error(f"Batch update failed: Environment '{e_data.env_name}' not found.")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Environment '{e_data.env_name}' not found. Batch update aborted."
                    )

                # 2. Apply updates
                db_env.tier = e_data.tier
                db_env.is_ephemeral = e_data.is_ephemeral
                self.session.add(db_env)

            self.session.commit()
            logger.info(f"Successfully updated batch of {len(envs_in)} environments.")
            return envs_in

        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Environment update failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch environment update."
            )

    def delete_environment(self, env_id: int) -> None:
        """Full CRUD: Deletes a single environment record.

        Args:
            env_id (int): The primary key ID to delete.
        """
        db_env = self._get_dim_env_or_404(env_id)
        self.session.delete(db_env)
        self.session.commit()

    def delete_environments_batch(self, ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes multiple environments by their primary key IDs.
        
        Uses col() to satisfy Mypy strict typing.

        Args:
            ids (List[int]): List of primary key IDs to remove.

        Raises:
            HTTPException: 404 if one or more IDs do not exist.
        """
        try:
            statement = select(DimEnvironment).where(col(DimEnvironment.id).in_(ids))
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
            logger.info(f"Successfully deleted batch of {len(items_to_delete)} environments.")

        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Environment deletion failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch environment deletion."
            )

    # --- GOLD LAYER ANALYTICS ---

    def get_criticality_summary_gold(self) -> dict[str, Any]:
        """Requirement: Querying Gold Layer.
        
        Refines environment data to provide a count of mission-critical
        vs non-critical deployment stages.

        Returns:
            Dict[str, Any]: A summarized count by Tier.
        """
        envs = self.get_all_environments()
        summary: dict[str, int] = {}

        for e in envs:
            summary[e.tier] = summary.get(e.tier, 0) + 1

        return {
            "total_environments": len(envs),
            "tier_breakdown": summary,
            "report_label": "Infrastructure Criticality Overview"
        }
