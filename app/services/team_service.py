import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimTeam

# Layer 3: Domain Entities
from app.domain.team import TeamDomain


logger = logging.getLogger(__name__)

class TeamService:
    """Service layer for managing Organizational Teams.

    This service coordinates the lifecycle of teams, ensuring standardized
    departmental mapping and providing refined ownership reports
    for the Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_team: DimTeam) -> TeamDomain:
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_team (DimTeam): The database record.

        Returns:
            TeamDomain: The Pydantic domain representation.
        """
        return TeamDomain.model_validate(db_team)

    def _get_dim_team_or_404(self, id: int) -> DimTeam:
        """Internal helper to retrieve an active team or raise 404.

        Args:
            id (int): The primary key ID.

        Returns:
            DimTeam: The database record.

        Raises:
            HTTPException: 404 status if not found or inactive.
        """
        # 1. Fetch record ensuring it is currently active
        statement = select(DimTeam).where(
            DimTeam.id == id,
            col(DimTeam.is_active)
        )
        team = self.session.exec(statement).first()

        if not team:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Team with ID {id} not found."
            )
        return team

    # --- 1. create_team ---
    def create_team(self, team_in: TeamDomain) -> TeamDomain:
        """Full CRUD: Validates business rules and persists a new organizational team.

        Args:
            team_in (TeamDomain): Input data from the API.

        Returns:
            TeamDomain: The created team entity.
        """
        # 1. Unique Name Check (Business Key)
        statement = select(DimTeam).where(DimTeam.team_name == team_in.team_name)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Team '{team_in.team_name}' is already registered."
            )

        # 2. Extract data excluding system-managed fields
        team_data = team_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
        new_team = DimTeam(**team_data)

        # 3. Handle Medallion Metadata
        now = datetime.now(UTC)
        new_team.source_timestamp = getattr(team_in, "source_timestamp", None) or now
        new_team.updated_at = now

        try:
            # 4. Persist to Database
            self.session.add(new_team)
            self.session.commit()
            
            # 5. Refresh to capture generated ID
            self.session.refresh(new_team)
            return self._map_to_domain(new_team)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create team: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during team creation."
            )

    # --- 2. create_teams_batch ---
    def create_teams_batch(self, teams_in: list[TeamDomain]) -> list[TeamDomain]:
        """Requirement: Batch CRUD. Ingests multiple teams in one transaction.

        Args:
            teams_in (list[TeamDomain]): List of team objects.

        Returns:
            list[TeamDomain]: The list of created teams.
        """
        # 1. Batch Duplicate Check (Performance Optimized)
        input_names = [t.team_name for t in teams_in]
        statement = select(DimTeam).where(col(DimTeam.team_name).in_(input_names))
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch contains team names that already exist."
            )

        # 2. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for t_in in teams_in:
            entry_data = t_in.model_dump(exclude={"id", "source_timestamp", "updated_at"})
            db_team = DimTeam(**entry_data)
            db_team.source_timestamp = getattr(t_in, "source_timestamp", None) or now
            db_team.updated_at = now
            db_entries.append(db_team)

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
            logger.error(f"Batch team creation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch team creation."
            )

    # --- 3. get_all_teams ---
    def get_all_teams(self, limit: int = 100, offset: int = 0) -> list[TeamDomain]:
        """Full CRUD: Retrieves all active teams with pagination and sorting.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip for pagination.

        Returns:
            list[TeamDomain]: Paginated list of teams.
        """
        # 1. Build statement with pagination and alphabetical sorting
        statement = (
            select(DimTeam)
            .where(col(DimTeam.is_active))
            .order_by(col(DimTeam.team_name))
            .offset(offset)
            .limit(limit)
        )
        
        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(t) for t in results]

    # --- 4. get_team ---
    def get_team(self, id: int) -> TeamDomain:
        """Full CRUD: Retrieves a single team record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            TeamDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_team = self._get_dim_team_or_404(id)
        return self._map_to_domain(db_team)

    # --- 5. update_team ---
    def update_team(self, id: int, data: TeamDomain) -> TeamDomain:
        """Full CRUD: Updates an existing organizational team's properties.

        Args:
            id (int): The ID to update.
            data (TeamDomain): Updated team data.

        Returns:
            TeamDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_team = self._get_dim_team_or_404(id)

        # 2. Check for name conflicts if renaming
        if db_team.team_name != data.team_name:
            statement = select(DimTeam).where(DimTeam.team_name == data.team_name)
            if self.session.exec(statement).first():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot rename to '{data.team_name}'; name already taken."
                )

        # 3. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id", "updated_at"})
        db_team.sqlmodel_update(update_data)
        
        # 4. Refresh update timestamp
        db_team.updated_at = datetime.now(UTC)

        try:
            # 5. Persist and return
            self.session.add(db_team)
            self.session.commit()
            self.session.refresh(db_team)
            
            logger.info(f"Team {id} updated successfully.")
            return self._map_to_domain(db_team)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update team {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during team update."
            )

    # --- 6. update_teams_batch ---
    def update_teams_batch(self, teams_in: list[TeamDomain]) -> list[TeamDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple teams using 'team_name' as the business key.

        Args:
            teams_in (list[TeamDomain]): Updated team data list.

        Returns:
            list[TeamDomain]: Refreshed list of updated entities.
        """
        # 1. Performance Optimized Fetch (Single Roundtrip)
        input_names = [t.team_name for t in teams_in]
        statement = select(DimTeam).where(col(DimTeam.team_name).in_(input_names))
        db_teams = self.session.exec(statement).all()
        
        # 2. Create Lookup Map for O(1) access
        db_map = {t.team_name: t for t in db_teams}

        # 3. Atomic Validation: Ensure all teams in batch exist
        for t_data in teams_in:
            if t_data.team_name not in db_map:
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Team '{t_data.team_name}' not found. Batch aborted."
                )

        now = datetime.now(UTC)
        try:
            # 4. Apply updates in loop
            updated_entries = []
            for t_data in teams_in:
                db_team = db_map[t_data.team_name]
                update_dict = t_data.model_dump(exclude={"id", "updated_at"})
                db_team.sqlmodel_update(update_dict)
                db_team.updated_at = now
                self.session.add(db_team)
                updated_entries.append(db_team)

            # 5. Atomic Commit
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch team update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch update execution."
            )

    # --- 7. delete_team ---
    def delete_team(self, id: int) -> None:
        """Full CRUD: Marks an organizational team as inactive (soft-delete).

        Args:
            id (int): Primary key ID to deactivate.
        """
        # 1. Fetch record via helper
        db_team = self._get_dim_team_or_404(id)
        
        # 2. Early return if already inactive
        if not db_team.is_active:
            return

        try:
            # 3. Apply Soft-Delete and update metadata
            db_team.is_active = False
            db_team.updated_at = datetime.now(UTC)
            self.session.add(db_team)
            self.session.commit()
            logger.info(f"Team {id} deactivated (Soft-Deleted).")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to soft-delete team {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during team deactivation."
            )

    # --- 8. delete_teams_batch ---
    def delete_teams_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Marks multiple teams as inactive.

        Args:
            ids (list[int]): Primary key IDs to deactivate.
        """
        # 1. Input Safety Check
        if not ids:
            return

        try:
            # 2. Fetch targeted items in single query
            statement = select(DimTeam).where(col(DimTeam.id).in_(ids))
            items = self.session.exec(statement).all()

            # 3. Validation: Strict All-or-Nothing
            if len(items) != len(ids):
                found_ids = {item.id for item in items if item.id is not None}
                missing_ids = set(ids) - found_ids
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch aborted. Team IDs not found: {list(missing_ids)}"
                )

            # 4. Apply Soft-Delete metadata to all
            now = datetime.now(UTC)
            for item in items:
                item.is_active = False
                item.updated_at = now
                self.session.add(item)

            # 5. Atomic Commit
            self.session.commit()
            logger.info(f"Successfully deactivated {len(items)} teams. IDs: {ids}")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch team deactivation failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch deactivation."
            )
