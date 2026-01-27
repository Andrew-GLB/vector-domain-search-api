import logging
from typing import Any

from fastapi import HTTPException, status

# Added 'col' to satisfy Mypy [union-attr] errors on Optional fields
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimTeam

# Layer 3: Domain Entities
from app.domain.team import TeamDomain


logger = logging.getLogger(__name__)

class TeamService:
    """Service layer for managing Organizational Teams.

    Orchestrates team data and provides refined departmental
    ownership views for the Gold Layer.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session."""
        self.session = session

    def _map_to_domain(self, db_team: DimTeam) -> TeamDomain:
        """Maps a Data Access model to a Domain entity."""
        return TeamDomain(
            team_name=db_team.team_name,
            department=db_team.department,
            lead_email=db_team.lead_email
        )

    def _get_dim_team_or_404(self, team_id: int) -> DimTeam:
        """Internal helper to find team or 404."""
        team = self.session.get(DimTeam, team_id)
        if not team:
            raise HTTPException(status_code=404, detail="Team not found.")
        return team

    def create_team(self, team_in: TeamDomain) -> TeamDomain:
        """Persists a new organizational team."""
        statement = select(DimTeam).where(DimTeam.team_name == team_in.team_name)
        if self.session.exec(statement).first():
            raise HTTPException(status_code=400, detail="Team name already exists.")

        new_db_team = DimTeam(**team_in.model_dump())
        self.session.add(new_db_team)
        self.session.commit()
        self.session.refresh(new_db_team)
        return self._map_to_domain(new_db_team)

    def create_teams_batch(self, teams_in: list[TeamDomain]) -> list[TeamDomain]:
        """Requirement: Batch CRUD operations."""
        db_entries = [DimTeam(**t.model_dump()) for t in teams_in]
        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return teams_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Team load failed: {e}")
            raise HTTPException(status_code=500, detail="Batch creation failed.")

    def get_all_teams(self) -> list[TeamDomain]:
        """Retrieves all teams."""
        results = self.session.exec(select(DimTeam)).all()
        return [self._map_to_domain(t) for t in results]

    def delete_team(self, team_id: int) -> None:
        """Full CRUD: Deletes a single organizational team record.

        Args:
            team_id (int): The primary key ID of the team to delete.

        Raises:
            HTTPException: 404 status if the team ID is not found.
        """
        db_team = self._get_dim_team_or_404(team_id)

        try:
            self.session.delete(db_team)
            self.session.commit()
            logger.info(f"Team {team_id} deleted successfully.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to delete team {team_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error during deletion."
            )

    def delete_teams_batch(self, team_ids: list[int]) -> None:
        """Requirement: CRUD should allow batch operations.
        Deletes multiple teams in a single database transaction. This method
        is optimized for bulk cleanup operations.

        Args:
            team_ids (List[int]): A list of team primary key IDs to remove.

        Raises:
            HTTPException: 500 status if the batch transaction fails.
        """
        if not team_ids:
            return

        try:
            # Fix: Use col() to ensure Mypy recognizes attributes on the Optional ID field
            statement = select(DimTeam).where(col(DimTeam.id).in_(team_ids))
            teams_to_delete = self.session.exec(statement).all()

            if not teams_to_delete:
                logger.warning("Batch delete called but no matching IDs were found in SQL.")
                return

            # Remove each found entity from the session
            for team in teams_to_delete:
                self.session.delete(team)

            self.session.commit()
            logger.info(f"Batch delete successful. {len(teams_to_delete)} teams removed.")

        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Team deletion failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process batch deletion."
            )

    def update_teams_batch(self, teams_in: list[TeamDomain]) -> list[TeamDomain]:
        """Requirement: CRUD should allow batch operations.
        Updates multiple organizational teams in a single transaction. This method
        uses the 'team_name' as the business key to identify existing records.

        Args:
            teams_in (List[TeamDomain]): A list of domain models containing the updated data.

        Returns:
            List[TeamDomain]: The list of updated teams as domain models.

        Raises:
            HTTPException: 404 status if any of the team names in the batch do not exist.
            HTTPException: 500 status if the database transaction fails.
        """
        updated_domains = []

        try:
            for team_data in teams_in:
                # 1. Locate the existing record by the business key (team_name)
                statement = select(DimTeam).where(DimTeam.team_name == team_data.team_name)
                db_team = self.session.exec(statement).first()

                if not db_team:
                    # Rolling back because we want 'All or Nothing' for the batch
                    self.session.rollback()
                    logger.error(f"Update failed: Team '{team_data.team_name}' not found.")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Team '{team_data.team_name}' not found. Batch update aborted."
                    )

                # 2. Update the fields
                db_team.department = team_data.department
                db_team.lead_email = team_data.lead_email

                self.session.add(db_team)
                updated_domains.append(team_data)

            # 3. Commit the entire transaction
            self.session.commit()
            logger.info(f"Batch update successful. {len(updated_domains)} teams updated.")
            return updated_domains

        except HTTPException:
            # Re-raise HTTP exceptions (like our 404 above)
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Unexpected error during batch Team update: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error during batch update."
            )

    def update_team(self, team_id: int, team_in: TeamDomain) -> TeamDomain:
        """Full CRUD: Updates an existing organizational team record.

        This method retrieves the record from the database, applies the
        changes from the domain model, and persists the update.

        Args:
            team_id (int): The primary key ID of the team to update.
            team_in (TeamDomain): The updated data from the API request.

        Returns:
            TeamDomain: The updated team record as a pure domain entity.

        Raises:
            HTTPException: 404 status if the team ID is not found.
            HTTPException: 400 status if the new team_name conflicts with
                           another existing team.
        """
        # 1. Retrieve the existing record using our helper (raises 404 if not found)
        db_team = self._get_dim_team_or_404(team_id)

        # 2. Check for name conflicts if the name is being changed
        if db_team.team_name != team_in.team_name:
            statement = select(DimTeam).where(DimTeam.team_name == team_in.team_name)
            if self.session.exec(statement).first():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Cannot rename to '{team_in.team_name}'; that name is already taken."
                )

        # 3. Apply updates to the database model
        db_team.team_name = team_in.team_name
        db_team.department = team_in.department
        db_team.lead_email = team_in.lead_email

        try:
            # 4. Persist and refresh
            self.session.add(db_team)
            self.session.commit()
            self.session.refresh(db_team)

            logger.info(f"Team {team_id} updated successfully.")

            # 5. Return mapped back to Domain (Layered Architecture Requirement)
            return self._map_to_domain(db_team)

        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update team {team_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error during record update."
            )

    def get_team(self, team_id: int) -> TeamDomain | None:
        """Full CRUD: Retrieves a single organizational team record by ID.

        This method fetches the record from the Data Access layer (DimTeam)
        and transforms it into a Domain entity (TeamDomain).

        Args:
            team_id (int): The primary key ID of the team to retrieve.

        Returns:
            Optional[TeamDomain]: The team domain object if found, otherwise None.
        """
        # Fetch from SQLModel Data Access layer
        db_team = self.session.get(DimTeam, team_id)

        if not db_team:
            logger.warning(f"Retrieval failed: Team ID {team_id} not found.")
            return None

        # Map to Domain Layer (Requirement: Domain != Data Access)
        return self._map_to_domain(db_team)

    # --- GOLD LAYER ANALYTICS ---

    def get_department_coverage_gold(self) -> list[dict[str, Any]]:
        """Requirement: Querying Gold Layer.
        Refines team data to show how many unique teams exist per department.
        """
        teams = self.get_all_teams()
        # Fix: Add explicit type annotation for Mypy
        dept_map: dict[str, int] = {}
        for t in teams:
            dept_map[t.department] = dept_map.get(t.department, 0) + 1

        return [
            {"department": dept, "team_count": count}
            for dept, count in dept_map.items()
        ]
