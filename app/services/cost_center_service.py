import logging
from typing import Any

from fastapi import HTTPException, status

# Added col to satisfy Mypy strict typing on Optional fields
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import DimCostCenter

# Layer 3: Domain Entities
from app.domain.cost_center import CostCenterDomain


logger = logging.getLogger(__name__)

class CostCenterService:
    """Service layer for managing Financial Cost Centers.

    This service coordinates the lifecycle of cost centers, ensuring that
    financial budget codes are unique and providing refined reports for the
    Gold Layer of the Medallion Architecture.
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_cc: DimCostCenter) -> CostCenterDomain:
        """Maps a Data Access model back to a Domain entity.

        Args:
            db_cc (DimCostCenter): The database record.

        Returns:
            CostCenterDomain: The pure domain representation.
        """
        return CostCenterDomain(
            center_code=db_cc.center_code,
            department=db_cc.department,
            budget_limit=db_cc.budget_limit
        )

    def _get_dim_cc_or_404(self, cost_center_id: int) -> DimCostCenter:
        """Internal helper to retrieve a cost center or raise 404.

        Args:
            cost_center_id (int): The primary key ID.

        Returns:
            DimCostCenter: The database record.

        Raises:
            HTTPException: 404 status if not found.
        """
        cc = self.session.get(DimCostCenter, cost_center_id)
        if not cc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cost Center with ID {cost_center_id} not found."
            )
        return cc

    def create_cost_center(self, cc_in: CostCenterDomain) -> CostCenterDomain:
        """Validates and persists a new cost center.

        Args:
            cc_in (CostCenterDomain): Input data from the API.

        Returns:
            CostCenterDomain: The created cost center.

        Raises:
            HTTPException: 400 status if the center_code already exists.
        """
        # 1. Unique Constraint Check
        statement = select(DimCostCenter).where(DimCostCenter.center_code == cc_in.center_code)
        if self.session.exec(statement).first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cost Center code '{cc_in.center_code}' is already registered."
            )

        # 2. Map and Persist
        new_cc = DimCostCenter(
            center_code=cc_in.center_code,
            department=cc_in.department,
            budget_limit=cc_in.budget_limit
        )
        self.session.add(new_cc)
        self.session.commit()
        self.session.refresh(new_cc)

        return self._map_to_domain(new_cc)

    def create_cost_centers_batch(self, ccs_in: list[CostCenterDomain]) -> list[CostCenterDomain]:
        """Requirement: CRUD should allow batch operations.
        
        Optimizes the ingestion of multiple cost centers in a single transaction.

        Args:
            ccs_in (List[CostCenterDomain]): A list of cost center objects.

        Returns:
            List[CostCenterDomain]: The list of created centers.
        """
        db_entries = [
            DimCostCenter(
                center_code=cc.center_code,
                department=cc.department,
                budget_limit=cc.budget_limit
            ) for cc in ccs_in
        ]

        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return ccs_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Cost Center load failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process batch creation."
            )

    def get_all_cost_centers(self) -> list[CostCenterDomain]:
        """Retrieves all registered cost centers."""
        statement = select(DimCostCenter)
        results = self.session.exec(statement).all()
        return [self._map_to_domain(cc) for cc in results]

    def get_cost_center(self, cost_center_id: int) -> CostCenterDomain | None:
        """Full CRUD: Retrieves a single cost center record by ID.

        Args:
            cost_center_id (int): The primary key ID.

        Returns:
            Optional[CostCenterDomain]: The domain entity if found.
        """
        db_cc = self.session.get(DimCostCenter, cost_center_id)
        return self._map_to_domain(db_cc) if db_cc else None

    def update_cost_center(self, cc_id: int, cc_in: CostCenterDomain) -> CostCenterDomain:
        """Full CRUD: Updates an existing cost center.

        Args:
            cc_id (int): The ID to update.
            cc_in (CostCenterDomain): The updated data.

        Returns:
            CostCenterDomain: The updated entity.
        """
        db_cc = self._get_dim_cc_or_404(cc_id)

        db_cc.department = cc_in.department
        db_cc.budget_limit = cc_in.budget_limit

        self.session.add(db_cc)
        self.session.commit()
        self.session.refresh(db_cc)
        return self._map_to_domain(db_cc)

    def update_cost_centers_batch(self, ccs_in: list[CostCenterDomain]) -> list[CostCenterDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple centers using 'center_code' as the business key.
        
        Ensures atomicity: if one code is not found, the whole transaction is rolled back.

        Args:
            ccs_in (List[CostCenterDomain]): List of updated cost center data.

        Returns:
            List[CostCenterDomain]: The original input list on success.
        """
        try:
            for cc_data in ccs_in:
                statement = select(DimCostCenter).where(DimCostCenter.center_code == cc_data.center_code)
                db_cc = self.session.exec(statement).first()

                if not db_cc:
                    self.session.rollback()
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Cost Center '{cc_data.center_code}' not found. Batch aborted."
                    )

                db_cc.department = cc_data.department
                db_cc.budget_limit = cc_data.budget_limit
                self.session.add(db_cc)

            self.session.commit()
            return ccs_in
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Cost Center update failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch update.")

    def delete_cost_center(self, cc_id: int) -> bool:
        """Full CRUD: Deletes a cost center record."""
        db_cc = self._get_dim_cc_or_404(cc_id)
        self.session.delete(db_cc)
        self.session.commit()
        return True

    def delete_cost_centers_batch(self, ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes multiple cost centers by their primary key IDs.
        """
        try:
            # Fix: Use col() to satisfy Mypy for Optional ID fields
            statement = select(DimCostCenter).where(col(DimCostCenter.id).in_(ids))
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
            logger.info(f"Successfully deleted batch of {len(items_to_delete)} cost centers.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Cost Center deletion failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch deletion.")

    # --- GOLD LAYER ANALYTICS ---

    def get_budget_utilization_report(self) -> list[dict[str, Any]]:
        """Requirement: Querying Gold Layer.
        
        Generates a refined report showing budget limits formatted for
        a financial dashboard.

        Returns:
            List[Dict[str, Any]]: A summary of financial health per cost center.
        """
        centers = self.get_all_cost_centers()
        return [
            {
                "center": cc.center_code,
                "department": cc.department.upper(),
                "allocation": f"${cc.budget_limit:,.2f}",
                "status": "High" if cc.budget_limit > 100000 else "Standard"
            } for cc in centers
        ]
