import logging
from typing import List, Optional

from fastapi import HTTPException, status
from sqlmodel import Session, and_, col, or_, select

# Layer 4: Data Access
from app.data_access.m_views import (
    AssetUtilizationMView,
    FactAssetMetricsMView,
    ResourceEfficiencyMView,
    SecurityComplianceMView,
    TeamCostMView,
)

# Layer 3: Domain Entities
from app.domain.gold_entities import (
    AssetMetricContext,
    AssetUtilization,
    ResourceEfficiency,
    SecurityCompliance,
    TeamCost,
)

logger = logging.getLogger(__name__)

class GoldSearchService:
    """Service layer for searching and retrieving analytics from the Gold Layer Views."""

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session."""
        self.session = session

    # --- Private Mapping Helpers ---

    def _map_to_metric_context(self, db_obj: FactAssetMetricsMView) -> AssetMetricContext:
        """Safe mapping for wide metric context."""
        return AssetMetricContext.model_validate(db_obj.model_dump())

    def _map_to_utilization(self, db_obj: AssetUtilizationMView) -> AssetUtilization:
        return AssetUtilization.model_validate(db_obj.model_dump())

    def _map_to_team_cost(self, db_obj: TeamCostMView) -> TeamCost:
        return TeamCost.model_validate(db_obj.model_dump())

    def _map_to_security(self, db_obj: SecurityComplianceMView) -> SecurityCompliance:
        return SecurityCompliance.model_validate(db_obj.model_dump())

    def _map_to_efficiency(self, db_obj: ResourceEfficiencyMView) -> ResourceEfficiency:
        return ResourceEfficiency.model_validate(db_obj.model_dump())

    # --- Public Search Methods ---

    def read_comprehensive_metrics(
        self,
        provider_name: Optional[str] = None,
    ) -> List[AssetMetricContext]:
        """
        Retrieves fully enriched asset metrics from the 10-way join view.
        Provides robust multi-dimensional filtering for comprehensive reporting.
        """
        try:
            statement = select(FactAssetMetricsMView)

            if provider_name and provider_name != "--":
                statement = statement.where(FactAssetMetricsMView.provider_name == provider_name)
            
            results = self.session.exec(statement).all()

            final_list = []
            for r in results:
                try:
                    # Handles SQLModel -> Pydantic conversion per row
                    final_list.append(self._map_to_metric_context(r))
                except Exception as map_err:
                    # Prevents a single corrupt row from failing the entire API request
                    logger.warning(f"Skipping malformed metric record (ID: {getattr(r, 'id', 'Unknown')}): {map_err}")
                    continue
            
            return final_list

        except Exception as e:
            logger.error(f"Error in read_comprehensive_metrics: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="A database error occurred while reading metrics data."
            )

    def search_assets_utilization(
        self,
        provider_name: Optional[str] = None,  
    ) -> List[AssetUtilization]:
        """Filters asset utilization based on provider or all resources."""
        try:
            statement = select(AssetUtilizationMView)

            if provider_name and provider_name != "--":
                statement = statement.where(AssetUtilizationMView.provider_name == provider_name)

            results = self.session.exec(statement).all()
            return [self._map_to_utilization(r) for r in results]
        except Exception as e:
            logger.error(f"Error in search_assets_utilization: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error while fetching assets utilization."
            )

    def get_team_cost_report(self) -> List[TeamCost]:
        """Chargeback reporting rolled up by team or department."""
        try:
            statement = select(TeamCostMView)
            results = self.session.exec(statement).all()
            return [self._map_to_team_cost(r) for r in results]
        except Exception as e:
            logger.error(f"Error fetching team cost report: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error while fetching team cost report."
            )

    def search_security_risks(self) -> List[SecurityCompliance]:
        """Retrieves critical assets that are vulnerable/inactive based on view DDL."""
        try:
            statement = select(SecurityComplianceMView)
            results = self.session.exec(statement).all()
            return [self._map_to_security(r) for r in results]
        except Exception as e:
            logger.error(f"Error fetching security risks: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error while fetching compliance posture."
            )

    def get_efficiency_metrics(
        self, 
        waste_category: Optional[str] = None,
        limit: int = 100
    ) -> List[ResourceEfficiency]:
        """Identifies efficiency status. Correctly handles row conversion failures."""
        try:
            statement = select(ResourceEfficiencyMView)
            
            if waste_category and waste_category != "--":
                statement = statement.where(ResourceEfficiencyMView.waste_index == waste_category)
                
            statement = statement.limit(limit)
            results = self.session.exec(statement).all()
            
            final_list = []
            for r in results:
                try:
                    final_list.append(self._map_to_efficiency(r))
                except Exception as map_err:
                    logger.warning(f"Skipping malformed efficiency record (Asset ID: {getattr(r, 'asset_id', 'Unknown')}): {map_err}")
                    continue
            return final_list
        except Exception as e:
            logger.error(f"Error fetching efficiency metrics: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error while fetching efficiency analysis."
            )