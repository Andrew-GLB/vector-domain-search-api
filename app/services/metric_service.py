import logging
from typing import Any

from fastapi import HTTPException, status

# Added 'col' to imports to satisfy Mypy [attr-defined] errors
from sqlmodel import Session, col, func, select

# Layer 4: Data Access
from app.data_access.models import FactAssetMetrics, MetricEntry

# Layer 3: Domain Entities
from app.domain.metric_entry import MetricEntryDomain


logger = logging.getLogger(__name__)

class MetricService:
    """Service layer for managing Cloud Infrastructure Performance Metrics.

    This service orchestrates the ingestion of metric snapshots into the
    Silver layer (physical table) and provides analytical access to the
    Gold layer (virtual Star Schema view).
    """

    def __init__(self, session: Session) -> None:
        """Initializes the service with a database session.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session

    def _map_to_domain(self, db_obj: MetricEntry) -> MetricEntryDomain:
        """Requirement: Domain entities != Data Access entities.
        Maps a SQLModel database record to a Pydantic Domain model.

        Args:
            db_obj (MetricEntry): The database record.

        Returns:
            MetricEntryDomain: The pure domain representation.
        """
        return MetricEntryDomain(**db_obj.model_dump())

    def _get_dim_metric_or_404(self, metric_id: int) -> MetricEntry:
        """Internal helper to retrieve a metric entry or raise a 404 error.

        Args:
            metric_id (int): The primary key ID.

        Returns:
            MetricEntry: The database record found.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        metric = self.session.get(MetricEntry, metric_id)
        if not metric:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Metric entry with ID {metric_id} not found."
            )
        return metric

    def ingest_metric(self, metric_in: MetricEntryDomain) -> MetricEntryDomain:
        """Fulfills Full CRUD: Persists a single metric snapshot.

        Args:
            metric_in (MetricEntryDomain): The validated metric data.

        Returns:
            MetricEntryDomain: The persisted record.
        """
        db_metric = MetricEntry(**metric_in.model_dump())

        self.session.add(db_metric)
        self.session.commit()
        self.session.refresh(db_metric)

        return self._map_to_domain(db_metric)

    def ingest_metrics_batch(self, metrics_in: list[MetricEntryDomain]) -> list[MetricEntryDomain]:
        """Requirement: CRUD should allow batch operations.
        
        Optimizes the ingestion of high-volume metric data using a single transaction.

        Args:
            metrics_in (List[MetricEntryDomain]): A list of snapshots to ingest.

        Returns:
            List[MetricEntryDomain]: The list of ingested snapshots.

        Raises:
            HTTPException: 500 status if the batch transaction fails.
        """
        db_entries = [MetricEntry(**m.model_dump()) for m in metrics_in]

        try:
            self.session.add_all(db_entries)
            self.session.commit()
            return metrics_in
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch metric ingestion failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Critical error during batch metrics processing."
            )

    def get_all_silver_metrics(self, limit: int = 100) -> list[MetricEntryDomain]:
        """Retrieves raw snapshots from the Silver layer table."""
        statement = select(MetricEntry).limit(limit)
        results = self.session.exec(statement).all()
        return [self._map_to_domain(m) for m in results]

    def get_metric(self, metric_id: int) -> MetricEntryDomain | None:
        """Full CRUD: Retrieves a single metric entry record by ID.

        Args:
            metric_id (int): The primary key ID.

        Returns:
            Optional[MetricEntryDomain]: The domain entity if found.
        """
        db_metric = self.session.get(MetricEntry, metric_id)
        return self._map_to_domain(db_metric) if db_metric else None

    def update_metric(self, metric_id: int, metric_in: MetricEntryDomain) -> MetricEntryDomain:
        """Full CRUD: Updates an existing metric entry record.

        Args:
            metric_id (int): The ID to update.
            metric_in (MetricEntryDomain): The updated data.

        Returns:
            MetricEntryDomain: The updated entity.
        """
        db_metric = self._get_dim_metric_or_404(metric_id)

        # Update quantitative and relational fields
        data_dict = metric_in.model_dump()
        for key, value in data_dict.items():
            setattr(db_metric, key, value)

        self.session.add(db_metric)
        self.session.commit()
        self.session.refresh(db_metric)
        return self._map_to_domain(db_metric)

    def update_metrics_batch(self, metrics_in: list[MetricEntryDomain]) -> list[MetricEntryDomain]:
        """Requirement: CRUD batch operations.
        Updates multiple metric entries.
        """
        try:
            for m_data in metrics_in:
                # In metric ingestion, we typically match by the composite unique keys
                # for demonstration, we assume business logic matches by Asset and Date
                statement = select(MetricEntry).where(
                    MetricEntry.asset_id == m_data.asset_id,
                    MetricEntry.date_id == m_data.date_id
                )
                db_metric = self.session.exec(statement).first()

                if not db_metric:
                    self.session.rollback()
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="One or more metric records not found for batch update."
                    )

                # Update fields
                for key, value in m_data.model_dump().items():
                    setattr(db_metric, key, value)
                self.session.add(db_metric)

            self.session.commit()
            return metrics_in
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch metric update failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch update.")

    def delete_metric(self, metric_id: int) -> None:
        """Full CRUD: Deletes a single metric entry."""
        db_metric = self._get_dim_metric_or_404(metric_id)
        self.session.delete(db_metric)
        self.session.commit()

    def delete_metrics_batch(self, ids: list[int]) -> None:
        """Requirement: CRUD batch operations.
        Bulk deletes metrics by their primary key IDs.
        """
        try:
            # Use col() to satisfy Mypy
            statement = select(MetricEntry).where(col(MetricEntry.id).in_(ids))
            items_to_delete = self.session.exec(statement).all()

            if len(items_to_delete) != len(ids):
                self.session.rollback()
                raise HTTPException(status_code=404, detail="Batch delete failed: Some IDs not found.")

            for item in items_to_delete:
                self.session.delete(item)

            self.session.commit()
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Metric deletion failed: {e}")
            raise HTTPException(status_code=500, detail="Internal error during batch delete.")

    # --- GOLD LAYER ANALYTICS (Requirement: Querying Gold Layer) ---

    def get_fact_view_gold(self, limit: int = 100) -> list[FactAssetMetrics]:
        """Queries the Virtual SQL View 'fact_asset_metrics'."""
        statement = select(FactAssetMetrics).limit(limit)
        # Cast Sequence to List to satisfy Mypy [return-value]
        return list(self.session.exec(statement).all())

    def get_utilization_report_gold(self) -> list[dict[str, Any]]:
        """Analytical Query: Calculates average CPU and Memory per Team."""
        # Use col() to ensure Mypy recognizes attributes on the SQLModel
        statement = (
            select(
                col(FactAssetMetrics.team_name),
                func.avg(col(FactAssetMetrics.cpu_usage_avg)).label("avg_cpu"),
                func.avg(col(FactAssetMetrics.memory_usage_avg)).label("avg_ram"),
                func.sum(col(FactAssetMetrics.hourly_cost)).label("total_cost")
            )
            .group_by(col(FactAssetMetrics.team_name))
        )

        results = self.session.exec(statement).all()
        return [
            {
                "team": r[0],
                "performance": {"cpu": round(r[1], 2), "ram": round(r[2], 2)},
                "hourly_spend": round(r[3], 2)
            } for r in results
        ]

    def get_critical_assets_gold(self, cost_threshold: float = 1.0) -> list[FactAssetMetrics]:
        """Identifies high-cost assets in the Gold Layer."""
        # Use col() for comparison to satisfy Mypy [attr-defined]
        statement = select(FactAssetMetrics).where(col(FactAssetMetrics.hourly_cost) > cost_threshold)
        # Cast to List to satisfy Mypy [return-value]
        return list(self.session.exec(statement).all())

    def get_cost_summary_gold(self) -> list[dict[str, Any]]:
        """Analytical Query: Aggregates total hourly cost by Service Category."""
        # Use col() to satisfy Mypy [attr-defined]
        statement = (
            select(
                col(FactAssetMetrics.service_category),
                func.sum(col(FactAssetMetrics.hourly_cost)).label("total_hourly_cost")
            )
            .group_by(col(FactAssetMetrics.service_category))
        )

        results = self.session.exec(statement).all()

        return [
            {
                "category": r[0],
                "total_hourly_cost": round(r[1], 4)
            } for r in results
        ]

    def get_gold_fact_view(self, limit: int = 100, offset: int = 0) -> list[FactAssetMetrics]:
        """Requirement: Querying Gold Layer."""
        statement = select(FactAssetMetrics).offset(offset).limit(limit)
        # Fix: Cast Sequence to List to satisfy Mypy [return-value]
        results = list(self.session.exec(statement).all())
        return results
