import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlmodel import Session, col, select

# Layer 4: Data Access
from app.data_access.models import MetricEntry

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
        """Maps a Data Access model back to a pure Domain entity.

        Args:
            db_obj (MetricEntry): The database record.

        Returns:
            MetricEntryDomain: The Pydantic domain representation.
        """
        return MetricEntryDomain.model_validate(db_obj)

    def _get_dim_metric_or_404(self, id: int) -> MetricEntry:
        """Internal helper to retrieve a metric entry or raise a 404 error.

        Args:
            id (int): The primary key ID.

        Returns:
            MetricEntry: The database record.

        Raises:
            HTTPException: 404 status if the ID does not exist.
        """
        # 1. Fetch record by primary key
        metric = self.session.get(MetricEntry, id)
        
        # 2. Raise 404 if missing
        if not metric:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Metric entry with ID {id} not found."
            )
        return metric

    # --- 1. ingest_metric (Create) ---
    def ingest_metric(self, metric_in: MetricEntryDomain) -> MetricEntryDomain:
        """Full CRUD: Validates and persists a single metric snapshot.

        Args:
            metric_in (MetricEntryDomain): The validated metric data.

        Returns:
            MetricEntryDomain: The persisted domain record.
        """
        # 1. Extract data and initialize model
        metric_data = metric_in.model_dump(exclude={"id"})
        db_metric = MetricEntry(**metric_data)

        # 2. Handle Medallion Metadata
        now = datetime.now(UTC)
        if hasattr(db_metric, "source_timestamp"):
            db_metric.source_timestamp = getattr(metric_in, "source_timestamp", None) or now

        try:
            # 3. Persist to Database
            self.session.add(db_metric)
            self.session.commit()
            
            # 4. Refresh to capture generated ID
            self.session.refresh(db_metric)
            return self._map_to_domain(db_metric)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to ingest metric: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during metric ingestion."
            )

    # --- 2. ingest_metrics_batch (Batch Create) ---
    def ingest_metrics_batch(self, metrics_in: list[MetricEntryDomain]) -> list[MetricEntryDomain]:
        """Requirement: Batch CRUD. Ingests multiple metrics in one transaction.

        Args:
            metrics_in (list[MetricEntryDomain]): A list of snapshots to ingest.

        Returns:
            list[MetricEntryDomain]: The list of ingested domain snapshots.
        """
        # 1. Prepare DB entries with Metadata
        now = datetime.now(UTC)
        db_entries = []

        for m_in in metrics_in:
            entry_data = m_in.model_dump(exclude={"id"})
            db_metric = MetricEntry(**entry_data)
            
            if hasattr(db_metric, "source_timestamp"):
                db_metric.source_timestamp = getattr(m_in, "source_timestamp", None) or now
            db_entries.append(db_metric)

        try:
            # 2. Batch insert and Atomic Commit
            self.session.add_all(db_entries)
            self.session.commit()
            
            # 3. Refresh all to capture IDs
            for entry in db_entries:
                self.session.refresh(entry)
            
            # 4. Map back to Domain objects
            return [self._map_to_domain(e) for e in db_entries]
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch metric ingestion failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch metrics processing."
            )

    # --- 3. get_all_silver_metrics (Read All) ---
    def get_all_silver_metrics(self, limit: int = 100, offset: int = 0) -> list[MetricEntryDomain]:
        """Full CRUD: Retrieves raw snapshots from the Silver layer table.

        Args:
            limit (int): Max records to return.
            offset (int): Records to skip for pagination.

        Returns:
            list[MetricEntryDomain]: Paginated list of metric snapshots.
        """
        # 1. Build statement with pagination
        statement = select(MetricEntry).offset(offset).limit(limit)
        
        # 2. Execute and return mapped list
        results = self.session.exec(statement).all()
        return [self._map_to_domain(m) for m in results]

    # --- 4. get_metric (Read One) ---
    def get_metric(self, id: int) -> MetricEntryDomain:
        """Full CRUD: Retrieves a single metric entry record by ID.

        Args:
            id (int): Primary key ID.

        Returns:
            MetricEntryDomain: The domain entity representation.
        """
        # 1. Fetch via helper and map
        db_metric = self._get_dim_metric_or_404(id)
        return self._map_to_domain(db_metric)

    # --- 5. update_metric (Update) ---
    def update_metric(self, id: int, data: MetricEntryDomain) -> MetricEntryDomain:
        """Full CRUD: Updates an existing metric entry record.

        Args:
            id (int): The ID to update.
            data (MetricEntryDomain): Updated metric data.

        Returns:
            MetricEntryDomain: The updated entity.
        """
        # 1. Fetch existing record
        db_metric = self._get_dim_metric_or_404(id)

        # 2. Dump data and update model using SQLModel helper
        update_data = data.model_dump(exclude={"id"})
        db_metric.sqlmodel_update(update_data)

        try:
            # 3. Persist and return
            self.session.add(db_metric)
            self.session.commit()
            self.session.refresh(db_metric)
            return self._map_to_domain(db_metric)
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to update metric {id}: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal database error during metric update."
            )

    # --- 6. update_metrics_batch (Batch Update) ---
    def update_metrics_batch(self, metrics_in: list[MetricEntryDomain]) -> list[MetricEntryDomain]:
        """Requirement: Batch CRUD. Atomic batch update of metrics.
        
        Note: Metrics are typically unique by Asset + Date. This method uses
        that composite logic to locate and update specific snapshots.
        """
        try:
            updated_entries = []
            for m_data in metrics_in:
                # 1. Lookup by composite business key (Asset + Date)
                statement = select(MetricEntry).where(
                    col(MetricEntry.asset_id) == m_data.asset_id,
                    col(MetricEntry.date_id) == m_data.date_id
                )
                db_metric = self.session.exec(statement).first()

                if not db_metric:
                    self.session.rollback()
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Metric snapshot for Asset {m_data.asset_id} on Date {m_data.date_id} not found."
                    )

                # 2. Update model using automated mapping
                update_dict = m_data.model_dump(exclude={"id"})
                db_metric.sqlmodel_update(update_dict)
                
                self.session.add(db_metric)
                updated_entries.append(db_metric)

            # 3. Atomic Commit
            self.session.commit()
            return [self._map_to_domain(e) for e in updated_entries]

        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch metric update failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch metric update."
            )

    # --- 7. delete_metric (Delete) ---
    def delete_metric(self, id: int) -> None:
        """Full CRUD: Removes a single metric entry from the Silver layer."""
        # 1. Fetch record
        db_metric = self._get_dim_metric_or_404(id)
        
        try:
            # 2. Perform hard delete (standard for fact snapshots)
            self.session.delete(db_metric)
            self.session.commit()
            logger.info(f"Metric entry {id} deleted.")
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to delete metric {id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error during metric deletion."
            )

    # --- 8. delete_metrics_batch (Batch Delete) ---
    def delete_metrics_batch(self, ids: list[int]) -> None:
        """Requirement: Batch CRUD. Bulk deletes metrics by primary key IDs."""
        if not ids:
            return

        try:
            # 1. Fetch targeted items
            statement = select(MetricEntry).where(col(MetricEntry.id).in_(ids))
            items = self.session.exec(statement).all()

            # 2. Validation: Strict All-or-Nothing
            if len(items) != len(ids):
                found_ids = {item.id for item in items if item.id is not None}
                missing_ids = set(ids) - found_ids
                self.session.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Batch aborted. Metric IDs not found: {list(missing_ids)}"
                )

            # 3. Perform the deletions
            for item in items:
                self.session.delete(item)

            # 4. Atomic Commit
            self.session.commit()
            logger.info(f"Successfully deleted {len(items)} metric entries.")
        except HTTPException:
            raise
        except Exception as e:
            self.session.rollback()
            logger.error(f"Batch Metric deletion failed: {e!s}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal error during batch metric deletion."
            )
