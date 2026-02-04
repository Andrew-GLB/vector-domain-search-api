import io
import logging
import re
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, SQLModel, create_engine, delete, select, text
from supabase import Client, create_client

# Core Config & Database structure
from app.core.config import settings
from app.data_access.database import create_db_and_tables

# Layer 4: Data Access
from app.data_access.models import (
    DimAsset,
    DimCostCenter,
    DimDate,
    DimEnvironment,
    DimHardwareProfile,
    DimProvider,
    DimRegion,
    DimSecurityTier,
    DimServiceType,
    DimStatus,
    DimTeam,
    MetricEntry,
)

# Layer 3: Domain Models (Pydantic)
from app.domain.asset import AssetDomain
from app.domain.cost_center import CostCenterDomain
from app.domain.environment import EnvironmentDomain
from app.domain.hardware_profile import HardwareProfileDomain
from app.domain.provider import ProviderDomain
from app.domain.region import RegionDomain
from app.domain.security_tier import SecurityTierDomain
from app.domain.service_type import ServiceTypeDomain
from app.domain.status import StatusDomain
from app.domain.team import TeamDomain

# Layer 2: ETL & Services
from app.etl.pipeline import DataExtractor, DateDimensionGenerator
from app.services.ai_service import AIService
from app.services.search_service import SearchService

logger = logging.getLogger(__name__)

class SeedService:
    """Orchestration service for the Medallion Data Pipeline.
    
    Coordinates the "Bronze to Silver" flow by fetching raw files from 
    Supabase Storage (files/data/bronze), standardizing them into Postgres, 
    and indexing them into Typesense for search.
    """

    def __init__(self) -> None:
        """Initializes database engine and Supabase Storage client."""
        # 1. Initialize DB Engine from centralized settings
        self.engine = create_engine(settings.DATABASE_URL)
        
        # 2. Initialize Supabase Client for Storage access
        self.supabase: Client = create_client(
            settings.SUPABASE_URL, 
            settings.SUPABASE_SERVICE_ROLE_KEY
        )
        
        # 3. Define specific cloud storage paths
        self.bucket_name = "initial_seeding_files"
        self.bronze_folder = "files/data/bronze"
        
        # 4. Initialize auxiliary services
        self.ai_service = AIService()
        self.search_service = SearchService()

    def run_seed_process(self) -> dict[str, str]:
        """Main entry point for the Medallion Pipeline (Bronze -> Silver -> Gold)."""
        try:
            # 1. CLEAN SLATE: Wipe Database and Search index
            logger.info("🗑️ Wiping existing database schemas for a fresh seed...")
            self._cleanup_database()

            logger.info("🗑️ Wiping Typesense collections...")
            self._cleanup_typesense()

            # 2. PREPARE: Re-create structure (Schemas, Tables, and Gold Views)
            logger.info("🏗️ Initializing database structure (Schemas, Tables, Views)...")
            self._prepare_database_environment()

            # 3. BRONZE PHASE: Landing raw data from Supabase Storage
            logger.info("🚀 Starting Medallion Pipeline: Bronze Layer")
            self._ingest_all_to_bronze()

            # 4. SILVER PHASE: Standardization and Dimension loading
            logger.info("✨ Processing Silver Layer Dimensions...")
            self._seed_calendar()
            self._process_dimensions()

            # 5. FACT PHASE: Metric ingestion with CDC logic
            logger.info("📊 Processing Fact Metrics...")
            self._process_metrics()

            # 6. GOLD PHASE: Refresh Materialized Views to reflect new data
            logger.info("🏆 Finalizing Gold Layer Analytics...")
            self._refresh_gold_views()

            return {"status": "success", "message": "Pipeline and Gold Layer Sync Complete."}
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e!s}")
            return {"status": "error", "message": str(e)}

    def _cleanup_database(self) -> None:
        """Requirement: Drop all tables by dropping and recreating schemas."""
        with Session(self.engine) as session:
            # 1. Drop existing schemas entirely to remove all constraints
            session.execute(text("DROP SCHEMA IF EXISTS bronze CASCADE;"))
            session.execute(text("DROP SCHEMA IF EXISTS silver CASCADE;"))
            session.execute(text("DROP SCHEMA IF EXISTS gold CASCADE;"))
            session.commit()
        logger.info("✅ Database schemas dropped.")

    def _cleanup_typesense(self) -> None:
        """Deletes existing collections from Typesense to prevent stale data."""
        collections = [
            "CostCenterDomain", "EnvironmentDomain", "HardwareProfileDomain",
            "ProviderDomain", "RegionDomain", "SecurityTierDomain",
            "ServiceTypeDomain", "StatusDomain", "TeamDomain", "AssetDomain"
        ]

        for entity in collections:
            try:
                # 1. Atomic deletion of collection
                self.search_service.client.collections[entity].delete()
                logger.info(f"🔥 Collection '{entity}' deleted.")
            except Exception:
                logger.debug(f"ℹ️ Collection '{entity}' not found, skipping.")

    def _prepare_database_environment(self) -> None:
        """Calls the centralized database initialization logic."""
        # 1. Execute external database script to create Schemas, Tables, and Gold Views
        create_db_and_tables()
        logger.info("🏗️ Database structure and Gold Layer virtualization complete.")

    def _ingest_all_to_bronze(self) -> None:
        """Requirement: Fetch files from Supabase Storage and dump to 'bronze' schema."""
        # 1. List files specifically in the files/data/bronze path
        try:
            storage_files = self.supabase.storage.from_(self.bucket_name).list(self.bronze_folder)
        except Exception as e:
            logger.error(f"❌ Could not access Supabase path {self.bronze_folder}: {e}")
            return

        for file_info in storage_files:
            file_name = file_info['name']
            if file_name == ".emptyFolderPlaceholder":
                continue

            # 2. Extract Table Name and Extension
            file_stem = file_name.rsplit('.', 1)[0]
            extension = f".{file_name.rsplit('.', 1)[-1].lower()}"
            table_parts = re.split(r'_\d{4}_', file_stem)
            table_name = table_parts[0].lower()

            logger.info(f"📥 Landing {file_name} -> bronze.{table_name}")

            # 3. Download to memory buffer
            try:
                full_storage_path = f"{self.bronze_folder}/{file_name}"
                file_bytes = self.supabase.storage.from_(self.bucket_name).download(full_storage_path)
                file_buffer = io.BytesIO(file_bytes)
            except Exception as e:
                logger.error(f"❌ Failed download for {file_name}: {e}")
                continue

            # 4. Extract data using Polars
            if extension == ".csv":
                df = DataExtractor.read_csv(file_buffer)
            elif extension == ".json":
                df = DataExtractor.read_json(file_buffer)
            elif extension == ".pdf":
                raw_text = DataExtractor.extract_pdf_text(file_buffer)
                df = DataExtractor.convert_text_to_df(raw_text, "provider_name")
            else:
                continue

            if df.is_empty():
                continue

            # 5. Standardize column names (snake_case)
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]

            # 6. Write to Bronze Schema (Replace mode)
            full_table_path = f"bronze.{table_name}"
            df.write_database(
                table_name=full_table_path,
                connection=self.engine,
                if_table_exists="replace",
                engine="sqlalchemy"
            )
            logger.info(f"✅ Bronze: {full_table_path} landed.")

    def _process_dimensions(self) -> None:
        """Processes Bronze data into Silver and syncs to Typesense."""
        # Mapping: (SQLModel, BronzeTableName, BusinessKey, DomainModel, CollectionName)
        mappings: list[tuple[Any, str, str, Any, str]] = [
            (DimCostCenter, "cost_centers", "center_code", CostCenterDomain, "CostCenterDomain"),
            (DimEnvironment, "environments", "env_name", EnvironmentDomain, "EnvironmentDomain"),
            (DimHardwareProfile, "hardware_profiles", "profile_name", HardwareProfileDomain, "HardwareProfileDomain"),
            (DimProvider, "providers", "provider_name", ProviderDomain, "ProviderDomain"),
            (DimRegion, "regions", "region_code", RegionDomain, "RegionDomain"),
            (DimSecurityTier, "security_tiers", "tier_name", SecurityTierDomain, "SecurityTierDomain"),
            (DimServiceType, "service_types", "service_name", ServiceTypeDomain, "ServiceTypeDomain"),
            (DimStatus, "statuses", "status_name", StatusDomain, "StatusDomain"),
            (DimTeam, "teams", "team_name", TeamDomain, "TeamDomain"),
            (DimAsset, "assets", "serial_number", AssetDomain, "AssetDomain")
        ]

        for sql_model, bronze_table, u_key, domain_model, collection_name in mappings:
            # 1. Pull from Bronze Layer
            query = f"SELECT * FROM bronze.{bronze_table}"
            df = pl.read_database(query=query, connection=self.engine)
            
            # 2. Standardization and Upsert to Silver Layer
            self._upsert_polars_to_silver(df, sql_model, u_key)

            # 3. Pull fresh Silver data for Search Sync
            t_name = getattr(sql_model, "__tablename__")
            silver_df = pl.read_database(query=f"SELECT * FROM silver.{t_name}", connection=self.engine)
            self._sync_to_typesense(silver_df, domain_model, collection_name)

    def _sync_to_typesense(self, df: pl.DataFrame, domain_model: Any, collection_name: str) -> None:
        """Indexes data into Typesense after Domain validation."""
        logger.info(f"🔍 Syncing {collection_name} to Typesense...")
        records = df.to_dicts()

        for record in records:
            try:
                # 1. Validate through Pydantic Domain Layer
                domain_obj = domain_model.model_validate(record)
                doc = domain_obj.model_dump()

                # 2. Format for Typesense (IDs as strings, Dates as Unix Timestamps)
                doc["id"] = str(doc.get("id"))
                for key, value in doc.items():
                    if isinstance(value, datetime):
                        doc[key] = int(value.timestamp())
                    elif isinstance(value, date):
                        doc[key] = int(datetime.combine(value, datetime.min.time()).timestamp())

                # 3. Perform indexing
                self.search_service.index_asset(collection_name, doc)
            except Exception as e:
                logger.error(f"⚠️ Validation skipped for {collection_name}: {e}")

    def _upsert_polars_to_silver(self, df: pl.DataFrame, model: Any, unique_col: str) -> None:
        """Standard SQL UPSERT logic with automated metadata handling."""
        data_dicts = df.to_dicts()
        now = datetime.now(UTC)
        
        with Session(self.engine) as session:
            for row in data_dicts:
                # 1. Filter data to match model fields
                fields = getattr(model, "model_fields", {})
                valid_data = {k: v for k, v in row.items() if k in fields}
                
                # 2. Inject standard Medallion architecture metadata
                if "is_active" in fields:
                    valid_data["is_active"] = True
                if "updated_at" in fields:
                    valid_data["updated_at"] = now
                if "source_timestamp" in fields:
                    valid_data["source_timestamp"] = row.get("source_timestamp") or now

                # 3. Execute PostgreSQL UPSERT
                stmt = insert(model).values(**valid_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[getattr(model, unique_col)],
                    set_=valid_data
                )
                session.exec(stmt)
            session.commit()

        logger.info(f"✨ Silver: {getattr(model, '__tablename__')} updated.")

    def _seed_calendar(self) -> None:
        """Seeds the Date dimension for time-series analytics."""
        with Session(self.engine) as session:
            # 1. Check for data to prevent duplicates
            existing_row = session.exec(select(DimDate).limit(1)).first()
            if existing_row:
                logger.info("📅 DimDate already exists. Skipping.")
                return

        # 2. Generate and write range
        logger.info("📅 Generating Calendar Dimension...")
        dates_df = DateDimensionGenerator.generate_range(2023, 2026)
        dates_df.write_database(
            table_name="silver.dim_date",
            connection=self.engine,
            if_table_exists="append",
            engine="sqlalchemy"
        )
        logger.info("📅 DimDate successfully seeded.")

    def _process_metrics(self) -> None:
        """Fact Layer: Processes Metrics using Change Data Capture (UPSERT/DELETE) logic."""
        logger.info("📊 Processing Fact Metrics (Silver Layer)...")
        df = pl.read_database(query="SELECT * FROM bronze.metric_entries", connection=self.engine)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        
        allowed_fields = set(MetricEntry.model_fields.keys())

        with Session(self.engine) as session:
            for row in df.to_dicts():
                # 1. Determine CDC Action
                action = str(row.get("action", "UPSERT")).upper()
                clean_row = {k: v for k, v in row.items() if k in allowed_fields}

                if action == "DELETE":
                    # 2a. Execute Delete
                    del_stmt = delete(MetricEntry).where(
                        MetricEntry.asset_id == clean_row["asset_id"],
                        MetricEntry.date_id == clean_row["date_id"]
                    )
                    session.exec(del_stmt)
                else:
                    # 2b. Execute Upsert
                    upsert_stmt = insert(MetricEntry).values(**clean_row)
                    upsert_stmt = upsert_stmt.on_conflict_do_update(
                        index_elements=["asset_id", "date_id"],
                        set_=clean_row
                    )
                    session.exec(upsert_stmt)
            session.commit()
            logger.info("✨ Silver: metric_entry loaded.")

    def _refresh_gold_views(self) -> None:
        """Finalizes the Gold Layer by refreshing Materialized Views."""
        logger.info("♻️ Refreshing Gold Materialized Views...")
        views = [
            "gold.fact_asset_metrics",
            "gold.fact_asset_utilization_daily",
            "gold.agg_team_costs_monthly",
            "gold.view_security_compliance_posture",
            "gold.agg_resource_efficiency"
        ]
        with Session(self.engine) as session:
            for view in views:
                # 1. Execute refresh command
                session.execute(text(f"REFRESH MATERIALIZED VIEW {view};"))
            session.commit()
        logger.info("✅ Gold Layer analytics ready.")