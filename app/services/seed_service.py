import logging
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, SQLModel, create_engine, delete, select, text

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


database_uri = os.getenv("DATABASE_URI")

logger = logging.getLogger(__name__)

class SeedService:
    def __init__(self) -> None:
        """Initializes the Seeding service and database engine."""
        if not database_uri:
            raise ValueError("DATABASE_URI not found in environment variables.")

        self.connection_uri = database_uri
        self.bronze_path = Path("data/bronze")
        self.engine = create_engine(self.connection_uri)
        self.ai_service = AIService()
        self.search_service = SearchService()

    def run_seed_process(self) -> dict[str, str]:
        """Orchestrates the Medallion Pipeline from Bronze to Silver."""
        try:
            # 1. CLEAN SLATE: Database & Vector Search
            logger.info("🗑️ Wiping existing database schemas for a fresh seed...")
            self._cleanup_database()

            logger.info("🗑️ Wiping Typesense collections...")
            self._cleanup_typesense()

            logger.info("🚀 Starting Medallion Pipeline: Bronze -> Silver")

            # 2. PREPARE: Re-create schemas
            self._prepare_database_environment()

            # 3. BRONZE PHASE: Raw ingestion using Polars
            self._ingest_all_to_bronze()

            # 4. SILVER PHASE: Process Silver Layer & Search Indexing
            self._seed_calendar()
            self._process_dimensions()

            # 5. FACT PHASE: Metrics
            self._process_metrics()

            return {"status": "success", "message": "Delta Sync Complete."}
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return {"status": "error", "message": str(e)}

    def _cleanup_database(self) -> None:
        """Requirement #1: Drop all tables by dropping and recreating schemas."""
        with Session(self.engine) as session:
            # We drop the schemas entirely to ensure all tables, types, and constraints are gone
            session.execute(text("DROP SCHEMA IF EXISTS bronze CASCADE;"))
            session.execute(text("DROP SCHEMA IF EXISTS silver CASCADE;"))
            session.execute(text("DROP SCHEMA IF EXISTS gold CASCADE;"))
            session.commit()
        logger.info("✅ Database schemas dropped.")

    def _cleanup_typesense(self) -> None:
        """Deletes existing collections from Typesense."""
        domain_entities = [
            "CostCenterDomain", "EnvironmentDomain", "HardwareProfileDomain",
            "ProviderDomain", "RegionDomain", "SecurityTierDomain",
            "ServiceTypeDomain", "StatusDomain", "TeamDomain", "AssetDomain"
        ]

        for entity in domain_entities:
            try:
                self.search_service.client.collections[entity].delete()
                logger.info(f"🔥 Collection '{entity}' deleted.")
            except Exception:
                logger.debug(f"ℹ️ Collection '{entity}' not found, skipping.")

    def _prepare_database_environment(self) -> None:
        """Ensures the Medallion schemas exist in Supabase."""
        with Session(self.engine) as session:
            # Fix: Use session.execute() for TextClause to satisfy Mypy [call-overload]
            session.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            session.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
            session.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
            session.commit()

        SQLModel.metadata.create_all(self.engine)
        logger.info("🏗️ Database tables initialized.")

    def _ingest_all_to_bronze(self) -> None:
        """Requirement: Input includes structured (CSV, JSON) and unstructured (PDF).
        Dumps all source files into the 'bronze' schema.
        """
        extensions = ["*.csv", "*.json", "*.pdf"]

        for ext in extensions:
            files = list(self.bronze_path.glob(ext))
            for file in files:
                # Use regex to isolate table name from date-stamped files
                table_parts = re.split(r'_\d{4}_', file.stem)
                table_name = table_parts[0].lower()

                logger.info(f"📥 Landing {file.name} (Format: {ext[2:]}) -> bronze.{table_name}")

                if ext == "*.csv":
                    df = DataExtractor.read_csv(file)
                elif ext == "*.json":
                    df = DataExtractor.read_json(file)
                elif ext == "*.pdf":
                    # Requirement: Processing Unstructured Data
                    raw_text = DataExtractor.extract_pdf_text(file)
                    df = DataExtractor.convert_text_to_df(raw_text, "provider_name")
                else:
                    continue

                if df.is_empty():
                    logger.warning(f"⚠️ No data extracted from {file.name}")
                    continue

                # Standardize column names (snake_case)
                df.columns = [c.lower().replace(" ", "_") for c in df.columns]

                # Write to Bronze Schema
                full_table_path = f"bronze.{table_name}"
                df.write_database(
                    table_name=full_table_path,
                    connection=self.engine,
                    if_table_exists="replace",
                    engine="sqlalchemy"
                )
                logger.info(f"✅ Bronze: {full_table_path} landed.")

    def _process_dimensions(self) -> None:
        """Processes Bronze data into Silver and syncs to Typesense via Domain Models."""
        # Use Any for the mapping list to bypass the strict SQLModelMetaclass check
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
            query = f"SELECT * FROM bronze.{bronze_table}"
            df = pl.read_database(query=query, connection=self.engine)
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]

            # 1. Update Silver Layer (Postgres)
            self._upsert_polars_to_silver(df, sql_model, u_key)

            # 2. Sync to Search Layer (Typesense)
            # Use getattr to safely access __tablename__ and satisfy Mypy [attr-defined]
            table_name = sql_model.__tablename__
            silver_query = f"SELECT * FROM silver.{table_name}"
            silver_df = pl.read_database(query=silver_query, connection=self.engine)
            self._sync_to_typesense(silver_df, domain_model, collection_name)

    def _sync_to_typesense(self, df: pl.DataFrame, domain_model: Any, collection_name: str) -> None:
        """Indexes data into Typesense after Domain validation and timestamp conversion."""
        logger.info(f"🔍 Syncing {collection_name} to Typesense...")
        records = df.to_dicts()

        for record in records:
            try:
                # Validate & Clean data through the Domain Layer
                domain_obj = domain_model.model_validate(record)
                doc = domain_obj.model_dump()

                # Format for Typesense requirements
                doc["id"] = str(doc.get("id"))
                for key, value in doc.items():
                    # Handle datetime (subclass of date, check this first)
                    if isinstance(value, datetime):
                        doc[key] = int(value.timestamp())
                    # Handle pure date (e.g., 2026-01-27)
                    elif isinstance(value, date):
                        # Convert date to midnight datetime then to timestamp
                        doc[key] = int(datetime.combine(value, datetime.min.time()).timestamp())

                self.search_service.index_asset(collection_name, doc)
            except Exception as e:
                logger.error(f"⚠️ Validation skipped for {collection_name} record: {e}")

    def _upsert_polars_to_silver(self, df: pl.DataFrame, model: Any, unique_col: str) -> None:
        """Standard SQL UPSERT logic."""
        data_dicts = df.to_dicts()
        with Session(self.engine) as session:
            for row in data_dicts:
                # Access model_fields safely for Mypy
                fields = getattr(model, "model_fields", {})
                valid_data = {k: v for k, v in row.items() if k in fields}
                if not valid_data:
                    continue

                stmt = insert(model).values(**valid_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=[getattr(model, unique_col)],
                    set_=valid_data
                )
                session.exec(stmt)
            session.commit()

        # Access __tablename__ safely
        t_name = getattr(model, "__tablename__", "unknown")
        logger.info(f"✨ Silver: {t_name} updated.")

    def _enrich_assets(self, df: pl.DataFrame) -> pl.DataFrame:
        """Reference method for AI Enrichment (Not currently called in loop)."""
        logger.info("🤖 Running AI Enrichment on Assets...")
        assets = df.to_dicts()
        for asset in assets[:5]:
            raw_desc = str(asset.get("description", ""))
            name = str(asset.get("resource_name", "Unknown Resource"))
            enriched = self.ai_service.enrich_product_description(name, raw_desc)
            asset["description"] = enriched
        return pl.from_dicts(assets)

    def _seed_calendar(self) -> None:
        """Checks if dim_date is empty and seeds it if necessary."""
        with Session(self.engine) as session:
            query = select(DimDate).limit(1)
            existing_row = session.exec(query).first()

            if existing_row:
                logger.info("📅 DimDate already has data. Skipping.")
                return

        logger.info("📅 Generating and Seeding Calendar Dimension...")
        dates_df = DateDimensionGenerator.generate_range(2023, 2026)

        try:
            dates_df.write_database(
                table_name="silver.dim_date",
                connection=self.engine,
                if_table_exists="append",
                engine="sqlalchemy"
            )
            logger.info("📅 DimDate successfully seeded.")
        except Exception as e:
            logger.error(f"❌ Failed to seed calendar: {e}")

    def _process_metrics(self) -> None:
        """Processes Fact Metrics with CDC (UPSERT/DELETE) logic.
        This entity is NOT synced to Typesense.
        """
        logger.info("📊 Processing Fact Metrics (Database only)...")
        df = pl.read_database(query="SELECT * FROM bronze.metric_entries", connection=self.engine)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        allowed_fields = set(MetricEntry.model_fields.keys())

        with Session(self.engine) as session:
            for row in df.to_dicts():
                action = str(row.get("action", "UPSERT")).upper()
                clean_row = {k: v for k, v in row.items() if k in allowed_fields}

                if action == "DELETE":
                    # Fix: Use specific variable name to avoid type collision in Mypy
                    del_stmt = delete(MetricEntry).where(
                        MetricEntry.asset_id == clean_row["asset_id"],
                        MetricEntry.date_id == clean_row["date_id"]
                    )
                    session.exec(del_stmt)
                    logger.info(f"🗑️ Deleted metric: Asset {clean_row['asset_id']} Date {clean_row['date_id']}")

                else:
                    # Fix: Use specific variable name for Insert type
                    upsert_stmt = insert(MetricEntry).values(**clean_row)
                    upsert_stmt = upsert_stmt.on_conflict_do_update(
                        index_elements=["asset_id", "date_id"],
                        set_=clean_row
                    )
                    session.exec(upsert_stmt)
            session.commit()
            logger.info("✨ Silver: metric_entry loaded.")
