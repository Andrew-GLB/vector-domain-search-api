import logging
import os
from collections.abc import Generator

from dotenv import load_dotenv
from sqlmodel import Session, SQLModel, create_engine, text


load_dotenv()
database_url = os.getenv("DATABASE_URL")

if not database_url:
    raise ValueError("DATABASE_URL environment variable is not set in .env")

# Use pool_pre_ping for stability with Supabase/PgBouncer
engine = create_engine(database_url, pool_pre_ping=True)
logger = logging.getLogger(__name__)

def create_db_and_tables() -> None:
    """Creates physical tables and the 10-dimension Virtual Fact View.
    
    Requirement: REST API should create necessary schemas if they don't exist.
    This fulfills the Medallion Architecture by virtualizing the 'Gold Layer'.
    """
    # 1. Create the schemas (namespaces) for the layers
    with Session(engine) as session:
        # Fix for [call-overload]: use session.execute() for text() clauses
        session.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        session.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        session.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
        session.commit()

    # 2. Create all physical tables
    # NOTE: This creates physical tables in 'gold' for your MView classes.
    # We will remove them in the next step.
    SQLModel.metadata.create_all(engine)

    # 3. Drop physical 'gold' tables that should be Views
    # This prevents the (psycopg2.errors.WrongObjectType) error.
    cleanup_sql = """
    DROP SCHEMA gold CASCADE;
    CREATE SCHEMA IF NOT EXISTS gold;
    """

    # 4. Define the Virtual Fact Tables / Views / Aggregations (Gold Layer MVs)
    # This Materialized View performs a 10-way JOIN to provide a complete context for every metric.
    views_sql = [
        # --- View 1: Master Fact View (Materialized) ---
        """
        CREATE MATERIALIZED VIEW gold.fact_asset_metrics AS
        SELECT
            m.id,
            m.asset_id,
            e.resource_name,
            e.serial_number,
            p.provider_name,
            hp.profile_name AS hardware_spec,
            r.region_code,
            t.team_name,
            sv.service_name,
            sv.category AS service_category,
            t.department,
            env.env_name,
            st.status_name,
            cc.center_code,
            sec.tier_name AS security_tier,
            d.full_date,
            m.cpu_usage_avg,
            m.memory_usage_avg,
            m.hourly_cost,
            m.uptime_seconds,
            m.source_timestamp,
            m.updated_at
        FROM silver.metric_entry m
        LEFT JOIN silver.dim_asset e ON m.asset_id = e.id
        LEFT JOIN silver.dim_provider p ON m.provider_id = p.id
        LEFT JOIN silver.dim_hardware_profile hp ON m.hardware_profile_id = hp.id
        LEFT JOIN silver.dim_region r ON m.region_id = r.id
        LEFT JOIN silver.dim_team t ON m.team_id = t.id
        LEFT JOIN silver.dim_service_type sv ON m.service_type_id = sv.id
        LEFT JOIN silver.dim_environment env ON m.environment_id = env.id
        LEFT JOIN silver.dim_status st ON m.status_id = st.id
        LEFT JOIN silver.dim_cost_center cc ON m.cost_center_id = cc.id
        LEFT JOIN silver.dim_security_tier sec ON m.security_tier_id = sec.id
        LEFT JOIN silver.dim_date d ON m.date_id = d.id;

        CREATE UNIQUE INDEX idx_master_metric_id ON gold.fact_asset_metrics (id);
        CREATE INDEX idx_master_resource ON gold.fact_asset_metrics (resource_name);
        """,
        # --- View 2: Asset Utilization (Materialized) ---
        """
        CREATE MATERIALIZED VIEW gold.fact_asset_utilization_daily AS
        SELECT
            m.id AS metric_id,
            d.full_date,
            a.resource_name,
            a.serial_number,
            p.provider_name,
            t.team_name,
            cc.center_code,
            m.cpu_usage_avg,
            m.memory_usage_avg,
            a.description,
            (m.hourly_cost * 24) AS daily_cost
        FROM silver.metric_entry m
            LEFT JOIN silver.dim_asset a ON m.asset_id = a.id
            LEFT JOIN silver.dim_provider p ON m.provider_id = p.id
            LEFT JOIN silver.dim_team t ON m.team_id = t.id
            LEFT JOIN silver.dim_cost_center cc ON m.cost_center_id = cc.id
            LEFT JOIN silver.dim_date d ON m.date_id = d.id;

        CREATE UNIQUE INDEX idx_utilization_metric_id ON gold.fact_asset_utilization_daily (metric_id);
        """,
         # --- View 3: Team Costs (Materialized Aggregation) ---
        """
        CREATE MATERIALIZED VIEW gold.agg_team_costs_monthly AS
        SELECT
            EXTRACT(YEAR FROM d.full_date)::INT AS year,
            TRIM(TO_CHAR(d.full_date, 'Month')) AS month_name,
            t.team_name,
            t.department,
            ROUND(SUM(m.hourly_cost * 24)::NUMERIC, 2) AS total_monthly_cost,
            ROUND(AVG(m.cpu_usage_avg)::NUMERIC, 2) AS avg_cpu_efficiency
        FROM silver.metric_entry m
            LEFT JOIN silver.dim_team t ON m.team_id = t.id
            LEFT JOIN silver.dim_date d ON m.date_id = d.id
        GROUP BY 1, 2, 3, 4;

        -- Unique Index on the composite primary key fields
        CREATE UNIQUE INDEX idx_team_cost_unique ON gold.agg_team_costs_monthly (year, month_name, team_name, department);
        """,
        # --- View 4: Security Compliance (Materialized Risk Tracker) ---
        """
        CREATE MATERIALIZED VIEW gold.view_security_compliance_posture AS
        SELECT
            a.id AS asset_id,
            a.resource_name,
            a.serial_number,
            env.tier AS tier_name,
            env.env_name,
            st.status_name,
            MAX(m.updated_at) AS last_seen
        FROM silver.metric_entry m
            LEFT JOIN silver.dim_asset a ON m.asset_id = a.id
            LEFT JOIN silver.dim_environment env ON m.environment_id = env.id
            LEFT JOIN silver.dim_status st ON m.status_id = st.id
        WHERE
            env.tier = 'Mission Critical'
            AND env.env_name = 'Production'
            AND st.status_name IN ('STOPPED', 'MAINTENANCE', 'TERMINATED', 'PENDING')
        GROUP BY 1, 2, 3, 4, 5, 6;

        CREATE UNIQUE INDEX idx_security_asset_id ON gold.view_security_compliance_posture (asset_id);
        """,
        # --- View 5: Resource Efficiency (Materialized Complex Calculation) ---
        """
        CREATE MATERIALIZED VIEW gold.agg_resource_efficiency AS
        WITH asset_metrics_summary AS (
            SELECT
                asset_id,
                AVG(cpu_usage_avg) AS raw_avg_cpu,
                AVG(memory_usage_avg) AS raw_avg_mem,
                SUM(hourly_cost * 24) AS raw_total_cost
            FROM silver.metric_entry
            GROUP BY asset_id
        )
        SELECT
            s.asset_id,
            a.resource_name,
            ROUND(s.raw_avg_cpu::numeric, 2) AS avg_cpu,
            ROUND(s.raw_avg_mem::numeric, 2) AS avg_mem,
            ROUND(s.raw_total_cost::numeric, 2) AS total_cost,
            
            -- Efficiency Score: (Avg CPU / Total Cost)
            -- Handled NULLIF to avoid division by zero errors
            ROUND((s.raw_avg_cpu / NULLIF(s.raw_total_cost, 0))::numeric, 4) AS efficiency_score,
            
            -- Waste Index Logic:
            -- Identifying servers with < 5% CPU usage costing > $100
            CASE
                WHEN s.raw_avg_cpu < 5 AND s.raw_total_cost > 100 THEN 'High Waste'
                WHEN s.raw_avg_cpu < 10 AND s.raw_total_cost > 50 THEN 'Potential Waste'
                WHEN s.raw_avg_cpu > 70 THEN 'Optimized'
                ELSE 'Normal'
            END AS waste_index
        FROM asset_metrics_summary s
        JOIN silver.dim_asset a ON s.asset_id = a.id;

        CREATE UNIQUE INDEX idx_efficiency_asset_id ON gold.agg_resource_efficiency (asset_id);
        """
    ]

    # 5. Execute Cleanup and View Creation in a single transaction block
    try:
        with Session(engine) as session:
            # First, delete the accidental tables
            session.execute(text(cleanup_sql))
            # Then, create all the views
            for sql in views_sql:
                session.execute(text(sql))
            session.commit()
        logger.info("Database Gold Materialized Views created successfully after cleanup.")
    except Exception as e:
        logger.error(f"Error during database virtualization: {e}")

   
def get_session() -> Generator[Session, None, None]:
    """FastAPI dependency to provide a database session."""
    with Session(engine) as session:
        yield session
