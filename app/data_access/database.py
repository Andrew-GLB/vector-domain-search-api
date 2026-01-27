import logging
import os
from collections.abc import Generator

from dotenv import load_dotenv
from sqlmodel import Session, SQLModel, create_engine, text


# Configuration: Ensuring the directory for the SQLite file exists
# DB_PATH = "./data/gold/warehouse.db"
# os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
# DATABASE_URL = f"sqlite:///{DB_PATH}"
load_dotenv()
database_url = os.getenv("DATABASE_URL")

# Fix for [arg-type]: Ensure database_url is not None before creating engine
if not database_url:
    raise ValueError("DATABASE_URL environment variable is not set in .env")

# echo=True is recommended for the technical challenge so the reviewer can see the schema creation
# engine = create_engine(database_url, echo=True)

engine = create_engine(
    database_url,
    # This is crucial when using external poolers like PgBouncer
    pool_pre_ping=True,
    # Use NullPool if you want PgBouncer to handle all pooling logic
    # from sqlalchemy.pool import NullPool
    # poolclass=NullPool
)

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

    # 2. Create all 11 physical tables (10 Dimensions + 1 Metric Snapshot)
    SQLModel.metadata.create_all(engine)

    # 3. Define the Virtual Fact Table (Gold Layer View)
    # This View performs a 10-way JOIN to provide a complete context for every metric.
    view_sql = """
    CREATE OR REPLACE VIEW gold.fact_asset_metrics AS
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
    """

    # 4. Execute the view creation via a raw SQL transaction
    try:
        with Session(engine) as session:
            # Fix for [call-overload]: use session.execute() for text() clauses
            session.execute(text(view_sql))
            session.commit()
        logger.info("Database schemas and Virtual Fact View created successfully.")
    except Exception as e:
        logger.error(f"Error creating Virtual View: {e}")

# Fix for [type-arg]: Add specific parameters to the Generator type hint
def get_session() -> Generator[Session, None, None]:
    """FastAPI dependency to provide a database session per request."""
    with Session(engine) as session:
        yield session
