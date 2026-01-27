from datetime import UTC, date, datetime

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, SQLModel


# --- 1. DIMENSION TABLES (Silver Layer) ---

class DimAsset(SQLModel, table=True):
    """Anchor dimension for cloud resources with Delta tracking."""
    __tablename__ = "dim_asset"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    resource_name: str
    serial_number: str = Field(index=True, unique=True)
    description: str
    created_at: date
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    # Use datetime for the ISO string with 'Z'
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimCostCenter(SQLModel, table=True):
    """Dimension for Financial Tracking with Delta tracking."""
    __tablename__ = "dim_cost_center"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    center_code: str = Field(index=True, unique=True)
    department: str
    budget_limit: float
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimEnvironment(SQLModel, table=True):
    """Dimension for SDLC Environment (Prod, Staging, etc.) with Delta tracking."""
    __tablename__ = "dim_environment"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    env_name: str = Field(index=True, unique=True)
    tier: str
    is_ephemeral: bool
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimHardwareProfile(SQLModel, table=True):
    """Dimension for Hardware Profile with Delta tracking."""
    __tablename__ = "dim_hardware_profile"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    profile_name: str = Field(index=True, unique=True)
    cpu_count: int
    ram_gb: int
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimProvider(SQLModel, table=True):
    """Dimension for Cloud Providers (AWS, Azure, etc.) with Delta tracking."""
    __tablename__ = "dim_provider"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    provider_name: str = Field(index=True, unique=True)
    provider_type: str
    support_contact: str
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimRegion(SQLModel, table=True):
    """Dimension for Geographic Regions with Delta tracking."""
    __tablename__ = "dim_region"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    region_code: str = Field(index=True, unique=True)
    display_name: str
    continent: str
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimSecurityTier(SQLModel, table=True):
    """Dimension for Governance and Compliance with Delta tracking."""
    __tablename__ = "dim_security_tier"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    tier_name: str = Field(index=True, unique=True)
    encryption_required: bool
    compliance_standard: str
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimServiceType(SQLModel, table=True):
    """Dimension for Service Classification with Delta tracking."""
    __tablename__ = "dim_service_type"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    service_name: str = Field(index=True, unique=True)
    category: str
    is_managed: bool
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimStatus(SQLModel, table=True):
    """Dimension for Operational States with Delta tracking."""
    __tablename__ = "dim_status"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    status_name: str = Field(index=True, unique=True)
    is_billable: bool
    description: str
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimTeam(SQLModel, table=True):
    """Dimension for Organizational Ownership with Delta tracking."""
    __tablename__ = "dim_team"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    # Core Data
    team_name: str = Field(index=True, unique=True)
    department: str
    lead_email: str
    # Pipeline Metadata
    is_active: bool = Field(default=True, index=True)
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

class DimDate(SQLModel, table=True):
    """Requirement: Calendar Table (Optional Feature #1)."""
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "silver"}
    id: int | None = Field(default=None, primary_key=True)
    full_date: date = Field(index=True, unique=True)
    year: int
    month: int
    month_name: str
    day: int
    day_of_week: int
    day_name: str
    quarter: int
    is_weekend: bool


# --- 2. PHYSICAL METRIC TABLE (The Source of Truth) ---

class MetricEntry(SQLModel, table=True):
    """Physical storage for quantitative performance metrics.
    Silver layer table with composite unique constraints for Delta tracking.
    """
    __tablename__ = "metric_entry"

    # Composite unique constraint to prevent duplicate metrics for the same asset/date
    __table_args__ = (
        UniqueConstraint("asset_id", "date_id", name="uq_asset_date"),
        {"schema": "silver"}
    )

    id: int | None = Field(default=None, primary_key=True)

    # Foreign Keys to the Dimensions
    asset_id: int = Field(foreign_key="silver.dim_asset.id", index=True)
    provider_id: int = Field(foreign_key="silver.dim_provider.id")
    region_id: int = Field(foreign_key="silver.dim_region.id")
    team_id: int = Field(foreign_key="silver.dim_team.id")
    service_type_id: int = Field(foreign_key="silver.dim_service_type.id")
    date_id: int = Field(foreign_key="silver.dim_date.id", index=True)
    environment_id: int = Field(foreign_key="silver.dim_environment.id")
    status_id: int = Field(foreign_key="silver.dim_status.id")
    cost_center_id: int = Field(foreign_key="silver.dim_cost_center.id")
    security_tier_id: int = Field(foreign_key="silver.dim_security_tier.id")
    hardware_profile_id: int = Field(foreign_key="silver.dim_hardware_profile.id")

    # The Facts
    cpu_usage_avg: float
    memory_usage_avg: float
    hourly_cost: float
    uptime_seconds: int

    # Pipeline Metadata
    source_timestamp: datetime = Field(index=True)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column_kwargs={"onupdate": lambda: datetime.now(UTC)}
    )

# --- 3. VIRTUAL VIEW MODEL (Gold Layer Representation) ---

class FactAssetMetrics(SQLModel):
    """Python representation of the 'fact_asset_metrics' SQL View.
    This provides a flattened, human-readable version of the star schema.
    """
    # Keys
    id: int
    asset_id: int

    # Refined/Joined Data
    resource_name: str
    serial_number: str
    provider_name: str
    hardware_spec: str
    region_code: str
    team_name: str
    service_name: str
    service_category: str
    department: str  # From DimTeam
    env_name: str
    status_name: str
    center_code: str
    security_tier: str
    full_date: date
    profile_name: str

    # Facts
    cpu_usage_avg: float
    memory_usage_avg: float
    hourly_cost: float
    uptime_seconds: int

    # Audit Metadata (Crucial for Gold Layer Freshness)
    source_timestamp: datetime
    updated_at: datetime
