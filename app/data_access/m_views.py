from datetime import date, datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class FactAssetMetricsMView(SQLModel, table=True):
    """Maps to gold.fact_asset_metrics.
    This is the "Universal Fact Table" providing full context for every metric entry.
    """
    __tablename__ = "fact_asset_metrics"
    __table_args__ = {"schema": "gold"}

    id: int = Field(primary_key=True) # From m.id
    asset_id: int
    resource_name: str
    serial_number: str
    provider_name: Optional[str] = Field(default=None, description="Mapped from silver.dim_provider")
    hardware_spec: str
    region_code: str
    team_name: str
    service_name: str
    service_category: str
    department: str
    env_name: str
    status_name: str
    center_code: str
    security_tier: str
    full_date: date
    cpu_usage_avg: float = Field(ge=0.0, le=100.00)
    memory_usage_avg: float = Field(ge=0.00)
    hourly_cost: float = Field(ge=0.00)
    uptime_seconds: int = Field(ge=0)
    source_timestamp: datetime
    updated_at: datetime

class AssetUtilizationMView(SQLModel, table=True):
    """Python model for gold.fact_asset_utilization_daily.
    Use Case: Efficiency Auditing. Identify underutilized or "zombie" resources to right-size infrastructure and cut immediate waste.
    """
    __tablename__ = "fact_asset_utilization_daily"
    __table_args__ = {"schema": "gold"}
    metric_id: int = Field(primary_key=True)
    full_date: date
    resource_name: str
    serial_number: str
    provider_name: str
    team_name: str
    center_code: str
    cpu_usage_avg: float
    memory_usage_avg: float
    # We include fields from the AI enrichment here too
    description: str | None = None
    daily_cost: float

class TeamCostMView(SQLModel, table=True):
    """Python model for gold.agg_team_costs_monthly.
    Use Case: Chargeback Reporting. Roll up monthly cloud spend by department for budget tracking and executive financial oversight.
    """
    __tablename__ = "agg_team_costs_monthly"
    __table_args__ = {"schema": "gold"}
    year: int = Field(primary_key=True)
    month_name: str = Field(primary_key=True)
    team_name: str = Field(primary_key=True)
    department: str = Field(primary_key=True)
    total_monthly_cost: float
    avg_cpu_efficiency: float

class SecurityComplianceMView(SQLModel, table=True):
    """Matches gold.view_security_compliance_posture
    Use Case: Risk Posture. Spot active "Critical" assets in production that are currently "Inactive" or "Decommissioned"
    but still existing in production.
    """
    __tablename__ = "view_security_compliance_posture"
    __table_args__ = {"schema": "gold"}
    asset_id: int = Field(primary_key=True)
    resource_name: str
    serial_number: str
    tier_name: str  # e.g., 'Mission Critical', 'Internal'
    env_name: str   # e.g., 'Production', 'Staging'
    status_name: str
    last_seen: datetime | None

class ResourceEfficiencyMView(SQLModel, table=True):
    """Matches gold.agg_resource_efficiency
    Use Case: Identifying servers with < 5 percent CPU usage that are costing more than $100/month.
    """
    __tablename__ = "agg_resource_efficiency"
    __table_args__ = {"schema": "gold"}
    asset_id: int = Field(primary_key=True)
    resource_name: str
    avg_cpu: float
    avg_mem: float
    total_cost: float
    efficiency_score: float # A calculated field: (Usage / Cost)
    waste_index: str        # e.g., 'High Waste', 'Optimized'

