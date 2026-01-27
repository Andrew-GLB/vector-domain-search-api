from datetime import date, datetime

from sqlmodel import Field, SQLModel


class AssetUtilizationMView(SQLModel, table=False):
    """Python model for gold.fact_asset_utilization_daily.
    Use Case: Efficiency Auditing. Identify underutilized or "zombie" resources to right-size infrastructure and cut immediate waste.
    """
    metric_id: int = Field(primary_key=True)
    full_date: date
    resource_name: str
    serial_number: str
    provider_name: str
    team_name: str
    center_code: str
    cpu_usage_avg: float
    memory_usage_avg: float
    daily_cost: float
    # We include fields from the AI enrichment here too
    description: str | None = None

class TeamCostMView(SQLModel, table=False):
    """Python model for gold.agg_team_costs_monthly.
    Use Case: Chargeback Reporting. Roll up monthly cloud spend by department for budget tracking and executive financial oversight.
    """
    year: int
    month_name: str
    team_name: str
    department: str
    total_monthly_cost: float
    avg_cpu_efficiency: float

class SecurityComplianceMView(SQLModel, table=False):
    """Matches gold.view_security_compliance_posture
    Use Case: Risk Posture. Spot active "Critical" assets in production that are currently "Inactive" or "Decommissioned"
    but still existing in production.
    """
    asset_id: int = Field(primary_key=True)
    resource_name: str
    serial_number: str
    tier_name: str  # e.g., 'Critical', 'Internal'
    env_name: str   # e.g., 'Production', 'Staging'
    status_name: str
    last_seen: datetime | None

class ResourceEfficiencyMView(SQLModel, table=False):
    """Matches gold.agg_resource_efficiency
    Use Case: Identifying servers with < 5 percent CPU usage that are costing more than $100/month.
    """
    asset_id: int = Field(primary_key=True)
    resource_name: str
    avg_cpu: float
    avg_mem: float
    total_cost: float
    efficiency_score: float # A calculated field: (Usage / Cost)
    waste_index: str        # e.g., 'High Waste', 'Optimized'

class AssetAuditTrailMView(SQLModel, table=False):
    """Matches gold.fact_asset_history
    Use Case: Seeing how an asset's cost or status changed over the last 30 days.
    """
    history_id: int = Field(primary_key=True)
    asset_id: int
    resource_name: str
    change_date: date
    previous_status: str
    current_status: str
    cost_impact: float
