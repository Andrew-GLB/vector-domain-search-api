from datetime import UTC, datetime

from pydantic import BaseModel, Field, field_validator


class MetricEntryDomain(BaseModel):
    """The pure domain representation of a Cloud Metric Snapshot.

    This model represents the 'Facts' being ingested. It captures
    performance and cost data which will eventually be virtualized
    through the Gold Layer Fact View.

    Attributes:
        asset_id (int): The ID of the resource being measured.
        # ... other IDs will be handled in the Service/Data Access layer ...
        cpu_usage_avg (float): Average CPU utilization (0-100).
        memory_usage_avg (float): Average Memory usage in GB.
        hourly_cost (float): Financial cost per hour.
        uptime_seconds (int): Operational uptime in the interval.
    """

    # Links required for the record (Foreign Keys)
    asset_id: int = Field(..., description="ID of the monitored asset")
    provider_id: int = Field(..., description="ID of the cloud provider")
    region_id: int = Field(..., description="ID of the geographic region")
    team_id: int = Field(..., description="ID of the owning team")
    service_type_id: int = Field(..., description="ID of the service classification")
    date_id: int = Field(..., description="ID of the date dimension")
    environment_id: int = Field(..., description="ID of the environment")
    status_id: int = Field(..., description="ID of the operational status")
    cost_center_id: int = Field(..., description="ID of the financial cost center")
    security_tier_id: int = Field(..., description="ID of the security classification")

    # Quantitative Measurements (The Facts)
    cpu_usage_avg: float = Field(..., description="Average CPU utilization (0-100)", ge=0, le=100)
    memory_usage_avg: float = Field(..., description="Average Memory usage in GB", ge=0)
    hourly_cost: float = Field(..., description="Financial cost per hour of operation", ge=0)
    uptime_seconds: int = Field(..., description="Total seconds of operation in the interval", ge=0)

    # Metadata (Crucial for Silver Layer)
    source_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="The moment the metric was ingested"
    )

    @field_validator('hourly_cost')
    @classmethod
    def format_cost(cls, v: float) -> float:
        return round(v, 4)

    def to_searchable_text(self) -> str:
        """Helper to create a string for Vector DB embedding.
        Uses IDs for now, but in the Service Layer, you'd map these to names.
        """
        return f"Asset:{self.asset_id} Team:{self.team_id} Type:{self.service_type_id}"

    model_config = {
        "from_attributes": True, # Allows easy conversion from SQLModel
        "json_schema_extra": {
            "example": {
                "asset_id": 1, "provider_id": 1, "region_id": 1,
                "team_id": 1, "service_type_id": 1, "date_id": 20260127,
                "environment_id": 1, "status_id": 1, "cost_center_id": 1,
                "security_tier_id": 1, "hardware_profile_id": 1,
                "cpu_usage_avg": 24.5, "memory_usage_avg": 4.0,
                "hourly_cost": 0.085, "uptime_seconds": 3600
            }
        }
    }
