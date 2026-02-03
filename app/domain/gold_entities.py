from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BaseDomainModel(BaseModel):
    """Base config for all domain entities."""
    model_config = ConfigDict(from_attributes=True)

class AssetMetricContext(BaseDomainModel):
    """The complete Enriched Domain Entity representing the 'fact_asset_metrics' view.
    This model provides the full business context for every single metric entry.
    """
    # Identifiers
    id: int
    asset_id: int
    
    # Asset Details
    resource_name: str
    serial_number: str
     
    # Dimensions Details
    provider_name: str | None = Field(default=None, description="Mapped from silver.dim_provider")
    hardware_spec: str = Field(..., description="Mapped from silver.dim_hardware_profile")
    region_code: str = Field(..., description="Mapped from silver.dim_region")
    team_name: str = Field(..., description="Mapped from silver.dim_team")
    service_name: str = Field(..., description="Mapped from silver.dim_service_type")
    service_category: str = Field(..., description="Mapped from silver.dim_service_type")
    department: str = Field(..., description="Mapped from silver.dim_team")
    env_name: str = Field(..., description="Mapped from silver.dim_environment")
    status_name: str = Field(..., description="Mapped from silver.dim_status")
    center_code: str = Field(..., description="Mapped from silver.dim_cost_center")
    security_tier: str = Field(..., description="Mapped from silver.dim_security_tier")
    
    # Metrics & Timestamps
    full_date: date
    cpu_usage_avg: float = Field(ge=0.0, le=100.00)
    memory_usage_avg: float = Field(ge=0.00)
    hourly_cost: float = Field(ge=0.00)
    uptime_seconds: int = Field(ge=0)
    source_timestamp: datetime
    updated_at: datetime

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": 1,
                "asset_id": 501,
                "resource_name": "prod-api-server-01",
                "serial_number": "XYZ-99L-001",
                "provider_name": "AWS",
                "hardware_spec": "m5.xlarge",
                "region_code": "us-east-1",
                "team_name": "Cloud-Ops",
                "service_name": "EC2-Compute",
                "service_category": "IaaS",
                "department": "Engineering",
                "env_name": "Production",
                "status_name": "Active",
                "center_code": "CC-104",
                "security_tier": "Critical",
                "full_date": "2023-10-27",
                "cpu_usage_avg": 42.5,
                "memory_usage_avg": 68.2,
                "hourly_cost": 0.192,
                "uptime_seconds": 3600,
                "source_timestamp": "2023-10-27T10:00:00Z",
                "updated_at": "2023-10-27T10:05:00Z"
            }
        }
    )

class AssetUtilization(BaseModel):
    """Domain entity for Asset Utilization.
    Includes validation to ensure metric percentages are realistic.
    """
    metric_id: int
    full_date: date
    resource_name: str
    serial_number: str
    provider_name: str
    team_name: str
    center_code: str
    cpu_usage_avg: float = Field(ge=0.00, le=100.00)
    memory_usage_avg: float = Field(ge=0.00)
    description: str | None = None
    daily_cost: float = Field(ge=0.00)

    @field_validator('cpu_usage_avg')
    @classmethod
    def validate_percentages(cls, v: float) -> float:
        # Business Rule: If the sensor sends a value > 100 due to a spike,
        # we cap it at 100 for domain logic.
        return min(v, 100.0)
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "metric_id": 101,
                "full_date": "2023-10-27",
                "resource_name": "aws-ec2-prod-01",
                "serial_number": "SN-998877",
                "provider_name": "AWS",
                "team_name": "Platform-Eng",
                "center_code": "FIN-01",
                "cpu_usage_avg": 4.5,
                "memory_usage_avg": 12.2,
                "description": "Primary web server",
                "daily_cost": 150.50
            }
        }
    )

class TeamCost(BaseModel):
    """Domain entity for Team Costs.
    Validates that financial data remains non-negative.
    """
    year: int = Field(gt=2000)
    month_name: str
    team_name: str
    department: str
    total_monthly_cost: float = Field(ge=0)
    avg_cpu_efficiency: float = Field(ge=0)

    @field_validator('month_name')
    @classmethod
    def validate_month(cls, v: str) -> str:
        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        if v.capitalize() not in months:
            raise ValueError(f"Invalid month name: {v}")
        return v.capitalize()
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "year": 2023,
                "month_name": "October",
                "team_name": "Data-Science",
                "department": "R&D",
                "total_monthly_cost": 12500.75,
                "avg_cpu_efficiency": 0.65
            }
        }
    )

class SecurityCompliance(BaseModel):
    """Domain entity for Security Posture.
    Ensures environment names follow corporate standards.
    """
    asset_id: int
    resource_name: str
    serial_number: str
    tier_name: str
    env_name: str = "Sandbox"
    status_name: str
    last_seen: datetime | None = None

    @field_validator('env_name')
    @classmethod
    def enforce_env_standard(cls, v: str) -> str:
        allowed = ['Production', 'Staging', 'Development', 'UAT', 'Sandbox']
        if v not in allowed:
            # Domain logic: classify unknown envs as 'Sandbox'
            return 'Sandbox'
        return v
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "asset_id": 5001,
                "resource_name": "db-prod-sql",
                "serial_number": "VOL-123",
                "tier_name": "Mission Critical",
                "env_name": "Production",
                "status_name": "MAINTENANCE",
                "last_seen": "2023-10-27T10:00:00"
            }
        }
    )

class ResourceEfficiency(BaseModel):
    """Domain entity for Resource Efficiency.
    Includes a waste_index validator.
    """
    asset_id: int
    resource_name: str
    avg_cpu: float = Field(ge=0)
    avg_mem: float = Field(ge=0)
    total_cost: float = Field(ge=0)
    efficiency_score: float = Field(ge=0)
    waste_index: str = "Normal"

    @field_validator('waste_index')
    @classmethod
    def validate_waste_category(cls, v: str) -> str:
        if v not in ["High Waste", "Potential Waste", "Optimized", "Normal"]:
            return "Normal"
        return v
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "asset_id": 99,
                "resource_name": "legacy-app-server",
                "avg_cpu": 2.1,
                "avg_mem": 45.0,
                "total_cost": 450.00,
                "efficiency_score": 0.05,
                "waste_index": "High Waste"
            }
        }
    )
