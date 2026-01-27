# 1. Standard Library
from collections.abc import Generator
from datetime import date
from typing import Any

import pytest
from pydantic import ValidationError

# 2. Third-Party Libraries
from sqlmodel import Session, SQLModel, create_engine, text

# Layer 3: Domain Entities
from app.domain.asset import AssetDomain
from app.domain.metric_entry import MetricEntryDomain


# Setup: In-memory SQLite for isolated testing
@pytest.fixture(name="session")
def session_fixture() -> Generator[Session, Any, None]:
    """
    Creates a clean, in-memory database for every test.
    This ensures tests don't interfere with each other or the real data.
    """
    engine = create_engine("sqlite:///:memory:")
    # We use our actual creation logic to ensure the VIEW is also created
    SQLModel.metadata.create_all(engine)
    # Create the Virtual View for testing Gold Layer logic
    view_sql = """
    CREATE VIEW fact_entity_metrics AS
    SELECT m.id, m.cpu_usage_avg, e.resource_name
    FROM metric_entry m
    LEFT JOIN dim_entity e ON m.entity_id = e.id;
    """
    with Session(engine) as session:
        session.exec(text(view_sql))
        session.commit()
        yield session

# --- 1. Testing Entity Domain Logic (Regex & Formatting) ---

def test_entity_serial_format_correct() -> None:
    """Validates that a correctly formatted serial number is accepted."""
    asset = AssetDomain(
        resource_name="Test Server",
        serial_number="RES-A1B2-C3D4", # Correct Format
        description="Testing unit tests",
        created_at=date(2024, 1, 1)
    )
    assert asset.serial_number == "RES-A1B2-C3D4"

def test_entity_serial_format_incorrect() -> None:
    """Validates that malformed serial numbers raise a ValidationError."""
    with pytest.raises(ValidationError):
        AssetDomain(
            resource_name="Test Server",
            serial_number="INVALID-SERIAL", # Wrong Format
            description="Testing failure",
            created_at=date(2024, 1, 1)
        )

def test_entity_name_cleansing() -> None:
    """Validates that the model automatically trims and title-cases the name."""
    asset = AssetDomain(
        resource_name="  production-db  ",
        serial_number="RES-SQLD-1234",
        description="Testing title case",
        created_at=date(2024, 1, 1)
    )
    # Result of .strip().title()
    assert asset.resource_name == "Production-Db"

# --- 2. Testing Metric Logic (Rounding & Constraints) ---

def test_metric_cost_rounding() -> None:
    """Validates that hourly cost is rounded to 4 decimal places."""
    metric = MetricEntryDomain(
        asset_id=1, provider_id=1, region_id=1, team_id=1,
        service_type_id=1, date_id=1, environment_id=1,
        status_id=1, cost_center_id=1, security_tier_id=1,
        cpu_usage_avg=50.0, memory_usage_avg=8.0,
        hourly_cost=0.1234567, # Should round
        uptime_seconds=3600
    )
    assert metric.hourly_cost == 0.1235

def test_metric_cpu_usage_constraints() -> None:
    """Validates that CPU usage cannot exceed 100%."""
    with pytest.raises(ValidationError):
        MetricEntryDomain(
            # ... all IDs ...
            asset_id=1, provider_id=1, region_id=1, team_id=1,
            service_type_id=1, date_id=1, environment_id=1,
            status_id=1, cost_center_id=1, security_tier_id=1,
            cpu_usage_avg=150.0, # Invalid value (> 100)
            memory_usage_avg=8.0, hourly_cost=0.1, uptime_seconds=3600
        )
