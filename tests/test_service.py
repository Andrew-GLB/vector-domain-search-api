# 1. Standard Library
from collections.abc import Generator
from datetime import date, datetime
from typing import Any

import pytest
from pydantic import ValidationError

# 2. Third-Party Libraries
from sqlmodel import Session, SQLModel, create_engine, text

from app.data_access.m_views import FactAssetMetricsMView

# 3. Application Layers
from app.domain.gold_entities import AssetMetricContext, AssetUtilization
from app.services.search_gold import GoldSearchService


# --- Setup: Isolated Testing Environment ---

@pytest.fixture(name="session")
def session_fixture() -> Generator[Session, Any, None]:
    """
    Creates a clean, in-memory SQLite database for every test.
    Note: SQLite doesn't support schemas (gold.silver), so we simulate the tables
    in the default schema for testing.
    """
    engine = create_engine("sqlite:///:memory:")
    
    # Create the physical tables based on SQLModels
    SQLModel.metadata.create_all(engine)
    
    # Fix for [call-overload]: Use session.execute() for text() clauses
    # We create a simplified version of the Comprehensive View for testing
    view_sql = """
    CREATE VIEW fact_asset_metrics AS
    SELECT * FROM factassetmetricsmview;
    """
    with Session(engine) as session:
        session.execute(text(view_sql))
        session.commit()
    
    with Session(engine) as session:
        yield session


# --- 1. Testing Domain Validation (Business Rules) ---

def test_asset_utilization_valid_data() -> None:
    """Validates that a correctly formed AssetUtilization object is accepted."""
    asset = AssetUtilization(
        asset_id=1,
        full_date=date(2023, 10, 27),
        resource_name="prod-sql-01",
        serial_number="SN-12345",
        provider_name="AWS",
        team_name="Data-Ops",
        center_code="CC-99",
        cpu_usage_avg=45.0,
        memory_usage_avg=60.0,
        daily_cost=15.50
    )
    assert asset.resource_name == "prod-sql-01"
    assert asset.cpu_usage_avg == 45.0

def test_asset_metrics_constraints() -> None:
    """Validates that CPU usage must be between 0 and 100 via Pydantic."""
    with pytest.raises(ValidationError):
        AssetMetricContext(
            id=1, asset_id=10, resource_name="Test", serial_number="S1",
            hardware_spec="m5.xl", provider_name="AWS", region_code="us-east-1",
            team_name="IT", department="Fin", center_code="C1",
            service_name="EC2", service_category="Compute", env_name="Production",
            status_name="Active", security_tier="Critical", full_date=date.today(),
            cpu_usage_avg=150.0,  # INVALID: > 100
            memory_usage_avg=50.0, hourly_cost=0.5, uptime_seconds=3600,
            source_timestamp=datetime.now(), updated_at=datetime.now()
        )

def test_asset_cost_non_negative() -> None:
    """Validates that cost cannot be negative."""
    with pytest.raises(ValidationError):
        AssetUtilization(
            asset_id=1, full_date=date.today(), resource_name="Test",
            serial_number="S1", provider_name="AWS", team_name="IT",
            center_code="C1", cpu_usage_avg=10, memory_usage_avg=10,
            daily_cost=-100.0  # INVALID: negative cost
        )


# --- 2. Testing Service Logic (Search & Filtering) ---

def test_get_comprehensive_asset_metrics_service(session: Session) -> None:
    """
    Tests the GoldSearchService logic by seeding the DB and retrieving data.
    """
    # 1. Seed the database with a mock record
    mock_record = FactAssetMetricsMView(
        id=1, asset_id=101, resource_name="search-target-server",
        serial_number="SN-SEARCH", provider_name="Azure", hardware_spec="Standard_D2",
        region_code="westus", team_name="Analytics", department="Marketing",
        center_code="MKT-01", service_name="VM", service_category="IaaS",
        env_name="Production", status_name="Active", security_tier="Internal",
        full_date=date(2023, 10, 27), cpu_usage_avg=12.5, memory_usage_avg=30.0,
        hourly_cost=0.45, uptime_seconds=3600,
        source_timestamp=datetime.now(), updated_at=datetime.now()
    )
    session.add(mock_record)
    session.commit()

    # 2. Execute service call
    results = GoldSearchService.get_comprehensive_asset_metrics(
        session=session,
        search_query="target",
        env="Production"
    )

    # 3. Assertions
    assert len(results) == 1
    assert results[0].resource_name == "search-target-server"
    assert results[0].team_name == "Analytics"
    # Verify Pydantic conversion worked
    assert isinstance(results[0], AssetMetricContext)
