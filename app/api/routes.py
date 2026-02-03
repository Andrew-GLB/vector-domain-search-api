from enum import Enum
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlmodel import Session

# Security
from app.api.auth import authenticate

# Layer 4: Data Access (Session)
from app.data_access.database import get_session

#from app.data_access.models import FactAssetMetrics
# Layer 3: Domain Entities (Pydantic models)
from app.domain import (
    AssetDomain,
    CostCenterDomain,
    EnvironmentDomain,
    HardwareProfileDomain,
    MetricEntryDomain,
    ProviderDomain,
    RegionDomain,
    SecurityTierDomain,
    ServiceTypeDomain,
    StatusDomain,
    TeamDomain,
)
from app.domain.gold_entities import (
    AssetMetricContext,
    AssetUtilization,
    ResourceEfficiency,
    SecurityCompliance,
    TeamCost,
)

# Layer 2: Services
from app.services.asset_service import AssetService
from app.services.cost_center_service import CostCenterService
from app.services.environment_service import EnvironmentService
from app.services.hardware_profile_service import HardwareProfileService
from app.services.metric_service import MetricService
from app.services.provider_service import ProviderService
from app.services.region_service import RegionService
from app.services.search_gold import GoldSearchService
from app.services.search_service import SearchService
from app.services.security_tier_service import SecurityTierService
from app.services.seed_service import SeedService
from app.services.service_type_service import ServiceTypeService
from app.services.status_service import StatusService
from app.services.team_service import TeamService


router = APIRouter(prefix="/v1")

# --- 1. ADMIN & SEEDING ---
@router.post("/seed", tags=["Admin"])
def seed_database(username: Annotated[str, Depends(authenticate)]) -> dict[str, str]:
    """Requirement: Seeding data at the beginning.
    Triggers the 11-table ETL pipeline and AI enrichment.
    """
    service = SeedService()
    return service.run_seed_process()

# Define your 10 Domain Entities as an Enum
class DomainEntity(str, Enum):
    ASSET = "AssetDomain"
    COST_CENTER = "CostCenterDomain"
    ENVIRONMENT = "EnvironmentDomain"
    HARDWARE_PROFILE = "HardwareProfileDomain"
    PROVIDER = "ProviderDomain"
    REGION = "RegionDomain"
    SECURITY_TIER = "SecurityTierDomain"
    SERVICE_TYPE = "ServiceTypeDomain"
    STATUS = "StatusDomain"
    TEAM = "TeamDomain"

# --- Vector Search ---
@router.post("/search", tags=["Search Domain Entities"])
def search_database(
    # FastAPI will now render this as a dropdown in /docs
    collection_name: DomainEntity,
    q: Annotated[str, Query(min_length=1)],
    username: Annotated[str, Depends(authenticate)]
) -> list[dict[str, Any]]:
    """Requirement: Vector DB Search.
    Select a domain entity from the dropdown to search within that specific collection.
    """
    service = SearchService()
    # collection_name.value gets the string (e.g., "AssetDomain")
    return service.search(collection_name=collection_name.value, query=q)

@router.get("/search/global", tags=["Global Search in All Domain Entities"])
def global_search(
    q: Annotated[str, Query(min_length=1)],
    username: Annotated[str, Depends(authenticate)]
) -> list[dict[str, Any]]:
    """Search across Teams, Assets, Environments, etc. simultaneously.
    Each result includes an 'entity_type' field.
    """
    service = SearchService()
    return service.global_search(query_text=q)


# --- GOLD LAYER ANALYTICS (Querying the Virtual View) ---
@router.get("/comprehensive-metrics", tags=["Gold Layer - Analytics"])
def read_comprehensive_metrics(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)],
    provider_name: Annotated[
        str,
        Query(
            description="Select a category to filter resources. Use '--' to see all rows.",
            enum=["--", "AWS", "AZURE", "GCP", "ON-PREMISE"]
        )
    ] = "--"
) -> list[AssetMetricContext]:
    """Retrieve fully enriched asset metrics.
    This endpoint queries a 10-way join across Assets, Teams, Providers,
    Regions, Environments, and Security tiers.
    """
    # Defensive check for MyPy to ensure session exists
    if session is None:
        raise RuntimeError("Database session not available")
    
    service = GoldSearchService(session)
    return service.read_comprehensive_metrics(provider_name=provider_name)

@router.get("/assets/utilization", tags=["Gold Layer - Analytics"])
def search_assets_utilization(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)],
    provider_name: Annotated[
        str,
        Query(
            description="Select a category to filter resources. Use '--' to see all rows.",
            enum=["--", "AWS", "AZURE", "GCP", "ON-PREMISE"]
        )
    ] = "--"
) -> list[AssetUtilization]:
    """Search asset utilization data."""
    # Note: Added check for session to satisfy MyPy since it's Optional
    service = GoldSearchService(session)
    return service.search_assets_utilization(provider_name=provider_name)

@router.get("/costs/team-report", tags=["Gold Layer - Analytics"])
def get_team_costs(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[TeamCost]:
    """Monthly cloud spend rollup by department or team for chargeback reporting."""
    service = GoldSearchService(session)
    return service.get_team_cost_report()
        

@router.get("/security/compliance", tags=["Gold Layer - Analytics"])
def get_security_posture(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[SecurityCompliance]:
    """Identifies 'Mission Critical' assets in 'Production' that need immediate attention.
    This report identifies assets currently in Stopped, Maintenance, or Terminated states.
    """
    service = GoldSearchService(session)
    return service.search_security_risks()

@router.get("/efficiency/waste-analysis", tags=["Gold Layer - Analytics"])
def get_efficiency(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)],
    waste_category: Annotated[
        str,
        Query(
            description="Select a category to filter resources. Use '--' to see all rows.",
            enum=["--", "High Waste", "Potential Waste", "Optimized", "Normal"]
        )
    ] = "--"
) -> list[ResourceEfficiency]:
    """Retrieves assets with pre-calculated efficiency scores and waste indexing.
    - **--**: Fetches all results without filtering.
    - **High Waste**: Resources with < 5% CPU and > $100 cost.
    """
    service = GoldSearchService(session)
    return service.get_efficiency_metrics(waste_category=waste_category)


# --- ASSET MANAGEMENT (CRUD + BATCH) ---
@router.post("/assets/", tags=["Dimensions - Assets"], status_code=status.HTTP_201_CREATED)
def create_asset(
    data: AssetDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> AssetDomain:
    """Full CRUD: Persists a single cloud infrastructure asset."""
    service = AssetService(session)
    return service.create_asset(data)

@router.post("/assets/batch", tags=["Dimensions - Assets"], status_code=status.HTTP_201_CREATED)
def create_assets_batch(
    data: list[AssetDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[AssetDomain]:
    """Requirement: Batch CRUD. Ingests multiple assets in one transaction."""
    service = AssetService(session)
    return service.create_assets_batch(data)

@router.get("/assets/", tags=["Dimensions - Assets"])
def get_all_assets(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[AssetDomain]:
    """Full CRUD: Retrieves all assets registered in the Silver layer."""
    service = AssetService(session)
    return service.get_all_assets()

@router.get("/assets/{id}", tags=["Dimensions - Assets"])
def get_asset(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> AssetDomain:
    """Full CRUD: Retrieves a single asset by primary key ID."""
    service = AssetService(session)
    db_obj = service.get_asset(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Asset not found")
    return db_obj

@router.put("/assets/{id}", tags=["Dimensions - Assets"])
def update_asset(
    id: int,
    data: AssetDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> AssetDomain:
    """Full CRUD: Updates an existing asset's metadata."""
    service = AssetService(session)
    return service.update_asset(id, data)

@router.put("/assets/batch", tags=["Dimensions - Assets"])
def update_assets_batch(
    data: list[AssetDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[AssetDomain]:
    """Requirement: Batch CRUD. Updates multiple assets using their serial numbers."""
    service = AssetService(session)
    return service.update_assets_batch(data)

@router.delete("/assets/{id}", tags=["Dimensions - Assets"], status_code=status.HTTP_204_NO_CONTENT)
def delete_asset(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes an asset from the database."""
    service = AssetService(session)
    service.delete_asset(id)
    return None

@router.post("/assets/delete/batch", tags=["Dimensions - Assets"], status_code=status.HTTP_204_NO_CONTENT)
def delete_assets_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes assets by primary key IDs."""
    service = AssetService(session)
    service.delete_assets_batch(ids)
    return None

# --- Cost Center ---
@router.post("/cost-centers/", tags=["Dimensions - Cost Center"], status_code=status.HTTP_201_CREATED)
def create_cost_center(
    data: CostCenterDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> CostCenterDomain:
    """Full CRUD: Persists a single financial cost center."""
    service = CostCenterService(session)
    return service.create_cost_center(data)

@router.post("/cost-centers/batch", tags=["Dimensions - Cost Center"], status_code=status.HTTP_201_CREATED)
def create_cost_centers_batch(
    data: list[CostCenterDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[CostCenterDomain]:
    """Requirement: Batch CRUD. Ingests multiple cost centers in one transaction."""
    service = CostCenterService(session)
    return service.create_cost_centers_batch(data)

@router.get("/cost-centers/", tags=["Dimensions - Cost Center"])
def get_all_cost_centers(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[CostCenterDomain]:
    """Full CRUD: Retrieves all cost centers registered in the Silver layer."""
    service = CostCenterService(session)
    return service.get_all_cost_centers()

@router.get("/cost-centers/{id}", tags=["Dimensions - Cost Center"])
def get_cost_center(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> CostCenterDomain:
    """Full CRUD: Retrieves a single cost center by primary key ID."""
    service = CostCenterService(session)
    db_obj = service.get_cost_center(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Cost Center not found")
    return db_obj

@router.put("/cost-centers/{id}", tags=["Dimensions - Cost Center"])
def update_cost_center(
    id: int,
    data: CostCenterDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> CostCenterDomain:
    """Full CRUD: Updates an existing cost center's budget or department."""
    service = CostCenterService(session)
    return service.update_cost_center(id, data)

@router.put("/cost-centers/batch", tags=["Dimensions - Cost Center"])
def update_cost_centers_batch(
    data: list[CostCenterDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[CostCenterDomain]:
    """Requirement: Batch CRUD. Updates multiple centers using their center_code."""
    service = CostCenterService(session)
    return service.update_cost_centers_batch(data)

@router.delete("/cost-centers/{id}", tags=["Dimensions - Cost Center"], status_code=status.HTTP_204_NO_CONTENT)
def delete_cost_center(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a cost center from the database."""
    service = CostCenterService(session)
    service.delete_cost_center(id)
    return None

@router.post("/cost-centers/delete/batch", tags=["Dimensions - Cost Center"], status_code=status.HTTP_204_NO_CONTENT)
def delete_cost_centers_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes cost centers by primary key IDs."""
    service = CostCenterService(session)
    service.delete_cost_centers_batch(ids)
    return None

# --- Environment ---
@router.post("/environments/", tags=["Dimensions - Environment"], status_code=status.HTTP_201_CREATED)
def create_environment(
    data: EnvironmentDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> EnvironmentDomain:
    """Full CRUD: Persists a single deployment environment."""
    service = EnvironmentService(session)
    return service.create_environment(data)

@router.post("/environments/batch", tags=["Dimensions - Environment"], status_code=status.HTTP_201_CREATED)
def create_environments_batch(
    data: list[EnvironmentDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[EnvironmentDomain]:
    """Requirement: Batch CRUD. Ingests multiple environments in one transaction."""
    service = EnvironmentService(session)
    return service.create_environments_batch(data)

@router.get("/environments/", tags=["Dimensions - Environment"])
def get_all_environments(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[EnvironmentDomain]:
    """Full CRUD: Retrieves all registered environments."""
    service = EnvironmentService(session)
    return service.get_all_environments()

@router.get("/environments/{id}", tags=["Dimensions - Environment"])
def get_environment(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> EnvironmentDomain:
    """Full CRUD: Retrieves a single environment by primary key ID."""
    service = EnvironmentService(session)
    db_obj = service.get_environment(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Environment not found")
    return db_obj

@router.put("/environments/{id}", tags=["Dimensions - Environment"])
def update_environment(
    id: int,
    data: EnvironmentDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> EnvironmentDomain:
    """Full CRUD: Updates an existing environment record."""
    service = EnvironmentService(session)
    return service.update_environment(id, data)

@router.put("/environments/batch", tags=["Dimensions - Environment"])
def update_environments_batch(
    data: list[EnvironmentDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[EnvironmentDomain]:
    """Requirement: Batch CRUD. Updates multiple environments using their env_name."""
    service = EnvironmentService(session)
    return service.update_environments_batch(data)

@router.delete("/environments/{id}", tags=["Dimensions - Environment"], status_code=status.HTTP_204_NO_CONTENT)
def delete_environment(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single environment from the system."""
    service = EnvironmentService(session)
    service.delete_environment(id)
    return None

@router.post("/environments/delete/batch", tags=["Dimensions - Environment"], status_code=status.HTTP_204_NO_CONTENT)
def delete_environments_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes environments by primary key IDs."""
    service = EnvironmentService(session)
    service.delete_environments_batch(ids)
    return None

# --- Hardware Profile ---
@router.post("/hardware-profiles/", tags=["Dimensions - Hardware Profile"], status_code=status.HTTP_201_CREATED)
def create_hardware_profile(
    data: HardwareProfileDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> HardwareProfileDomain:
    """Full CRUD: Persists a single hardware profile specification."""
    service = HardwareProfileService(session)
    return service.create_hardware_profile(data)

@router.post("/hardware-profiles/batch", tags=["Dimensions - Hardware Profile"], status_code=status.HTTP_201_CREATED)
def create_hardware_profiles_batch(
    data: list[HardwareProfileDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[HardwareProfileDomain]:
    """Requirement: Batch CRUD. Ingests multiple hardware profiles in one transaction."""
    service = HardwareProfileService(session)
    return service.create_hardware_profiles_batch(data)

@router.get("/hardware-profiles/", tags=["Dimensions - Hardware Profile"])
def get_all_hardware_profiles(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[HardwareProfileDomain]:
    """Full CRUD: Retrieves all registered hardware profiles."""
    service = HardwareProfileService(session)
    return service.get_all_profiles()

@router.get("/hardware-profiles/{id}", tags=["Dimensions - Hardware Profile"])
def get_hardware_profile(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> HardwareProfileDomain:
    """Full CRUD: Retrieves a single hardware profile by primary key ID."""
    service = HardwareProfileService(session)
    db_obj = service.get_hardware_profile(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Hardware Profile not found")
    return db_obj

@router.put("/hardware-profiles/{id}", tags=["Dimensions - Hardware Profile"])
def update_hardware_profile(
    id: int,
    data: HardwareProfileDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> HardwareProfileDomain:
    """Full CRUD: Updates an existing hardware profile record."""
    service = HardwareProfileService(session)
    return service.update_hardware_profile(id, data)

@router.put("/hardware-profiles/batch", tags=["Dimensions - Hardware Profile"])
def update_hardware_profiles_batch(
    data: list[HardwareProfileDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[HardwareProfileDomain]:
    """Requirement: Batch CRUD. Updates multiple hardware profiles using their profile_name."""
    service = HardwareProfileService(session)
    return service.update_hardware_profiles_batch(data)

@router.delete("/hardware-profiles/{id}", tags=["Dimensions - Hardware Profile"], status_code=status.HTTP_204_NO_CONTENT)
def delete_hardware_profile(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single hardware profile from the system."""
    service = HardwareProfileService(session)
    service.delete_hardware_profile(id)
    return None

@router.post("/hardware-profiles/delete/batch", tags=["Dimensions - Hardware Profile"], status_code=status.HTTP_204_NO_CONTENT)
def delete_hardware_profiles_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes hardware profiles by primary key IDs."""
    service = HardwareProfileService(session)
    service.delete_hardware_profiles_batch(ids)
    return None

# --- Provider ---
@router.post("/providers/", tags=["Dimensions - Provider"], status_code=status.HTTP_201_CREATED)
def create_provider(
    data: ProviderDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> ProviderDomain:
    """Full CRUD: Persists a single cloud infrastructure provider."""
    service = ProviderService(session)
    return service.create_provider(data)

@router.post("/providers/batch", tags=["Dimensions - Provider"], status_code=status.HTTP_201_CREATED)
def create_providers_batch(
    data: list[ProviderDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[ProviderDomain]:
    """Requirement: Batch CRUD. Ingests multiple providers in one transaction."""
    service = ProviderService(session)
    return service.create_providers_batch(data)

@router.get("/providers/", tags=["Dimensions - Provider"])
def get_all_providers(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[ProviderDomain]:
    """Full CRUD: Retrieves all registered cloud providers."""
    service = ProviderService(session)
    return service.get_all_providers()

@router.get("/providers/{id}", tags=["Dimensions - Provider"])
def get_provider(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> ProviderDomain:
    """Full CRUD: Retrieves a single provider by primary key ID."""
    service = ProviderService(session)
    db_obj = service.get_provider(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Provider not found")
    return db_obj

@router.put("/providers/{id}", tags=["Dimensions - Provider"])
def update_provider(
    id: int,
    data: ProviderDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> ProviderDomain:
    """Full CRUD: Updates an existing cloud provider record."""
    service = ProviderService(session)
    return service.update_provider(id, data)

@router.put("/providers/batch", tags=["Dimensions - Provider"])
def update_providers_batch(
    data: list[ProviderDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[ProviderDomain]:
    """Requirement: Batch CRUD. Updates multiple providers using their provider_name."""
    service = ProviderService(session)
    return service.update_providers_batch(data)

@router.delete("/providers/{id}", tags=["Dimensions - Provider"], status_code=status.HTTP_204_NO_CONTENT)
def delete_provider(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single cloud provider from the system."""
    service = ProviderService(session)
    service.delete_provider(id)
    return None

@router.post("/providers/delete/batch", tags=["Dimensions - Provider"], status_code=status.HTTP_204_NO_CONTENT)
def delete_providers_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes cloud providers by primary key IDs."""
    service = ProviderService(session)
    service.delete_providers_batch(ids)
    return None

# --- Region ---
@router.post("/regions/", tags=["Dimensions - Region"], status_code=status.HTTP_201_CREATED)
def create_region(
    data: RegionDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> RegionDomain:
    """Full CRUD: Persists a single geographic or logical cloud region."""
    service = RegionService(session)
    return service.create_region(data)

@router.post("/regions/batch", tags=["Dimensions - Region"], status_code=status.HTTP_201_CREATED)
def create_regions_batch(
    data: list[RegionDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[RegionDomain]:
    """Requirement: Batch CRUD. Ingests multiple regions in one transaction."""
    service = RegionService(session)
    return service.create_regions_batch(data)

@router.get("/regions/", tags=["Dimensions - Region"])
def get_all_regions(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[RegionDomain]:
    """Full CRUD: Retrieves all registered cloud regions."""
    service = RegionService(session)
    return service.get_all_regions()

@router.get("/regions/{id}", tags=["Dimensions - Region"])
def get_region(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> RegionDomain:
    """Full CRUD: Retrieves a single region by primary key ID."""
    service = RegionService(session)
    db_obj = service.get_region(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Region not found")
    return db_obj

@router.put("/regions/{id}", tags=["Dimensions - Region"])
def update_region(
    id: int,
    data: RegionDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> RegionDomain:
    """Full CRUD: Updates an existing region record."""
    service = RegionService(session)
    return service.update_region(id, data)

@router.put("/regions/batch", tags=["Dimensions - Region"])
def update_regions_batch(
    data: list[RegionDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[RegionDomain]:
    """Requirement: Batch CRUD. Updates multiple regions using their region_code."""
    service = RegionService(session)
    return service.update_regions_batch(data)

@router.delete("/regions/{id}", tags=["Dimensions - Region"], status_code=status.HTTP_204_NO_CONTENT)
def delete_region(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single region from the system."""
    service = RegionService(session)
    service.delete_region(id)
    return None

@router.post("/regions/delete/batch", tags=["Dimensions - Region"], status_code=status.HTTP_204_NO_CONTENT)
def delete_regions_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes regions by primary key IDs."""
    service = RegionService(session)
    service.delete_regions_batch(ids)
    return None

# --- Security Tier ---
@router.post("/security-tiers/", tags=["Dimensions - Security Tier"], status_code=status.HTTP_201_CREATED)
def create_security_tier(
    data: SecurityTierDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> SecurityTierDomain:
    """Full CRUD: Persists a single security and compliance tier."""
    service = SecurityTierService(session)
    return service.create_security_tier(data)

@router.post("/security-tiers/batch", tags=["Dimensions - Security Tier"], status_code=status.HTTP_201_CREATED)
def create_security_tiers_batch(
    data: list[SecurityTierDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[SecurityTierDomain]:
    """Requirement: Batch CRUD. Ingests multiple security tiers in one transaction."""
    service = SecurityTierService(session)
    return service.create_security_tiers_batch(data)

@router.get("/security-tiers/", tags=["Dimensions - Security Tier"])
def get_all_security_tiers(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[SecurityTierDomain]:
    """Full CRUD: Retrieves all registered security tiers."""
    service = SecurityTierService(session)
    return service.get_all_security_tiers()

@router.get("/security-tiers/{id}", tags=["Dimensions - Security Tier"])
def get_security_tier(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> SecurityTierDomain:
    """Full CRUD: Retrieves a single security tier by primary key ID."""
    service = SecurityTierService(session)
    db_obj = service.get_security_tier(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Security Tier not found")
    return db_obj

@router.put("/security-tiers/{id}", tags=["Dimensions - Security Tier"])
def update_security_tier(
    id: int,
    data: SecurityTierDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> SecurityTierDomain:
    """Full CRUD: Updates an existing security tier record."""
    service = SecurityTierService(session)
    return service.update_security_tier(id, data)

@router.put("/security-tiers/batch", tags=["Dimensions - Security Tier"])
def update_security_tiers_batch(
    data: list[SecurityTierDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[SecurityTierDomain]:
    """Requirement: Batch CRUD. Updates multiple security tiers using their tier_name."""
    service = SecurityTierService(session)
    return service.update_security_tiers_batch(data)

@router.delete("/security-tiers/{id}", tags=["Dimensions - Security Tier"], status_code=status.HTTP_204_NO_CONTENT)
def delete_security_tier(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single security tier from the system."""
    service = SecurityTierService(session)
    service.delete_security_tier(id)
    return None

@router.post("/security-tiers/delete/batch", tags=["Dimensions - Security Tier"], status_code=status.HTTP_204_NO_CONTENT)
def delete_security_tiers_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes security tiers by primary key IDs."""
    service = SecurityTierService(session)
    service.delete_security_tiers_batch(ids)
    return None

# --- ServiceType ---
@router.post("/service-types/", tags=["Dimensions - Service Type"], status_code=status.HTTP_201_CREATED)
def create_service_type(
    data: ServiceTypeDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> ServiceTypeDomain:
    """Full CRUD: Persists a single technical service classification."""
    service = ServiceTypeService(session)
    return service.create_service_type(data)

@router.post("/service-types/batch", tags=["Dimensions - Service Type"], status_code=status.HTTP_201_CREATED)
def create_service_types_batch(
    data: list[ServiceTypeDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[ServiceTypeDomain]:
    """Requirement: Batch CRUD. Ingests multiple service types in one transaction."""
    service = ServiceTypeService(session)
    return service.create_service_types_batch(data)

@router.get("/service-types/", tags=["Dimensions - Service Type"])
def get_all_service_types(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[ServiceTypeDomain]:
    """Full CRUD: Retrieves the catalog of all service classifications."""
    service = ServiceTypeService(session)
    return service.get_all_service_types()

@router.get("/service-types/{id}", tags=["Dimensions - Service Type"])
def get_service_type(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> ServiceTypeDomain:
    """Full CRUD: Retrieves a single service type by primary key ID."""
    service = ServiceTypeService(session)
    db_obj = service.get_service_type(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="ServiceType not found")
    return db_obj

@router.put("/service-types/{id}", tags=["Dimensions - Service Type"])
def update_service_type(
    id: int,
    data: ServiceTypeDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> ServiceTypeDomain:
    """Full CRUD: Updates an existing service classification record."""
    service = ServiceTypeService(session)
    return service.update_service_type(id, data)

@router.put("/service-types/batch", tags=["Dimensions - Service Type"])
def update_service_types_batch(
    data: list[ServiceTypeDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[ServiceTypeDomain]:
    """Requirement: Batch CRUD. Updates multiple service types using their service_name."""
    service = ServiceTypeService(session)
    return service.update_service_types_batch(data)

@router.delete("/service-types/{id}", tags=["Dimensions - Service Type"], status_code=status.HTTP_204_NO_CONTENT)
def delete_service_type(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single service type from the system."""
    service = ServiceTypeService(session)
    service.delete_service_type(id)
    return None

@router.post("/service-types/delete/batch", tags=["Dimensions - Service Type"], status_code=status.HTTP_204_NO_CONTENT)
def delete_service_types_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes service types by primary key IDs."""
    service = ServiceTypeService(session)
    service.delete_service_types_batch(ids)
    return None

# --- Status ---
def create_status(
    data: StatusDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> StatusDomain:
    """Creates a new status entry in the dimension table.

    Args:
        data: The status data to create.
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        StatusDomain: The created status record.
    """
    service = StatusService(session)
    return service.create_status(data)

@router.post("/statuses/batch", tags=["Dimensions - Status"], status_code=status.HTTP_201_CREATED)
def create_statuses_batch(
    data: list[StatusDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[StatusDomain]:
    """Creates multiple status entries in a single batch operation.

    Args:
        data: A list of status objects to create.
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        list[StatusDomain]: The list of created status records.
    """
    service = StatusService(session)
    return service.create_statuses_batch(data)

@router.get("/statuses/", tags=["Dimensions - Status"])
def get_all_statuses(session: Annotated[Session, Depends(get_session)], username: Annotated[str, Depends(authenticate)]) -> list[StatusDomain]:
    """Retrieves all status entries from the dimension table.

    Args:
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        list[StatusDomain]: A list of all status records.
    """
    service = StatusService(session)
    return service.get_all_statuses()

@router.get("/statuses/{id}", tags=["Dimensions - Status"])
def get_status(id: int, session: Annotated[Session, Depends(get_session)], username: Annotated[str, Depends(authenticate)]) -> StatusDomain:
    """Retrieves a specific status entry by its unique identifier.

    Args:
        id: The ID of the status to retrieve.
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        StatusDomain: The requested status record.

    Raises:
        HTTPException: If the status ID does not exist in the database.
    """
    service = StatusService(session)
    db_obj = service.get_status(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Status not found")
    return db_obj

@router.put("/statuses/{id}", tags=["Dimensions - Status"])
def update_status(
    id: int,
    data: StatusDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> StatusDomain:
    """Updates an existing status entry by its ID.

    Args:
        id: The ID of the status to update.
        data: The updated status data.
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        StatusDomain: The updated status record.
    """
    service = StatusService(session)
    return service.update_status(id, data)

@router.put("/statuses/batch", tags=["Dimensions - Status"])
def update_statuses_batch(
    data: list[StatusDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[StatusDomain]:
    """Updates multiple status entries in a single batch operation.

    Args:
        data: A list of status objects with updated values.
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        list[StatusDomain]: The list of updated status records.
    """
    service = StatusService(session)
    return service.update_statuses_batch(data)

@router.delete("/statuses/{id}", tags=["Dimensions - Status"], status_code=status.HTTP_204_NO_CONTENT)
def delete_status(id: int, session: Annotated[Session, Depends(get_session)], username: Annotated[str, Depends(authenticate)]) -> None:
    """Removes a specific status entry from the dimension table.

    Args:
        id: The ID of the status to delete.
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        None
    """
    service = StatusService(session)
    service.delete_status(id)
    return None

@router.post("/statuses/delete/batch", tags=["Dimensions - Status"], status_code=status.HTTP_204_NO_CONTENT)
def delete_statuses_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Removes multiple status entries identified by a list of IDs.

    Args:
        ids: A list of status IDs to delete.
        session: The database session dependency.
        username: The authenticated user's username.

    Returns:
        None
    """
    service = StatusService(session)
    service.delete_statuses_batch(ids)
    return None

# --- Team ---
@router.post("/teams/", tags=["Dimensions - Team"], status_code=status.HTTP_201_CREATED)
def create_team(
    data: TeamDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> TeamDomain:
    """Full CRUD: Persists a single organizational team."""
    service = TeamService(session)
    return service.create_team(data)

@router.post("/teams/batch", tags=["Dimensions - Team"], status_code=status.HTTP_201_CREATED)
def create_teams_batch(
    data: list[TeamDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[TeamDomain]:
    """Requirement: Batch CRUD. Ingests multiple teams in one transaction."""
    service = TeamService(session)
    return service.create_teams_batch(data)

@router.get("/teams/", tags=["Dimensions - Team"])
def get_all_teams(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[TeamDomain]:
    """Full CRUD: Retrieves all registered organizational teams."""
    service = TeamService(session)
    return service.get_all_teams()

@router.get("/teams/{id}", tags=["Dimensions - Team"])
def get_team(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> TeamDomain:
    """Full CRUD: Retrieves a single team by primary key ID."""
    service = TeamService(session)
    db_obj = service.get_team(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Team not found")
    return db_obj

@router.put("/teams/{id}", tags=["Dimensions - Team"])
def update_team(
    id: int,
    data: TeamDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> TeamDomain:
    """Full CRUD: Updates an existing team record."""
    service = TeamService(session)
    return service.update_team(id, data)

@router.put("/teams/batch", tags=["Dimensions - Team"])
def update_teams_batch(
    data: list[TeamDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[TeamDomain]:
    """Requirement: Batch CRUD. Updates multiple teams using their team_name."""
    service = TeamService(session)
    return service.update_teams_batch(data)

@router.delete("/teams/{id}", tags=["Dimensions - Team"], status_code=status.HTTP_204_NO_CONTENT)
def delete_team(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single team from the system."""
    service = TeamService(session)
    service.delete_team(id)
    return None

@router.post("/teams/delete/batch", tags=["Dimensions - Team"], status_code=status.HTTP_204_NO_CONTENT)
def delete_teams_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes teams by primary key IDs."""
    service = TeamService(session)
    service.delete_teams_batch(ids)
    return None

# --- Metrics ---
@router.post("/metrics/", tags=["Metrics"], status_code=status.HTTP_201_CREATED)
def ingest_metric(
    data: MetricEntryDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> MetricEntryDomain:
    """Full CRUD: Ingests a single performance metric snapshot."""
    service = MetricService(session)
    return service.ingest_metric(data)

@router.post("/metrics/batch", tags=["Metrics"], status_code=status.HTTP_201_CREATED)
def ingest_metrics_batch(
    data: list[MetricEntryDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[MetricEntryDomain]:
    """Requirement: Batch CRUD. Ingests multiple metric snapshots in one transaction."""
    service = MetricService(session)
    return service.ingest_metrics_batch(data)

@router.get("/metrics/", tags=["Metrics"])
def get_all_metrics(
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[MetricEntryDomain]:
    """Full CRUD: Retrieves all metric entries from the Silver layer."""
    service = MetricService(session)
    return service.get_all_silver_metrics()

@router.get("/metrics/{id}", tags=["Metrics"])
def get_metric(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> MetricEntryDomain:
    """Full CRUD: Retrieves a single metric entry by primary key ID."""
    service = MetricService(session)
    db_obj = service.get_metric(id)
    if not db_obj:
        raise HTTPException(status_code=404, detail="Metric entry not found")
    return db_obj

@router.put("/metrics/{id}", tags=["Metrics"])
def update_metric(
    id: int,
    data: MetricEntryDomain,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> MetricEntryDomain:
    """Full CRUD: Updates an existing metric entry record."""
    service = MetricService(session)
    return service.update_metric(id, data)

@router.put("/metrics/batch", tags=["Metrics"])
def update_metrics_batch(
    data: list[MetricEntryDomain],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> list[MetricEntryDomain]:
    """Requirement: Batch CRUD. Updates multiple metric entries in one transaction."""
    service = MetricService(session)
    return service.update_metrics_batch(data)

@router.delete("/metrics/{id}", tags=["Metrics"], status_code=status.HTTP_204_NO_CONTENT)
def delete_metric(
    id: int,
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Full CRUD: Removes a single metric entry from the system."""
    service = MetricService(session)
    service.delete_metric(id)
    return None

@router.post("/metrics/delete/batch", tags=["Metrics"], status_code=status.HTTP_204_NO_CONTENT)
def delete_metrics_batch(
    ids: list[int],
    session: Annotated[Session, Depends(get_session)],
    username: Annotated[str, Depends(authenticate)]
) -> None:
    """Requirement: Batch CRUD. Bulk deletes metric entries by primary key IDs."""
    service = MetricService(session)
    service.delete_metrics_batch(ids)
    return None


