import logging
import os
from typing import Any, cast

# Import Client directly to resolve the Mypy [attr-defined] error
from typesense.client import Client
from typesense.exceptions import ObjectAlreadyExists


logger = logging.getLogger(__name__)

class SearchService:
    def __init__(self) -> None:
        """Initializes the Typesense client using environment variables strictly.
        No fallbacks are provided; environment must contain all required keys.
        """
        # Strictly load values from environment
        ts_host = os.environ['TYPESENSE_HOST']
        ts_port = int(os.environ['TYPESENSE_PORT'])
        ts_protocol = os.environ['TYPESENSE_PROTOCOL']
        ts_api_key = os.environ['TYPESENSE_API_KEY']
        ts_timeout = int(os.environ['TYPESENSE_TIMEOUT'])

        self.client = Client({
            'nodes': [{
                'host': ts_host,
                'port': ts_port,
                'protocol': ts_protocol
            }],
            'api_key': ts_api_key,
            'connection_timeout_seconds': ts_timeout
        })

    def get_schema(self, collection_name: str) -> list[dict[str, Any]]:
        """Defines explicit schemas for the 10 Dimensions."""
        schemas = {
            "AssetDomain": [
                # Explicit ID for cross-referencing with Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "resource_name", "type": "string"},
                {"name": "serial_number", "type": "string"},
                {"name": "description", "type": "string"},
                # Dates in Typesense are often best stored as Unix timestamps for sorting
                {"name": "created_at", "type": "int64", "facet": True},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "CostCenterDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "center_code", "type": "string", "facet": True},
                {"name": "department", "type": "string", "facet": True},
                {"name": "budget_limit", "type": "float"}, # Corrected to float for decimal precision

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "EnvironmentDomain": [
                # IDs should be strings in Typesense for easier lookups
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "env_name", "type": "string", "facet": True},
                {"name": "tier", "type": "string", "facet": True},
                {"name": "is_ephemeral", "type": "bool", "facet": True},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                # Use int64 for timestamps in Typesense for faster sorting
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "HardwareProfileDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "profile_name", "type": "string"},
                {"name": "cpu_count", "type": "int32", "facet": True},
                {"name": "ram_gb", "type": "int32", "facet": True},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "ProviderDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "provider_name", "type": "string", "facet": True},
                {"name": "provider_type", "type": "string", "facet": True},
                {"name": "support_contact", "type": "string"},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "RegionDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "region_code", "type": "string", "facet": True},
                {"name": "display_name", "type": "string"},
                {"name": "continent", "type": "string", "facet": True},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "SecurityTierDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "tier_name", "type": "string", "facet": True},
                {"name": "encryption_required", "type": "bool", "facet": True},
                {"name": "compliance_standard", "type": "string", "facet": True},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "ServiceTypeDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "service_name", "type": "string", "facet": True},
                {"name": "category", "type": "string", "facet": True},
                {"name": "is_managed", "type": "bool", "facet": True},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "StatusDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "status_name", "type": "string", "facet": True},
                {"name": "is_billable", "type": "bool", "facet": True},
                {"name": "description", "type": "string"},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ],
            "TeamDomain": [
                # Explicit ID from Supabase
                {"name": "id", "type": "string"},

                # Core Data
                {"name": "team_name", "type": "string", "facet": True},
                {"name": "department", "type": "string", "facet": True},
                {"name": "lead_email", "type": "string"},

                # Pipeline Metadata
                {"name": "is_active", "type": "bool", "facet": True},
                {"name": "source_timestamp", "type": "int64"},
                {"name": "updated_at", "type": "int64"}
            ]
        }
        # Use cast to satisfy Mypy that this is indeed a list of dictionaries
        return cast(list[dict[str, Any]], schemas.get(collection_name, [{"name": ".*", "type": "auto"}]))

    def create_collection_if_not_exists(self, collection_name: str) -> None:
        """Creates collection using the predefined schema."""
        schema = {
            'name': collection_name,
            'fields': self.get_schema(collection_name)
        }
        try:
            self.client.collections.create(cast(Any, schema))
            logger.info(f"✅ Collection '{collection_name}' created.")
        except ObjectAlreadyExists:
            pass
        except Exception as e:
            logger.warning(f"Could not create collection {collection_name}: {e}")

    def index_asset(self, collection_name: str, document: dict[str, Any]) -> None:
        """Upserts data. Ensures collection exists before indexing."""
        try:
            # Re-create if missing (safety for the wipe-and-seed process)
            self.create_collection_if_not_exists(collection_name)
            self.client.collections[collection_name].documents.upsert(document)
        except Exception as e:
            logger.warning(f"⚠️ Indexing error in {collection_name}: {e}")

    def search(self, collection_name: str, query: str, filter_by: str = "") -> list[dict[str, Any]]:
        """Ensure the 10 searchable Domain Entities are mapped to their fields
        Search across the defined fields.
        """
        field_mapping = {
            "AssetDomain": "resource_name,serial_number,description",
            "CostCenterDomain": "center_code,department",
            "EnvironmentDomain": "env_name,tier",
            "HardwareProfileDomain": "profile_name",
            "ProviderDomain": "provider_name,provider_type,support_contact",
            "RegionDomain": "region_code,display_name,continent",
            "SecurityTierDomain": "tier_name,compliance_standard",
            "ServiceTypeDomain": "service_name,category",
            "StatusDomain": "status_name,description",
            "TeamDomain": "team_name,department,lead_email"
        }

        query_by = field_mapping.get(collection_name, "id")

        search_parameters = {
            'q': query,
            'query_by': query_by,
            'filter_by': filter_by,
            'prioritize_exact_match': True  # Good for codes like 'CC-1234'
        }

        try:
            result = self.client.collections[collection_name].documents.search(cast(Any, search_parameters))
            return cast(list[dict[str, Any]], [hit['document'] for hit in result['hits']])
        except Exception as e:
            logger.error(f"❌ Search failed for collection '{collection_name}': {e}")
            return []

    def global_search(self, query_text: str) -> list[dict[str, Any]]:
        """Searches all 10 dimension collections in a single network request.
        Identifies the source domain for each result.
        """
        collections_to_search = [
            "AssetDomain", "CostCenterDomain", "EnvironmentDomain",
            "HardwareProfileDomain", "ProviderDomain", "RegionDomain",
            "SecurityTierDomain", "ServiceTypeDomain", "StatusDomain",
            "TeamDomain"
        ]

        # Field mapping: Collection -> comma-separated fields to search
        FIELD_MAPPING = {
            "AssetDomain": "resource_name,description,serial_number",
            "CostCenterDomain": "center_code,description",
            "EnvironmentDomain": "env_name,tier",
            "HardwareProfileDomain": "profile_name,processor_type",
            "ProviderDomain": "provider_name",
            "RegionDomain": "region_code,region_name",
            "SecurityTierDomain": "tier_name",
            "ServiceTypeDomain": "service_name",
            "StatusDomain": "status_name",
            "TeamDomain": "team_name,lead_name"
        }

        # Use the mapping to generate collection-specific search requests
        search_requests: Any = {
            "searches": [
                {
                    "collection": col,
                    "q": query_text,
                    # 🔥 DYNAMIC QUERY_BY START
                    "query_by": FIELD_MAPPING.get(col, "id"),
                    # 🔥 DYNAMIC QUERY_BY END
                    "prefix": True,
                    "typo_tokens_threshold": 2
                } for col in collections_to_search
            ]
        }

        # Execute
        multi_results = cast(dict[str, Any], self.client.multi_search.perform(search_requests, {}))

        final_hits = []

        # 2. Use enumerate to match the result to the original collection list
        # The order in multi_results['results'] matches 'collections_to_search'
        for i, result in enumerate(multi_results.get('results', [])):

            # Identify the collection using the index
            source_collection = collections_to_search[i]

            for hit in result.get('hits', []):
                doc = hit.get('document', {})
                # 🏷️ Now this will correctly return the domain entity
                doc['domain_entity'] = source_collection
                final_hits.append(doc)

        return final_hits
