import os
import logging
import typesense
from typesense.exceptions import ObjectAlreadyExists, TypesenseClientError
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class SearchService:
    def __init__(self):
        self.client = typesense.Client({
            'nodes': [{
                'host': os.getenv('TYPESENSE_HOST', 'localhost'),
                'port': os.getenv('TYPESENSE_PORT', '8108'),
                'protocol': os.getenv('TYPESENSE_PROTOCOL', 'http')
            }],
            'api_key': os.getenv('TYPESENSE_API_KEY', 'xyz123'),
            'connection_timeout_seconds': 2
        })

    def create_collection_if_not_exists(self, collection_name: str, fields: List[Dict[str, Any]]):
        """Creates a collection schema. Doesn't block if already exists."""
        schema = {
            'name': collection_name,
            'fields': fields
        }
        try:
            self.client.collections.create(schema)
            logger.info(f"Collection '{collection_name}' created.")
        except ObjectAlreadyExists:
            pass
        except Exception as e:
            logger.warning(f"Could not create collection {collection_name}: {e}")

    def index_entity(self, collection_name: str, document: Dict[str, Any]):
        """Indexes data. If Typesense is lagging, it logs a warning but doesn't crash."""
        try:
            self.client.collections[collection_name].documents.upsert(document)
        except Exception as e:
            logger.warning(f"Failed to index into {collection_name}: {e}")

    def search(self, collection_name: str, query: str, filter_by: str = "") -> List[Dict[str, Any]]:
        """Standard search helper."""
        search_parameters = {
            'q': query,
            'query_by': 'name,description' if collection_name == 'products' else 'name',
            'filter_by': filter_by
        }
        try:
            result = self.client.collections[collection_name].documents.search(search_parameters)
            return [hit['document'] for hit in result['hits']]
        except Exception:
            return []