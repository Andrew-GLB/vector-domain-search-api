import logging
from pathlib import Path
from typing import Dict, List, Any
import polars as pl
from sqlmodel import Session, select

# Layer 4: Data Access
from app.data_access.database import engine
from app.data_access.models import (
    DimCategory, DimProduct, DimDate, DimStore, 
    DimSalesPerson, DimOrder, DimCustomer
)

# Layer 2: Services & ETL
from app.etl.pipeline import DataExtractor, DataTransformer, DataLoader, DateDimensionGenerator
from app.services.search_service import SearchService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SeedService:
    """
    Orchestrator for the Data Warehouse seeding process and Medallion Architecture.

    This service coordinates the extraction of raw data from the Bronze layer (CSVs/PDFs),
    transformation into the Silver layer (Dimensions), and the population of the 
    SQL Warehouse. It also handles the synchronization of domain entities into 
    the Typesense Vector DB for fast search capabilities.
    """

    def __init__(self):
        """
        Initializes the SeedService with local paths and search service client.
        """
        self.bronze_path = Path("data/bronze")
        self.search_service = SearchService()

    def run_seed_process(self) -> Dict[str, str]:
        """
        Executes the full ETL seeding pipeline in a non-blocking sequence.

        Order of operations:
        1. Pre-flight file verification.
        2. SQL Seeding: Basic Dimensions -> Products -> Orders (The Fact Source).
        3. Vector DB Seeding: Indexing all major domain entities.
        4. Unstructured Data: Processing and indexing PDFs.

        Returns:
            Dict[str, str]: A status report of the seeding operation.
        """
        try:
            logger.info("Starting Refined Medallion Seed Process (View-based Fact Layer)...")
            
            # 1. PRE-FLIGHT (Verify CSVs exist)
            self._verify_source_files()

            # 2. SQL SEEDING - LEVEL 1 (Independent)
            logger.info("SQL Seeding Phase 1: Basic Dimensions...")
            self._seed_basic_dimensions()
            self._seed_sales_people() 
            self._seed_calendar()

            # 3. SQL SEEDING - LEVEL 2 (Dependent)
            logger.info("SQL Seeding Phase 2: Products...")
            resolved_products = self._seed_products_sql()

            # 4. SQL SEEDING - LEVEL 3 (Transactions / View Source)
            # This replaces the old FactSales seeding. Seeding DimOrder now populates the View.
            logger.info("SQL Seeding Phase 3: Orders (Source of Virtual Fact View)...")
            self._seed_orders()

            # 5. VECTOR DB SEEDING
            logger.info("Vector DB Seeding Phase: Indexing Domain Entities...")
            self._index_all_domains_in_vector_db(resolved_products)

            # 6. UNSTRUCTURED DATA
            logger.info("Unstructured Seeding Phase: Processing PDFs...")
            self._process_unstructured_pdfs()

            return {
                "status": "success", 
                "message": "SQL Warehouse populated. Fact View is live. Vector DB updated."
            }

        except Exception as e:
            logger.error(f"Seed process failed: {e}")
            return {"status": "error", "message": str(e)}

    def _verify_source_files(self):
        """
        Ensures all required Bronze CSV files are present in the data directory.

        Raises:
            FileNotFoundError: If a mandatory CSV file is missing.
        """
        required = [
            "categories.csv", "stores.csv", "sales_people.csv", 
            "orders.csv", "products.csv", "customers.csv"
        ]
        for f in required:
            if not (self.bronze_path / f).exists():
                raise FileNotFoundError(f"Missing mandatory Bronze file: {f}")

    def _seed_basic_dimensions(self):
        """
        Seeds static dimensions: Categories, Stores, and Customers.
        """
        mappings = {
            "categories.csv": DimCategory, 
            "stores.csv": DimStore, 
            "customers.csv": DimCustomer
        }
        for file_name, model in mappings.items():
            df = DataExtractor.read_csv(self.bronze_path / file_name)
            DataLoader.load_to_sql(df, model)

    def _seed_sales_people(self):
        """
        Loads SalesPeople with date-type cleaning for hire_date.
        """
        df = DataExtractor.read_csv(self.bronze_path / "sales_people.csv")
        df = DataTransformer.clean_sales_people(df)
        DataLoader.load_to_sql(df, DimSalesPerson)

    def _seed_calendar(self):
        """
        Generates and seeds the 2023-2026 Calendar table (Optional Feature #1).
        """
        dates_df = DateDimensionGenerator.generate_range(2023, 2026)
        DataLoader.load_to_sql(dates_df, DimDate)

    def _seed_orders(self):
        """
        Loads Orders. This is the source table for the 'FactSales' SQL View.
        """
        df = DataExtractor.read_csv(self.bronze_path / "orders.csv")
        df = DataTransformer.clean_orders(df)
        DataLoader.load_to_sql(df, DimOrder)

    def _seed_products_sql(self) -> pl.DataFrame:
        """
        Resolves Product-Category relationships and loads them into SQL.

        Returns:
            pl.DataFrame: The resolved product dataframe with category names.
        """
        raw_prod_df = DataExtractor.read_csv(self.bronze_path / "products.csv")
        with Session(engine) as session:
            categories_df = pl.from_dicts([c.model_dump() for c in session.exec(select(DimCategory)).all()])
        
        resolved_products = DataTransformer.resolve_products(raw_prod_df, categories_df)
        DataLoader.load_to_sql(resolved_products.drop("category_name"), DimProduct)
        return resolved_products

    def _index_all_domains_in_vector_db(self, products_df: pl.DataFrame):
        """
        Creates collections and indexes all requested domain entities in Typesense.

        Args:
            products_df (pl.DataFrame): Dataframe of resolved products.
        """
        # 1. Index Products
        self.search_service.create_collection_if_not_exists('products', [
            {'name': 'name', 'type': 'string'},
            {'name': 'description', 'type': 'string'},
            {'name': 'category_name', 'type': 'string', 'facet': True},
            {'name': 'sku', 'type': 'string'}
        ])
        for prod in products_df.to_dicts():
            self.search_service.index_entity('products', prod)

        # 2. Index Customers
        self.search_service.create_collection_if_not_exists('customers', [
            {'name': 'name', 'type': 'string'},
            {'name': 'email', 'type': 'string'}
        ])
        cust_df = DataExtractor.read_csv(self.bronze_path / "customers.csv")
        for cust in cust_df.to_dicts():
            self.search_service.index_entity('customers', cust)

        # 3. Index Stores
        self.search_service.create_collection_if_not_exists('stores', [
            {'name': 'location_name', 'type': 'string'},
            {'name': 'address', 'type': 'string'}
        ])
        store_df = DataExtractor.read_csv(self.bronze_path / "stores.csv")
        for store in store_df.to_dicts():
            # Standardize key for search service
            store['name'] = store.pop('location_name') 
            self.search_service.index_entity('stores', store)

    def _process_unstructured_pdfs(self):
        """
        Extracts content from PDF files and adds them to the product search index.
        """
        pdf_files = list(self.bronze_path.glob("*.pdf"))
        for pdf_path in pdf_files:
            content = DataExtractor.extract_pdf_text(pdf_path) 
            self.search_service.index_entity('products', {
                "sku": f"PDF-{pdf_path.stem.upper()}",
                "name": pdf_path.name,
                "description": content[:500],
                "category_name": "Unstructured"
            })