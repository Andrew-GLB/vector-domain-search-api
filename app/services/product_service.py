import re
from typing import List, Optional
from sqlmodel import Session, select
from fastapi import HTTPException, status

# Layer 4: Data Access
from app.data_access.models import DimProduct, DimCategory

# Layer 3: Domain Entities
from app.domain.product import ProductDomain

# Layer 2: Supporting Services
from app.services.search_service import SearchService


class ProductService:
    """
    Service layer for managing Product business logic and data orchestration.

    This service handles the lifecycle of products, including validation, 
    relational persistence in the SQL Gold Layer, and synchronization with 
    the Typesense Vector DB for fast search capabilities.
    """

    def __init__(self, session: Session):
        """
        Initializes the ProductService with a database session and search service.

        Args:
            session (Session): The active SQLModel/SQLAlchemy session.
        """
        self.session = session
        # Initialize the search service for the Vector DB dual-write
        self.search_service = SearchService()

    # --- 1. LAYERED MAPPING HELPERS ---

    def _map_to_domain(self, db_product: DimProduct) -> ProductDomain:
        """
        Converts a Data Access entity (SQLModel) into a Domain entity (Pydantic).

        This fulfills the requirement to keep Domain entities separate from the 
        Data Access layer by resolving relational IDs into human-readable names.

        Args:
            db_product (DimProduct): The database record to be transformed.

        Returns:
            ProductDomain: The clean business representation of the product.
        """
        # Resolve the category name for the domain representation
        category = self.session.get(DimCategory, db_product.category_id)
        category_name = category.name if category else "Unknown"

        return ProductDomain(
            sku=db_product.sku,
            name=db_product.name,
            description=db_product.description,
            price=db_product.price,
            category_name=category_name
        )

    def _get_or_create_category(self, category_name: str) -> int:
        """
        Ensures Star Schema integrity by resolving or creating a category dimension.

        Args:
            category_name (str): The name of the category to look up or create.

        Returns:
            int: The primary key ID of the category dimension.
        """
        statement = select(DimCategory).where(DimCategory.name == category_name)
        category = self.session.exec(statement).first()

        if not category:
            category = DimCategory(name=category_name)
            self.session.add(category)
            self.session.commit()
            self.session.refresh(category)
        
        return category.id

    # --- 2. BUSINESS VALIDATIONS ---

    def validate_sku(self, sku: str):
        """
        Enforces the business rule regarding SKU formatting.

        Args:
            sku (str): The SKU string to validate.

        Raises:
            HTTPException: 400 status if the SKU does not follow the PROD-XXXX format.
        """
        pattern = r"^PROD-\d{4}$"
        if not re.match(pattern, sku):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid SKU format '{sku}'. Must follow PROD-XXXX."
            )

    def apply_seasonal_discount(self, price: float, discount_percent: float = 10.0) -> float:
        """
        Performs a logical business transformation to calculate discounted prices.

        Args:
            price (float): The original product price.
            discount_percent (float): The percentage to discount. Defaults to 10.0.

        Returns:
            float: The calculated discounted price rounded to two decimal places.
        """
        if price <= 0:
            return 0.0
        return round(price * (1 - (discount_percent / 100)), 2)

    # --- 3. FULL CRUD OPERATIONS ---

    def create_product(self, product_in: ProductDomain) -> ProductDomain:
        """
        Persists a new product to the SQL Gold Layer and indexes it in the Vector DB.

        This method implements the dual-write strategy required for fast searching 
        capabilities while maintaining relational integrity.

        Args:
            product_in (ProductDomain): The validated domain data from the API.

        Returns:
            ProductDomain: The newly created product as a domain entity.

        Raises:
            HTTPException: 400 status if the SKU validation fails.
        """
        self.validate_sku(product_in.sku)
        cat_id = self._get_or_create_category(product_in.category_name)

        new_product = DimProduct(
            sku=product_in.sku,
            name=product_in.name,
            description=product_in.description,
            price=product_in.price,
            category_id=cat_id
        )
        
        # 1. SQL Write
        self.session.add(new_product)
        self.session.commit()
        self.session.refresh(new_product)

        # 2. Vector DB Sync (Dual-Write)
        try:
            self.search_service.create_collection_if_not_exists()
            self.search_service.index_product({
                "name": new_product.name,
                "description": new_product.description,
                "sku": new_product.sku,
                "price": float(new_product.price),
                "category_name": product_in.category_name
            })
        except Exception as e:
            print(f"Warning: Vector Indexing failed: {e}")

        return self._map_to_domain(new_product)

    def create_products_batch(self, products_in: List[ProductDomain]) -> List[ProductDomain]:
        """
        Fulfills the mandatory feature for batch CRUD operations.

        Args:
            products_in (List[ProductDomain]): A list of products to be created.

        Returns:
            List[ProductDomain]: The list of created products.
        """
        return [self.create_product(p) for p in products_in]

    def get_product_by_id(self, product_id: int) -> ProductDomain:
        """
        Retrieves a single product by its database ID.

        Args:
            product_id (int): The ID of the product to retrieve.

        Returns:
            ProductDomain: The product domain entity.

        Raises:
            HTTPException: 404 status if the product is not found.
        """
        db_product = self.session.get(DimProduct, product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail="Product not found")
        return self._map_to_domain(db_product)

    def get_all_products(self) -> List[ProductDomain]:
        """
        Retrieves all products currently stored in the Gold Layer.

        Returns:
            List[ProductDomain]: A list of all products as domain entities.
        """
        statement = select(DimProduct)
        products = self.session.exec(statement).all()
        return [self._map_to_domain(p) for p in products]

    def update_product(self, product_id: int, data: ProductDomain) -> ProductDomain:
        """
        Updates an existing product in both the SQL database and the Vector DB.

        Args:
            product_id (int): The ID of the product to update.
            data (ProductDomain): The new data to apply.

        Returns:
            ProductDomain: The updated product domain entity.

        Raises:
            HTTPException: 404 if the product does not exist.
            HTTPException: 400 if the updated SKU is invalid.
        """
        db_product = self.session.get(DimProduct, product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail="Product not found")

        self.validate_sku(data.sku)
        db_product.sku = data.sku
        db_product.name = data.name
        db_product.description = data.description
        db_product.price = data.price
        db_product.category_id = self._get_or_create_category(data.category_name)

        self.session.add(db_product)
        self.session.commit()
        self.session.refresh(db_product)

        # Update Vector DB record
        self.search_service.index_product({
            "name": db_product.name,
            "description": db_product.description,
            "sku": db_product.sku,
            "price": float(db_product.price),
            "category_name": data.category_name
        })

        return self._map_to_domain(db_product)

    def delete_product(self, product_id: int) -> bool:
        """
        Removes a product from the SQL database.

        Args:
            product_id (int): The ID of the product to delete.

        Returns:
            bool: True if the deletion was successful.

        Raises:
            HTTPException: 404 status if the product is not found.
        """
        db_product = self.session.get(DimProduct, product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        self.session.delete(db_product)
        self.session.commit()
        return True

    # --- 4. DATA REFINEMENT (GOLD LAYER VIEWS) ---

    def get_discounted_products(self) -> List[dict]:
        """
        Queries the Gold Layer to provide a refined 'Sale' view with discounts.

        This satisfies the requirement for querying the Gold Layer using 
        specific business transformations.

        Returns:
            List[dict]: A list of dictionaries containing refined sales data.
        """
        statement = select(DimProduct)
        products = self.session.exec(statement).all()
        
        results = []
        for p in products:
            results.append({
                "sku": p.sku,
                "original_price": p.price,
                "sale_price": self.apply_seasonal_discount(p.price),
                "name": p.name
            })
        return results