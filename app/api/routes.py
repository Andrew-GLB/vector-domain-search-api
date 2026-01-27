from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional
from sqlmodel import Session

# Layer 4: Data Access
from app.data_access.database import get_session

# Layer 3: Domain Entities
from app.domain import (
    ProductDomain, 
    CategoryDomain, 
    SalesPersonDomain, 
    StoreDomain,
    CustomerDomain, 
    OrderDomain,
    FactSalesDomain
)

# Layer 2: Services
from app.services.seed_service import SeedService
from app.services.product_service import ProductService
from app.services.search_service import SearchService
from app.services.category_service import CategoryService
from app.services.sales_person_service import SalesPersonService
from app.services.store_service import StoreService
from app.services.customer_service import CustomerService
from app.services.order_service import OrderService
from app.services.fact_sales_service import FactSalesService

# Security
from app.api.auth import authenticate

router = APIRouter(prefix="/v1")

# --- 1. ADMIN & SEEDING ---
@router.post("/seed", tags=["Admin"])
def seed_database(current_user: str = Depends(authenticate)):
    """Requirement: Seeding data at the beginning."""
    # Create an instance of the service
    service = SeedService()
    # Call the instance method
    return service.run_seed_process()


# --- 2. PRODUCT ROUTES (CRUD + Search) ---
@router.post("/products/", response_model=ProductDomain, tags=["Products"], status_code=status.HTTP_201_CREATED)
def create_product(product: ProductDomain, session: Session = Depends(get_session)):
    service = ProductService(session)
    return service.create_product(product)

@router.post("/products/batch", response_model=List[ProductDomain], tags=["Products"], status_code=status.HTTP_201_CREATED)
def create_products_batch(products: List[ProductDomain], session: Session = Depends(get_session)):
    """Mandatory Feature: Batch operations."""
    service = ProductService(session)
    return service.create_products_batch(products)

@router.get("/products/search", tags=["Products"])
def search_products(q: str, category: Optional[str] = None):
    """Requirement: Fast searching leveraging Vector DB (Typesense) with filters."""
    search_service = SearchService()
    search_service.create_collection_if_not_exists()
    results = search_service.search_products(query=q, category_filter=category)
    return {"results": results}

@router.get("/products/", response_model=List[ProductDomain], tags=["Products"])
def list_products(session: Session = Depends(get_session)):
    service = ProductService(session)
    return service.get_all_products()

@router.get("/products/{id}", response_model=ProductDomain, tags=["Products"])
def get_product(id: int, session: Session = Depends(get_session)):
    service = ProductService(session)
    return service.get_product_by_id(id)

@router.put("/products/{id}", response_model=ProductDomain, tags=["Products"])
def update_product(id: int, product_data: ProductDomain, session: Session = Depends(get_session)):
    service = ProductService(session)
    return service.update_product(id, product_data)

@router.delete("/products/{id}", tags=["Products"], status_code=status.HTTP_204_NO_CONTENT)
def delete_product(id: int, session: Session = Depends(get_session)):
    service = ProductService(session)
    service.delete_product(id)
    return None


# --- 3. CATEGORY ROUTES ---
@router.post("/categories/", response_model=CategoryDomain, tags=["Categories"])
def create_category(data: CategoryDomain, session: Session = Depends(get_session)):
    return CategoryService(session).create_category(data)

@router.get("/categories/", response_model=List[CategoryDomain], tags=["Categories"])
def list_categories(session: Session = Depends(get_session)):
    return CategoryService(session).get_all_categories()

@router.delete("/categories/{id}", tags=["Categories"])
def delete_category(id: int, session: Session = Depends(get_session)):
    return CategoryService(session).delete_category(id)


# --- 4. SALES TEAM ROUTES ---
@router.post("/sales-people/", response_model=SalesPersonDomain, tags=["Sales Team"])
def create_sales_person(data: SalesPersonDomain, session: Session = Depends(get_session)):
    return SalesPersonService(session).create_sales_person(data)

@router.get("/sales-people/", response_model=List[SalesPersonDomain], tags=["Sales Team"])
def list_sales_people(session: Session = Depends(get_session)):
    return SalesPersonService(session).get_all_sales_people()


# --- 5. STORE ROUTES ---
@router.post("/stores/", response_model=StoreDomain, tags=["Stores"])
def create_store(store: StoreDomain, session: Session = Depends(get_session)):
    return StoreService(session).create_store(store)

@router.post("/stores/batch", response_model=List[StoreDomain], tags=["Stores"])
def create_stores_batch(stores: List[StoreDomain], session: Session = Depends(get_session)):
    return StoreService(session).create_stores_batch(stores)

@router.get("/stores/", response_model=List[StoreDomain], tags=["Stores"])
def list_stores(session: Session = Depends(get_session)):
    return StoreService(session).get_all_stores()


# --- 6. CUSTOMER ROUTES ---
@router.post("/customers/", response_model=CustomerDomain, tags=["Customers"])
def create_customer(data: CustomerDomain, session: Session = Depends(get_session)):
    return CustomerService(session).create_customer(data)

@router.post("/customers/batch", response_model=List[CustomerDomain], tags=["Customers"])
def create_customers_batch(data: List[CustomerDomain], session: Session = Depends(get_session)):
    return CustomerService(session).create_customers_batch(data)

@router.get("/customers/", response_model=List[CustomerDomain], tags=["Customers"])
def list_customers(session: Session = Depends(get_session)):
    return CustomerService(session).get_all_customers()


# --- 7. ORDER ROUTES ---
@router.post("/orders/", response_model=OrderDomain, tags=["Orders"])
def create_order(order: OrderDomain, session: Session = Depends(get_session)):
    return OrderService(session).create_order(order)

@router.post("/orders/batch", response_model=List[OrderDomain], tags=["Orders"])
def create_orders_batch(orders: List[OrderDomain], session: Session = Depends(get_session)):
    return OrderService(session).create_orders_batch(orders)

@router.get("/orders/", response_model=List[OrderDomain], tags=["Orders"])
def list_orders(session: Session = Depends(get_session)):
    return OrderService(session).get_all_orders()


# --- 8. TRANSACTIONAL ROUTES (Fact Table) ---
@router.post("/sales/", response_model=FactSalesDomain, tags=["Sales (Fact Table)"])
def create_sale(sale: FactSalesDomain, session: Session = Depends(get_session)):
    return FactSalesService(session).create_fact_sale(sale)


# --- 9. GOLD LAYER ANALYTICS ---
@router.get("/gold/products", response_model=List[ProductDomain], tags=["Gold Layer"])
def query_gold_products(session: Session = Depends(get_session)):
    """Requirement: Querying Gold Layer (Refined data)."""
    return ProductService(session).get_all_products()

@router.get("/gold/sale-prices", tags=["Gold Layer"])
def get_discounted_items(session: Session = Depends(get_session)):
    """Gold Layer view with business logic (discounts)."""
    return ProductService(session).get_discounted_products_gold()

@router.get("/gold/revenue-by-store", tags=["Gold Layer"])
def get_revenue_by_store(session: Session = Depends(get_session)):
    return FactSalesService(session).get_sales_by_store_gold()

@router.get("/gold/sales-performance", tags=["Gold Layer"])
def get_sales_performance(session: Session = Depends(get_session)):
    return FactSalesService(session).get_sales_performance_gold()

@router.get("/gold/order-analytics", tags=["Gold Layer"])
def get_order_analytics(session: Session = Depends(get_session)):
    return OrderService(session).get_gold_order_analytics()