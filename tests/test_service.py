import pytest
from fastapi import HTTPException
from app.services.product_service import ProductService
from sqlmodel import Session, create_engine, SQLModel

# We use an in-memory SQLite for tests so we don't mess up our real warehouse.db
@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session

def test_validate_sku_correct(session: Session):
    service = ProductService(session)
    # This should not raise any error
    service.validate_sku("PROD-1234")

def test_validate_sku_incorrect(session: Session):
    service = ProductService(session)
    # This SHOULD raise an HTTPException
    with pytest.raises(HTTPException) as excinfo:
        service.validate_sku("INVALID-SKU")
    assert excinfo.value.status_code == 400
    assert "Invalid SKU format" in excinfo.value.detail

def test_apply_seasonal_discount(session: Session):
    service = ProductService(session)
    price = 100.0
    discounted = service.apply_seasonal_discount(price, 20.0)
    assert discounted == 80.0