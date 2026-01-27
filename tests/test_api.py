from fastapi.testclient import TestClient
from app.api.main import app

client = TestClient(app)

def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the Vector Domain Search API"}

def test_seed_endpoint_no_auth():
    """Requirement check: Seeding should be protected."""
    response = client.post("/v1/seed")
    # Should fail because we didn't provide Basic Auth
    assert response.status_code == 401

def test_seed_endpoint_with_auth():
    """Requirement check: Seeding works with correct credentials."""
    # admin:password123 in Basic Auth
    response = client.post(
        "/v1/seed",
        auth=("admin", "password123")
    )
    # If the file exists, it should return 200. If not, our service returns a dict.
    assert response.status_code == 200