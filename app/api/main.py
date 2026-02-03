import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.data_access.database import create_db_and_tables


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Handles system startup and shutdown events.
    
    Requirement: REST API should create necessary schemas if they don't exist.
    Initializes logging and triggers the creation of SQL tables and Virtual Views.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()] # This sends it to the Terminal
    )

    # Create database schemas on startup
    create_db_and_tables()

    yield

# Define the FastAPI app with metadata for Swagger UI
app = FastAPI(
    title="Vector Domain Search API",
    description="A layered architecture API for vector-based search using Typesense and SQLModel",
    version="0.2.0",
    lifespan=lifespan
)

# Include our routes
app.include_router(router)

@app.get("/")
def read_root() -> dict[str, str]:
    """Landing endpoint for the API.
    
    Returns:
        Dict[str, str]: A welcome message.
    """
    return {"message": "Welcome to the Vector Domain Search API"}
