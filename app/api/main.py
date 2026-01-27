from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.data_access.database import create_db_and_tables
from app.api.routes import router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Requirement: REST API should create necessary schemas if they don't exist
    create_db_and_tables()
    yield

# Define the FastAPI app with metadata for Swagger UI
app = FastAPI(
    title="Vector Domain Search API",
    description="A layered architecture API for vector-based search using Typesense and SQLModel",
    version="0.1.0",
    lifespan=lifespan
)


# Include our routes
app.include_router(router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Vector Domain Search API"}