from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # API Credentials
    SUPABASE_URL: str
    SUPABASE_SERVICE_ROLE_KEY: str
    DATABASE_URI: str
    DATABASE_URL: str
    
    # Database Connection
    DATABASE_URL: str

    PYTHON_VERSION: str

    #Basic Authentication
    ADMIN_USERNAME: str
    ADMIN_PASSWORD: str

    #AI Service API Details
    GEMINI_API_KEY: str

    # Typesense Connection Details
    # The internal directory where Typesense stores its data

    TYPESENSE_DATA_DIR: str
    TYPESENSE_HOST: str
    TYPESENSE_PORT: int
    TYPESENSE_PROTOCOL: str
    TYPESENSE_API_KEY: str
    TYPESENSE_TIMEOUT: int

    class Config:
        env_file = ".env"

settings = Settings()