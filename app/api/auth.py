import os
import secrets

from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials


# Load variables from .env
load_dotenv()

# Setup Basic Auth Security object
security = HTTPBasic()

# Retrieve credentials from environment
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "password123")

def authenticate(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """Implements Basic Authentication using environment-stored credentials.

    Args:
        credentials (HTTPBasicCredentials): The credentials provided via the
            Authorization header in the request.

    Returns:
        str: The authenticated username.

    Raises:
        HTTPException: 401 status code if credentials do not match the environment.
    """
    # Use secrets.compare_digest to prevent timing attacks
    # We cast variables to str to ensure Mypy strict compatibility
    is_user_ok = secrets.compare_digest(credentials.username, str(ADMIN_USERNAME))
    is_pass_ok = secrets.compare_digest(credentials.password, str(ADMIN_PASSWORD))

    if not (is_user_ok and is_pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    return credentials.username
