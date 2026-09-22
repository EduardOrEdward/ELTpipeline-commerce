"""Configuration for the ingestion layer.

Public service endpoints are kept as code defaults. Credentials and other
deployment-specific values are read from environment variables instead of
being committed to the repository.
"""

import os


# Public/internal service endpoints.
API_URL = os.getenv("API_URL", "http://mock_api:8000")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")

# Object storage configuration.
S3_BUCKET = os.getenv("S3_BUCKET", "elt-raw")

# PostgreSQL connection settings.
# Defaults are provided for local development; real credentials should be
# supplied through the environment and must not be committed.
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "elt_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgre_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not POSTGRES_PASSWORD:
    raise RuntimeError("POSTGRES_PASSWORD environment variable is required")
