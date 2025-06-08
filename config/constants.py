"""
Database Configuration Constants

This module centralizes all database connection configurations and common constants
used across the ETL pipeline, following best practices for maintainability and security.
"""

import os
from typing import Any

# Database connection configurations
DB_OPERATIONAL_CONFIG = {
    "dbname": os.getenv("OPERATIONAL_DB_NAME", "db_operational"),
    "user": os.getenv("OPERATIONAL_DB_USER", "postgres"),
    "password": os.getenv("OPERATIONAL_DB_PASSWORD", "napoleonlm10"),
    "host": os.getenv("OPERATIONAL_DB_HOST", "db_operational"),
    "port": int(os.getenv("OPERATIONAL_DB_PORT", 5432))
}

DB_ANALYTICAL_CONFIG = {
    "dbname": os.getenv("ANALYTICAL_DB_NAME", "db_analytical"),
    "user": os.getenv("ANALYTICAL_DB_USER", "postgres"),
    "password": os.getenv("ANALYTICAL_DB_PASSWORD", "napoleonlm10"),
    "host": os.getenv("ANALYTICAL_DB_HOST", "db_analytical"),
    "port": int(os.getenv("ANALYTICAL_DB_PORT", 5432))
}

# API endpoints
API_ENDPOINTS = {
    "co2_factors": "http://host.docker.internal:8000/co2-factors",
    "electricity_prices": "http://host.docker.internal:8001/electricity-prices"
}

# Common SQL constants
SCD2_END_DATE = '9999-12-31'
BATCH_SIZE = 1000
DEFAULT_TIMEOUT = 15

# Schema names
SCHEMAS = {
    "landing": "landing",
    "archive": "archive",
    "cleaned": "cleaned",
    "public": "public",
    "company": "company_schema"
}

def clean_value(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, str):
        return val.strip()
    return val 