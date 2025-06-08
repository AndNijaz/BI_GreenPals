"""
Improved Incremental Load ETL Process

This module handles incremental data loading from operational database and external APIs
to the analytical database landing layer, with enhanced error handling, logging, and
maintainability improvements.

Original functionality preserved with enhanced:
- Structured logging
- Better error handling
- Resource management
- Configuration management
- Type hints and documentation
"""

import requests
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Any
from psycopg2.extras import execute_values

# Import our new utilities
from constants import DB_OPERATIONAL_CONFIG, DB_ANALYTICAL_CONFIG, API_ENDPOINTS, DEFAULT_TIMEOUT
from utils.logging_config import setup_logger, log_etl_start, log_etl_end, log_table_operation, log_error_with_context
from utils.database_manager import get_db_connection, get_max_updated_timestamp, DatabaseConnectionError
from tables_config import TABLES_PUBLIC, TABLES_COMPANY

# Set up logging
logger = setup_logger(__name__)

# Combine all operational tables
ALL_TABLES = {**TABLES_PUBLIC, **TABLES_COMPANY}

# API configuration with enhanced structure
API_TABLES = {
    "co2_factors": {
        "endpoint": API_ENDPOINTS["co2_factors"],
        "landing": "landing.co2_factors",
        "columns": ["source_name", "country", "co2_factor", "unit", "updated_at"],
        "pk": ["source_name", "country"]
    },
    "electricity_prices": {
        "endpoint": API_ENDPOINTS["electricity_prices"],
        "landing": "landing.electricity_prices",
        "columns": ["country", "price_per_kwh", "updated_at"],
        "pk": ["country"]
    }
}

def upsert_rows(
    connection, 
    landing_table: str, 
    columns: List[str], 
    pk_columns: List[str], 
    rows: List[Tuple]
) -> None:
    """
    Perform upsert operation on landing table.
    
    Args:
        connection: Database connection
        landing_table: Target landing table name
        columns: List of column names
        pk_columns: List of primary key column names
        rows: List of row tuples to upsert
    """
    if not rows:
        logger.debug(f"No rows to upsert for {landing_table}")
        return

    try:
        col_list = ", ".join(columns)
        pk_list = ", ".join(pk_columns)
        upd_list = ", ".join(f"{c}=EXCLUDED.{c}" for c in columns if c not in pk_columns)
        
        sql = f"""
            INSERT INTO {landing_table} ({col_list})
            VALUES %s
            ON CONFLICT ({pk_list}) DO UPDATE SET {upd_list};
        """
        
        with connection.cursor() as cursor:
            execute_values(cursor, sql, rows)
        connection.commit()
        
        logger.debug(f"Successfully upserted {len(rows)} rows to {landing_table}")
        
    except Exception as e:
        connection.rollback()
        log_error_with_context(logger, e, f"Upsert operation to {landing_table}")
        raise

def fetch_api_data(endpoint: str, since_timestamp: str) -> List[Dict[str, Any]]:
    """
    Fetch data from external API with proper error handling.
    
    Args:
        endpoint: API endpoint URL
        since_timestamp: ISO timestamp for incremental loading
        
    Returns:
        List of dictionaries containing API response data
        
    Raises:
        requests.RequestException: If API request fails
    """
    try:
        logger.debug(f"Fetching data from API: {endpoint}")
        response = requests.get(
            endpoint, 
            params={"since": since_timestamp}, 
            timeout=DEFAULT_TIMEOUT
        )
        response.raise_for_status()
        
        data = response.json()
        logger.debug(f"Successfully fetched {len(data)} records from API")
        return data
        
    except requests.RequestException as e:
        log_error_with_context(logger, e, f"API request to {endpoint}")
        raise
    except Exception as e:
        log_error_with_context(logger, e, f"Unexpected error fetching from {endpoint}")
        raise

def process_operational_tables(op_connection, an_connection) -> None:
    """
    Process incremental load for all operational database tables.
    
    Args:
        op_connection: Operational database connection
        an_connection: Analytical database connection
    """
    logger.info("Starting operational database incremental load")
    
    for table_name, meta_config in ALL_TABLES.items():
        try:
            # Get last updated timestamp from landing table
            last_updated = get_max_updated_timestamp(an_connection, meta_config["landing"])
            
            # Build query for incremental data
            select_columns = ", ".join(meta_config["columns"])
            source_table = f"{meta_config['schema']}.{table_name}"
            
            sql = f"""
                SELECT {select_columns} 
                FROM {source_table} 
                WHERE updated_at > %s
                ORDER BY updated_at
            """
            
            # Execute query
            with op_connection.cursor() as cursor:
                cursor.execute(sql, (last_updated,))
                rows = cursor.fetchall()
            
            # Log and upsert results
            log_table_operation(logger, "INCREMENTAL", source_table, meta_config["landing"], len(rows))
            
            if rows:
                upsert_rows(
                    an_connection, 
                    meta_config["landing"], 
                    meta_config["columns"], 
                    ["id"], 
                    rows
                )
            else:
                logger.debug(f"No new records found for {source_table}")
                
        except Exception as e:
            log_error_with_context(logger, e, f"Processing table {table_name}")
            # Continue with other tables even if one fails
            continue

def process_api_tables(an_connection) -> None:
    """
    Process incremental load for all external API data sources.
    
    Args:
        an_connection: Analytical database connection
    """
    logger.info("Starting API data incremental load")
    
    for api_name, api_config in API_TABLES.items():
        try:
            # Get last updated timestamp
            last_updated = get_max_updated_timestamp(an_connection, api_config["landing"])
            
            # Fetch data from API
            api_data = fetch_api_data(api_config["endpoint"], last_updated.isoformat())
            
            # Transform API data to rows
            rows = []
            for record in api_data:
                row_data = []
                for column in api_config["columns"][:-1]:  # Exclude updated_at
                    row_data.append(record.get(column, None))
                row_data.append(datetime.utcnow())  # Add current timestamp
                rows.append(tuple(row_data))
            
            # Log and upsert results
            log_table_operation(logger, "API_LOAD", api_config["endpoint"], api_config["landing"], len(rows))
            
            if rows:
                upsert_rows(
                    an_connection,
                    api_config["landing"],
                    api_config["columns"],
                    api_config["pk"],
                    rows
                )
            else:
                logger.debug(f"No new records from API: {api_name}")
                
        except Exception as e:
            log_error_with_context(logger, e, f"Processing API {api_name}")
            # Continue with other APIs even if one fails
            continue

def incremental_load() -> None:
    """
    Main incremental load process coordinator.
    
    This function orchestrates the entire incremental load process,
    handling both operational database and external API data sources.
    """
    process_name = "Incremental Load (Operational DB + APIs → Landing Layer)"
    log_etl_start(logger, process_name)
    
    try:
        # Test database connections first
        logger.info("Testing database connections...")
        
        if not test_database_connectivity():
            raise DatabaseConnectionError("Database connectivity test failed")
        
        # Perform incremental load
        with get_db_connection(DB_OPERATIONAL_CONFIG) as op_conn, \
             get_db_connection(DB_ANALYTICAL_CONFIG) as an_conn:
            
            # Process operational tables
            process_operational_tables(op_conn, an_conn)
            
            # Process API data
            process_api_tables(an_conn)
            
        log_etl_end(logger, process_name)
        logger.info("Incremental load completed successfully")
        
    except Exception as e:
        log_error_with_context(logger, e, "Incremental load process")
        logger.error("Incremental load process failed - check logs for details")
        sys.exit(1)

def test_database_connectivity() -> bool:
    """
    Test connectivity to both operational and analytical databases.
    
    Returns:
        True if both connections successful, False otherwise
    """
    from utils.database_manager import test_database_connection
    
    op_success = test_database_connection(DB_OPERATIONAL_CONFIG)
    an_success = test_database_connection(DB_ANALYTICAL_CONFIG)
    
    if op_success and an_success:
        logger.info("All database connectivity tests passed")
        return True
    else:
        logger.error("Database connectivity tests failed")
        return False

def main() -> None:
    """Main entry point for the incremental load process."""
    try:
        incremental_load()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        log_error_with_context(logger, e, "Main process")
        sys.exit(1)

if __name__ == "__main__":
    main() 