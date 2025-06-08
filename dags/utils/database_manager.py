"""
Database Connection Manager for GreenPals ETL Pipeline

This module provides safe, consistent database connection management with
proper error handling, connection pooling considerations, and resource cleanup.
"""

import psycopg2
from contextlib import contextmanager
from typing import Dict, Any, Optional, Tuple
import logging
from .logging_config import setup_logger, log_error_with_context

logger = setup_logger(__name__)

class DatabaseConnectionError(Exception):
    """Custom exception for database connection issues."""
    pass

class DatabaseOperationError(Exception):
    """Custom exception for database operation issues."""
    pass

@contextmanager
def get_db_connection(config: Dict[str, Any]):
    """
    Context manager for database connections with automatic cleanup.
    
    Args:
        config: Database connection configuration dictionary
        
    Yields:
        psycopg2.connection: Database connection object
        
    Raises:
        DatabaseConnectionError: If connection fails
    """
    connection = None
    try:
        logger.debug(f"Connecting to database: {config['host']}:{config['port']}/{config['dbname']}")
        connection = psycopg2.connect(**config)
        logger.debug("Database connection established successfully")
        yield connection
        
    except psycopg2.Error as e:
        error_msg = f"Failed to connect to database {config['host']}:{config['port']}/{config['dbname']}"
        log_error_with_context(logger, e, error_msg)
        raise DatabaseConnectionError(error_msg) from e
        
    except Exception as e:
        log_error_with_context(logger, e, "Unexpected error during database connection")
        raise DatabaseConnectionError("Unexpected database connection error") from e
        
    finally:
        if connection:
            try:
                connection.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")

@contextmanager
def get_db_cursor(connection):
    """
    Context manager for database cursors with automatic cleanup.
    
    Args:
        connection: psycopg2 connection object
        
    Yields:
        psycopg2.cursor: Database cursor object
    """
    cursor = None
    try:
        cursor = connection.cursor()
        yield cursor
        
    except psycopg2.Error as e:
        log_error_with_context(logger, e, "Database cursor operation failed")
        if connection:
            connection.rollback()
        raise DatabaseOperationError("Database operation failed") from e
        
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logger.warning(f"Error closing database cursor: {e}")

def execute_sql_safely(
    connection, 
    sql: str, 
    params: Optional[Tuple] = None,
    fetch_results: bool = False
) -> Optional[list]:
    """
    Execute SQL with proper error handling and optional result fetching.
    
    Args:
        connection: Database connection object
        sql: SQL statement to execute
        params: Optional parameters for the SQL statement
        fetch_results: Whether to fetch and return results
        
    Returns:
        Query results if fetch_results=True, otherwise None
        
    Raises:
        DatabaseOperationError: If SQL execution fails
    """
    try:
        with get_db_cursor(connection) as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
                
            if fetch_results:
                results = cursor.fetchall()
                logger.debug(f"SQL executed successfully, fetched {len(results)} rows")
                return results
            else:
                logger.debug("SQL executed successfully")
                return None
                
    except Exception as e:
        log_error_with_context(logger, e, f"SQL execution failed: {sql[:100]}...")
        raise

def test_database_connection(config: Dict[str, Any]) -> bool:
    """
    Test database connectivity.
    
    Args:
        config: Database connection configuration
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        with get_db_connection(config) as conn:
            execute_sql_safely(conn, "SELECT 1")
        logger.info(f"Database connection test successful: {config['host']}:{config['port']}")
        return True
        
    except Exception as e:
        logger.error(f"Database connection test failed: {config['host']}:{config['port']} - {e}")
        return False

def get_max_updated_timestamp(connection, table_name: str, timestamp_column: str = 'updated_at'):
    """
    Get the maximum timestamp from a table for incremental loading.
    
    Args:
        connection: Database connection
        table_name: Name of the table to query
        timestamp_column: Name of the timestamp column
        
    Returns:
        Maximum timestamp value or minimum date if table is empty
    """
    from datetime import datetime
    
    sql = f"""
        SELECT COALESCE(MAX({timestamp_column}), '1900-01-01'::timestamp) 
        FROM {table_name}
    """
    
    try:
        results = execute_sql_safely(connection, sql, fetch_results=True)
        max_timestamp = results[0][0] if results else datetime(1900, 1, 1)
        logger.debug(f"Max timestamp from {table_name}.{timestamp_column}: {max_timestamp}")
        return max_timestamp
        
    except Exception as e:
        logger.warning(f"Could not get max timestamp from {table_name}, using default: {e}")
        return datetime(1900, 1, 1) 