"""
Database Extraction Module

Handles extraction of data from operational databases with support for
full loads, incremental loads, and custom queries.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from src.utils.database_manager import get_db_connection, get_max_updated_timestamp
from src.utils.logging_config import setup_logger, log_table_operation, log_error_with_context

logger = setup_logger(__name__)

class DatabaseExtractor:
    """
    Database extractor for operational data with support for full and incremental loads.
    """
    
    def __init__(self, source_config: Dict[str, Any]):
        """
        Initialize the database extractor.
        
        Args:
            source_config: Database connection configuration
        """
        self.source_config = source_config
        self.logger = logger
    
    def extract_table_full(
        self, 
        table_schema: str, 
        table_name: str, 
        columns: List[str]
    ) -> List[Tuple]:
        """
        Extract all data from a table.
        
        Args:
            table_schema: Schema name
            table_name: Table name
            columns: List of columns to extract
            
        Returns:
            List of tuples containing extracted data
        """
        try:
            with get_db_connection(self.source_config) as connection:
                source_table = f"{table_schema}.{table_name}"
                select_columns = ", ".join(columns)
                
                sql = f"SELECT {select_columns} FROM {source_table} ORDER BY updated_at"
                
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                
                log_table_operation(
                    self.logger, 
                    "EXTRACT_FULL", 
                    source_table, 
                    "memory", 
                    len(rows)
                )
                return rows
                
        except Exception as e:
            log_error_with_context(
                self.logger, 
                e, 
                f"Full extraction from {table_schema}.{table_name}"
            )
            raise
    
    def extract_table_incremental(
        self,
        table_schema: str,
        table_name: str,
        columns: List[str],
        last_updated: datetime,
        timestamp_column: str = 'updated_at'
    ) -> List[Tuple]:
        """
        Extract incremental data from a table since last update.
        
        Args:
            table_schema: Schema name
            table_name: Table name
            columns: List of columns to extract
            last_updated: Last update timestamp for incremental load
            timestamp_column: Name of the timestamp column
            
        Returns:
            List of tuples containing extracted data
        """
        try:
            with get_db_connection(self.source_config) as connection:
                source_table = f"{table_schema}.{table_name}"
                select_columns = ", ".join(columns)
                
                sql = f"""
                    SELECT {select_columns} 
                    FROM {source_table} 
                    WHERE {timestamp_column} > %s
                    ORDER BY {timestamp_column}
                """
                
                with connection.cursor() as cursor:
                    cursor.execute(sql, (last_updated,))
                    rows = cursor.fetchall()
                
                log_table_operation(
                    self.logger, 
                    "EXTRACT_INCREMENTAL", 
                    source_table, 
                    "memory", 
                    len(rows)
                )
                return rows
                
        except Exception as e:
            log_error_with_context(
                self.logger, 
                e, 
                f"Incremental extraction from {table_schema}.{table_name}"
            )
            raise
    
    def extract_multiple_tables(
        self, 
        table_configs: Dict[str, Dict[str, Any]], 
        incremental: bool = True,
        target_connection_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[Tuple]]:
        """
        Extract data from multiple tables efficiently.
        
        Args:
            table_configs: Dictionary of table configurations
            incremental: Whether to perform incremental extraction
            target_connection_config: Target DB config for getting last timestamps
            
        Returns:
            Dictionary mapping table names to extracted data
        """
        results = {}
        
        self.logger.info(f"Starting {'incremental' if incremental else 'full'} extraction for {len(table_configs)} tables")
        
        for table_name, config in table_configs.items():
            try:
                schema = config['schema']
                columns = config['columns']
                
                if incremental and target_connection_config:
                    # Get last updated timestamp from target
                    with get_db_connection(target_connection_config) as target_conn:
                        landing_table = config.get('landing', f"landing.{table_name}")
                        last_updated = get_max_updated_timestamp(target_conn, landing_table)
                    
                    rows = self.extract_table_incremental(
                        schema, table_name, columns, last_updated
                    )
                else:
                    rows = self.extract_table_full(schema, table_name, columns)
                
                results[table_name] = rows
                
            except Exception as e:
                log_error_with_context(
                    self.logger, 
                    e, 
                    f"Extraction failed for table {table_name}"
                )
                # Continue with other tables
                results[table_name] = []
                continue
        
        return results
    
    def extract_with_custom_query(
        self, 
        sql: str, 
        params: Optional[Tuple] = None
    ) -> List[Tuple]:
        """
        Execute custom SQL query for data extraction.
        
        Args:
            sql: Custom SQL query
            params: Optional query parameters
            
        Returns:
            List of tuples containing query results
        """
        try:
            with get_db_connection(self.source_config) as connection:
                with connection.cursor() as cursor:
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)
                    
                    rows = cursor.fetchall()
                
                self.logger.debug(f"Custom query executed successfully, returned {len(rows)} rows")
                return rows
                
        except Exception as e:
            log_error_with_context(self.logger, e, "Custom query execution")
            raise 