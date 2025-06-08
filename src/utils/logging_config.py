"""
Centralized Logging Configuration for GreenPals ETL Pipeline

This module provides consistent logging setup across all ETL processes,
replacing print statements with structured logging for better monitoring and debugging.
"""

import logging
import sys
from datetime import datetime
from typing import Optional

def setup_logger(
    name: str, 
    level: int = logging.INFO,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Set up a logger with consistent formatting for ETL processes.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (default: INFO)
        log_file: Optional file path for logging to file
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if logger already configured
    if logger.handlers:
        return logger
        
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def log_etl_start(logger: logging.Logger, process_name: str) -> None:
    """Log the start of an ETL process."""
    logger.info("=" * 60)
    logger.info(f"Starting {process_name}")
    logger.info(f"Timestamp: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

def log_etl_end(logger: logging.Logger, process_name: str) -> None:
    """Log the completion of an ETL process."""
    logger.info("=" * 60)
    logger.info(f"Completed {process_name}")
    logger.info(f"Timestamp: {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

def log_table_operation(
    logger: logging.Logger, 
    operation: str, 
    source: str, 
    target: str, 
    row_count: int
) -> None:
    """Log a table operation with row count."""
    logger.info(f"{operation}: {source} → {target} | {row_count:,} rows")

def log_error_with_context(
    logger: logging.Logger, 
    error: Exception, 
    context: str
) -> None:
    """Log an error with additional context."""
    logger.error(f"Error in {context}: {type(error).__name__}: {str(error)}")
    logger.debug("Full traceback:", exc_info=True) 