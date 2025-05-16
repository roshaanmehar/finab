"""
logging_config.py - Logging configuration
---------------------------------------
Functions for configuring logging.
"""
import logging
import logging.handlers
import os
import sys
from pathlib import Path

# Fix for Windows encoding issues - use ASCII-compatible arrow instead of Unicode
ARROW = "->"  # Replace Unicode arrow with ASCII compatible version

# Create logs directory
LOG_DIR = Path("logs/googlemaps_scraper")
LOG_DIR.mkdir(parents=True, exist_ok=True)

def setup_logging(log_file: str = None, debug: bool = False) -> logging.Logger:
    """
    Set up logging for the application with both console and file handlers.
    
    Args:
        log_file: Path to log file (if None, a default name will be used)
        debug: Enable debug logging
        
    Returns:
        Logger object
    """
    # Set default log file if not provided
    if log_file is None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = LOG_DIR / f"gmaps_scraper_{timestamp}.log"
    elif not isinstance(log_file, Path):
        log_file = LOG_DIR / log_file
    
    # Create logger
    logger = logging.getLogger("googlemaps_scraper")
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Set level
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    # Create formatter
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-7s – %(message)s", "%Y-%m-%d %H:%M:%S")
    
    # Create console handler - safely handle encoding
    try:
        # Create a standard StreamHandler - safer in multithreaded environments
        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG if debug else logging.INFO)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    except Exception as e:
        # If we can't set up console logging, log to file only
        print(f"Warning: Could not set up console logging: {e}. Continuing with file logging only.")
    
    try:
        # Create rotating file handler for better log management
        # Ensure UTF-8 encoding for the file handler
        fh = logging.handlers.RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
            delay=True  # Delay file creation until first log
        )
        fh.setFormatter(fmt)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)
        
        # Use print instead of logger.info since logger might not have handlers yet
        print(f"Logging to file: {log_file}")
    except (PermissionError, OSError) as e:
        # If we can't set up file logging, just log to console and continue
        print(f"Warning: Could not set up file logging: {e}. Continuing with console logging only.")
    
    # Set levels for other loggers
    selenium_logger = logging.getLogger('selenium.webdriver.remote.remote_connection')
    selenium_logger.setLevel(logging.WARNING if not debug else logging.DEBUG)
    urllib3_logger = logging.getLogger('urllib3.connectionpool')
    urllib3_logger.setLevel(logging.WARNING if not debug else logging.DEBUG)
    
    return logger
