"""
logging_config.py - Logging configuration
---------------------------------------
Functions for configuring logging.
"""
import logging
import logging.handlers
import sys
from pathlib import Path

from email_scraper.config import LOG_DIR

def setup_logging(debug: bool = False) -> logging.Logger:
    """
    Set up logging with both console and file handlers.
    
    Args:
        debug: Enable debug logging
        
    Returns:
        Logger object
    """
    # Create logger
    logger = logging.getLogger("email_scraper")
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Set level
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    # Create formatter
    fmt = logging.Formatter("[%(asctime)s] %(levelname)-7s – (%(threadName)s) %(message)s", "%Y-%m-%d %H:%M:%S")
    
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
    
    # Ensure log directory exists
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        # Create file handler with a safer approach for Windows
        log_file = LOG_DIR / "email_scraper.log"
        
        # Use RotatingFileHandler instead of TimedRotatingFileHandler for better Windows compatibility
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
    except (PermissionError, OSError) as e:
        # If we can't set up file logging, just log to console and continue
        print(f"Warning: Could not set up file logging: {e}. Continuing with console logging only.")
    
    # Set levels for other loggers
    selenium_logger = logging.getLogger('selenium.webdriver.remote.remote_connection')
    selenium_logger.setLevel(logging.WARNING if not debug else logging.DEBUG)
    urllib3_logger = logging.getLogger('urllib3.connectionpool')
    urllib3_logger.setLevel(logging.WARNING if not debug else logging.DEBUG)
    
    return logger
