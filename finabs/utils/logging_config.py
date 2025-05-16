"""
logging_config.py - Logging configuration
---------------------------------------
Functions for configuring logging.
"""
import logging
import sys
from pathlib import Path


def setup_logging(log_file: str = "postcode_scraper.log", level: int = logging.INFO) -> logging.Logger:
    """
    Set up logging for the application.
    
    Args:
        log_file: Path to log file
        level: Logging level
        
    Returns:
        Logger object
    """
    # Create logger
    logger = logging.getLogger("postcode_scraper")
    logger.setLevel(level)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create console handler with UTF-8 encoding
    # Use sys.stdout with encoding='utf-8' for Windows compatibility
    import io
    import sys
    utf8_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
    console_handler = logging.StreamHandler(utf8_stream)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Create file handler with UTF-8 encoding
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
