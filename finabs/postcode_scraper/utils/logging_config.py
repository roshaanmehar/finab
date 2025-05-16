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
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
