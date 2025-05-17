"""
logging_config.py - Logging configuration
----------------------------------------
Configure logging for the Google Maps scraper.
"""
import logging
import os
import sys
import io
from datetime import datetime

# Arrow symbol for logging
ARROW = "->"

def setup_logging(log_file=None, debug=False):
    """
    Set up logging for the Google Maps scraper.
    
    Args:
        log_file: Optional log file path
        debug: Enable debug logging
        
    Returns:
        Logger instance
    """
    # Create logger
    logger = logging.getLogger("googlemaps_scraper")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.propagate = False
    
    # Clear existing handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter('[%(asctime)s] %(levelname)-8s – %(message)s', 
                                 datefmt='%Y-%m-%d %H:%M:%S')
    
    # Create console handler with UTF-8 encoding to handle non-English characters
    try:
        # For Windows, create a UTF-8 stream handler
        if sys.platform == 'win32':
            # Use io.TextIOWrapper with UTF-8 encoding
            utf8_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='backslashreplace')
            console_handler = logging.StreamHandler(utf8_stream)
        else:
            # For non-Windows platforms, use standard StreamHandler
            console_handler = logging.StreamHandler(sys.stdout)
        
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        logger.addHandler(console_handler)
    except Exception as e:
        # Fallback to standard handler if UTF-8 handler fails
        fallback_handler = logging.StreamHandler()
        fallback_handler.setFormatter(formatter)
        fallback_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        logger.addHandler(fallback_handler)
        logger.warning(f"Failed to create UTF-8 console handler: {e}. Using fallback handler.")
    
    # Add file handler if log_file is provided
    if log_file:
        try:
            # Ensure directory exists
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            # Create file handler with UTF-8 encoding
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Failed to create log file {log_file}: {e}")
    
    return logger
