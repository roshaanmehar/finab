"""
url_builder.py - URL construction
--------------------------------
Functions for constructing URLs for scraping.
"""
from urllib.parse import urlencode


BASE_URL = "https://www.doogal.co.uk/UKPostcodes"


def build_url(prefix: str, page: int) -> str:
    """
    Build a URL for searching postcodes.
    
    Args:
        prefix: Outward prefix to search for
        page: Page number
        
    Returns:
        URL for searching postcodes
    """
    return f"{BASE_URL}?{urlencode({'Search': prefix, 'page': page})}"
