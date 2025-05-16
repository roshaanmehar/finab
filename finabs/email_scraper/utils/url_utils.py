"""
url_utils.py - URL utilities
--------------------------
Functions for handling URLs.
"""
import urllib.parse
from typing import Optional

def normalize_url(url: str) -> str:
    """
    Normalize a URL by adding https:// if needed and validating format.
    
    Args:
        url: URL to normalize
        
    Returns:
        Normalized URL or empty string if invalid
    """
    if not url or not isinstance(url, str):
        return ""
    
    url = url.strip().lower()
    if not url:
        return ""
    
    if not url.startswith(("http://", "https://")):
        if '.' in url and ' ' not in url and any(url.endswith(tld) for tld in ['.com', '.org', '.net', '.co', '.uk', '.ca', '.de', '.io', '.ai', '.app']):
            return "https://" + url
        else:
            return ""
    
    return url

def get_domain(url: str) -> str:
    """
    Extract domain from a URL.
    
    Args:
        url: URL to extract domain from
        
    Returns:
        Domain or empty string if invalid
    """
    try:
        parsed_url = urllib.parse.urlparse(url)
        domain = parsed_url.netloc
        return domain[4:] if domain.startswith('www.') else domain.lower()
    except Exception:
        return ""
