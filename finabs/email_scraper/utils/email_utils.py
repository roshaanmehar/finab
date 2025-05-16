"""
email_utils.py - Email utilities
------------------------------
Functions for handling emails.
"""
import re
from typing import List, Set

from email_scraper.config import EMAIL_RE

def clean_emails(raw_emails: List[str]) -> List[str]:
    """
    Clean and validate a list of email addresses.
    
    Args:
        raw_emails: List of raw email addresses
        
    Returns:
        List of cleaned and validated email addresses
    """
    seen_emails: Set[str] = set()
    cleaned_list = []
    disposable_domains = {"mailinator.com", "temp-mail.org", "10minutemail.com", "guerrillamail.com"}
    image_extensions = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp", ".tiff"}
    common_non_email_suffixes = {".css", ".js", ".xml", ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"}

    for email_str in raw_emails:
        if not email_str or not isinstance(email_str, str):
            continue
        
        email_str = email_str.strip().lower()
        
        if "@" not in email_str or "." not in email_str.split('@')[-1]:
            continue
        
        if any(p in email_str for p in ("example.com", "sentry.wixpress.com", "your@email.com", "email@example.com", 
                                       "info@yourdomain.com", "name@domain.com", "user@example.com", 
                                       "username@example.com", "email@domain.com", "@localhost", 
                                       "example.org", "example.net", "domain.com", "contact@example.com", 
                                       "privacy@example.com", "email@here.com", "john.doe@example.com", 
                                       "test@example.com", "demo@example.com", "dummy@example.com", 
                                       "anonymous@example.com")):
            continue
        
        if any(email_str.endswith(ext) for ext in image_extensions.union(common_non_email_suffixes)):
            continue
        
        try:
            if email_str.split('@')[1] in disposable_domains:
                continue
        except IndexError:
            continue
        
        if not EMAIL_RE.fullmatch(email_str):
            continue
        
        if email_str not in seen_emails:
            seen_emails.add(email_str)
            cleaned_list.append(email_str)
    
    return cleaned_list

def emails_from_text(text: str) -> List[str]:
    """
    Extract email addresses from text.
    
    Args:
        text: Text to extract emails from
        
    Returns:
        List of extracted email addresses
    """
    if not text or not isinstance(text, str):
        return []
    
    try:
        import html
        text = html.unescape(text)
    except ImportError:
        pass
    
    # Replace common obfuscation patterns
    text = re.sub(r'\s*\[\s*(at|@)\s*\]\s*', '@', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*\[\s*(dot|\.)\s*\]\s*', '.', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*$$\s*(at|@)\s*$$\s*', '@', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*$$\s*(dot|\.)\s*$$\s*', '.', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+(at)\s+', '@', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+(dot)\s+', '.', text, flags=re.IGNORECASE)
    text = text.replace(' AT ', '@').replace(' DOT ', '.')
    text = text.replace(' at ', '@').replace(' dot ', '.')
    text = text.replace('\\/', '/')
    
    return EMAIL_RE.findall(text)
