"""
email_extractor.py - Email extraction
-----------------------------------
Functions for extracting emails from web pages.
"""
import logging
import random
import time
from typing import List, Set, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    NoSuchElementException,
    StaleElementReferenceException
)

from email_scraper.utils.email_utils import emails_from_text
from email_scraper.scraping.cookie_handler import dismiss_cookie_consent
from email_scraper.scraping.browser_manager import is_driver_alive
from email_scraper.config import UA_POOL, PAGE_LOAD_TIMEOUT

def selenium_extract_page_emails(
    driver: webdriver.Chrome,
    url: str,
    is_contact_page: bool,
    debug: bool = False
) -> Tuple[List[str], Optional[str]]:
    """
    Extract unique emails and page source from a single page using Selenium.
    
    Args:
        driver: Selenium WebDriver
        url: URL to extract emails from
        is_contact_page: Whether this is a contact page
        debug: Enable debug logging
        
    Returns:
        Tuple of (list of emails, page HTML source)
    """
    logger = logging.getLogger("email_scraper")
    extracted_emails: Set[str] = set()
    page_html_source: Optional[str] = None

    if not is_driver_alive(driver):
        logger.warning(f"Driver not alive when trying to process {url}")
        raise WebDriverException("Driver is not alive for page data extraction")

    try:
        logger.debug(f"Selenium navigating to: {url}")
        # Set page load timeout to prevent getting stuck
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)  # Use config value
        driver.get(url)
        dismiss_cookie_consent(driver, debug)
        
        # Use a shorter wait time for 404 pages
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Check for common 404 indicators
        if "404" in driver.title.lower() or "not found" in driver.title.lower() or "error" in driver.title.lower():
            logger.warning(f"Detected possible 404/error page for {url} - title: {driver.title}")
            # Still grab the source in case there are emails in the error page
            page_html_source = driver.page_source
            return [], page_html_source
            
        page_html_source = driver.page_source
        
        if not page_html_source:
            logger.warning(f"Page source is empty for {url} after Selenium load.")
            return [], None
    
    except TimeoutException:
        logger.warning(f"Timeout loading {url} with Selenium.")
        try:
            page_html_source = driver.page_source
        except:
            pass
        if not page_html_source:
            return [], None
    
    except WebDriverException as e:
        logger.error(f"WebDriverException during Selenium navigation to {url}: {e}")
        raise

    def add_emails_from_source(text_source: str):
        if text_source:
            for email_item in emails_from_text(text_source):
                extracted_emails.add(email_item)
    
    # Extract from body text
    try:
        body_el = driver.find_element(By.TAG_NAME, "body")
        add_emails_from_source(body_el.get_attribute("textContent"))
    except (NoSuchElementException, StaleElementReferenceException) as e:
        logger.debug(f"Could not get body text from {url}: {e}")
    
    # Extract from mailto links
    try:
        mailto_links = driver.find_elements(By.XPATH, "//a[starts-with(@href, 'mailto:')]")
        for link in mailto_links:
            try:
                href = link.get_attribute("href") or ""
                add_emails_from_source(href.split('?')[0].replace('mailto:', '', 1))
            except StaleElementReferenceException:
                continue
    except WebDriverException as e:
        logger.debug(f"Error finding mailto links on {url}: {e}")
    
    # Extract from meta tags
    try:
        meta_tags = driver.find_elements(By.TAG_NAME, "meta")
        for tag in meta_tags:
            try:
                content = tag.get_attribute("content") or ""
                if "@" in content:
                    add_emails_from_source(content)
            except StaleElementReferenceException:
                continue
    except WebDriverException as e:
        logger.debug(f"Error finding meta tags on {url}: {e}")
    
    # Extract from script tags
    try:
        script_tags = driver.find_elements(By.TAG_NAME, "script")
        for script in script_tags[:20]:  # Limit checks
            try:
                script_content = script.get_attribute("textContent") or ""
                if "@" in script_content and len(script_content) < 75000:  # Avoid huge scripts
                    add_emails_from_source(script_content)
            except StaleElementReferenceException:
                continue
    except WebDriverException as e:
        logger.debug(f"Error finding script tags on {url}: {e}")
    
    # Extract from forms
    try:
        form_tags = driver.find_elements(By.TAG_NAME, "form")
        for form in form_tags:
            try:
                action = form.get_attribute("action") or ""
                if "mailto:" in action:
                    add_emails_from_source(action.split('?')[0].replace('mailto:', '', 1))
                
                hidden_inputs = form.find_elements(By.XPATH, ".//input[@type='hidden' and @value and contains(@value, '@')]")
                for h_input in hidden_inputs:
                    add_emails_from_source(h_input.get_attribute("value") or "")
            
            except StaleElementReferenceException:
                continue
    
    except WebDriverException as e:
        logger.debug(f"Error processing forms on {url}: {e}")
    
    # Fallback for comments/hidden text in the raw source
    if page_html_source:
        add_emails_from_source(page_html_source) 

    logger.debug(f"Selenium extracted {len(extracted_emails)} unique email instances from {url}")
    return list(extracted_emails), page_html_source

def requests_extract_page_emails(
    url: str,
    is_contact_page: bool,
    debug: bool = False
) -> Tuple[List[str], Optional[str]]:
    """
    Extract unique emails and page source from a single page using requests.
    
    Args:
        url: URL to extract emails from
        is_contact_page: Whether this is a contact page
        debug: Enable debug logging
        
    Returns:
        Tuple of (list of emails, page HTML source)
    """
    logger = logging.getLogger("email_scraper")
    extracted_emails: Set[str] = set()
    html_content: Optional[str] = None
    
    try:
        import urllib.parse
        
        headers = {
            "User-Agent": random.choice(UA_POOL),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": urllib.parse.urlparse(url).scheme + "://" + urllib.parse.urlparse(url).netloc if urllib.parse.urlparse(url).netloc else url,
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1"
        }
        
        logger.debug(f"Requests GET: {url}")
        resp = requests.get(url, timeout=15, headers=headers, allow_redirects=True)
        resp.raise_for_status()
        
        content_type = resp.headers.get('Content-Type', '').lower()
        if 'text/html' not in content_type:
            logger.debug(f"Skipping non-HTML content type '{content_type}' for {url} via requests")
            return [], None
        
        html_content = resp.text
        if not html_content:
            logger.warning(f"Empty content received from {url} via requests")
            return [], None
        
        soup = BeautifulSoup(html_content, "html.parser")
        
        def add_emails_from_source_req(text_source: str):
            if text_source:
                for email_item in emails_from_text(text_source):
                    extracted_emails.add(email_item)
        
        # Extract from text content
        add_emails_from_source_req(soup.get_text(separator=' '))
        
        # Extract from mailto links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            if href.startswith('mailto:'):
                add_emails_from_source_req(href.split('?')[0].replace('mailto:', '', 1))
        
        # Extract from meta tags
        for meta_tag in soup.find_all('meta', content=True):
            content_attr = meta_tag.get('content', '')
            if "@" in content_attr:
                add_emails_from_source_req(content_attr)
        
        # Extract from full source
        add_emails_from_source_req(html_content)
        
        logger.debug(f"Requests extracted {len(extracted_emails)} unique email instances from {url}")
    
    except requests.exceptions.Timeout:
        logger.warning(f"Requests timeout for {url}")
    except requests.exceptions.RequestException as e:
        logger.warning(f"Requests error for {url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in requests_extract_page_emails for {url}: {e}", exc_info=debug)
    
    return list(extracted_emails), html_content
