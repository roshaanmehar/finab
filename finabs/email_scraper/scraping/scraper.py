"""
scraper.py - Core scraping functionality
---------------------------------------
Functions for scraping email addresses from websites.
"""
import logging
import random
import time
import urllib.parse
from typing import List, Set, Tuple, Optional, Dict, Any

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

from email_scraper.utils.url_utils import normalize_url, get_domain
from email_scraper.utils.email_utils import clean_emails
from email_scraper.utils.circuit_breaker import CircuitBreaker
from email_scraper.scraping.browser_manager import is_driver_alive
from email_scraper.scraping.email_extractor import selenium_extract_page_emails, requests_extract_page_emails
from email_scraper.config import CONTACT_PATHS, WEBSITE_WAIT_MIN, WEBSITE_WAIT_MAX, CONTACT_WAIT_MIN, CONTACT_WAIT_MAX

# Global circuit breaker instance
circuit_breaker = CircuitBreaker()

def rdelay(min_sec: float, max_sec: float):
    """Random delay between min_sec and max_sec seconds."""
    time.sleep(random.uniform(min_sec, max_sec))

def harvest_site_emails(
    site_url: str,
    business_name: str,
    driver: webdriver.Chrome,
    debug: bool = False
) -> Tuple[List[str], str, Optional[str]]:
    """
    Harvest email addresses from a website.
    
    Args:
        site_url: Website URL
        business_name: Business name
        driver: Selenium WebDriver
        debug: Enable debug logging
        
    Returns:
        Tuple of (list of emails, status, error message)
    """
    logger = logging.getLogger("email_scraper")
    final_status = "checked_no_email"  # Default status
    error_message: Optional[str] = None
    all_found_emails: Set[str] = set()
    
    # Flag to track if main domain is accessible
    main_domain_accessible = True

    # Validate URL
    if not site_url or site_url.lower() == "n/a":
        return [], "skipped_invalid_url", "Invalid or N/A URL provided"
    
    normalized_site_url = normalize_url(site_url)
    if not normalized_site_url:
        return [], "skipped_invalid_url", f"Invalid URL format: {site_url}"
    
    domain = get_domain(normalized_site_url)
    if not domain:
        return [], "skipped_bad_domain", f"Could not parse domain from URL: {normalized_site_url}"
    
    if circuit_breaker.is_open(domain):
        return [], "failed_circuit_breaker", f"Circuit breaker open for domain {domain}"

    # Attempt 1: Requests on main page (quick check)
    try:
        logger.debug(f"[{domain}] Trying requests method for main page: {normalized_site_url}")
        req_emails, _ = requests_extract_page_emails(normalized_site_url, False, debug)
        for email in req_emails:
            all_found_emails.add(email)
    except Exception as e:
        logger.warning(f"[{domain}] Requests method failed for main page {normalized_site_url}: {e}")
        # Don't set main_domain_accessible to False here, as some sites block requests but work with Selenium

    # Attempt 2: Selenium on main page (primary method)
    selenium_main_page_html: Optional[str] = None
    try:
        logger.debug(f"[{domain}] Trying Selenium method for main page: {normalized_site_url}")
        # Set a timeout for the entire operation
        import threading
        from functools import partial
        
        # Function to execute with timeout
        def selenium_with_timeout():
            nonlocal selenium_main_page_html, all_found_emails
            try:
                sel_main_emails, selenium_main_page_html = selenium_extract_page_emails(driver, normalized_site_url, False, debug)
                for email in sel_main_emails:
                    all_found_emails.add(email)
                return True
            except Exception as e:
                logger.warning(f"[{domain}] Error in selenium_with_timeout: {e}")
                return False
        
        # Execute with timeout
        selenium_thread = threading.Thread(target=selenium_with_timeout)
        selenium_thread.daemon = True
        selenium_thread.start()
        selenium_thread.join(30)  # 30 second timeout for entire operation
        
        if selenium_thread.is_alive():
            logger.warning(f"[{domain}] Selenium operation timed out after 30 seconds for {normalized_site_url}")
            # Try to interrupt the thread by refreshing the page
            try:
                driver.refresh()
            except:
                pass
            # Mark as timeout error
            error_message = "Selenium main page timeout after 30 seconds"
            main_domain_accessible = False  # Mark domain as inaccessible
        else:
            circuit_breaker.record_success(domain)  # If Selenium main page load succeeds
            
    except WebDriverException as e:
        logger.warning(f"[{domain}] Selenium failed on main page {normalized_site_url}: {e}")
        circuit_breaker.record_failure(domain)
        error_message = f"Selenium main page error: {type(e).__name__}"
        
        # Check for specific error types that indicate domain issues
        error_str = str(e).lower()
        if any(err in error_str for err in [
            "err_name_not_resolved", 
            "err_connection_refused", 
            "err_connection_timed_out",
            "err_ssl_protocol_error",
            "err_connection_reset",
            "err_address_unreachable"
        ]):
            logger.error(f"[{domain}] Domain appears to be unreachable: {error_str}")
            main_domain_accessible = False  # Don't try contact pages if main domain doesn't resolve
        
        if not is_driver_alive(driver):  # Critical if driver died
            logger.error(f"[{domain}] Driver died after Selenium main page failure. Cannot continue for this site.")
            return list(all_found_emails), "failed_driver_dead", error_message  # Return emails found so far
    except Exception as e:
        logger.error(f"[{domain}] Unexpected error during Selenium main page processing for {normalized_site_url}: {e}", exc_info=debug)
        circuit_breaker.record_failure(domain)
        error_message = f"Unexpected Selenium main page error: {type(e).__name__}"
        main_domain_accessible = False  # Be conservative and don't try contact pages
        
    # Early success check - if we already found enough emails, skip contact pages
    if len(all_found_emails) >= 3:
        logger.info(f"[{domain}] Already found {len(all_found_emails)} emails from main page, skipping contact pages")
        return clean_emails(list(all_found_emails)), "found", None

    # Attempt 3: Selenium on Contact Pages - ONLY if main domain was accessible
    if main_domain_accessible and (len(all_found_emails) < 2 or selenium_main_page_html is not None):
        logger.debug(f"[{domain}] Checking contact pages (current emails: {len(all_found_emails)})...")
        processed_contact_paths = set()
        contact_links_from_main = set()  # For dynamically found contact links

        # Try to find contact links from the main page HTML (if available)
        if selenium_main_page_html:
            soup_main = BeautifulSoup(selenium_main_page_html, "html.parser")
            for a_tag in soup_main.find_all('a', href=True):
                href_text = (a_tag.get_text(strip=True) or "").lower()
                href_val = a_tag.get('href', "")
                # Keywords to identify contact-like links
                if any(cp_keyword in href_text for cp_keyword in ["contact", "about", "imprint", "legal", "support", "connect"]) and \
                   href_val and not href_val.startswith(("#", "mailto:", "tel:")):
                    try:
                        abs_contact_link = urllib.parse.urljoin(normalized_site_url, href_val)
                        if get_domain(abs_contact_link) == domain:  # Stay on the same domain
                             path_part = urllib.parse.urlparse(abs_contact_link).path
                             if path_part and path_part not in CONTACT_PATHS and path_part != "/":  # Add new, valid paths
                                contact_links_from_main.add(path_part)
                    except:
                        pass  # Ignore errors forming URL
        
        dynamic_contact_paths = list(CONTACT_PATHS) + sorted(list(contact_links_from_main))

        for path_suffix in dynamic_contact_paths:
            if not path_suffix or path_suffix == '/' or path_suffix in processed_contact_paths:
                continue
            
            processed_contact_paths.add(path_suffix)
            
            contact_url = urllib.parse.urljoin(normalized_site_url, path_suffix)
            if get_domain(contact_url) != domain:  # Ensure still on the same primary domain
                logger.debug(f"[{domain}] Skipping contact URL on different domain: {contact_url}")
                continue

            logger.debug(f"[{domain}] Checking contact page: {contact_url}")
            try:
                if not is_driver_alive(driver):
                    logger.error(f"[{domain}] Driver died before Selenium contact page attempt for {contact_url}")
                    error_message = error_message or "Driver died before contact page"  # Preserve earlier error if any
                    final_status = "failed_driver_dead"
                    break  # Stop checking contact pages
                
                sel_contact_emails, _ = selenium_extract_page_emails(driver, contact_url, True, debug)
                for email in sel_contact_emails:
                    all_found_emails.add(email)
                
                if len(all_found_emails) >= 3:  # Heuristic: Stop if we have a few emails
                    logger.debug(f"[{domain}] Found {len(all_found_emails)} emails, stopping contact page search.")
                    break
                
                rdelay(CONTACT_WAIT_MIN, CONTACT_WAIT_MAX)  # Delay between contact page checks
            
            except WebDriverException as e:
                logger.warning(f"[{domain}] Selenium failed on contact page {contact_url}: {e}")
                
                # Check if this is a domain resolution issue
                error_str = str(e).lower()
                if any(err in error_str for err in [
                    "err_name_not_resolved", 
                    "err_connection_refused", 
                    "err_connection_timed_out"
                ]):
                    logger.warning(f"[{domain}] Domain resolution issue on contact page, stopping further contact page checks")
                    break  # Stop trying more contact pages if we hit a resolution error
                
                if not is_driver_alive(driver):
                    logger.error(f"[{domain}] Driver died during contact page processing for {contact_url}")
                    circuit_breaker.record_failure(domain)  # Record driver death as domain failure
                    error_message = error_message or "Driver died on contact page"
                    final_status = "failed_driver_dead"
                    break
            
            except Exception as e:
                logger.error(f"[{domain}] Unexpected error processing contact page {contact_url}: {e}", exc_info=debug)
    else:
        if not main_domain_accessible:
            logger.info(f"[{domain}] Main domain not accessible, skipping contact page checks")
        else:
            logger.debug(f"[{domain}] Skipping contact pages (emails: {len(all_found_emails)}, main page HTML: {'available' if selenium_main_page_html else 'not available'})")

    # Final email cleaning and status determination
    cleaned_emails_list = clean_emails(list(all_found_emails))

    if cleaned_emails_list:
        final_status = "found"
        logger.info(f"[{domain}] SUCCESS for {normalized_site_url}: Found {len(cleaned_emails_list)} emails.")
        for i, email_addr in enumerate(cleaned_emails_list):
            logger.info(f"[{domain}]   Email {i+1}: {email_addr}")  # Log each found email
    elif not final_status.startswith("failed"):  # Don't override critical failure status
        if not main_domain_accessible:
            final_status = "failed_domain_unreachable"
            logger.info(f"[{domain}] FAILED: Domain unreachable for {normalized_site_url}.")
        else:
            final_status = "checked_no_email"
            logger.info(f"[{domain}] CHECKED: No emails found for {normalized_site_url}.")
    
    # Update circuit breaker based on final outcome for this site
    if final_status.startswith("failed"): 
        circuit_breaker.record_failure(domain)  # Ensure failure is recorded if status indicates it
    elif final_status == "found" or final_status == "checked_no_email":
        # If it was marked failed by an earlier step but emails were found or checked without driver death,
        # consider it a success for circuit breaker purposes.
        circuit_breaker.record_success(domain) 
    
    return cleaned_emails_list, final_status, error_message

def process_business_record(
    record_data: Dict[str, Any],
    collection,
    headless_mode: bool,
    debug_mode: bool,
    shutdown_flag: bool = False
) -> Tuple[str, str, int]:
    """
    Process a single business record.
    
    Args:
        record_data: Business record data
        collection: MongoDB collection
        headless_mode: Whether to run Chrome in headless mode
        debug_mode: Enable debug logging
        shutdown_flag: Whether shutdown has been requested
        
    Returns:
        Tuple of (record ID, status, number of emails found)
    """
    logger = logging.getLogger("email_scraper")
    
    business_id = record_data.get('_id')
    website_url = record_data.get('website')
    business_name = record_data.get('businessname', 'Unknown Business')
    logger.info(f"Processing record ID: {business_id}, Name: {business_name}, Website: {website_url}")

    if shutdown_flag:  # Check before starting intensive work
        logger.warning(f"Shutdown requested. Skipping processing for {business_name} ({business_id}).")
        return str(business_id), "skipped_shutdown", 0

    from email_scraper.scraping.browser_manager import make_driver
    from email_scraper.db_management.db_operations import update_record_with_email_results

    driver_instance = None
    scrape_emails: List[str] = []
    scrape_status = "failed_unexpected"  # Default to failure
    scrape_error_msg: Optional[str] = "Initialization error"

    try:
        logger.debug(f"[{business_name}] Creating new WebDriver instance...")
        driver_instance = make_driver(headless_mode, debug_mode)
        
        if driver_instance is None:
            logger.error(f"[{business_name}] Failed to create WebDriver. Marking as failed.")
            scrape_status, scrape_error_msg = "failed_driver_creation", "WebDriver creation failed"
            domain_for_cb = get_domain(normalize_url(website_url or ""))  # Get domain for CB
            if domain_for_cb:
                circuit_breaker.record_failure(domain_for_cb)
        else:
            scrape_emails, scrape_status, scrape_error_msg = harvest_site_emails(
                website_url, business_name, driver_instance, debug_mode
            )
    
    except Exception as e:
        logger.error(f"[{business_name}] Critical error processing record {business_id} for site {website_url}: {e}", exc_info=debug_mode)
        scrape_status, scrape_error_msg = "failed_exception_in_worker", f"Worker exception: {type(e).__name__} - {str(e)[:100]}"
        domain_for_cb = get_domain(normalize_url(website_url or ""))  # Get domain for CB
        if domain_for_cb:
            circuit_breaker.record_failure(domain_for_cb)
    
    finally:
        if driver_instance:
            logger.debug(f"[{business_name}] Closing WebDriver instance.")
            try:
                driver_instance.quit()
            except WebDriverException as e:
                logger.warning(f"[{business_name}] Error quitting WebDriver: {e}")
            except Exception as e:
                logger.error(f"[{business_name}] Unexpected error quitting WebDriver: {e}", exc_info=debug_mode)
        
        # Update MongoDB record with results/status
        logger.debug(f"[{business_name}] Updating DB for ID {business_id} with status: {scrape_status}")
        update_record_with_email_results(collection, business_id, scrape_status, scrape_emails, scrape_error_msg)

    return str(business_id), scrape_status, len(scrape_emails)
