"""
scraper.py - Core scraping functionality
---------------------------------------
Functions for scraping business data from Google Maps.
"""
import logging
import random
import re
import time
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional, Set, Callable
import inspect

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
    ElementClickInterceptedException,
    ElementNotInteractableException
)
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from googlemaps_scraper.db_management.db_operations import insert_record
from googlemaps_scraper.utils.config import (
    SEARCH_DELAY_MIN, SEARCH_DELAY_MAX,
    CLICK_WAIT_MIN, CLICK_WAIT_MAX,
    CLOSE_WAIT_MIN, CLOSE_WAIT_MAX,
    SCROLL_WAIT_MIN, SCROLL_WAIT_MAX,
    PHONE_WAIT_TIME, ADDRESS_WAIT_TIME, WEBSITE_WAIT_TIME,
    MAX_SCROLL_ATTEMPTS, RESULT_LIMIT, MAX_STALE_RETRIES,
    PAGE_REFRESH_THRESHOLD, DRIVER_RESET_THRESHOLD,
    NAME_CSS, NAME_XPATH, RATING_CSS, RATING_XPATH,
    REVIEWS_CSS, REVIEWS_XPATH, ADDRESS_SELECTORS,
    WEBSITE_SELECTORS, PHONE_SELECTORS, TILE_NAME_CSS,
    FALLBACK_NAME, FALLBACK_STARS, FALLBACK_REVIEWS,
    CARD_PROCESSING_DELAY
)
from googlemaps_scraper.utils.logging_config import ARROW

def rdelay(a: float, b: float, fast_mode: bool = False):
    """Random delay with option for fast mode"""
    if fast_mode:
        time.sleep(random.uniform(a * 0.5, b * 0.5))  # 50% faster in fast mode
    else:
        time.sleep(random.uniform(a, b))

def digits(text: str) -> str:
    """Extract digits from text."""
    return re.sub(r"\D", "", text or "")

def safe_text_with_fallbacks(driver: webdriver.Chrome, css: str, xpath: str, fallback: str = None) -> str:
    """Try to get text using CSS, then XPath, then fallback selector."""
    for attempt in range(MAX_STALE_RETRIES):
        try:
            try:
                return driver.find_element(By.CSS_SELECTOR, css).text.strip()
            except NoSuchElementException:
                try:
                    return driver.find_element(By.XPATH, xpath).text.strip()
                except NoSuchElementException:
                    if fallback:
                        try:
                            return driver.find_element(By.CSS_SELECTOR, fallback).text.strip()
                        except NoSuchElementException:
                            return ""
                    return ""
        except StaleElementReferenceException:
            if attempt < MAX_STALE_RETRIES - 1:
                time.sleep(0.5)
                continue
            else:
                return ""
        except Exception:
            return ""
    return ""

def get_tile_name(tile) -> str:
    """Extract the name from a tile element before clicking it."""
    for attempt in range(MAX_STALE_RETRIES):
        try:
            name_element = tile.find_element(By.CSS_SELECTOR, TILE_NAME_CSS)
            return name_element.text.strip()
        except (NoSuchElementException, StaleElementReferenceException):
            if attempt < MAX_STALE_RETRIES - 1:
                time.sleep(0.5)
                continue
            else:
                return ""
    return ""

def extract_address(driver: webdriver.Chrome, debug: bool = False) -> str:
    """Extract address using multiple selectors and methods."""
    log = logging.getLogger("googlemaps_scraper")
    
    # Always log that we're starting extraction
    log.debug("Starting extraction of address")
    
    # Wait a moment for address to load
    time.sleep(ADDRESS_WAIT_TIME)
    
    # Try all selectors in order
    for selector in ADDRESS_SELECTORS:
        try:
            # Determine if it's XPath or CSS
            by_method = By.XPATH if selector.startswith('/') else By.CSS_SELECTOR
            
            elements = driver.find_elements(by_method, selector)
            for element in elements:
                text = element.text.strip()
                if debug:
                    log.debug("Found potential address element: %s", text)
                
                # Check if text looks like an address (contains numbers and letters)
                if re.search(r'\d', text) and len(text) > 5:
                    if debug:
                        log.debug("Extracted address: %s", text)
                    return text
        except Exception as e:
            if debug:
                log.debug("Error with address selector %s: %s", selector, e)
            continue
    
    # If we still don't have an address, try JavaScript
    try:
        address = driver.execute_script("""
            // Try to find address elements
            var addressElements = [
                // Buttons with address
                ...Array.from(document.querySelectorAll('button[data-item-id="address"] div.Io6YTe')),
                ...Array.from(document.querySelectorAll('button[aria-label*="address"] div.Io6YTe')),
                // Any element with address text
                ...Array.from(document.querySelectorAll('div.Io6YTe.fontBodyMedium')),
                // Any element with a location icon
                ...Array.from(document.querySelectorAll('div.Io6YTe'))
            ];
            
            for (let el of addressElements) {
                if (el && el.textContent && el.textContent.trim().length > 5 && /\\d/.test(el.textContent)) {
                    return el.textContent.trim();
                }
            }
            
            return "";
        """)
        
        if address:
            if debug:
                log.debug("Extracted address via JavaScript: %s", address)
            return address
    except Exception as e:
        if debug:
            log.debug("JavaScript address extraction error: %s", e)
    
    log.debug("No address found")
    return "N/A"

def extract_website(driver: webdriver.Chrome, business_name: str, debug: bool = False) -> str:
    """Extract website URL using multiple selectors and methods."""
    log = logging.getLogger("googlemaps_scraper")
    
    # Always log that we're starting extraction
    log.debug(f"Starting extraction of website for business: {business_name}")
    
    # Wait a moment for website to load
    time.sleep(WEBSITE_WAIT_TIME)
    
    # Try all selectors in order
    for selector in WEBSITE_SELECTORS:
        try:
            # Determine if it's XPath or CSS
            by_method = By.XPATH if selector.startswith('/') else By.CSS_SELECTOR
            
            elements = driver.find_elements(by_method, selector)
            for element in elements:
                # First try to get href attribute if it's an anchor
                if element.tag_name == 'a':
                    href = element.get_attribute('href')
                    if href and 'google.com' not in href and href.startswith('http'):
                        if debug:
                            log.debug(f"Extracted website URL from href for {business_name}: {href}")
                        log.info(f"WEBSITE-BUSINESS MAPPING: Found website {href} for business {business_name}")
                        return href
                
                # Then try to get text
                text = element.text.strip()
                if debug:
                    log.debug(f"Found potential website element for {business_name}: {text}")
                
                # Check if text looks like a website URL
                if text and ('.' in text) and ('http' in text or 'www' in text):
                    if debug:
                        log.debug(f"Extracted website from text for {business_name}: {text}")
                    log.info(f"WEBSITE-BUSINESS MAPPING: Found website {text} for business {business_name}")
                    return text
        except Exception as e:
            if debug:
                log.debug(f"Error with website selector {selector} for {business_name}: {e}")
            continue
    
    # If we still don't have a website, try JavaScript
    try:
        website = driver.execute_script("""
            // Try to find website elements
            var websiteElements = [
                // Direct website links
                ...Array.from(document.querySelectorAll('a[data-item-id="authority"]')),
                ...Array.from(document.querySelectorAll('a[aria-label*="website"]')),
                // Any external link that's not to Google
                ...Array.from(document.querySelectorAll('a[target="_blank"]'))
            ];
            
            for (let el of websiteElements) {
                if (el && el.href && el.href.startsWith('http') && !el.href.includes('google.com')) {
                    return el.href;
                }
            }
            
            // Try to find website text
            var textElements = document.querySelectorAll('div.Io6YTe.fontBodyMedium');
            for (let el of textElements) {
                if (el && el.textContent && (el.textContent.includes('http') || el.textContent.includes('www'))) {
                    return el.textContent.trim();
                }
            }
            
            return "";
        """)
        
        if website:
            if debug:
                log.debug(f"Extracted website via JavaScript for {business_name}: {website}")
            log.info(f"WEBSITE-BUSINESS MAPPING: Found website {website} for business {business_name} via JavaScript")
            return website
    except Exception as e:
        if debug:
            log.debug(f"JavaScript website extraction error for {business_name}: {e}")
    
    log.debug(f"No website found for {business_name}")
    log.info(f"WEBSITE-BUSINESS MAPPING: No website found for business {business_name}")
    return "N/A"

def extract_phone_number(driver: webdriver.Chrome, business_name: str, debug: bool = False) -> Optional[int]:
    """Extract phone number using optimized selectors."""
    log = logging.getLogger("googlemaps_scraper")
    
    # Always log that we're starting extraction
    log.debug(f"Starting extraction of phone number for business: {business_name}")
    
    # Wait a moment for phone to load
    time.sleep(PHONE_WAIT_TIME)
    
    # Try the most reliable selectors first
    for selector in PHONE_SELECTORS:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                if debug:
                    log.debug(f"Found potential phone element for {business_name}: {text}")
                
                # Check if text looks like a phone number
                if re.search(r'\d', text):  # Contains at least one digit
                    digits_only = digits(text)
                    if digits_only and len(digits_only) >= 5:  # Reasonable phone number length
                        if debug:
                            log.debug(f"Extracted phone digits for {business_name}: {digits_only}")
                        return int(digits_only)
        except Exception as e:
            if debug:
                log.debug(f"Error with selector {selector} for {business_name}: {e}")
            continue
    
    # If we still don't have a phone number, try JavaScript
    try:
        phone_text = driver.execute_script("""
            // Try to find phone elements
            var phoneElements = [
                // Direct phone buttons
                ...Array.from(document.querySelectorAll('button[data-item-id="phone:tel"] div.Io6YTe')),
                ...Array.from(document.querySelectorAll('button[aria-label*="phone"] div.Io6YTe')),
                // Any element that might contain a phone number
                ...Array.from(document.querySelectorAll('div.Io6YTe.fontBodyMedium'))
            ];
            
            for (let el of phoneElements) {
                if (el && el.textContent && /\\d/.test(el.textContent)) {
                    return el.textContent.trim();
                }
            }
            
            return "";
        """)
        
        if phone_text:
            digits_only = digits(phone_text)
            if digits_only and len(digits_only) >= 5:
                if debug:
                    log.debug(f"Extracted phone via JavaScript for {business_name}: {digits_only}")
                return int(digits_only)
    except Exception as e:
        if debug:
            log.debug(f"JavaScript phone extraction error for {business_name}: {e}")
    
    log.debug(f"No phone number found for {business_name}")
    return None

def dismiss_banners(driver: webdriver.Chrome):
    """Close GDPR or consent banners if present."""
    log = logging.getLogger("googlemaps_scraper")
    
    for label in ("Reject all", "Accept all", "I agree", "Dismiss"):
        try:
            btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, f'button[aria-label="{label}"]')
                )
            )
            btn.click()
            log.debug("✕ dismissed popup (%s)", label)
            return
        except TimeoutException:
            continue

def check_end_of_results(driver: webdriver.Chrome) -> bool:
    """Check if we've reached the end of search results."""
    log = logging.getLogger("googlemaps_scraper")
    
    try:
        # Look for "end of list" indicators
        end_markers = [
            "You've reached the end of the list",
            "No more results",
            "End of results",
            "No additional results found"
        ]
        
        for marker in end_markers:
            try:
                if driver.find_element(By.XPATH, f"//*[contains(text(), '{marker}')]"):
                    log.info("Detected end of results marker: %s", marker)
                    return True
            except NoSuchElementException:
                continue
        
        # Check if the scrollable feed has reached its maximum scroll position
        try:
            feed = driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
            scroll_height = driver.execute_script("return arguments[0].scrollHeight", feed)
            scroll_top = driver.execute_script("return arguments[0].scrollTop", feed)
            client_height = driver.execute_script("return arguments[0].clientHeight", feed)
            
            # If we're at the bottom of the scrollable area
            if scroll_top + client_height >= scroll_height - 10:  # 10px margin for rounding errors
                log.info("Reached end of scrollable feed (scroll_top: %d, client_height: %d, scroll_height: %d)",
                         scroll_top, client_height, scroll_height)
                return True
        except Exception as e:
            log.debug("Error checking scroll position: %s", e)
        
        return False
    except Exception as e:
        log.debug("Error checking for end of results: %s", e)
        return False

def scroll_results_feed(driver: webdriver.Chrome, code: str) -> Tuple[int, bool]:
    """
    Scroll the results feed down in a controlled, sequential manner.
    
    Args:
        driver: Selenium WebDriver
        code: Subsector code for logging
        
    Returns:
        Tuple of (new_tile_count, scroll_successful)
    """
    log = logging.getLogger("googlemaps_scraper")
    
    # Use a more consistent scroll distance for predictable results
    scroll_distance = 300  # Fixed distance for more predictable scrolling
    
    for attempt in range(MAX_STALE_RETRIES):
        try:
            # Try to find the feed element
            try:
                feed = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"]'))
                )
            except TimeoutException:
                # If we can't find the feed, try to scroll the whole page
                driver.execute_script(f"""
                    window.scrollBy({{
                        top: {scroll_distance},
                        left: 0,
                        behavior: 'smooth'
                    }});
                """)
                time.sleep(1.0)  # Consistent wait time
                return len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")), True
            
            # Get current scroll position before scrolling
            current_scroll_position = driver.execute_script("return arguments[0].scrollTop", feed)
            
            # Scroll in a single smooth motion
            driver.execute_script(f"""
                arguments[0].scrollBy({{
                    top: {scroll_distance},
                    left: 0,
                    behavior: 'smooth'
                }});
            """, feed)
            
            # Wait a consistent amount of time for content to load
            time.sleep(1.5)
            
            # Check if we actually scrolled
            new_scroll_position = driver.execute_script("return arguments[0].scrollTop", feed)
            
            # If we didn't scroll much, try a slightly different approach
            if new_scroll_position - current_scroll_position < 50:  # Less than 50px movement
                # Try a direct scrollTop assignment
                driver.execute_script(f"""
                    arguments[0].scrollTop = {current_scroll_position + scroll_distance};
                """, feed)
                time.sleep(1.0)
                
                # Check again if we scrolled
                final_scroll_position = driver.execute_script("return arguments[0].scrollTop", feed)
                if final_scroll_position - current_scroll_position < 50:
                    log.debug("%s %s Scroll didn't move feed position significantly", code, ARROW)
                    return len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")), False
            
            # Count tiles
            count = len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))
            log.info("%s %s scrolled feed (tiles now %d)", code, ARROW, count)
            return count, True
            
        except StaleElementReferenceException:
            if attempt < MAX_STALE_RETRIES - 1:
                log.debug("%s %s Stale element during scroll, retrying", code, ARROW)
                time.sleep(1)
                continue
            else:
                # If we keep getting stale elements, try JavaScript scrolling
                try:
                    driver.execute_script(f"""
                        var feeds = document.querySelectorAll('div[role="feed"]');
                        if (feeds.length > 0) {{
                            feeds[0].scrollTop += {scroll_distance};
                        }} else {{
                            window.scrollBy(0, {scroll_distance});
                        }}
                    """)
                    time.sleep(1.0)
                    return len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")), True
                except Exception as e:
                    log.error("%s %s JavaScript scroll error: %s", code, ARROW, e)
                    return 0, False
        except Exception as e:
            log.error("%s %s scroll error: %s", code, ARROW, e)
            return 0, False
    
    return 0, False

def get_tile_position(driver: webdriver.Chrome, tile) -> int:
    """Get the vertical position of a tile element."""
    try:
        return driver.execute_script("return arguments[0].getBoundingClientRect().top", tile)
    except Exception:
        return 0

def get_tile_identifier(driver: webdriver.Chrome, tile) -> str:
    """
    Get a unique identifier for a tile based on persistent attributes.
    This helps track which tiles have been processed.
    """
    try:
        # Get the business name
        name = get_tile_name(tile)
        
        # Try to get persistent data attributes
        data_cid = tile.get_attribute("data-cid") or ""
        data_index = tile.get_attribute("data-result-index") or ""
        data_item_id = tile.get_attribute("data-item-id") or ""
        
        # If we have any persistent attributes, use them
        if data_cid or data_index or data_item_id:
            return f"{name}|{data_cid}|{data_index}|{data_item_id}"
        
        # Fallback to using the element's HTML content as a fingerprint
        # This is more stable than position which changes with scrolling
        inner_html = driver.execute_script("""
            var html = arguments[0].innerHTML;
            // Limit length to avoid excessive memory usage
            return html.substring(0, 100);
        """, tile)
        
        # Create a hash of the inner HTML for a more compact identifier
        html_hash = hash(inner_html)
        
        return f"{name}|{html_hash}"
    except Exception as e:
        # If all else fails, just use the name
        try:
            return get_tile_name(tile)
        except:
            return f"unknown_tile_{time.time()}"

def safe_click_tile(driver: webdriver.Chrome, tile, code: str, tile_idx: int, total_tiles: int) -> bool:
    """Safely click a tile with improved reliability."""
    log = logging.getLogger("googlemaps_scraper")
    
    # Always log which tile we're trying to click
    tile_name = get_tile_name(tile)
    log.info("%s %s Clicking tile %d/%d: %s", code, ARROW, tile_idx + 1, total_tiles, tile_name)
    
    for attempt in range(MAX_STALE_RETRIES):
        try:
            # First make sure the tile is in view
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tile)
            time.sleep(0.5)
            
            # Check if the tile is still visible and valid before clicking
            try:
                # This will throw an exception if the element is stale or not visible
                if not tile.is_displayed():
                    log.debug("%s %s Tile is not displayed, skipping", code, ARROW)
                    return False
            except Exception:
                if attempt < MAX_STALE_RETRIES - 1:
                    log.debug("%s %s Tile visibility check failed, retrying", code, ARROW)
                    time.sleep(0.5)
                    continue
                else:
                    return False
            
            # Try direct click first
            try:
                WebDriverWait(driver, 3).until(EC.element_to_be_clickable(tile))
                tile.click()
                log.debug("%s %s Successfully clicked tile with direct click", code, ARROW)
                return True
            except (ElementClickInterceptedException, ElementNotInteractableException):
                # If direct click fails, try JavaScript click
                log.debug("%s %s Direct click failed, trying JavaScript click", code, ARROW)
                driver.execute_script("arguments[0].click();", tile)
                return True
                
        except StaleElementReferenceException:
            if attempt < MAX_STALE_RETRIES - 1:
                log.debug("%s %s Stale element, retrying click (%d/%d)", 
                         code, ARROW, attempt + 1, MAX_STALE_RETRIES)
                time.sleep(0.5)
            else:
                log.warning("%s %s Stale element, max retries reached for tile: %s", code, ARROW, tile_name)
                return False
        except Exception as e:
            log.warning("%s %s Click error: %s", code, ARROW, e)
            if attempt < MAX_STALE_RETRIES - 1:
                time.sleep(0.5)
            else:
                return False
    
    return False

def safe_close_card(driver: webdriver.Chrome) -> bool:
    """Safely close the card with retry logic."""
    log = logging.getLogger("googlemaps_scraper")
    
    for attempt in range(MAX_STALE_RETRIES):
        try:
            actions = ActionChains(driver)
            actions.send_keys(Keys.ESCAPE).perform()
            return True
        except Exception as e:
            if attempt < MAX_STALE_RETRIES - 1:
                log.debug("Error closing card, retrying: %s", e)
                time.sleep(0.5)
            else:
                log.debug("Failed to close card after %d attempts: %s", MAX_STALE_RETRIES, e)
                # Try JavaScript fallback
                try:
                    driver.execute_script("""
                        document.querySelectorAll('button[aria-label="Back"]').forEach(b => b.click());
                        document.querySelectorAll('button[jsaction*="close"]').forEach(b => b.click());
                        document.dispatchEvent(new KeyboardEvent('keydown', {'key': 'Escape'}));
                    """)
                    return True
                except:
                    return False
    
    return False

def get_unprocessed_tiles(driver: webdriver.Chrome, processed_tile_ids: Set[str], code: str) -> List[Tuple[Any, str]]:
    """
    Get all visible tiles that haven't been processed yet, sorted by vertical position.
    
    Args:
        driver: Selenium WebDriver
        processed_tile_ids: Set of already processed tile IDs
        code: Subsector code for logging
        
    Returns:
        List of tuples (tile_element, tile_id) sorted by vertical position
    """
    log = logging.getLogger("googlemaps_scraper")
    
    try:
        # Get all visible tiles
        all_tiles = driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
        log.debug("%s %s Found %d total visible tiles", code, ARROW, len(all_tiles))
        
        # Filter out already processed tiles and get positions
        unprocessed_tiles = []
        for tile in all_tiles:
            try:
                tile_id = get_tile_identifier(driver, tile)
                if tile_id not in processed_tile_ids:
                    position = get_tile_position(driver, tile)
                    unprocessed_tiles.append((tile, tile_id, position))
            except Exception as e:
                log.debug("%s %s Error processing tile: %s", code, ARROW, e)
                continue
        
        # Sort by vertical position (top to bottom)
        unprocessed_tiles.sort(key=lambda x: x[2])
        
        # Return just the tile elements and their IDs
        result = [(t[0], t[1]) for t in unprocessed_tiles]
        log.info("%s %s Found %d unprocessed tiles", code, ARROW, len(result))
        return result
    except Exception as e:
        log.error("%s %s Error getting unprocessed tiles: %s", code, ARROW, e)
        return []

def scrape_subsector(
    doc: dict, driver: webdriver.Chrome, rest_col, service: str, city: str,
    debug: bool = False, fast_mode: bool = False,
    termination_check: Optional[Callable[[], bool]] = None
) -> Tuple[List[dict], int]:
    """
    Scrape a subsector for business data.
    
    Args:
        doc: Subsector document from MongoDB
        driver: Selenium WebDriver
        rest_col: MongoDB collection for business data
        service: Service to search for (e.g., "restaurants in")
        city: City to search in (e.g., "leeds")
        debug: Enable debug logging
        fast_mode: Use faster scraping (less human-like)
        termination_check: Optional function to check if scraping should be terminated
        
    Returns:
        Tuple of (records, card_count)
    """
    log = logging.getLogger("googlemaps_scraper")
    
    code = doc["subsector"].strip()
    log.info("=" * 60)
    log.info("Starting scrape for subsector: %s", code)
    log.info("=" * 60)
    
    start_time = datetime.now()

    # Check for termination request
    if termination_check and termination_check():
        log.info("%s %s Termination requested before starting", code, ARROW)
        return [], 0
    
    # Override default wait times for faster processing
    global SCROLL_WAIT_MIN, SCROLL_WAIT_MAX, CLICK_WAIT_MIN, CLICK_WAIT_MAX, CLOSE_WAIT_MIN, CLOSE_WAIT_MAX
    SCROLL_WAIT_MIN, SCROLL_WAIT_MAX = 1.0, 1.5  # Faster scrolling
    CLICK_WAIT_MIN, CLICK_WAIT_MAX = 0.5, 1.0    # Faster clicking
    CLOSE_WAIT_MIN, CLOSE_WAIT_MAX = 0.3, 0.6    # Faster closing

    # 1 · open Google Maps and submit search
    try:
        driver.get("https://www.google.com/maps")
        dismiss_banners(driver)

        # Check for termination request
        if termination_check and termination_check():
            log.info("%s %s Termination requested during search setup", code, ARROW)
            return [], 0

        search_box = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "searchboxinput"))
        )
        rdelay(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX, fast_mode)
        search_box.clear()
        query = f"{service} {code} {city}"
        search_box.send_keys(query)
        rdelay(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX, fast_mode)
        search_box.send_keys(Keys.ENTER)
        log.info("%s %s search query launched: %s", code, ARROW, query)

        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.Nv2PK"))
            )
        except TimeoutException:
            log.error("%s %s No results found", code, ARROW)
            return [], 0
    except Exception as e:
        log.error("%s %s Error during search setup: %s", code, ARROW, e)
        return [], 0

    total_cards, scroll_attempts = 0, 0
    records: List[dict] = []
    
    # Track processed businesses to avoid duplicates WITHIN THIS SUBSECTOR ONLY
    # This is reset for each new subsector
    processed_businesses: Set[str] = set()
    processed_phones: Set[int] = set()
    processed_tile_ids: Set[str] = set()  # Track processed tile IDs
    
    # Track consecutive stale element errors
    consecutive_stale_errors = 0
    total_errors = 0
    
    # Track consecutive scrolls with no new data
    consecutive_no_new_data = 0
    
    # 2 · iterate visible tiles, open each card
    while total_cards < RESULT_LIMIT and scroll_attempts < MAX_SCROLL_ATTEMPTS:
        # Check for termination request more frequently
        if termination_check and termination_check():
            log.info("%s %s Termination requested during scraping loop", code, ARROW)
            return records, total_cards
            
        # Check if we need to refresh the page due to too many stale errors
        if consecutive_stale_errors >= PAGE_REFRESH_THRESHOLD:
            log.warning("%s %s Too many consecutive stale errors, skipping to next subsector", code, ARROW)
            break  # Instead of refreshing, just move to the next subsector

        # Only consider stale errors a problem if we're not making progress
        if consecutive_stale_errors >= PAGE_REFRESH_THRESHOLD and new_tiles_processed == 0:
            log.warning("%s %s Too many consecutive stale errors with no progress, skipping to next subsector", code, ARROW)
            break  # Instead of refreshing, just move to the next subsector
        elif consecutive_stale_errors >= PAGE_REFRESH_THRESHOLD:
            # If we're still making progress despite stale errors, just reset the counter
            log.info("%s %s Resetting stale error counter since we're still making progress", code, ARROW)
            consecutive_stale_errors = 0
                
        # Check if driver is still alive
        try:
            # Try to get the current URL - this will fail if the driver is dead
            driver.current_url
        except Exception:
            log.error("%s %s Driver session is no longer valid", code, ARROW)
            return records, total_cards
                
        # Check if we need to reset the driver due to too many errors
        if total_errors >= DRIVER_RESET_THRESHOLD:
            log.warning("%s %s Too many total errors, returning current results", code, ARROW)
            return records, total_cards
            
        # Check if we've reached the end of results
        if check_end_of_results(driver):
            log.info("%s %s Reached end of search results", code, ARROW)
            break
                
        # Get all unprocessed tiles, sorted by vertical position
        unprocessed_tiles = get_unprocessed_tiles(driver, processed_tile_ids, code)
            
        if not unprocessed_tiles:
            log.info("%s %s No new tiles to process, scrolling to find more", code, ARROW)
                
            # Check for termination before scrolling
            if termination_check and termination_check():
                log.info("%s %s Termination requested before scrolling", code, ARROW)
                return records, total_cards
                    
            # Scroll and check if the scroll was successful
            new_count, scroll_successful = scroll_results_feed(driver, code)
                
            # Only increment scroll_attempts if the scroll wasn't successful
            # or if we've had multiple consecutive scrolls with no new data
            if not scroll_successful:
                scroll_attempts += 1
                log.info("%s %s Scroll was not successful (attempt %d/%d)", 
                         code, ARROW, scroll_attempts, MAX_SCROLL_ATTEMPTS)
            else:
                consecutive_no_new_data += 1
                if consecutive_no_new_data >= 3:
                    scroll_attempts += 1
                    log.info("%s %s Multiple scrolls with no new data (attempt %d/%d)", 
                             code, ARROW, scroll_attempts, MAX_SCROLL_ATTEMPTS)
                
            continue
            
        # Reset consecutive no new data counter since we found tiles to process
        consecutive_no_new_data = 0
            
        # Track how many new tiles we process in this batch
        new_tiles_processed = 0
            
        for tile_idx, (tile, tile_id) in enumerate(unprocessed_tiles):
            # Check for termination request for every tile
            if termination_check and termination_check():
                log.info("%s %s Termination requested during tile processing", code, ARROW)
                return records, total_cards
                    
            if total_cards >= RESULT_LIMIT:
                break
                
            # Try to get the business name from the tile before clicking
            tile_name = get_tile_name(tile)
            log.debug("%s %s Attempting to click tile for business: %s", code, ARROW, tile_name)

            # Skip if we've already processed this business IN THIS SUBSECTOR
            if tile_name and tile_name in processed_businesses:
                log.debug("%s %s Skipping already processed business: %s", code, ARROW, tile_name)
                processed_tile_ids.add(tile_id)  # Mark this tile as processed
                continue
                    
            # Mark this tile as processed BEFORE clicking to prevent re-processing
            # if something goes wrong during processing
            processed_tile_ids.add(tile_id)
                
            # Safely click tile with retry logic
            if not safe_click_tile(driver, tile, code, tile_idx, len(unprocessed_tiles)):
                consecutive_stale_errors += 1
                total_errors += 1
                log.debug("%s %s Failed to click tile, skipping", code, ARROW)
                continue
            else:
                consecutive_stale_errors = 0  # Reset counter on success

            # Wait until card shows name using the new selectors
            try:
                # First wait for any card to load
                WebDriverWait(driver, 12).until(
                    lambda d: (
                        d.find_elements(By.CSS_SELECTOR, NAME_CSS) or 
                        d.find_elements(By.XPATH, NAME_XPATH) or
                        d.find_elements(By.CSS_SELECTOR, FALLBACK_NAME)
                    )
                )
                
                # Then extract the name from the card
                card_name = safe_text_with_fallbacks(driver, NAME_CSS, NAME_XPATH, FALLBACK_NAME)
                
                # Log the card name that was loaded
                if card_name:
                    log.debug("%s %s Card details loaded successfully for: %s", code, ARROW, card_name)
                else:
                    log.debug("%s %s Card loaded but name not found", code, ARROW)
                
            except TimeoutException:
                log.debug("%s %s Card timeout, closing", code, ARROW)
                # close incomplete card and move on
                safe_close_card(driver)
                rdelay(CLOSE_WAIT_MIN, CLOSE_WAIT_MAX, fast_mode)
                continue
            except Exception as e:
                log.debug("%s %s Error waiting for card: %s", code, ARROW, e)
                safe_close_card(driver)
                rdelay(CLOSE_WAIT_MIN, CLOSE_WAIT_MAX, fast_mode)
                total_errors += 1
                continue

            rdelay(CLICK_WAIT_MIN, CLICK_WAIT_MAX, fast_mode)

            # Extract data using the new selectors with fallbacks
            name = safe_text_with_fallbacks(driver, NAME_CSS, NAME_XPATH, FALLBACK_NAME)
                
            # Skip if we've already processed this business IN THIS SUBSECTOR
            if name in processed_businesses:
                log.debug("%s %s Skipping already processed in this subsector (by card name): %s", code, ARROW, name)
                safe_close_card(driver)
                rdelay(CLOSE_WAIT_MIN, CLOSE_WAIT_MAX, fast_mode)
                continue
                    
            stars = safe_text_with_fallbacks(driver, RATING_CSS, RATING_XPATH, FALLBACK_STARS) or "N/A"
            rev_raw = safe_text_with_fallbacks(driver, REVIEWS_CSS, REVIEWS_XPATH, FALLBACK_REVIEWS)
            reviews = int(re.sub(r"[^\d]", "", rev_raw)) if rev_raw else 0
                
            # Enhanced address extraction with multiple methods
            address = extract_address(driver, debug) or "N/A"
                
            # Enhanced website extraction with multiple methods - pass business name for better logging
            website = extract_website(driver, name, debug) or "N/A"
                
            # Enhanced phone number extraction - optimized - pass business name for better logging
            phone_int = extract_phone_number(driver, name, debug)
                
            # Skip if we've already processed this phone number IN THIS SUBSECTOR
            if phone_int and phone_int in processed_phones:
                log.debug("%s %s Skipping already processed in this subsector (by phone): %s - %s", 
                         code, ARROW, name, phone_int)
                safe_close_card(driver)
                rdelay(CLOSE_WAIT_MIN, CLOSE_WAIT_MAX, fast_mode)
                continue
                    
            if address == str(phone_int):
                address = "N/A"

            # Add detailed logging for each field extraction
            log.info("%s %s EXTRACTION: Business Name: %s", code, ARROW, name)
            log.info("%s %s EXTRACTION: Stars: %s", code, ARROW, stars)
            log.info("%s %s EXTRACTION: Reviews: %s", code, ARROW, reviews)
            log.info("%s %s EXTRACTION: Address: %s", code, ARROW, address)
            log.info("%s %s EXTRACTION: Website: %s", code, ARROW, website)
            log.info("%s %s EXTRACTION: Phone: %s", code, ARROW, phone_int if phone_int else "None")
            
            # Log the business name and website to help debug website mismatches
            log.info("%s %s BUSINESS-WEBSITE MAPPING: %s -> %s", code, ARROW, name, website)

            # Log all extracted data together for debugging
            log.info("%s %s BUSINESS DATA SUMMARY: Name='%s', Website='%s', Phone='%s', Address='%s'", 
                     code, ARROW, name, website, phone_int if phone_int else "None", 
                     address[:30] + "..." if len(address) > 30 else address)

            # Note the difference between tile name and card name for debugging
            if name != tile_name and tile_name:
                log.info("%s %s NOTE: Card name '%s' differs from tile name '%s' (expected due to UI lag)", 
                         code, ARROW, name, tile_name)

            # Create the record with all the data we've extracted for THIS business
            # This ensures that the website is correctly associated with the business
            record = {
                "subsector": code,
                "businessname": name,
                "address": address,
                "stars": stars,
                "numberofreviews": reviews,
                "website": website,  # Use the website we just extracted for THIS business
                "emailstatus": "pending" if website != "N/A" else "nowebsite",
                "email": "N/A",
                "scraped_at": datetime.now(),
                "city": city  # Add the city to the record
            }
                
            # Only add phone number if it exists
            if phone_int:
                record["phonenumber"] = phone_int
                processed_phones.add(phone_int)  # Track in this subsector
                    
            # Mark this business as processed IN THIS SUBSECTOR
            processed_businesses.add(name)
                
            # Insert record immediately after scraping into MongoDB
            log.info("%s %s Scraped: %s (phone: %s, address: %s, website: %s)", 
                     code, ARROW, name, 
                     phone_int if phone_int else "none",
                     address[:30] + "..." if len(address) > 30 else address,
                     website[:30] + "..." if len(website) > 30 else website)
                
            success = insert_record(rest_col, record)
            if success:
                records.append(record)
                total_cards += 1
                new_tiles_processed += 1
                    
                # Check termination after each successful record insertion
                if termination_check and termination_check():
                    log.info("%s %s Termination requested after record insertion", code, ARROW)
                    return records, total_cards
                    
            # Add the constant delay here before closing card
            log.info("%s %s Waiting for %s seconds before closing card", code, ARROW, CARD_PROCESSING_DELAY)
            time.sleep(CARD_PROCESSING_DELAY)  # Add fixed delay between cards
            
            # close card
            safe_close_card(driver)
            rdelay(CLOSE_WAIT_MIN, CLOSE_WAIT_MAX, fast_mode)

        # If we processed new tiles in this batch, reset scroll attempts
        if new_tiles_processed > 0:
            log.info("%s %s Processed %d new tiles in this batch", code, ARROW, new_tiles_processed)
            scroll_attempts = 0
            # Also reset consecutive stale errors since we're making progress
            consecutive_stale_errors = 0
        else:
            log.info("%s %s No new tiles processed in this batch", code, ARROW)
                
        # Check for termination before scrolling
        if termination_check and termination_check():
            log.info("%s %s Termination requested before scrolling", code, ARROW)
            return records, total_cards
                
        # Scroll to find more results
        new_count, scroll_successful = scroll_results_feed(driver, code)
            
        # If scroll wasn't successful, increment scroll attempts
        if not scroll_successful:
            scroll_attempts += 1
            log.info("%s %s Scroll was not successful (attempt %d/%d)", 
                     code, ARROW, scroll_attempts, MAX_SCROLL_ATTEMPTS)
                
        # If we've been stuck for too long, give up on this subsector
        if scroll_attempts >= MAX_SCROLL_ATTEMPTS:
            log.warning("%s %s Maximum scroll attempts reached, moving to next subsector", code, ARROW)
            break
            
        # Log progress
        log.info("%s %s Total unique businesses processed in this subsector: %d", code, ARROW, len(processed_businesses))

    end_time = datetime.now()
    duration = end_time - start_time
    
    log.info("=" * 60)
    log.info("%s %s Scraping completed in %s", code, ARROW, duration)
    log.info("%s %s Total cards scraped: %d (unique businesses: %d)", 
             code, ARROW, total_cards, len(processed_businesses))
    log.info("=" * 60)
    
    return records, total_cards
