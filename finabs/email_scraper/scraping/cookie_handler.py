"""
cookie_handler.py - Cookie and popup handling
-------------------------------------------
Functions for handling cookie consent popups.
"""
import logging
import random
import time
from typing import Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.common.exceptions import (
    ElementNotInteractableException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    WebDriverException,
    NoSuchElementException
)

from email_scraper.config import COOKIE_BUTTON_PATTERNS
from email_scraper.scraping.browser_manager import is_driver_alive

def dismiss_cookie_consent(driver: Optional[webdriver.Chrome], debug: bool = False) -> bool:
    """
    Attempt to dismiss cookie consent popups.
    
    Args:
        driver: Selenium WebDriver
        debug: Enable debug logging
        
    Returns:
        True if popup was likely dismissed, False otherwise
    """
    logger = logging.getLogger("email_scraper")
    
    if not is_driver_alive(driver):
        return False
    
    strategies_succeeded = 0
    time.sleep(random.uniform(1.5, 2.5))
    
    # Strategy 1: Find and click buttons matching common patterns
    try:
        xpath_parts = []
        for pattern in COOKIE_BUTTON_PATTERNS:
            xpath_parts.append(f"contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern}')")
            xpath_parts.append(f"contains(translate(@value, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern}')")
            xpath_parts.append(f"contains(translate(@aria-label, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{pattern}')")
            xpath_parts.append(f"@id='{pattern}'")
            xpath_parts.append(f"contains(@id, '{pattern}')")
            xpath_parts.append(f"contains(concat(' ', normalize-space(@class), ' '), ' {pattern} ')")
            xpath_parts.append(f"contains(@class, '{pattern}')")
        
        xpath_query = (f"//button[{' or '.join(xpath_parts)}] | //a[{' or '.join(xpath_parts)}] | "
                      f"//div[@role='button' and ({' or '.join(xpath_parts)})] | "
                      f"//span[@role='button' and ({' or '.join(xpath_parts)})]")
        
        potential_buttons = driver.find_elements(By.XPATH, xpath_query)
        logger.debug(f"Found {len(potential_buttons)} potential cookie buttons via XPath.")
        
        for element in potential_buttons[:5]:  # Try first 5 buttons
            try:
                if element.is_displayed() and element.is_enabled():
                    el_text = (element.text or element.get_attribute("value") or 
                              element.get_attribute("aria-label") or "").strip().lower()[:50]
                    
                    logger.debug(f"Attempting to click cookie button: Text='{el_text}', Tag='{element.tag_name}'")
                    
                    try:
                        driver.execute_script("arguments[0].scrollIntoView(true); arguments[0].click();", element)
                        logger.debug("Clicked cookie button using JavaScript.")
                    except WebDriverException:
                        logger.debug("JS click failed, trying Selenium click.")
                        element.click()
                        logger.debug("Clicked cookie button using Selenium.")
                    
                    time.sleep(random.uniform(0.7, 1.2))
                    strategies_succeeded += 1
                    return True
            
            except (ElementNotInteractableException, StaleElementReferenceException, 
                   ElementClickInterceptedException) as e:
                logger.debug(f"Cookie button found but not interactable or stale: {e}")
            except WebDriverException as e:
                logger.debug(f"WebDriverException clicking cookie button: {e}")
            except Exception as e:
                logger.error(f"Unexpected error clicking cookie button: {e}", exc_info=debug)
    
    except WebDriverException as e:
        logger.debug(f"WebDriverException during cookie button XPath search: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in cookie strategy 1: {e}", exc_info=debug)
    
    # Strategy 2: Check for cookie banners in iframes
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        logger.debug(f"Found {len(iframes)} iframes to check for cookie banners.")
        
        for iframe_el in iframes:
            try:
                iframe_id = iframe_el.get_attribute("id") or ""
                iframe_name = iframe_el.get_attribute("name") or ""
                iframe_title = iframe_el.get_attribute("title") or ""
                iframe_src = iframe_el.get_attribute("src") or ""
                
                if any(term in s.lower() for s in [iframe_id, iframe_name, iframe_title, iframe_src] 
                      for term in ["cookie", "consent", "privacy", "gdpr", "cmp", "onetrust", 
                                  "trustarc", "banner", "notice"]):
                    
                    logger.debug(f"Switching to potential consent iframe: ID='{iframe_id}', Name='{iframe_name}', Title='{iframe_title}'")
                    driver.switch_to.frame(iframe_el)
                    
                    if dismiss_cookie_consent(driver, debug):
                        logger.debug("Cookie consent dismissed within iframe.")
                        driver.switch_to.default_content()
                        strategies_succeeded += 1
                        return True
                    
                    driver.switch_to.default_content()
                    logger.debug("Switched back from iframe, consent not found/dismissed within.")
            
            except NoSuchElementException:
                logger.debug("Iframe disappeared before/during processing.")
                try:
                    driver.switch_to.default_content()
                except:
                    pass
            
            except WebDriverException as e:
                logger.warning(f"Error processing iframe for cookies: {e}")
                try:
                    driver.switch_to.default_content()
                except:
                    pass
    
    except WebDriverException as e:
        logger.debug(f"WebDriverException finding iframes for cookies: {e}")
    
    # Strategy 3: Try Escape key
    if strategies_succeeded == 0:
        try:
            logger.debug("Trying Escape key for cookie dismissal...")
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.5)
        except WebDriverException as e:
            logger.debug(f"Error sending Escape key: {e}")
    
    if strategies_succeeded > 0:
        logger.info("Cookie consent likely dismissed.")
        return True
    else:
        logger.debug("No cookie consent popups dismissed after all strategies.")
        return False
