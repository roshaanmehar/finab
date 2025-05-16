"""
browser_manager.py - Browser management
-------------------------------------
Functions for managing Selenium browser instances.
"""
import logging
import random
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException, InvalidSessionIdException

from email_scraper.config import UA_POOL

def make_driver(headless: bool, debug: bool = False) -> Optional[webdriver.Chrome]:
    """
    Create a Selenium WebDriver with anti-detection measures.
    
    Args:
        headless: Whether to run Chrome in headless mode
        debug: Enable debug logging
        
    Returns:
        Selenium WebDriver or None if creation fails
    """
    logger = logging.getLogger("email_scraper")
    
    ua = random.choice(UA_POOL)
    logger.debug(f"Using User-Agent for new driver: {ua}")
    
    opt = Options()
    if headless:
        logger.debug("Running Chrome in headless mode using --headless=new")
        opt.add_argument("--headless=new")
    
    opt.add_argument(f"--user-agent={ua}")
    opt.add_argument("--window-size=1366,768")
    opt.add_argument("--disable-gpu")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-extensions")
    opt.add_argument("--log-level=3")
    opt.add_argument("--silent")
    opt.add_argument("--disable-logging")
    opt.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging', 'disable-default-apps'])
    opt.add_experimental_option("useAutomationExtension", False)
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument("--lang=en-US,en;q=0.9")
    opt.add_argument("--disable-software-rasterizer")
    opt.add_argument("--disable-webgl")
    opt.add_argument("--disable-3d-apis")
    
    # Performance optimizations
    opt.add_argument("--disable-notifications")
    opt.add_argument("--disable-popup-blocking")
    opt.add_argument("--disable-infobars")
    opt.add_argument("--disable-translate")
    opt.add_argument("--disable-save-password-bubble")
    opt.add_argument("--disable-background-networking")
    opt.add_argument("--disable-sync")
    opt.add_argument("--disable-default-apps")
    opt.add_argument("--disable-client-side-phishing-detection")
    
    # Use faster page load strategy - only wait for DOM, not full resources
    opt.page_load_strategy = 'eager'  # Changed from 'normal' to 'eager' for faster loading
    
    drv = None
    try:
        drv = webdriver.Chrome(options=opt)
        logger.debug("WebDriver created successfully.")
        
        stealth_script = """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
        """
        drv.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": stealth_script})
        logger.debug("CDP stealth script injected.")
        
        drv.set_page_load_timeout(45)
        drv.set_script_timeout(20)
        drv.implicitly_wait(5)
        logger.debug("Driver timeouts set.")
        
        return drv
    
    except WebDriverException as e:
        logger.error(f"Failed to create WebDriver: {e}")
        if "cannot find chrome binary" in str(e).lower():
            logger.error("Ensure Chrome/Chromium is installed and accessible in your PATH.")
        elif "permission denied" in str(e).lower() or "killed" in str(e).lower():
            logger.error("Permission error or process killed creating driver.")
    
    except Exception as e:
        logger.error(f"Unexpected error creating WebDriver: {e}", exc_info=debug)
    
    if drv:
        try:
            drv.quit()
        except:
            pass
    
    return None

def is_driver_alive(driver: Optional[webdriver.Chrome]) -> bool:
    """
    Check if a WebDriver is still alive and responsive.
    
    Args:
        driver: Selenium WebDriver
        
    Returns:
        True if driver is alive, False otherwise
    """
    if driver is None:
        return False
    
    try:
        _ = driver.current_url
        return driver.session_id is not None
    except (InvalidSessionIdException, WebDriverException):
        return False
    except Exception:
        return False
