"""
browser_manager.py - Browser management
-------------------------------------
Functions for managing Selenium browser instances.
"""
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions


def create_driver(headless: bool) -> webdriver.Chrome:
    """
    Create a Selenium WebDriver.
    
    Args:
        headless: Whether to run Chrome in headless mode
        
    Returns:
        Selenium WebDriver
    """
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,800")
    # Additional stability flags to prevent SIGTRAP and tab crashes
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-crash-reporter")
    opts.add_argument("--disable-in-process-stack-traces")
    opts.add_argument("--disable-breakpad")
    opts.add_argument("--disable-component-update")
    opts.add_argument("--disable-domain-reliability")
    opts.add_argument("--disable-background-networking")
    return webdriver.Chrome(options=opts)
