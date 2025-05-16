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
    return webdriver.Chrome(options=opts)
