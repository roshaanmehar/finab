"""
html_parser.py - HTML parsing
---------------------------
Functions for parsing HTML content.
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


TABLE_SELECTOR = "table.sortable tbody"
ROW_ANCHOR_SELECTOR = "td:first-child a"


def fetch_postcodes(driver: webdriver.Chrome, url: str, timeout: int) -> list[str]:
    """
    Fetch postcodes from a single results page.
    
    Args:
        driver: Selenium WebDriver
        url: URL to fetch
        timeout: Seconds to wait for table to appear
        
    Returns:
        List of postcode strings
    """
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, TABLE_SELECTOR))
        )
    except TimeoutException:
        return []

    rows = driver.find_elements(By.CSS_SELECTOR, f"{TABLE_SELECTOR} tr")
    pcs: list[str] = []
    for row in rows:
        try:
            anchor = row.find_element(By.CSS_SELECTOR, ROW_ANCHOR_SELECTOR)
            pcd = anchor.text.strip().upper()
            if pcd:
                pcs.append(pcd)
        except Exception:
            continue
    return pcs


def extract_data_from_html(html_content: str) -> list[str]:
    """
    Extract data from HTML content.
    
    Args:
        html_content: HTML content
        
    Returns:
        List of extracted data
    """
    # This is a placeholder for future implementation
    # Currently, the scraper uses Selenium to extract data directly
    return []
