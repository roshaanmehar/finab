"""
scraper.py - Core scraping functionality
---------------------------------------
Functions for scraping postcodes from doogal.co.uk.
"""
import time
from collections import defaultdict
from typing import Callable, Dict, List, Set

from selenium import webdriver

from postcode_scraper.scraping.url_builder import build_url
from postcode_scraper.scraping.html_parser import fetch_postcodes
from postcode_scraper.scraping.browser_manager import create_driver
from postcode_scraper.data_processing.data_validation import derive_sector_subsector


def create_worker(
    prefix: str,
    timeout: int,
    delay: float,
    headless: bool,
    page_lock: object,
    results_lock: object,
    get_stop_scraping: Callable[[], bool],
    set_stop_scraping: Callable[[bool], None],
    get_next_page_num: Callable[[], int],
    set_next_page_num: Callable[[int], None],
    all_postcodes: List[str],
    sector_to_subsectors: Dict[str, Set[str]]
) -> Callable[[], None]:
    """
    Create a worker function for scraping postcodes.
    
    Args:
        prefix: Outward prefix to search for
        timeout: Seconds to wait for table to appear
        delay: Polite delay between page fetches
        headless: Whether to run Chrome in headless mode
        page_lock: Lock for accessing page number
        results_lock: Lock for accessing results
        get_stop_scraping: Function to get stop_scraping flag
        set_stop_scraping: Function to set stop_scraping flag
        get_next_page_num: Function to get next_page_num
        set_next_page_num: Function to set next_page_num
        all_postcodes: List to store all postcodes
        sector_to_subsectors: Dictionary to store sector to subsectors mapping
        
    Returns:
        Worker function
    """
    def worker():
        driver = create_driver(headless)
        try:
            while True:
                with page_lock:
                    if get_stop_scraping():
                        break
                    page = get_next_page_num()
                    set_next_page_num(page + 1)
                
                url = build_url(prefix, page)
                pcs = fetch_postcodes(driver, url, timeout)
                
                if not pcs:
                    with page_lock:
                        set_stop_scraping(True)
                    break

                with results_lock:
                    for pcd in pcs:
                        if pcd not in all_postcodes:
                            all_postcodes.append(pcd)
                        sector, subsector = derive_sector_subsector(pcd)
                        if sector not in sector_to_subsectors:
                            sector_to_subsectors[sector] = set()
                        sector_to_subsectors[sector].add(subsector)
                
                time.sleep(delay)
        finally:
            driver.quit()
    
    return worker
