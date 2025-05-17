#!/usr/bin/env python3
"""
main.py - Main entry point for the Google Maps scraper
----------------------------------------------------
Orchestrates the scraping process by coordinating the different modules.
"""
import argparse
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional, Callable

from pymongo import MongoClient, ReturnDocument
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

from googlemaps_scraper.db_management.db_connection import setup_mongodb
from googlemaps_scraper.db_management.db_operations import save_json, save_csv
from googlemaps_scraper.scraping.scraper import scrape_subsector
from googlemaps_scraper.scraping.browser_manager import make_driver
from googlemaps_scraper.utils.logging_config import setup_logging, ARROW
from googlemaps_scraper.utils.config import (
    SUBSECTOR_WAIT_MIN, SUBSECTOR_WAIT_MAX, SERVICE, CITY
)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    p = argparse.ArgumentParser("Scrape Google‑Maps restaurants (card‑first)")
    p.add_argument("--start", type=int, help="Start queue index (inclusive)")
    p.add_argument("--end",   type=int, help="End   queue index (inclusive)")
    p.add_argument("--headless", action="store_true", help="Run Chrome headless")
    p.add_argument("--subsector", type=str, help="Scrape specific subsector")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--fast", action="store_true", help="Use faster scraping (less human-like)")
    p.add_argument("--mongo-uri", type=str, default="mongodb+srv://roshaanatck:DOcnGUEEB37bQtcL@scraper-db-cluster.88kc14b.mongodb.net/?retryWrites=true&w=majority&appName=scraper-db-cluster", 
                   help="MongoDB connection URI")
    p.add_argument("--db-name", type=str, default="Leeds",
                   help="MongoDB database name")
    p.add_argument("--queue-collection", type=str, default="subsector_queue",
                   help="MongoDB collection for subsector queue")
    p.add_argument("--business-collection", type=str, default="restaurants",
                   help="MongoDB collection for business data")
    return p.parse_args()

def process_subsectors(
    driver: webdriver.Chrome, 
    args: argparse.Namespace, 
    queue_col, 
    rest_col,
    log: logging.Logger,
    termination_check: Optional[Callable[[], bool]] = None
) -> int:
    """Process subsectors based on command line arguments."""
    # Define the document iterator based on command line arguments
    if args.subsector:
        log.info("Processing specific subsector: %s", args.subsector)
        
        def doc_iter():
            doc = queue_col.find_one({"subsector": args.subsector})
            if doc:
                yield doc
            else:
                # If subsector doesn't exist in queue, create it
                doc = {
                    "subsector": args.subsector,
                    "scrapedsuccessfully": False,
                    "processing": True
                }
                queue_col.insert_one(doc)
                yield doc
    
    elif args.start is not None and args.end is not None:
        log.info("Processing subsectors by index range: %d to %d", args.start, args.end)
        
        def doc_iter():
            for idx in range(args.start, args.end + 1):
                # Check for termination before getting next document
                if termination_check and termination_check():
                    return
                docs = list(
                    queue_col.find().sort("subsector", 1).skip(idx).limit(1)
                )
                if not docs:
                    continue
                doc = queue_col.find_one_and_update(
                    {"_id": docs[0]["_id"]},
                    {"$set": {"processing": True}},
                    return_document=ReturnDocument.AFTER
                )
                if doc:
                    yield doc
    
    else:
        log.info("Processing all unprocessed subsectors")
        
        def doc_iter():
            while True:
                # Check for termination before getting next document
                if termination_check and termination_check():
                    return
                doc = queue_col.find_one_and_update(
                    {"scrapedsuccessfully": False, "processing": False},
                    {"$set": {"processing": True}},
                    return_document=ReturnDocument.AFTER
                )
                if not doc:
                    break
                yield doc

    processed = 0
    total_subsectors = 0
    
    # Count total subsectors for progress tracking
    if args.start is not None and args.end is not None:
        total_subsectors = args.end - args.start + 1
    elif args.subsector:
        total_subsectors = 1
    else:
        total_subsectors = queue_col.count_documents({"scrapedsuccessfully": False})
    
    log.info("Total subsectors to process: %d", total_subsectors)

    # Maximum time to spend on a single subsector (25 minutes)
    MAX_SUBSECTOR_TIME = 25 * 60  # seconds

    for doc in doc_iter():
        # Check if we should terminate early
        if termination_check and termination_check():
            log.info("Termination requested, stopping subsector processing")
            # Mark current doc as not processing so it can be picked up later
            queue_col.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {"processing": False}}
            )
            break
            
        code = doc["subsector"]
        start = datetime.now()
        log.info("=" * 50)
        log.info("PROCESSING SUBSECTOR: %s (%d/%d)", code, processed + 1, total_subsectors)

        success = False
        rows = []
        card_count = 0
        
        # Set a timeout for this subsector
        subsector_timeout = time.time() + MAX_SUBSECTOR_TIME
        
        for attempt in range(1, 4):
            # Check for termination between attempts
            if termination_check and termination_check():
                log.info("Termination requested during attempts, stopping")
                break
                
            # Check if we've exceeded the maximum time for this subsector
            if time.time() > subsector_timeout:
                log.warning("%s %s Maximum processing time exceeded, moving to next subsector", code, ARROW)
                break
                
            try:
                # Check if driver is still alive, recreate if needed
                if not is_driver_alive(driver):
                    log.warning("Driver session is no longer valid, recreating...")
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = make_driver(args.headless)
                
                # Create a combined termination check that checks both the passed check and the timeout
                combined_check = lambda: (termination_check() if termination_check else False) or time.time() > subsector_timeout
                
                rows, card_count = scrape_subsector(
                    doc, driver, rest_col, SERVICE, args.db_name, 
                    debug=args.debug, fast_mode=args.fast,
                    termination_check=combined_check
                )
                if rows:
                    success = True
                    break
                else:
                    log.warning("%s %s attempt %d/3 returned no results", code, ARROW, attempt)
            except Exception as ex:
                log.error("%s %s attempt %d/3 failed: %s", code, ARROW, attempt, ex)
                log.error("Traceback: %s", traceback.format_exc())
                time.sleep(3)

        # Check if we've exceeded the maximum time for this subsector
        if time.time() > subsector_timeout:
            log.warning("%s %s Maximum processing time exceeded, marking as incomplete", code, ARROW)
            # Mark the subsector as partially processed with didresultsloadcompletely=false
            queue_col.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "processing": False,
                        "scrapedsuccessfully": bool(rows),  # True if we got any results
                        "didresultsloadcompletely": False,  # Explicitly mark as incomplete
                        "totalrecordsfound": len(rows),
                        "totaluniquerecordsfound": len([r for r in rows if r.get("phonenumber")]),
                        "timeout_occurred": True,
                        "last_processed": datetime.now()
                    }
                }
            )
            
            # Save any results we did get
            if rows:
                save_json(code, rows, args.db_name)
                save_csv(code, rows, args.db_name)
                processed += 1
            
            continue

        # Check for termination after attempts
        if termination_check and termination_check():
            # Mark as not processing if terminating
            queue_col.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {"processing": False}}
            )
            log.info("%s %s skipped due to termination request", code, ARROW)
            break
                
        # Mark as not processing if we're going to skip it
        if not success:
            queue_col.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {"processing": False}}
            )
            log.warning("%s %s skipped after failed attempts", code, ARROW)
            continue

        # write JSON after subsector is complete
        json_saved = save_json(code, rows, args.db_name)
        
        # Also save as CSV
        csv_saved = save_csv(code, rows, args.db_name)
        
        # Count unique records
        unique_count = len([r for r in rows if r.get("phonenumber")])
        total_count = len(rows)
        log.info("%s %s total records: %d (with phone: %d)", code, ARROW, total_count, unique_count)

        # mark subsector done
        queue_col.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "scrapedsuccessfully": bool(rows),
                    "didresultsloadcompletely": False,  # Always set to false as requested
                    "totalrecordsfound": len(rows),
                    "totaluniquerecordsfound": unique_count,
                    "processing": False,
                    "json_saved": json_saved,
                    "csv_saved": csv_saved,
                    "completed_at": datetime.now()
                }
            },
        )

        processed += 1
        duration = datetime.now() - start
        progress_percent = (processed / total_subsectors) * 100 if total_subsectors > 0 else 0
        
        log.info(
            "%s %s done in %s • processed=%d/%d (%.1f%%)",
            code,
            ARROW,
            duration,
            processed,
            total_subsectors,
            progress_percent
        )
        log.info("=" * 50)
        
        # Check for termination before waiting between subsectors
        if termination_check and termination_check():
            log.info("Termination requested after subsector completion, stopping")
            break
            
        # Only wait between subsectors if we have more to process and not terminating
        if processed < total_subsectors and (termination_check is None or not termination_check()):
            rdelay(SUBSECTOR_WAIT_MIN, SUBSECTOR_WAIT_MAX, args.fast)

    return processed

def is_driver_alive(driver: webdriver.Chrome) -> bool:
    """Check if the driver is still alive and responsive."""
    try:
        # Try to get the current URL - this will fail if the driver is dead
        driver.current_url
        return True
    except Exception:
        return False

def rdelay(a: float, b: float, fast_mode: bool = False):
    """Random delay with option for fast mode"""
    import random
    if fast_mode:
        time.sleep(random.uniform(a * 0.5, b * 0.5))  # 50% faster in fast mode
    else:
        time.sleep(random.uniform(a, b))

def setup_log_directory():
    """Set up the log directory for Google Maps scraper."""
    log_dir = os.path.join(os.getcwd(), "logs", "googlemaps_scraper")
    os.makedirs(log_dir, exist_ok=True)
    return log_dir

def main():
    """Main entry point for the Google Maps scraper."""
    args = parse_args()
    
    # Set up log directory
    log_dir = setup_log_directory()
    
    # Set up logging with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"gmaps_scraper_{timestamp}.log")
    
    # Set up logging
    log = setup_logging(debug=args.debug)
    
    # Add file handler to logger
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    log.addHandler(file_handler)
    
    log.info("=" * 80)
    log.info("Starting Google Maps scraper")
    log.info("Log file: %s", log_filename)
    log.info("Arguments: %s", args)
    log.info("=" * 80)
    
    # Set up MongoDB
    client, queue_col, rest_col = setup_mongodb(
        args.mongo_uri, 
        args.db_name, 
        args.queue_collection, 
        args.business_collection
    )

    driver = None
    try:
        driver = make_driver(headless=args.headless)
        log.info("Chrome driver initialized successfully")
    except WebDriverException as e:
        log.critical("Chrome driver launch failed: %s", e)
        return

    try:
        total = process_subsectors(driver, args, queue_col, rest_col, log)
        log.info("=" * 80)
        log.info("✓ finished – subsectors processed: %d", total)
        log.info("=" * 80)
    except KeyboardInterrupt:
        log.warning("Interrupted by user.")
    except Exception as e:
        log.critical("Unhandled exception: %s", e)
        log.critical("Traceback: %s", traceback.format_exc())
    finally:
        try:
            if driver:
                driver.quit()
                log.info("Chrome driver closed")
        except Exception:
            pass
        
        try:
            client.close()
            log.info("MongoDB connection closed")
        except Exception:
            pass

def run_scraper(db_name: str, queue_collection: str, business_collection: str, 
                mongo_uri: str = "mongodb://localhost:27017", headless: bool = False,
                debug: bool = False, fast: bool = False, 
                termination_check: Optional[Callable[[], bool]] = None) -> bool:
    """
    Run the Google Maps scraper with the specified parameters.
    This function is used by the Flask app to start the scraper.
    
    Args:
        db_name: MongoDB database name
        queue_collection: MongoDB collection for subsector queue
        business_collection: MongoDB collection for business data
        mongo_uri: MongoDB connection URI
        headless: Whether to run Chrome headless
        debug: Enable debug logging
        fast: Use faster scraping (less human-like)
        termination_check: Optional function to check if scraping should be terminated
        
    Returns:
        True if successful, False otherwise
    """
    # Set up log directory
    log_dir = setup_log_directory()
    
    # Set up logging with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"gmaps_scraper_api_{timestamp}.log")
    
    # Set up logging
    log = setup_logging(debug=debug)
    
    # Add file handler to logger
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    log.addHandler(file_handler)
    
    log.info("=" * 80)
    log.info("Starting Google Maps scraper from API")
    log.info("Log file: %s", log_filename)
    log.info("Parameters: db=%s, queue=%s, business=%s, headless=%s", 
             db_name, queue_collection, business_collection, headless)
    log.info("=" * 80)
    
    # Set up MongoDB
    try:
        client, queue_col, rest_col = setup_mongodb(
            mongo_uri, db_name, queue_collection, business_collection
        )
    except Exception as e:
        log.critical("MongoDB setup failed: %s", e)
        return False

    driver = None
    success = False
    
    try:
        driver = make_driver(headless=headless)
        log.info("Chrome driver initialized successfully")
        
        # Create args object with the necessary attributes
        class Args:
            pass
        
        args = Args()
        args.start = None
        args.end = None
        args.subsector = None
        args.debug = debug
        args.fast = fast
        args.headless = headless
        args.mongo_uri = mongo_uri
        args.db_name = db_name
        args.queue_collection = queue_collection
        args.business_collection = business_collection
        
        # Pass the termination check to process_subsectors
        total = process_subsectors(driver, args, queue_col, rest_col, log, termination_check)
        
        # Check if we were terminated
        if termination_check and termination_check():
            log.info("Google Maps scraper terminated by user request")
            success = False
        else:
            log.info("=" * 80)
            log.info("✓ finished – subsectors processed: %d", total)
            log.info("=" * 80)
            success = total > 0
            
    except Exception as e:
        log.critical("Unhandled exception: %s", e)
        log.critical("Traceback: %s", traceback.format_exc())
        success = False
    finally:
        try:
            if driver:
                driver.quit()
                log.info("Chrome driver closed")
        except Exception:
            pass
        
        try:
            client.close()
            log.info("MongoDB connection closed")
        except Exception:
            pass
    
    return success

if __name__ == "__main__":
    main()
