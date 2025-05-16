#!/usr/bin/env python3
"""
main.py - Main entry point for the postcode scraper
--------------------------------------------------
Orchestrates the scraping process by coordinating the different modules.
"""
import argparse
import json
from pathlib import Path
import threading
import time

from postcode_scraper.db_management.db_connection import connect_to_mongodb, get_or_create_database
from postcode_scraper.db_management.db_operations import load_subsectors_into_mongo
from postcode_scraper.scraping.scraper import create_worker
from postcode_scraper.utils.city_abbreviations import get_city_name
from postcode_scraper.utils.logging_config import setup_logging

# Thread-safe shared primitives
page_lock = threading.Lock()
results_lock = threading.Lock()
next_page_num = 1
stop_scraping = False

# Data containers
all_postcodes = []
sector_to_subsectors = {}


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    ap = argparse.ArgumentParser(description="Parallel Selenium scraper for doogal.co.uk → Mongo + JSON.")
    ap.add_argument("--prefix", required=True, help="Outward prefix to search for (e.g. LS, BD, SW1).")
    ap.add_argument("--city", required=True, help="Mongo database name (e.g. Leeds).")
    ap.add_argument("--mongo-uri", default="mongodb+srv://roshaanatck:DOcnGUEEB37bQtcL@scraper-db-cluster.88kc14b.mongodb.net/?retryWrites=true&w=majority&appName=scraper-db-cluster", help="Mongo connection URI.")
    ap.add_argument("--workers", type=int, default=4, help="Number of parallel Selenium sessions (default 4).")
    ap.add_argument("--delay", type=float, default=0.5, help="Polite delay between page fetches (seconds).")
    ap.add_argument("--timeout", type=int, default=15, help="Seconds to wait for table to appear.")
    ap.add_argument("--headless", action="store_true", help="Run Chrome in headless mode.")
    return ap.parse_args()


def save_results_to_json(prefix: str, all_postcodes: list, sector_to_subsectors: dict) -> tuple[Path, Path]:
    """Save scraped results to JSON files."""
    out_prefix = prefix.upper().rstrip()
    postcodes_file = Path(f"{out_prefix}_postcodes.json")
    stats_file = Path(f"{out_prefix}_stats.json")

    with postcodes_file.open("w", encoding="utf-8") as f:
        json.dump(sorted(all_postcodes), f, indent=2)

    stats = {sec: sorted(list(subs)) for sec, subs in sector_to_subsectors.items()}
    counts = {sec: len(subs) for sec, subs in sector_to_subsectors.items()}
    with stats_file.open("w", encoding="utf-8") as f:
        json.dump({"sectors": stats, "counts": counts}, f, indent=2)
    
    return postcodes_file, stats_file


def print_summary(all_postcodes: list, sector_to_subsectors: dict, postcodes_file: Path, stats_file: Path) -> None:
    """Print a summary of the scraping results."""
    print("\n--- Summary ---")
    print(f"Total postcodes scraped     : {len(all_postcodes):,}")
    print(f"Distinct sectors            : {len(sector_to_subsectors):,}")
    print(f"Distinct subsectors         : {sum(len(v) for v in sector_to_subsectors.values()):,}")
    print(f"Saved postcode list         : {postcodes_file}")
    print(f"Saved sector/subsector stats: {stats_file}")


def main() -> None:
    """Main entry point for the postcode scraper."""
    # Set up logging
    logger = setup_logging()
    logger.info("Starting postcode scraper")
    
    # Parse command line arguments
    args = parse_args()
    
    # Resolve city abbreviation if needed
    city_name = get_city_name(args.city)
    logger.info(f"Scraping postcodes for {city_name} with prefix {args.prefix}")
    
    # Initialize global variables
    global next_page_num, stop_scraping, all_postcodes, sector_to_subsectors
    next_page_num = 1
    stop_scraping = False
    all_postcodes = []
    sector_to_subsectors = {}
    
    # Create and start worker threads
    threads = []
    for _ in range(max(1, args.workers)):
        worker = create_worker(
            args.prefix, 
            args.timeout, 
            args.delay, 
            args.headless,
            page_lock,
            results_lock,
            lambda: stop_scraping,
            lambda val: setattr(globals(), 'stop_scraping', val),
            lambda: next_page_num,
            lambda val: setattr(globals(), 'next_page_num', val),
            all_postcodes,
            sector_to_subsectors
        )
        thread = threading.Thread(target=worker)
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Save results to JSON
    postcodes_file, stats_file = save_results_to_json(args.prefix, all_postcodes, sector_to_subsectors)
    
    # Load data into MongoDB
    client = connect_to_mongodb(args.mongo_uri)
    db = get_or_create_database(client, city_name)
    load_subsectors_into_mongo(db, sector_to_subsectors)
    
    # Print summary
    print_summary(all_postcodes, sector_to_subsectors, postcodes_file, stats_file)
    logger.info("Postcode scraping completed successfully")


if __name__ == "__main__":
    main()
