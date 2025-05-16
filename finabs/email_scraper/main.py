#!/usr/bin/env python3
"""
main.py - Main entry point for the email scraper
-----------------------------------------------
Orchestrates the email scraping process.
"""
import argparse
import logging
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import List, Dict, Any

from email_scraper.utils.logging_config import setup_logging
from email_scraper.db_management.db_connection import setup_mongodb
from email_scraper.db_management.db_operations import (
    list_business_records,
    check_database_status,
    get_pending_records,
    get_pending_records_atomic,
    recover_stale_processing_records
)
from email_scraper.scraping.scraper import process_business_record
from email_scraper.config import DEFAULT_MONGO_URI, DEFAULT_DB_NAME, DEFAULT_COLLECTION_NAME

# Global flag for graceful shutdown
shutdown_flag = False

def signal_handler(signum, frame):
    """Sets the shutdown flag upon receiving SIGINT or SIGTERM."""
    global shutdown_flag
    if not shutdown_flag:  # Log only on first signal
        logging.getLogger("email_scraper").warning(f"Signal {signal.Signals(signum).name} received. Initiating graceful shutdown...")
    shutdown_flag = True

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    p = argparse.ArgumentParser(
        description="Email scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    p.add_argument("--threads", type=int, default=4, help="Number of concurrent website workers")
    p.add_argument("--headless", action="store_true", help="Run Chrome headless")
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--mongo-uri", type=str, default=DEFAULT_MONGO_URI, help="MongoDB connection URI")
    p.add_argument("--db-name", type=str, default=DEFAULT_DB_NAME, help="MongoDB database name")
    p.add_argument("--collection", type=str, default=DEFAULT_COLLECTION_NAME, help="MongoDB collection name")
    p.add_argument("--max-sites", type=int, default=0, help="Maximum number of sites to process (0 = all pending)")
    p.add_argument("--list-records", action="store_true", help="List some processable records (limit 10) and exit")
    p.add_argument("--check-db", action="store_true", help="Show database statistics and exit")
    p.add_argument("--test-url", type=str, help="Test a single URL and print results, then exit")
    p.add_argument("--batch-size", type=int, default=10, help="Number of records to process in one batch")
    p.add_argument("--recover-stale", action="store_true", help="Recover stale processing records before starting")
    return p.parse_args()

def process_batch(
    records_batch: List[Dict[str, Any]], 
    collection, 
    args: argparse.Namespace, 
    run_stats: Dict[str, int], 
    start_time: float
) -> None:
    """
    Process a batch of records using the thread pool executor.
    
    Args:
        records_batch: List of records to process
        collection: MongoDB collection
        args: Command line arguments
        run_stats: Dictionary to track statistics
        start_time: Start time of the processing run
    """
    logger = logging.getLogger("email_scraper")
    
    # Skip if no records or shutdown requested
    if not records_batch or shutdown_flag:
        return
    
    active_futures: List[Future] = []
    
    with ThreadPoolExecutor(max_workers=args.threads, thread_name_prefix='ScraperWorker') as executor:
        # Submit tasks for this batch
        for record in records_batch:
            if shutdown_flag:
                logger.warning("Shutdown initiated, no more tasks will be submitted.")
                break
            
            active_futures.append(
                executor.submit(
                    process_business_record,
                    record,
                    collection,
                    args.headless,
                    args.debug,
                    shutdown_flag
                )
            )
        
        logger.info(f"Submitted {len(active_futures)} tasks to executor for this batch.")
        
        # Process results as they complete
        for future_item in as_completed(active_futures):
            if shutdown_flag and run_stats["processed"] % 5 == 0:
                logger.info("Shutdown in progress, waiting for active tasks...")
            
            run_stats["processed"] += 1
            
            try:
                _, res_status, num_e = future_item.result()
                
                if res_status == "found":
                    run_stats["found"] += 1
                    run_stats["emails_collected"] += num_e
                elif res_status == "checked_no_email":
                    run_stats["checked_no_email"] += 1
                elif res_status.startswith("failed"):
                    run_stats["failed"] += 1
                elif res_status.startswith("skipped"):
                    run_stats["skipped"] += 1
                else:
                    logger.warning(f"Unknown status from worker: {res_status}")
            
            except Exception as e:
                logger.error(f"Task resulted in an unhandled exception: {e}", exc_info=args.debug)
                run_stats["failed"] += 1
            
            # Log progress periodically
            if run_stats["processed"] % 10 == 0:
                elapsed = time.time() - start_time
                rate = run_stats["processed"] / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Progress: {run_stats['processed']} | "
                    f"Found: {run_stats['found']} | "
                    f"Checked: {run_stats['checked_no_email']} | "
                    f"Failed: {run_stats['failed']} | "
                    f"Skipped: {run_stats['skipped']} | "
                    f"Rate: {rate:.2f}/s"
                )

def main():
    """Main entry point for the email scraper."""
    global shutdown_flag
    
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    logger = setup_logging(args.debug)
    logger.info("--- Email Scraper Initializing ---")
    logger.info(f"Run arguments: {vars(args)}")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Set up MongoDB connection
    mongo_client, data_collection = setup_mongodb(args.mongo_uri, args.db_name, args.collection)
    
    if mongo_client is None or data_collection is None:
        logger.critical("Exiting due to MongoDB connection failure.")
        sys.exit(1)

    # Handle admin commands
    if args.list_records:
        list_business_records(data_collection, args.debug)
        logger.info("DB Admin: Record listing complete. Exiting.")
        mongo_client.close()
        sys.exit(0)
    
    if args.check_db:
        db_stats = check_database_status(data_collection)
        logger.info("--- Database Status ---")
        for key, val in db_stats.items():
            logger.info(f"  {key.replace('_', ' ').capitalize()}: {val}")
        logger.info("--- Status Check Complete ---")
        mongo_client.close()
        sys.exit(0)
    
    if args.test_url:
        logger.info(f"--- Testing single URL: {args.test_url} ---")
        from email_scraper.scraping.browser_manager import make_driver
        from email_scraper.scraping.scraper import harvest_site_emails
        
        test_driver_instance = None
        try:
            test_driver_instance = make_driver(args.headless, args.debug)
            if test_driver_instance is None:
                logger.error("Failed to create WebDriver for single URL test.")
            else:
                test_emails, test_status, test_err = harvest_site_emails(
                    args.test_url, "Test Business URL", test_driver_instance, args.debug
                )
                logger.info(f"--- Test Results for {args.test_url} ---")
                logger.info(f"Status: {test_status}")
                if test_err:
                    logger.info(f"Error: {test_err}")
                logger.info(f"Emails Found ({len(test_emails)}):")
                for em in test_emails:
                    logger.info(f"  - {em}")  # Log each email for test_url
                if not test_emails:
                    logger.info("  None")
                logger.info("--- Test Complete ---")
        except Exception as e:
            logger.error(f"Error during single URL test: {e}", exc_info=args.debug)
        finally:
            if test_driver_instance:
                try:
                    test_driver_instance.quit()
                except:
                    pass
            mongo_client.close()
            sys.exit(0)

    # Recover stale processing records if requested
    if args.recover_stale:
        recovered = recover_stale_processing_records(data_collection)
        logger.info(f"Recovered {recovered} stale processing records")

    # Main processing
    start_run_time = time.time()
    logger.info("--- Starting Main Processing Run ---")
    
    # Check database status
    initial_db_stats = check_database_status(data_collection)
    logger.info("Initial DB Stats:")
    for k, v in initial_db_stats.items():
        logger.info(f"  {k}: {v}")
    
    if initial_db_stats.get("pending_scrape", 0) == 0:
        logger.info("No businesses found with 'pending' status. Nothing to process.")
        mongo_client.close()
        sys.exit(0)

    # Process records
    run_stats = {
        "processed": 0,
        "found": 0,
        "checked_no_email": 0,
        "failed": 0,
        "skipped": 0,
        "emails_collected": 0
    }
    
    # Get records to process
    processing_limit = args.max_sites if args.max_sites > 0 else 0
    logger.info(f"Fetching records with 'pending' status (Limit: {'All' if processing_limit == 0 else processing_limit})...")
    
    try:
        # For tracking total processed
        total_processed = 0
        records_remaining = True
        batch_size = args.batch_size
        
        # Process in batches
        while records_remaining and not shutdown_flag:
            # Calculate how many more records we need
            remaining_limit = processing_limit - total_processed if processing_limit > 0 else 0
            current_batch_size = remaining_limit if remaining_limit > 0 else batch_size
            
            # Get a batch of records and mark them as processing atomically
            records_for_processing = get_pending_records_atomic(
                data_collection, 
                limit=current_batch_size, 
                batch_size=batch_size
            )
            
            num_to_process = len(records_for_processing)
            
            if num_to_process == 0:
                logger.info("No more pending records available. Finishing current batch.")
                records_remaining = False
                break
            
            logger.info(f"Acquired {num_to_process} records for processing in this batch.")
            total_processed += num_to_process
            
            # Process this batch
            process_batch(
                records_for_processing, 
                data_collection, 
                args, 
                run_stats, 
                start_run_time
            )
            
            # Check if we've reached our processing limit
            if processing_limit > 0 and total_processed >= processing_limit:
                logger.info(f"Reached processing limit of {processing_limit} records.")
                records_remaining = False
                break
                
            # Small delay between batches
            if records_remaining and not shutdown_flag:
                time.sleep(2)
    
    except Exception as e:
        logger.critical(f"Failed to fetch records from MongoDB: {e}. Exiting.")
        mongo_client.close()
        sys.exit(1)
    
    # Print summary
    end_run_time = time.time()
    total_duration = end_run_time - start_run_time
    
    logger.info("--- Processing Run Summary ---")
    logger.info(f"Total records targeted: {total_processed}")
    logger.info(f"Actually processed (attempted): {run_stats['processed']}")
    logger.info(f"  - Emails Found: {run_stats['found']}")
    logger.info(f"  - Checked (no email found): {run_stats['checked_no_email']}")
    logger.info(f"  - Failed: {run_stats['failed']}")
    logger.info(f"  - Skipped (invalid URL, shutdown, etc.): {run_stats['skipped']}")
    logger.info(f"Total unique emails collected in this run (approx): {run_stats['emails_collected']}")
    logger.info(f"Total execution time: {total_duration:.2f} seconds")
    
    if mongo_client:
        logger.info("Closing MongoDB connection.")
        mongo_client.close()
    
    logger.info("--- Email Scraper Finished ---")
    
    exit_code = 1 if run_stats['failed'] > 0 and not shutdown_flag else 0
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
