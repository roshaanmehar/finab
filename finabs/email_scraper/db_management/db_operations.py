"""
db_operations.py - Database operations
-------------------------------------
Functions for interacting with the database.
"""
import logging
import time
from typing import Dict, Any, List, Optional

from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId

# Default values for retry and batch operations
MONGO_RETRY_ATTEMPTS = 3
MONGO_RETRY_DELAY = 1.0
BATCH_SIZE = 10

def list_business_records(collection: Optional[Collection], debug: bool = False, limit: int = 10) -> int:
    """
    List business records with 'pending' status and valid websites.
    
    Args:
        collection: MongoDB collection
        debug: Enable debug logging
        limit: Maximum number of records to list
        
    Returns:
        Total number of matching records
    """
    logger = logging.getLogger("email_scraper")
    
    if collection is None: 
        logger.error("DB Admin: Cannot list records - MongoDB collection not available.")
        return 0
    
    try:
        logger.info(f"DB Admin: Listing up to {limit} business records with 'pending' status and valid websites...")
        query = {"website": {"$exists": True, "$nin": ["", None, "N/A"]}, "emailstatus": "pending"}
        records = list(collection.find(query, {"businessname": 1, "website": 1, "emailstatus": 1}).limit(limit))
        total_matching = collection.count_documents(query)
        
        logger.info(f"DB Admin: Found {total_matching} total 'pending' businesses with websites.")
        
        if not records:
            logger.info("DB Admin: No 'pending' records found matching the criteria.")
            return 0
        
        logger.info(f"DB Admin: Showing first {len(records)} 'pending' records:")
        for i, record in enumerate(records):
            logger.info(f"  Record {i + 1}: Name='{record.get('businessname', 'N/A')}', Website='{record.get('website', 'N/A')}', Status='{record.get('emailstatus', 'N/A')}'")
        
        if total_matching > len(records):
            logger.info(f"DB Admin: ... and {total_matching - len(records)} more 'pending' records.")
        
        return total_matching
    
    except PyMongoError as e:
        logger.error(f"DB Admin: MongoDB error listing records: {e}")
    except Exception as e:
        logger.error(f"DB Admin: Unexpected error listing records: {e}", exc_info=debug)
    
    return 0

def check_database_status(collection: Optional[Collection]) -> Dict[str, Any]:
    """
    Check database status.
    
    Args:
        collection: MongoDB collection
        
    Returns:
        Dictionary with database statistics
    """
    logger = logging.getLogger("email_scraper")
    stats: Dict[str, Any] = {
        "total_records": 0,
        "with_website": 0,
        "pending_scrape": 0,
        "emails_found": 0,
        "checked_no_email": 0,
        "failed_scrape": 0
    }
    
    if collection is None: 
        logger.error("DB Admin: Cannot check status - MongoDB collection not available.")
        stats["error"] = "MongoDB collection not available"
        return stats
    
    try:
        stats["total_records"] = collection.count_documents({})
        
        website_query = {"website": {"$exists": True, "$nin": ["", None, "N/A"]}}
        stats["with_website"] = collection.count_documents(website_query)
        
        stats["pending_scrape"] = collection.count_documents({"emailstatus": "pending", **website_query})
        stats["emails_found"] = collection.count_documents({"emailstatus": "found"})
        stats["checked_no_email"] = collection.count_documents({"emailstatus": "checked_no_email"})
        stats["failed_scrape"] = collection.count_documents({"emailstatus": {"$regex": "^failed"}})
    
    except PyMongoError as e:
        logger.error(f"DB Admin: MongoDB error checking database status: {e}")
        stats["error"] = str(e)
    except Exception as e:
        logger.error(f"DB Admin: Unexpected error checking database status: {e}")
        stats["error"] = str(e)
    
    return stats

def get_pending_records(collection: Optional[Collection], limit: int = 0) -> List[Dict[str, Any]]:
    """
    Get records with 'pending' status and valid websites.
    
    Args:
        collection: MongoDB collection
        limit: Maximum number of records to return (0 = all)
        
    Returns:
        List of records
    """
    logger = logging.getLogger("email_scraper")
    
    if collection is None:
        logger.error("Cannot get pending records - MongoDB collection not available.")
        return []
    
    try:
        query = {"website": {"$exists": True, "$nin": ["", None, "N/A"]}, "emailstatus": "pending"}
        cursor = collection.find(query, {"_id": 1, "website": 1, "businessname": 1})
        
        if limit > 0:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    except PyMongoError as e:
        logger.error(f"MongoDB error getting pending records: {e}")
    except Exception as e:
        logger.error(f"Unexpected error getting pending records: {e}")
    
    return []

def mark_record_as_processing(collection: Optional[Collection], record_id: Any) -> bool:
    """
    Mark a record as 'processing' to prevent other instances from picking it up.
    
    Args:
        collection: MongoDB collection
        record_id: Record ID
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger("email_scraper")
    
    if collection is None:
        logger.error(f"Cannot mark record {record_id} as processing - MongoDB collection not available.")
        return False
    
    try:
        from datetime import datetime, UTC
        
        # Use findOneAndUpdate with a query that ensures the record is still in 'pending' state
        result = collection.find_one_and_update(
            {"_id": record_id, "emailstatus": "pending"},  # Only update if still pending
            {
                "$set": {
                    "emailstatus": "processing",
                    "processing_started_at": datetime.now(UTC)
                }
            },
            return_document=False  # Return the document before update
        )
        
        if result is None:
            logger.warning(f"DB Lock: Record {record_id} was not in 'pending' state or doesn't exist.")
            return False
        
        logger.debug(f"DB Lock: Successfully marked record {record_id} as 'processing'.")
        return True
        
    except Exception as e:
        logger.error(f"DB Lock: Error marking record {record_id} as processing: {e}")
        return False
        
def get_pending_records_atomic(collection: Optional[Collection], limit: int = 0, batch_size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    """
    Get records with 'pending' status, mark them as 'processing', and return them.
    This is an atomic operation to prevent race conditions between multiple instances.
    
    Args:
        collection: MongoDB collection
        limit: Maximum number of records to return (0 = all)
        batch_size: Number of records to process in one batch
        
    Returns:
        List of records that were successfully marked as processing
    """
    logger = logging.getLogger("email_scraper")
    
    if collection is None:
        logger.error("Cannot get pending records - MongoDB collection not available.")
        return []
    
    try:
        # Determine how many records to fetch
        actual_batch_size = min(batch_size, limit) if limit > 0 else batch_size
        
        # Find records and mark them as processing in one atomic operation
        processing_records = []
        
        # Get pending records
        query = {"website": {"$exists": True, "$nin": ["", None, "N/A"]}, "emailstatus": "pending"}
        pending_records = list(collection.find(query, {"_id": 1, "website": 1, "businessname": 1}).limit(actual_batch_size))
        
        # Mark each record as processing and add to our list if successful
        for record in pending_records:
            if mark_record_as_processing(collection, record["_id"]):
                processing_records.append(record)
        
        logger.info(f"Atomically acquired {len(processing_records)} records for processing")
        return processing_records
        
    except Exception as e:
        logger.error(f"Error in atomic record acquisition: {e}")
        return []

def update_record_with_email_results(
    collection: Optional[Collection],
    record_id: Any,
    status: str,
    emails: List[str],
    error_message: Optional[str] = None
) -> bool:
    """
    Update a record with email scraping results.
    
    Args:
        collection: MongoDB collection
        record_id: Record ID
        status: Email status
        emails: List of found emails
        error_message: Optional error message
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger("email_scraper")
    
    if collection is None:
        logger.error(f"Cannot update record {record_id} - MongoDB collection not available.")
        return False
    
    try:
        from datetime import datetime, UTC
        
        set_operation: Dict[str, Any] = {
            "emailstatus": status,
            "email": emails[:15],  # Store top N emails
            "emailscraped_at": datetime.now(UTC)  # Use timezone-aware UTC datetime
        }
        
        unset_operation: Dict[str, Any] = {}
        
        if error_message:
            set_operation["error_message"] = error_message
        else:
            unset_operation["error_message"] = ""
        
        update_doc: Dict[str, Any] = {"$set": set_operation}
        if unset_operation:
            update_doc["$unset"] = unset_operation
        
        result = collection.update_one({"_id": record_id}, update_doc)
        
        if result.matched_count == 0:
            logger.warning(f"DB Update: No record found for ID {record_id}.")
            return False
        elif result.modified_count == 0 and result.matched_count == 1:
            logger.debug(f"DB Update: Record {record_id} data was unchanged (or matched existing).")
            return True
        else:
            logger.debug(f"DB Update: Successfully updated record {record_id}.")
            return True
    
    except PyMongoError as e:
        logger.error(f"MongoDB error updating record {record_id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error updating record {record_id}: {e}")
    
    return False

def recover_stale_processing_records(collection: Optional[Collection], max_processing_time: int = 3600) -> int:
    """
    Recover records that have been stuck in processing state for too long.
    
    Args:
        collection: MongoDB collection
        max_processing_time: Maximum processing time in seconds
        
    Returns:
        Number of records recovered
    """
    logger = logging.getLogger("email_scraper")
    
    if collection is None:
        logger.error("Cannot recover stale records - MongoDB collection not available.")
        return 0
    
    try:
        from datetime import datetime, UTC, timedelta
        
        # Calculate cutoff time
        cutoff_time = datetime.now(UTC) - timedelta(seconds=max_processing_time)
        
        # Find and update records
        result = collection.update_many(
            {
                "emailstatus": "processing",
                "processing_started_at": {"$lt": cutoff_time}
            },
            {
                "$set": {
                    "emailstatus": "pending",
                    "recovery_note": f"Reset from stale processing state after {max_processing_time} seconds"
                },
                "$unset": {
                    "processing_started_at": ""
                }
            }
        )
        
        count = result.modified_count
        if count > 0:
            logger.info(f"Recovered {count} stale processing records")
        
        return count
    
    except Exception as e:
        logger.error(f"Error recovering stale processing records: {e}")
        return 0
