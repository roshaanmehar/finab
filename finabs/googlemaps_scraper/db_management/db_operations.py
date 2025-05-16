"""
db_operations.py - Database operations
-------------------------------------
Functions for interacting with the database and saving data to files.
"""
import json
import logging
import os
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

from bson import ObjectId
from pymongo.collection import Collection
from pymongo.errors import PyMongoError, DuplicateKeyError

from googlemaps_scraper.utils.config import MONGO_RETRY_ATTEMPTS, MONGO_RETRY_DELAY
from googlemaps_scraper.utils.logging_config import ARROW

# ───────────────── JSON Encoder for MongoDB ───────────────────
class MongoJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that can handle MongoDB ObjectId."""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)  # Convert ObjectId to string
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO format
        return super().default(obj)

# ───────────────── File Output ───────────────────────
OUT_DIR = "business_data"
os.makedirs(OUT_DIR, exist_ok=True)

def save_json(code: str, rows: List[dict], city: str = None) -> bool:
    """
    Save records to JSON file with custom encoder for MongoDB types.
    
    Args:
        code: Subsector code
        rows: List of records to save
        city: City name to include in filename
        
    Returns:
        True if successful, False otherwise
    """
    log = logging.getLogger("googlemaps_scraper")
    
    if not rows:
        log.warning("%s %s No records to save to JSON", code, ARROW)
        return False
        
    try:
        # Clean records for JSON serialization
        clean_rows = []
        for row in rows:
            clean_row = {}
            for k, v in row.items():
                if k != "_id":  # Skip MongoDB _id field
                    clean_row[k] = v
            clean_rows.append(clean_row)
            
        city_name = city.lower() if city else (rows[0].get("city", "location").lower() if len(rows) > 0 and "city" in rows[0] else "location")
        fp = os.path.join(OUT_DIR, f"{city_name}_{code}.json")
        with open(fp, "w", encoding="utf-8") as fh:
            json.dump(clean_rows, fh, indent=4, cls=MongoJSONEncoder)
        log.info("%s %s JSON saved %s %s (%d records)", code, ARROW, ARROW, fp, len(clean_rows))
        return True
    except Exception as e:
        log.error("%s %s Failed to save JSON: %s", code, ARROW, e)
        return False

def save_csv(code: str, rows: List[dict], city: str = None) -> bool:
    """
    Save records to CSV file.
    
    Args:
        code: Subsector code
        rows: List of records to save
        city: City name to include in filename
        
    Returns:
        True if successful, False otherwise
    """
    log = logging.getLogger("googlemaps_scraper")
    
    if not rows:
        log.warning("%s %s No records to save to CSV", code, ARROW)
        return False
        
    try:
        import csv
        
        city_name = city.lower() if city else (rows[0].get("city", "location").lower() if len(rows) > 0 and "city" in rows[0] else "location")
        fp = os.path.join(OUT_DIR, f"{city_name}_{code}.csv")
        
        # Get all possible field names from all records
        fieldnames = set()
        for row in rows:
            fieldnames.update(row.keys())
        
        # Remove MongoDB _id field if present
        if "_id" in fieldnames:
            fieldnames.remove("_id")
            
        fieldnames = sorted(list(fieldnames))
        
        with open(fp, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in rows:
                # Create a clean row without _id
                clean_row = {k: v for k, v in row.items() if k != "_id"}
                writer.writerow(clean_row)
                
        log.info("%s %s CSV saved %s %s (%d records)", code, ARROW, ARROW, fp, len(rows))
        return True
    except Exception as e:
        log.error("%s %s Failed to save CSV: %s", code, ARROW, e)
        return False

def check_phone_exists(rest_col, phone_number: int) -> bool:
    """
    Check if a phone number already exists in MongoDB.
    
    Args:
        rest_col: MongoDB collection
        phone_number: Phone number to check
        
    Returns:
        True if it exists, False otherwise
    """
    log = logging.getLogger("googlemaps_scraper")
    
    if not phone_number:
        return False
        
    for attempt in range(MONGO_RETRY_ATTEMPTS):
        try:
            count = rest_col.count_documents({"phonenumber": phone_number})
            return count > 0
        except PyMongoError as e:
            if attempt < MONGO_RETRY_ATTEMPTS - 1:
                log.warning("MongoDB error checking phone (attempt %d/%d): %s. Retrying...", 
                           attempt + 1, MONGO_RETRY_ATTEMPTS, e)
                time.sleep(MONGO_RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                log.error("MongoDB error after %d attempts: %s", MONGO_RETRY_ATTEMPTS, e)
                return False
        except Exception as e:
            log.error("Unexpected error checking phone: %s", e)
            return False
    
    return False

def check_business_exists(rest_col, business_name: str, subsector: str) -> bool:
    """
    Check if a business with the same name and subsector already exists in MongoDB.
    
    Args:
        rest_col: MongoDB collection
        business_name: Business name to check
        subsector: Subsector to check
        
    Returns:
        True if it exists, False otherwise
    """
    log = logging.getLogger("googlemaps_scraper")
    
    if not business_name:
        return False
        
    for attempt in range(MONGO_RETRY_ATTEMPTS):
        try:
            count = rest_col.count_documents({
                "businessname": business_name,
                "subsector": subsector
            })
            return count > 0
        except PyMongoError as e:
            if attempt < MONGO_RETRY_ATTEMPTS - 1:
                log.warning("MongoDB error checking business (attempt %d/%d): %s. Retrying...", 
                           attempt + 1, MONGO_RETRY_ATTEMPTS, e)
                time.sleep(MONGO_RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                log.error("MongoDB error after %d attempts: %s", MONGO_RETRY_ATTEMPTS, e)
                return False
        except Exception as e:
            log.error("Unexpected error checking business: %s", e)
            return False
    
    return False

def insert_record(rest_col, record: dict) -> bool:
    """
    Insert a single record into MongoDB, handling uniqueness constraints.
    
    Args:
        rest_col: MongoDB collection
        record: Record to insert
        
    Returns:
        True if inserted successfully
    """
    log = logging.getLogger("googlemaps_scraper")
    
    # First check if phone number already exists in the database
    if record.get("phonenumber") and check_phone_exists(rest_col, record["phonenumber"]):
        log.debug("Skipping insert - phone number already exists in database: %s", record.get("phonenumber"))
        return True  # Consider it successful since we're intentionally skipping
    
    # Then check if business name already exists in the same subsector
    if record.get("businessname") and check_business_exists(rest_col, record["businessname"], record["subsector"]):
        log.debug("Skipping insert - business already exists in database: %s", record.get("businessname"))
        return True  # Consider it successful since we're intentionally skipping
    
    for attempt in range(MONGO_RETRY_ATTEMPTS):
        try:
            # If record has a phone number, use it as a unique key
            if record.get("phonenumber"):
                rest_col.update_one(
                    {"phonenumber": record["phonenumber"]},
                    {"$set": record},
                    upsert=True
                )
                log.debug("Inserted/updated record with phone: %s", record.get("phonenumber"))
            else:
                # For records without phone numbers, use business name and subsector as key
                rest_col.update_one(
                    {
                        "businessname": record["businessname"],
                        "subsector": record["subsector"]
                    },
                    {"$set": record},
                    upsert=True
                )
                log.debug("Inserted/updated record without phone: %s", record.get("businessname"))
            return True
        except DuplicateKeyError:
            # This shouldn't happen with update_one + upsert, but just in case
            log.warning("Duplicate key for record: %s", record.get("businessname"))
            return True  # Still consider it successful since the record exists
        except PyMongoError as e:
            if attempt < MONGO_RETRY_ATTEMPTS - 1:
                log.warning("MongoDB error (attempt %d/%d): %s. Retrying...", 
                           attempt + 1, MONGO_RETRY_ATTEMPTS, e)
                time.sleep(MONGO_RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                log.error("MongoDB error after %d attempts: %s", MONGO_RETRY_ATTEMPTS, e)
                return False
        except Exception as e:
            log.error("Unexpected error: %s", e)
            return False
    
    return False
