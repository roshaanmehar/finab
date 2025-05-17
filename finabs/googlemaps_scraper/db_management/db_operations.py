"""
db_operations.py - Database operations
------------------------------------
Functions for interacting with MongoDB.
"""
import json
import os
import csv
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, PyMongoError
from bson.objectid import ObjectId
import time

from googlemaps_scraper.utils.config import MONGO_RETRY_ATTEMPTS, MONGO_RETRY_DELAY
from googlemaps_scraper.utils.logging_config import ARROW

# ───────────────── JSON Encoder for MongoDB ───────────────────
from bson.objectid import ObjectId
import time
from datetime import datetime

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

def create_indexes(collection: Collection) -> None:
    """Create indexes for the collection."""
    collection.create_index([("businessname", ASCENDING)], background=True)
    collection.create_index([("phonenumber", ASCENDING)], background=True)
    collection.create_index([("subsector", ASCENDING)], background=True)
    collection.create_index([("emailstatus", ASCENDING)], background=True)

def save_json(code: str, rows: List[Dict[str, Any]], db_name: str) -> bool:
    """
    Save records to a JSON file with proper encoding for non-English characters.
    
    Args:
        code: Subsector code
        rows: Records to save
        db_name: Database name
        
    Returns:
        True if successful, False otherwise
    """
    log = logging.getLogger("googlemaps_scraper")
    
    if not rows:
        log.warning("No rows to save for %s", code)
        return False
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(f"data/{db_name}/json", exist_ok=True)
        
        # Process rows to make them JSON serializable
        processed_rows = []
        for row in rows:
            # Create a copy of the row to avoid modifying the original
            processed_row = {}
            for key, value in row.items():
                # Convert datetime objects to ISO format strings
                if isinstance(value, datetime):
                    processed_row[key] = value.isoformat()
                # Convert ObjectId to string if needed
                elif str(type(value)) == "<class 'bson.objectid.ObjectId'>":
                    processed_row[key] = str(value)
                else:
                    processed_row[key] = value
            processed_rows.append(processed_row)
        
        # Save with UTF-8 encoding to properly handle non-English characters
        with open(f"data/{db_name}/json/{code}.json", "w", encoding="utf-8") as f:
            # Use ensure_ascii=False to preserve non-ASCII characters
            json.dump(processed_rows, f, ensure_ascii=False, indent=2)
        
        log.info("Saved %d records to data/%s/json/%s.json", len(rows), db_name, code)
        return True
    except Exception as e:
        log.error("Error saving JSON: %s", e)
        return False

def save_csv(code: str, rows: List[Dict[str, Any]], db_name: str) -> bool:
    """
    Save records to a CSV file with proper encoding for non-English characters.
    
    Args:
        code: Subsector code
        rows: Records to save
        db_name: Database name
        
    Returns:
        True if successful, False otherwise
    """
    log = logging.getLogger("googlemaps_scraper")
    
    if not rows:
        log.warning("No rows to save for %s", code)
        return False
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(f"data/{db_name}/csv", exist_ok=True)
        
        # Get all possible field names from all rows
        fieldnames = set()
        for row in rows:
            fieldnames.update(row.keys())
        
        # Sort fieldnames for consistent output
        fieldnames = sorted(fieldnames)
        
        # Save with UTF-8 encoding to properly handle non-English characters
        with open(f"data/{db_name}/csv/{code}.csv", "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        log.info("Saved %d records to data/%s/csv/%s.csv", len(rows), db_name, code)
        return True
    except Exception as e:
        log.error("Error saving CSV: %s", e)
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

def insert_record(collection: Collection, record: Dict[str, Any]) -> bool:
    """
    Insert a record into MongoDB with proper handling of non-English characters.
    Handles duplicate records by updating them instead of inserting.
    
    Args:
        collection: MongoDB collection
        record: Record to insert
        
    Returns:
        True if successful, False otherwise
    """
    log = logging.getLogger("googlemaps_scraper")
    
    try:
        # Check if a record with the same phone number already exists
        if "phonenumber" in record and record["phonenumber"]:
            existing = collection.find_one({"phonenumber": record["phonenumber"]})
            if existing:
                # Update the existing record instead of inserting a new one
                result = collection.update_one(
                    {"phonenumber": record["phonenumber"]},
                    {"$set": record}
                )
                log.info("Updated existing record with phone: %s", record["phonenumber"])
                return True
        
        # If no phone number or no existing record, try to insert
        result = collection.insert_one(record)
        return bool(result.inserted_id)
    except DuplicateKeyError:
        # If there's still a duplicate key error, try to update the record
        try:
            # Use businessname and phonenumber as identifiers if available
            query = {}
            if "businessname" in record and record["businessname"]:
                query["businessname"] = record["businessname"]
            if "phonenumber" in record and record["phonenumber"]:
                query["phonenumber"] = record["phonenumber"]
            
            # If we have a query, try to update
            if query:
                result = collection.update_one(query, {"$set": record})
                log.info("Updated existing record after duplicate key error: %s", record.get("businessname", "unknown"))
                return True
            else:
                log.warning("Duplicate record with no identifiable fields: %s", record.get("businessname", "unknown"))
                return False
        except Exception as e:
            log.error("Error updating duplicate record: %s", e)
            return False
    except PyMongoError as e:
        log.error("MongoDB error: %s", e)
        return False
    except Exception as e:
        log.error("Unexpected error: %s", e)
        return False
