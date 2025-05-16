"""
db_connection.py - Database connection management
------------------------------------------------
Functions for establishing connections to MongoDB and managing databases and collections.
"""
import logging
import sys
import time
from typing import Tuple, Any

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError, ConnectionFailure

from googlemaps_scraper.utils.config import MONGO_RETRY_ATTEMPTS, MONGO_RETRY_DELAY

def setup_mongodb(mongo_uri: str, db_name: str, queue_collection: str, business_collection: str) -> Tuple[MongoClient, Any, Any]:
    """
    Set up MongoDB connection and collections with proper error handling.
    
    Args:
        mongo_uri: MongoDB connection URI
        db_name: Name of the database
        queue_collection: Name of the queue collection
        business_collection: Name of the business collection
        
    Returns:
        Tuple of (client, queue_collection, business_collection)
    """
    log = logging.getLogger("googlemaps_scraper")
    
    for attempt in range(MONGO_RETRY_ATTEMPTS):
        try:
            # Use a longer timeout for initial connection
            client = MongoClient(mongo_uri, 
                                serverSelectionTimeoutMS=10000,
                                connectTimeoutMS=20000,
                                socketTimeoutMS=45000,
                                maxPoolSize=50,
                                retryWrites=True)
            
            # Test connection
            client.admin.command('ping')
            log.info("Connected to MongoDB successfully")
            
            db = client[db_name]
            queue_col = db[queue_collection]
            rest_col = db[business_collection]
            
            # Create indexes
            queue_col.create_index(
                [("scrapedsuccessfully", ASCENDING), ("processing", ASCENDING)], 
                background=True
            )
            log.info("Created queue index")
            
            # Create sparse unique index on phonenumber
            try:
                rest_col.create_index(
                    [("phonenumber", ASCENDING)], 
                    unique=True, 
                    sparse=True, 
                    background=True
                )
                # Also create index on businessname for faster lookups
                rest_col.create_index([("businessname", ASCENDING)], background=True)
                # Create compound index on subsector and businessname
                rest_col.create_index([
                    ("subsector", ASCENDING),
                    ("businessname", ASCENDING)
                ], background=True)
                log.info("Created indexes")
            except PyMongoError as e:
                log.warning("Index creation issue (continuing anyway): %s", e)
            
            return client, queue_col, rest_col
            
        except (ServerSelectionTimeoutError, ConnectionFailure) as e:
            if attempt < MONGO_RETRY_ATTEMPTS - 1:
                log.warning("MongoDB connection failed (attempt %d/%d): %s. Retrying...", 
                           attempt + 1, MONGO_RETRY_ATTEMPTS, e)
                time.sleep(MONGO_RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                log.critical("MongoDB setup failed after %d attempts: %s", MONGO_RETRY_ATTEMPTS, e)
                sys.exit(1)
        except Exception as e:
            log.critical("MongoDB setup failed with unexpected error: %s", e)
            sys.exit(1)
