"""
db_connection.py - Database connection management
------------------------------------------------
Functions for establishing connections to MongoDB and managing databases and collections.
"""
import logging
import time
from typing import Tuple, Any, Optional

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError, ConnectionFailure

from email_scraper.config import MONGO_RETRY_ATTEMPTS, MONGO_RETRY_DELAY

def setup_mongodb(mongo_uri: str, db_name: str, collection_name: str) -> Tuple[Optional[MongoClient], Optional[Any]]:
    """
    Set up MongoDB connection and collections with proper error handling.
    
    Args:
        mongo_uri: MongoDB connection URI
        db_name: Name of the database
        collection_name: Name of the collection
        
    Returns:
        Tuple of (client, collection)
    """
    logger = logging.getLogger("email_scraper")
    
    for attempt in range(MONGO_RETRY_ATTEMPTS):
        try:
            logger.info(f"Attempting MongoDB connection ({attempt + 1}/{MONGO_RETRY_ATTEMPTS}) to {mongo_uri}...")
            client = MongoClient(
                mongo_uri, 
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=20000,
                socketTimeoutMS=45000,
                maxPoolSize=50,
                retryWrites=True,
                retryReads=True
            )
            
            # Test connection
            client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {db_name}/{collection_name}")
            
            db = client[db_name]
            collection = db[collection_name]
            
            # Create indexes
            try:
                collection.create_index([("emailstatus", ASCENDING)], background=True, name="emailstatus_idx")
                collection.create_index([("website", ASCENDING)], background=True, name="website_idx")
                collection.create_index([("emailscraped_at", ASCENDING)], background=True, name="scraped_at_idx")
                logger.info("Verified/created necessary MongoDB indexes.")
            except PyMongoError as e:
                logger.warning(f"MongoDB index check/creation issue (continuing): {e}")
            
            return client, collection
            
        except (ServerSelectionTimeoutError, ConnectionFailure) as e:
            if attempt < MONGO_RETRY_ATTEMPTS - 1:
                logger.warning(f"MongoDB connection failed (attempt {attempt + 1}/{MONGO_RETRY_ATTEMPTS}): {e}")
                delay = MONGO_RETRY_DELAY * (2 ** attempt)
                logger.info(f"Retrying MongoDB connection in {delay:.1f} seconds...")
                time.sleep(delay)
            else:
                logger.critical(f"MongoDB setup failed after {MONGO_RETRY_ATTEMPTS} attempts.")
                return None, None
        except Exception as e:
            logger.critical(f"MongoDB setup failed with unexpected error: {e}", exc_info=True)
            return None, None
    
    return None, None
