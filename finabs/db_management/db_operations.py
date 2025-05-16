"""
db_operations.py - Database operations
-------------------------------------
Functions for interacting with the database.
"""
from collections import defaultdict
from typing import Any, Dict, List, Set

from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError


DEFAULT_FIELDS: Dict[str, Any] = {
    "processing": False,
    "scrapedsuccessfully": False,  # Changed from True to False
    "didresultsloadcompletely": False,  # Changed from True to False
    "totalrecordsfound": 0,
    "totaluniquerecordsfound": 0,
    "emailstatus": "pending",
    "recordsfoundwithemail": 0,
}


def insert_data(collection: Collection, data: Dict[str, Any]) -> None:
    """
    Insert a dictionary of data into the specified collection.
    
    Args:
        collection: MongoDB collection
        data: Dictionary of data to insert
    """
    collection.insert_one(data)


def check_record_exists(collection: Collection, query: Dict[str, Any]) -> bool:
    """
    Check if a record matching the query exists in the collection.
    
    Args:
        collection: MongoDB collection
        query: Query to match against
        
    Returns:
        True if a matching record exists, False otherwise
    """
    return collection.count_documents(query) > 0


def check_collection_exists(db: Database, collection_name: str) -> bool:
    """
    Check if a collection exists in the database.
    
    Args:
        db: MongoDB database
        collection_name: Name of the collection
        
    Returns:
        True if the collection exists, False otherwise
    """
    return collection_name in db.list_collection_names()


def get_collection_count(db: Database, collection_name: str) -> int:
    """
    Get the number of documents in a collection.
    
    Args:
        db: MongoDB database
        collection_name: Name of the collection
        
    Returns:
        Number of documents in the collection, or 0 if the collection doesn't exist
    """
    if check_collection_exists(db, collection_name):
        return db[collection_name].count_documents({})
    return 0


def load_subsectors_into_mongo(db: Database, sector_to_subsectors: Dict[str, Set[str]], collection_name: str = "subsector_queue") -> None:
    """
    Load subsector data into MongoDB.
    
    Args:
        db: MongoDB database
        sector_to_subsectors: Dictionary mapping sectors to sets of subsectors
        collection_name: Name of the collection to use (default: "subsector_queue")
    """
    col = db[collection_name]
    col.drop()
    col.create_index([("subsector", 1)], unique=True)

    batch = []
    for sector, subs in sector_to_subsectors.items():
        for subsector in subs:
            batch.append({"subsector": subsector, "sector": sector, **DEFAULT_FIELDS})
    
    if batch:
        try:
            col.insert_many(batch, ordered=False)
        except BulkWriteError:
            pass  # duplicates ignored thanks to unique index
