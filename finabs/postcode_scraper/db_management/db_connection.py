"""
db_connection.py - Database connection management
------------------------------------------------
Functions for establishing connections to MongoDB and managing databases and collections.
"""
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection


def connect_to_mongodb(mongo_uri: str) -> MongoClient:
    """
    Establish a connection to MongoDB.
    
    Args:
        mongo_uri: MongoDB connection URI
        
    Returns:
        MongoDB client object
    """
    return MongoClient(mongo_uri)


def get_or_create_database(client: MongoClient, db_name: str) -> Database:
    """
    Get or create a database.
    
    Args:
        client: MongoDB client
        db_name: Name of the database
        
    Returns:
        MongoDB database object
    """
    return client[db_name]


def get_or_create_collection(db: Database, collection_name: str) -> Collection:
    """
    Get or create a collection.
    
    Args:
        db: MongoDB database
        collection_name: Name of the collection
        
    Returns:
        MongoDB collection object
    """
    # Check if collection exists
    if collection_name in db.list_collection_names():
        return db[collection_name]
    
    # Create collection
    db.create_collection(collection_name)
    return db[collection_name]
