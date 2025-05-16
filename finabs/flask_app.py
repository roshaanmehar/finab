"""
flask_app.py - Flask integration
------------------------------
Flask application for integrating the email scraper.
"""
import threading
import time
import os
from collections import defaultdict
from datetime import datetime, UTC
from typing import Dict, Any, List, Optional

from flask import Flask, request, jsonify
from flask_cors import CORS

from db_management.db_connection import connect_to_mongodb, get_or_create_database
from db_management.db_operations import load_subsectors_into_mongo, check_collection_exists, get_collection_count
from utils.city_abbreviations import get_city_name, get_city_abbreviation
from utils.logging_config import setup_logging as setup_main_logging
from googlemaps_scraper.utils.logging_config import setup_logging as setup_gmaps_logging
from googlemaps_scraper.main import run_scraper
from email_scraper.utils.logging_config import setup_logging as setup_email_logging
from email_scraper.db_management.db_connection import setup_mongodb
from email_scraper.scraping.scraper import process_business_record
from email_scraper.db_management.db_operations import (
    check_database_status as check_email_db_status,
    get_pending_records
)
from postcode_scraper.scraping.scraper import create_worker
import flask

# Global configuration for headless mode
# Set to False to show browser windows, True to run in headless mode
RUN_HEADLESS = False

# Number of simultaneous browser instances for email scraping
EMAIL_SCRAPER_INSTANCES = 5

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)
# Add this line to disable keeping connections alive
app.config['PRESERVE_CONTEXT_ON_EXCEPTION'] = False

# Also add a few more global settings
from werkzeug.serving import WSGIRequestHandler
WSGIRequestHandler.protocol_version = "HTTP/1.1"  # Use HTTP 1.1 for better connection handling

# Add response timeout
RESPONSE_TIMEOUT = 30  # seconds

# Add a before_request handler to set timeout
@app.before_request
def before_request():
    # Set request timeout
    flask.g.request_timeout = time.time() + RESPONSE_TIMEOUT

# Add a teardown_request handler to ensure connections are closed
@app.teardown_request
def teardown_request(exception=None):
    # Ensure any remaining connections are closed
    if hasattr(flask.g, 'mongo_client') and flask.g.mongo_client:
        try:
            flask.g.mongo_client.close()
        except:
            pass

logger = setup_main_logging()
email_logger = setup_email_logging()

# Configuration
MONGO_URI = "mongodb+srv://roshaanatck:DOcnGUEEB37bQtcL@scraper-db-cluster.88kc14b.mongodb.net/?retryWrites=true&w=majority&appName=scraper-db-cluster"
NUM_WORKERS = 4  # Hardcoded number of workers
DELAY = 0.5
TIMEOUT = 15
MAX_RECORDS = 120


# Background task status and data
ps_task_data = {}  # Postcode scraper tasks
gm_task_data = {}  # Google Maps scraper tasks
es_task_data = {}  # Email scraper tasks

# Store thread objects to allow termination
ps_threads = {}  # {task_id: [thread1, thread2, ...]}
gm_threads = {}  # {task_id: thread}
es_threads = {}  # {task_id: thread}


# Email Scraper Endpoints

@app.route('/api/dataES', methods=['GET'])
def get_email_data():
    """API endpoint to check if email data exists for a database and collection."""
    # Get parameters from URL
    db_name = request.args.get('db_name')
    collection_name = request.args.get('collection')
    
    if not db_name:
        return jsonify({'error': 'Missing required parameter: db_name'}), 400
    
    if not collection_name:
        collection_name = "restaurants"
    
    # Connect to MongoDB
    mongo_client, collection = setup_mongodb(MONGO_URI, db_name, collection_name)
    
    if mongo_client is None or collection is None:
        return jsonify({
            'error': 'Failed to connect to MongoDB',
            'db_name': db_name,
            'collection': collection_name
        }), 500
    
    # Get database status
    db_stats = check_email_db_status(collection)
    
    # Add additional information
    db_stats['db_name'] = db_name
    db_stats['collection'] = collection_name
    
    # Close MongoDB connection
    mongo_client.close()
    
    return jsonify(db_stats)


@app.route('/api/scrapeES', methods=['GET'])
def start_email_scrape():
    """API endpoint to start an email scraping task."""
    # Get parameters from URL
    db_name = request.args.get('db_name')
    collection_name = request.args.get('collection')
    max_sites = request.args.get('max_sites', 0, type=int)
    headless = request.args.get('headless', str(RUN_HEADLESS).lower()).lower() == 'true'
    
    if not db_name:
        return jsonify({'error': 'Missing required parameter: db_name'}), 400
    
    if not collection_name:
        collection_name = "restaurants"
    
    # Connect to MongoDB
    mongo_client, collection = setup_mongodb(MONGO_URI, db_name, collection_name)
    
    if mongo_client is None or collection is None:
        return jsonify({
            'error': 'Failed to connect to MongoDB',
            'db_name': db_name,
            'collection': collection_name
        }), 500
    
    # Check if there are pending records
    pending_records = get_pending_records(collection, max_sites)
    num_pending = len(pending_records)
    
    if num_pending == 0:
        mongo_client.close()
        return jsonify({
            'message': f'No pending records found in {db_name}.{collection_name}',
            'status': 'no_pending_records'
        })
    
    # Generate a task ID
    task_id = f"ES_{db_name}_{collection_name}_{threading.get_ident()}"
    
    # Initialize task data
    es_task_data[task_id] = {
        'status': 'starting',
        'db_name': db_name,
        'collection': collection_name,
        'max_sites': max_sites,
        'headless': headless,
        'total_records': num_pending,
        'processed': 0,
        'found': 0,
        'checked_no_email': 0,
        'failed': 0,
        'skipped': 0,
        'emails_collected': 0,
        'start_time': datetime.now(UTC).isoformat(),
        'should_terminate': False,
        'num_instances': EMAIL_SCRAPER_INSTANCES  # Use multiple instances
    }
    
    # Start background task
    es_thread = threading.Thread(
        target=run_email_scrape_task,
        args=(task_id, db_name, collection_name, max_sites, headless)
    )
    es_thread.daemon = True
    es_thread.start()
    
    # Store thread for termination
    es_threads[task_id] = es_thread
    
    response = jsonify({
        'task_id': task_id,
        'message': f'Email scraping task started for {db_name}.{collection_name} with {EMAIL_SCRAPER_INSTANCES} browser instances',
        'status_url': f'/api/statusES/{task_id}',
        'pending_records': num_pending,
        'headless_mode': headless
    })
    response.headers.add('Connection', 'close')  # Explicitly close connection
    return response


@app.route('/api/statusES/<task_id>', methods=['GET'])
def get_email_status(task_id):
    """API endpoint to get the status of an email scraping task."""
    if task_id not in es_task_data:
        return jsonify({'error': 'Task not found'}), 404
    
    task_info = es_task_data[task_id].copy()
    
    # Calculate progress percentage
    if task_info['total_records'] > 0:
        task_info['progress'] = (task_info['processed'] / task_info['total_records']) * 100
    else:
        task_info['progress'] = 0
    
    # Calculate elapsed time
    if 'start_time' in task_info:
        start_time = datetime.fromisoformat(task_info['start_time'])
        if task_info['status'] == 'completed':
            end_time = datetime.fromisoformat(task_info.get('end_time', datetime.now(UTC).isoformat()))
        else:
            end_time = datetime.now(UTC)
        
        elapsed_seconds = (end_time - start_time).total_seconds()
        task_info['elapsed_time'] = elapsed_seconds
        
        # Calculate rate
        if elapsed_seconds > 0 and task_info['processed'] > 0:
            task_info['rate'] = task_info['processed'] / elapsed_seconds
        else:
            task_info['rate'] = 0
    
    return jsonify(task_info)


@app.route('/api/terminateES/<task_id>', methods=['POST'])
def terminate_email_scraper_task(task_id):
    """API endpoint to terminate a running email scraping task."""
    if task_id not in es_task_data:
        return jsonify({'error': 'Task not found'}), 404
    
    # Check if task is already completed or failed
    current_status = es_task_data[task_id]['status']
    if current_status in ['completed', 'failed', 'terminated']:
        return jsonify({
            'message': f'Task {task_id} is already in state: {current_status}',
            'status': current_status
        })
    
    # Set termination flag
    es_task_data[task_id]['should_terminate'] = True
    es_task_data[task_id]['status'] = 'terminating'
    
    email_logger.info(f"Terminating email scraper task {task_id}")
    
    # Update task status to terminated
    es_task_data[task_id]['status'] = 'terminated'
    es_task_data[task_id]['end_time'] = datetime.now(UTC).isoformat()
    
    return jsonify({
        'message': f'Task {task_id} has been terminated',
        'status': 'terminated'
    })


# Existing Postcode Scraper Endpoints

@app.route('/api/scrapePS', methods=['GET'])
def start_postcode_scrape():
    """API endpoint to start a postcode scraping task."""
    # Get parameters from URL
    city = request.args.get('city')
    keyword = request.args.get('keyword', '')
    auto_run_gmaps = request.args.get('auto_run_gmaps', 'false').lower() == 'true'
    run_es_auto = request.args.get('run_es_auto', 'false').lower() == 'true'
    headless = request.args.get('headless', str(RUN_HEADLESS).lower()).lower() == 'true'
    
    if not city:
        return jsonify({'error': 'Missing required parameter: city'}), 400
    
    # Get city abbreviation for the prefix
    prefix = get_city_abbreviation(city)
    if not prefix:
        return jsonify({'error': f'Could not find abbreviation for city: {city}'}), 400
    
    # Check if data already exists in the database
    client = connect_to_mongodb(MONGO_URI)
    db = get_or_create_database(client, city)
    
    # Determine collection name
    collection_name = "subsector_queue"
    if keyword:
        collection_name = f"{keyword.replace(' ', '_').lower()}_subsector_queue"
    
    # Check if collection exists and has data
    if check_collection_exists(db, collection_name):
        count = get_collection_count(db, collection_name)
        if count > 0:
            # If auto_run_gmaps is true, start Google Maps scraper
            if auto_run_gmaps:
                business_collection = f"{keyword.replace(' ', '_').lower()}" if keyword else "restaurants"
                
                # Generate a task ID for Google Maps scraper
                gmaps_task_id = f"GM_{city}_{collection_name}_{threading.get_ident()}"
                
                # Initialize task data for Google Maps scraper
                gm_task_data[gmaps_task_id] = {
                    'status': 'starting',
                    'db_name': city,
                    'queue_collection': collection_name,
                    'business_collection': business_collection,
                    'should_terminate': False,
                    'run_es_auto': run_es_auto,  # Pass the email scraper flag
                    'headless': headless
                }
                
                # Start Google Maps scraper in a background thread
                gm_thread = threading.Thread(
                    target=run_gmaps_scrape_task,
                    args=(gmaps_task_id, city, collection_name, business_collection, run_es_auto, headless)
                )
                gm_thread.daemon = True
                gm_thread.start()
                
                # Store thread for later termination if needed
                gm_threads[gmaps_task_id] = gm_thread
                
                response = jsonify({
                    'message': f'Data for {city} with keyword {keyword} already exists. Starting Google Maps scraper.',
                    'database': city,
                    'collection': collection_name,
                    'count': count,
                    'status': 'gmaps_started',
                    'gmaps_task_id': gmaps_task_id,
                    'gmaps_status_url': f'/api/statusGM/{gmaps_task_id}',
                    'run_es_auto': run_es_auto,
                    'headless': headless
                })
                response.headers.add('Connection', 'close')  # Explicitly close connection
                return response
            else:
                response = jsonify({
                    'message': f'Data for {city} with keyword {keyword} already exists in the database',
                    'database': city,
                    'collection': collection_name,
                    'count': count,
                    'status': 'exists'
                })
                response.headers.add('Connection', 'close')  # Explicitly close connection
                return response
    
    # Generate a task ID
    task_id = f"PS_{prefix}_{city}_{keyword}_{threading.get_ident()}"
    
    # Initialize task data
    ps_task_data[task_id] = {
        'status': 'starting',
        'progress': 0,
        'postcodes_count': 0,
        'sectors_count': 0,
        'subsectors_count': 0,
        'city': city,
        'prefix': prefix,
        'keyword': keyword,
        'auto_run_gmaps': auto_run_gmaps,
        'run_es_auto': run_es_auto,  # Store the email scraper flag
        'next_page_num': 1,
        'stop_scraping': False,
        'all_postcodes': [],
        'sector_to_subsectors': defaultdict(set),
        'should_terminate': False,  # Flag to signal termination
        'headless': headless
    }
    
    # Start background task
    ps_threads[task_id] = []  # Initialize list of worker threads
    
    # Main thread to coordinate workers
    main_thread = threading.Thread(
        target=run_postcode_scrape_task,
        args=(task_id, prefix, city, keyword, auto_run_gmaps, run_es_auto, headless)
    )
    main_thread.daemon = True
    main_thread.start()
    
    # Store main thread for termination
    ps_threads[task_id].append(main_thread)
    
    response = jsonify({
        'task_id': task_id,
        'message': f'Postcode scraping task started for {prefix} in {city} with keyword {keyword}',
        'status_url': f'/api/statusPS/{task_id}',
        'run_es_auto': run_es_auto,
        'headless': headless
    })
    response.headers.add('Connection', 'close')  # Explicitly close connection
    return response


# Existing Google Maps Scraper Endpoints

@app.route('/api/scrapeGM', methods=['GET'])
def start_gmaps_scrape():
    """API endpoint to start a Google Maps scraping task."""
    # Get parameters from URL
    db_name = request.args.get('db_name')
    queue_collection = request.args.get('queue_collection')
    business_collection = request.args.get('business_collection')
    run_es_auto = request.args.get('run_es_auto', 'false').lower() == 'true'
    headless = request.args.get('headless', str(RUN_HEADLESS).lower()).lower() == 'true'
    
    if not db_name:
        return jsonify({'error': 'Missing required parameter: db_name'}), 400
    
    if not queue_collection:
        queue_collection = "subsector_queue"
    
    if not business_collection:
        # Default business collection name based on queue collection
        if queue_collection == "subsector_queue":
            business_collection = "restaurants"
        else:
            # Extract keyword from queue collection name
            parts = queue_collection.split('_')
            if len(parts) > 1 and parts[-1] == "queue":
                business_collection = '_'.join(parts[:-1])
            else:
                business_collection = "restaurants"
    
    # Check if data exists in the database
    client = connect_to_mongodb(MONGO_URI)
    
    # Check if database exists
    if db_name not in client.list_database_names():
        return jsonify({'error': f'Database {db_name} does not exist'}), 404
    
    db = client[db_name]
    
    # Check if queue collection exists
    if queue_collection not in db.list_collection_names():
        return jsonify({'error': f'Queue collection {queue_collection} does not exist in database {db_name}'}), 404
    
    # Generate a task ID
    task_id = f"GM_{db_name}_{queue_collection}_{threading.get_ident()}"
    
    # Initialize task data
    gm_task_data[task_id] = {
        'status': 'starting',
        'db_name': db_name,
        'queue_collection': queue_collection,
        'business_collection': business_collection,
        'should_terminate': False,  # Flag to signal termination
        'run_es_auto': run_es_auto,  # Store the email scraper flag
        'headless': headless
    }
    
    # Start background task
    gm_thread = threading.Thread(
        target=run_gmaps_scrape_task,
        args=(task_id, db_name, queue_collection, business_collection, run_es_auto, headless)
    )
    gm_thread.daemon = True
    gm_thread.start()
    
    # Store thread for termination
    gm_threads[task_id] = gm_thread
    
    response = jsonify({
        'task_id': task_id,
        'message': f'Google Maps scraping task started for {db_name}.{queue_collection}',
        'status_url': f'/api/statusGM/{task_id}',
        'run_es_auto': run_es_auto,
        'headless': headless
    })
    response.headers.add('Connection', 'close')  # Explicitly close connection
    return response


# Existing task status endpoints

@app.route('/api/statusPS/<task_id>', methods=['GET'])
def get_postcode_status(task_id):
    """API endpoint to get the status of a postcode scraping task."""
    if task_id not in ps_task_data:
        return jsonify({'error': 'Task not found'}), 404
    
    # Return only the status information, not the internal data
    status_info = {
        'status': ps_task_data[task_id]['status'],
        'progress': ps_task_data[task_id]['progress'],
        'postcodes_count': ps_task_data[task_id]['postcodes_count'],
        'sectors_count': ps_task_data[task_id]['sectors_count'],
        'subsectors_count': ps_task_data[task_id]['subsectors_count'],
        'city': ps_task_data[task_id]['city'],
        'prefix': ps_task_data[task_id]['prefix'],
        'keyword': ps_task_data[task_id]['keyword'],
        'headless': ps_task_data[task_id].get('headless', RUN_HEADLESS)
    }
    
    # Add error if present
    if 'error' in ps_task_data[task_id]:
        status_info['error'] = ps_task_data[task_id]['error']
    
    # Add database and collection if completed
    if ps_task_data[task_id]['status'] == 'completed':
        if 'database' in ps_task_data[task_id]:
            status_info['database'] = ps_task_data[task_id]['database']
        if 'collection' in ps_task_data[task_id]:
            status_info['collection'] = ps_task_data[task_id]['collection']
    
    # Add Google Maps task ID if it was triggered
    if 'gmaps_task_id' in ps_task_data[task_id]:
        status_info['gmaps_task_id'] = ps_task_data[task_id]['gmaps_task_id']
        status_info['gmaps_status_url'] = f'/api/statusGM/{ps_task_data[task_id]["gmaps_task_id"]}'
    
    return jsonify(status_info)


@app.route('/api/statusGM/<task_id>', methods=['GET'])
def get_gmaps_status(task_id):
    """API endpoint to get the status of a Google Maps scraping task."""
    if task_id not in gm_task_data:
        return jsonify({'error': 'Task not found'}), 404
    
    # Include email scraper information if available
    status_info = gm_task_data[task_id].copy()
    
    # Add email scraper status URL if available
    if 'email_task_id' in status_info:
        email_task_id = status_info['email_task_id']
        if email_task_id in es_task_data:
            status_info['email_scraper_status'] = es_task_data[email_task_id]['status']
    
    return jsonify(status_info)


# Existing data check endpoints

@app.route('/api/dataPS', methods=['GET'])
def get_postcode_data():
    """API endpoint to check if postcode data exists for a city and keyword."""
    # Get parameters from URL
    city = request.args.get('city')
    keyword = request.args.get('keyword', '')
    
    if not city:
        return jsonify({'error': 'Missing required parameter: city'}), 400
    
    # Connect to MongoDB
    client = connect_to_mongodb(MONGO_URI)
    
    # Check if database exists
    db_exists = city in client.list_database_names()
    
    # Prepare response
    response = {
        'city': city,
        'keyword': keyword,
        'database_exists': db_exists,
        'collections': {}
    }
    
    if db_exists:
        db = client[city]
        
        # Determine collection names
        collection_name = "subsector_queue"
        if keyword:
            collection_name = f"{keyword.replace(' ', '_').lower()}_subsector_queue"
        
        # Check collection
        collection_exists = collection_name in db.list_collection_names()
        count = 0
        if collection_exists:
            count = db[collection_name].count_documents({})
        
        response['collection'] = {
            'name': collection_name,
            'exists': collection_exists,
            'count': count
        }
        
        # Set the main exists flag
        response['exists'] = collection_exists and count > 0
    else:
        response['exists'] = False
        response['collection'] = {
            'name': f"{keyword.replace(' ', '_').lower()}_subsector_queue" if keyword else "subsector_queue",
            'exists': False,
            'count': 0
        }
    
    return jsonify(response)


@app.route('/api/dataGM', methods=['GET'])
def get_gmaps_data():
    """API endpoint to check if Google Maps data exists for a database and collection."""
    # Get parameters from URL
    db_name = request.args.get('db_name')
    business_collection = request.args.get('business_collection')
    
    if not db_name:
        return jsonify({'error': 'Missing required parameter: db_name'}), 400
    
    if not business_collection:
        business_collection = "restaurants"
    
    # Connect to MongoDB
    client = connect_to_mongodb(MONGO_URI)
    
    # Check if database exists
    db_exists = db_name in client.list_database_names()
    
    # Prepare response
    response = {
        'db_name': db_name,
        'business_collection': business_collection,
        'database_exists': db_exists,
    }
    
    if db_exists:
        db = client[db_name]
        
        # Check collection
        collection_exists = business_collection in db.list_collection_names()
        count = 0
        if collection_exists:
            count = db[business_collection].count_documents({})
        
        response['collection'] = {
            'name': business_collection,
            'exists': collection_exists,
            'count': count
        }
        
        # Set the main exists flag
        response['exists'] = collection_exists and count > 0
    else:
        response['exists'] = False
        response['collection'] = {
            'name': business_collection,
            'exists': False,
            'count': 0
        }
    
    return jsonify(response)


# Existing termination endpoints

@app.route('/api/terminatePS/<task_id>', methods=['POST'])
def terminate_postcode_task(task_id):
    """API endpoint to terminate a running postcode scraping task."""
    if task_id not in ps_task_data:
        return jsonify({'error': 'Task not found'}), 404
    
    # Check if task is already completed or failed
    current_status = ps_task_data[task_id]['status']
    if current_status in ['completed', 'failed', 'terminated']:
        return jsonify({
            'message': f'Task {task_id} is already in state: {current_status}',
            'status': current_status
        })
    
    # Set termination flag
    ps_task_data[task_id]['should_terminate'] = True
    ps_task_data[task_id]['stop_scraping'] = True
    ps_task_data[task_id]['status'] = 'terminating'
    
    logger.info(f"Terminating postcode scraper task {task_id}")
    
    # Update task status to terminated
    ps_task_data[task_id]['status'] = 'terminated'
    
    # If this task triggered a Google Maps task, terminate that too
    if 'gmaps_task_id' in ps_task_data[task_id]:
        gmaps_task_id = ps_task_data[task_id]['gmaps_task_id']
        if gmaps_task_id in gm_task_data and gm_task_data[gmaps_task_id]['status'] in ['starting', 'running']:
            # Set termination flag for Google Maps task
            gm_task_data[gmaps_task_id]['should_terminate'] = True
            gm_task_data[gmaps_task_id]['status'] = 'terminated'
            logger.info(f"Also terminating linked Google Maps scraper task {gmaps_task_id}")
    
    return jsonify({
        'message': f'Task {task_id} has been terminated',
        'status': 'terminated'
    })


@app.route('/api/terminateGM/<task_id>', methods=['POST'])
def terminate_gmaps_task(task_id):
    """API endpoint to terminate a running Google Maps scraping task."""
    if task_id not in gm_task_data:
        return jsonify({'error': 'Task not found'}), 404
    
    # Check if task is already completed or failed
    current_status = gm_task_data[task_id]['status']
    if current_status in ['completed', 'failed', 'terminated']:
        return jsonify({
            'message': f'Task {task_id} is already in state: {current_status}',
            'status': current_status
        })
    
    # Set termination flag
    gm_task_data[task_id]['should_terminate'] = True
    gm_task_data[task_id]['status'] = 'terminating'
    
    logger.info(f"Terminating Google Maps scraper task {task_id}")
    
    # Update task status to terminated
    gm_task_data[task_id]['status'] = 'terminated'
    
    return jsonify({
        'message': f'Task {task_id} has been terminated',
        'status': 'terminated'
    })


# Legacy endpoints for backward compatibility

@app.route('/api/terminate/<task_id>', methods=['POST'])
def terminate_task(task_id):
    """Legacy API endpoint to terminate a running task (for backward compatibility)."""
    if task_id.startswith("PS_") and task_id in ps_task_data:
        return terminate_postcode_task(task_id)
    elif task_id.startswith("GM_") and task_id in gm_task_data:
        return terminate_gmaps_task(task_id)
    elif task_id.startswith("ES_") and task_id in es_task_data:
        return terminate_email_scraper_task(task_id)
    elif task_id in ps_task_data:  # For legacy task IDs without prefix
        return terminate_postcode_task(task_id)
    else:
        return jsonify({'error': 'Task not found'}), 404


@app.route('/api/scrape', methods=['GET'])
def start_scrape():
    """Legacy API endpoint to start a scraping task (for backward compatibility)."""
    # Get parameters from URL
    city = request.args.get('city')
    keyword = request.args.get('keyword', '')
    run_gmaps = request.args.get('run_gmaps', 'false').lower() == 'true'
    
    if not city:
        return jsonify({'error': 'Missing required parameter: city'}), 400
    
    # Redirect to the new endpoint
    return start_postcode_scrape()


@app.route('/api/status/<task_id>', methods=['GET'])
def get_status(task_id):
    """Legacy API endpoint to get the status of a scraping task (for backward compatibility)."""
    if task_id.startswith("PS_") and task_id in ps_task_data:
        return get_postcode_status(task_id)
    elif task_id.startswith("GM_") and task_id in gm_task_data:
        return get_gmaps_status(task_id)
    elif task_id.startswith("ES_") and task_id in es_task_data:
        return get_email_status(task_id)
    elif task_id in ps_task_data:  # For legacy task IDs without prefix
        return get_postcode_status(task_id)
    else:
        return jsonify({'error': 'Task not found'}), 404


@app.route('/api/data', methods=['GET'])
def get_data():
    """Legacy API endpoint to check if data exists (for backward compatibility)."""
    return get_postcode_data()


# Background task functions

def run_email_scrape_task(task_id, db_name, collection_name, max_sites, headless):
    """Run an email scraping task in the background."""
    email_logger.info(f"Starting email scraping task {task_id} for {db_name}.{collection_name}")
    
    # Update task status
    es_task_data[task_id]['status'] = 'running'
    
    try:
        # Connect to MongoDB
        mongo_client, collection = setup_mongodb(MONGO_URI, db_name, collection_name)
        
        if mongo_client is None or collection is None:
            es_task_data[task_id]['status'] = 'failed'
            es_task_data[task_id]['error'] = 'Failed to connect to MongoDB'
            email_logger.error(f"Failed to connect to MongoDB for task {task_id}")
            return
        
        # Get pending records
        pending_records = get_pending_records(collection, max_sites)
        num_pending = len(pending_records)
        
        if num_pending == 0:
            es_task_data[task_id]['status'] = 'completed'
            es_task_data[task_id]['message'] = 'No pending records found'
            es_task_data[task_id]['end_time'] = datetime.now(UTC).isoformat()
            email_logger.info(f"No pending records found for task {task_id}")
            mongo_client.close()
            return
        
        # Update task data with actual count
        es_task_data[task_id]['total_records'] = num_pending
        
        # Determine number of instances to use
        num_instances = es_task_data[task_id].get('num_instances', EMAIL_SCRAPER_INSTANCES)
        email_logger.info(f"Using {num_instances} browser instances for task {task_id}")
        
        # Process records using multiple instances
        from concurrent.futures import ThreadPoolExecutor
        
        with ThreadPoolExecutor(max_workers=num_instances) as executor:
            # Submit tasks for processing
            futures = []
            
            for record in pending_records:
                # Check if task should be terminated
                if es_task_data[task_id]['should_terminate']:
                    email_logger.info(f"Task {task_id} termination requested, stopping processing")
                    break
                
                # Submit task to executor
                futures.append(
                    executor.submit(
                        process_business_record,
                        record,
                        collection,
                        headless,
                        False,  # debug mode
                        es_task_data[task_id]['should_terminate']
                    )
                )
            
            # Process results as they complete
            for future in futures:
                try:
                    record_id, status, num_emails = future.result()
                    
                    # Update task statistics
                    es_task_data[task_id]['processed'] += 1
                    
                    if status == "found":
                        es_task_data[task_id]['found'] += 1
                        es_task_data[task_id]['emails_collected'] += num_emails
                    elif status == "checked_no_email":
                        es_task_data[task_id]['checked_no_email'] += 1
                    elif status.startswith("failed"):
                        es_task_data[task_id]['failed'] += 1
                    elif status.startswith("skipped"):
                        es_task_data[task_id]['skipped'] += 1
                    
                    # Log progress periodically
                    if es_task_data[task_id]['processed'] % 5 == 0:
                        email_logger.info(
                            f"Task {task_id} progress: {es_task_data[task_id]['processed']}/{num_pending} records processed"
                        )
                
                except Exception as e:
                    email_logger.error(f"Error processing record in task {task_id}: {e}")
                    es_task_data[task_id]['failed'] += 1
                    es_task_data[task_id]['processed'] += 1
        
        # Update final status
        if es_task_data[task_id]['should_terminate']:
            es_task_data[task_id]['status'] = 'terminated'
        else:
            es_task_data[task_id]['status'] = 'completed'
        
        es_task_data[task_id]['end_time'] = datetime.now(UTC).isoformat()
        
        email_logger.info(
            f"Task {task_id} completed: {es_task_data[task_id]['processed']}/{num_pending} records processed, "
            f"{es_task_data[task_id]['found']} emails found"
        )
    
    except Exception as e:
        email_logger.error(f"Error in email scraping task {task_id}: {e}")
        es_task_data[task_id]['status'] = 'failed'
        es_task_data[task_id]['error'] = str(e)
        es_task_data[task_id]['end_time'] = datetime.now(UTC).isoformat()
    
    finally:
        # Clean up
        if 'mongo_client' in locals() and mongo_client is not None:
            mongo_client.close()
        
        # Remove thread reference
        if task_id in es_threads:
            del es_threads[task_id]


def run_postcode_scrape_task(task_id, prefix, city, keyword, auto_run_gmaps=False, run_es_auto=False, headless=RUN_HEADLESS):
    """Run a postcode scraping task in the background."""
    # Get the task's data
    task = ps_task_data[task_id]
    
    # Update task status
    task['status'] = 'running'
    
    try:
        # Create thread-safe locks for this task
        page_lock = threading.Lock()
        results_lock = threading.Lock()
        
        # Create and start worker threads
        threads = []
        for _ in range(NUM_WORKERS):
            worker = create_worker(
                prefix, 
                TIMEOUT, 
                DELAY, 
                headless,
                page_lock,
                results_lock,
                lambda: task['stop_scraping'] or task['should_terminate'],  # Check both flags
                lambda val: task.update({'stop_scraping': val}),
                lambda: task['next_page_num'],
                lambda val: task.update({'next_page_num': val}),
                task['all_postcodes'],
                task['sector_to_subsectors']
            )
            thread = threading.Thread(target=worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)
            ps_threads[task_id].append(thread)  # Store for termination
        
        # Wait for all threads to complete or task to be terminated
        for thread in threads:
            while thread.is_alive():
                # Check if task should be terminated
                if task['should_terminate']:
                    logger.info(f"Postcode scraper task {task_id} is terminating, not waiting for threads")
                    break
                thread.join(1.0)  # Join with timeout to periodically check termination
            
            # If terminating, don't wait for all threads
            if task['should_terminate']:
                break
        
        # Create database name based on city
        # Use the original case of the city name to avoid case sensitivity issues
        db_name = city
        
        # Create collection name based on keyword if provided
        collection_name = "subsector_queue"
        if keyword:
            collection_name = f"{keyword.replace(' ', '_').lower()}_subsector_queue"
        
        # Load data into MongoDB
        client = connect_to_mongodb(MONGO_URI)
        db = get_or_create_database(client, db_name)
        load_subsectors_into_mongo(db, task['sector_to_subsectors'], collection_name)
        
        # Update task status
        task.update({
            'status': 'completed',
            'progress': 100,
            'postcodes_count': len(task['all_postcodes']),
            'sectors_count': len(task['sector_to_subsectors']),
            'subsectors_count': sum(len(subs) for subs in task['sector_to_subsectors'].values()),
            'database': db_name,
            'collection': collection_name
        })
        
        # If Google Maps scraping is requested, start it after postcode scraping is complete
        if auto_run_gmaps and not task['should_terminate']:
            business_collection = "restaurants"
            if keyword:
                business_collection = f"{keyword.replace(' ', '_').lower()}"
            
            # Generate a task ID for Google Maps scraper
            gmaps_task_id = f"GM_{db_name}_{collection_name}_{threading.get_ident()}"
            
            # Initialize task data for Google Maps scraper
            gm_task_data[gmaps_task_id] = {
                'status': 'starting',
                'db_name': db_name,
                'queue_collection': collection_name,
                'business_collection': business_collection,
                'should_terminate': False,
                'run_es_auto': run_es_auto,  # Pass the email scraper flag
                'headless': headless
            }
            
            # Update postcode task with Google Maps task ID
            task.update({
                'gmaps_task_id': gmaps_task_id
            })
            
            # Start Google Maps scraper in a background thread
            gm_thread = threading.Thread(
                target=run_gmaps_scrape_task,
                args=(gmaps_task_id, db_name, collection_name, business_collection, run_es_auto, headless)
            )
            gm_thread.daemon = True
            gm_thread.start()
            
            # Store thread for termination
            gm_threads[gmaps_task_id] = gm_thread
        
    except Exception as e:
        logger.error(f"Error in postcode scraping task {task_id}: {str(e)}")
        task.update({
            'status': 'failed',
            'error': str(e)
        })
    finally:
        # Clean up task threads
        if task_id in ps_threads:
            ps_threads[task_id] = [t for t in ps_threads[task_id] if t.is_alive()]
            if not ps_threads[task_id]:
                del ps_threads[task_id]
                logger.info(f"Cleaned up postcode scraper threads for task {task_id}")


def run_gmaps_scrape_task(task_id, db_name, queue_collection, business_collection, run_es_auto=False, headless=RUN_HEADLESS):
    """Run a Google Maps scraping task in the background."""
    # Get the task's data
    task = gm_task_data[task_id]
    
    # Update task status
    task['status'] = 'running'
    
    # Set up dedicated logging for this task
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"gmaps_task_{task_id}_{timestamp}.log"
    
    # Create a logger without trying to wrap stdout
    log = setup_gmaps_logging(log_filename, debug=False)
    
    log.info("=" * 80)
    log.info(f"Starting Google Maps scraper task {task_id}")
    log.info(f"Database: {db_name}, Queue: {queue_collection}, Business: {business_collection}")
    log.info("=" * 80)
    
    # Connect to MongoDB to get subsector count
    try:
        client = connect_to_mongodb(MONGO_URI)
        db = client[db_name]
        
        # Check if queue collection exists
        if queue_collection in db.list_collection_names():
            # Count total subsectors for progress tracking
            total_subsectors = db[queue_collection].count_documents({})
            unprocessed_subsectors = db[queue_collection].count_documents({"scrapedsuccessfully": False})
            
            task.update({
                'total_subsectors': total_subsectors,
                'unprocessed_subsectors': unprocessed_subsectors
            })
            
            log.info(f"Found {total_subsectors} total subsectors in {db_name}.{queue_collection} ({unprocessed_subsectors} unprocessed)")
            
            if unprocessed_subsectors == 0:
                log.warning(f"No unprocessed subsectors found in {db_name}.{queue_collection}. Google Maps scraper may not process any records.")
        else:
            log.error(f"Queue collection {queue_collection} not found in database {db_name}")
            task.update({
                'status': 'failed',
                'error': f"Queue collection {queue_collection} not found in database {db_name}"
            })
            return
    except Exception as e:
        log.error(f"Error checking subsectors: {str(e)}")
        task.update({
            'status': 'failed',
            'error': f"Error checking subsectors: {str(e)}"
        })
        return
    
    log.info(f"Starting Google Maps scraper for {db_name}, queue: {queue_collection}, business: {business_collection}")
    
    # Track unique records count for limiting to 100
    unique_records_count = 0
    initial_count = 0
    record_limit = MAX_RECORDS  # Changed from 120 to 100 as per original functionality

    # Get initial count of records with phone numbers
    try:
        business_col = db[business_collection]
        initial_count = business_col.count_documents({"phonenumber": {"$exists": True}})
        log.info(f"Initial count of records with phone numbers: {initial_count}")
        
        # Add initial count to task data
        task.update({
            'initial_record_count': initial_count,
            'current_record_count': initial_count,
            'unique_records_count': 0,
            'record_limit': record_limit,
            'last_check_time': datetime.now(UTC).isoformat()
        })
    except Exception as e:
        log.warning(f"Error getting initial record count: {str(e)}")

    try:
        # Define a termination check function that also checks for record limit
        def termination_check():
            # Check if task should be terminated by user
            if task.get('should_terminate', False):
                log.info(f"Task {task_id} terminated by user request")
                return True
            
            # Only check count every few seconds to avoid excessive database queries
            now = datetime.now(UTC)
            last_check_time = datetime.fromisoformat(task.get('last_check_time', now.isoformat()))
            check_interval = 5  # seconds
            
            if (now - last_check_time).total_seconds() < check_interval:
                # Use cached value if we checked recently
                return task.get('unique_records_count', 0) >= record_limit
            
            # Check current count of unique records
            try:
                nonlocal unique_records_count
                current_count = db[business_collection].count_documents({"phonenumber": {"$exists": True}})
                previous_unique_count = unique_records_count
                unique_records_count = current_count - initial_count
            
                # Update the task data with current count
                task.update({
                    'current_record_count': current_count,
                    'unique_records_count': unique_records_count,
                    'progress': min(100, int((unique_records_count / record_limit) * 100)),
                    'last_check_time': now.isoformat()
                })
            
                # Log progress when new records are found
                if unique_records_count > previous_unique_count:
                    new_records = unique_records_count - previous_unique_count
                    log.info(f"Google Maps scraper found {new_records} new unique records (total: {unique_records_count}/{record_limit})")
                
                if unique_records_count >= record_limit:
                    log.info(f"Reached {record_limit} unique records with phone numbers ({unique_records_count}), stopping Google Maps scraper")
                    # Force early completion
                    task.update({
                        'limit_reached': True,
                        'status': 'completed',
                        'message': f'Google Maps scraping reached the limit of {record_limit} unique records'
                    })
                    return True
            except Exception as e:
                log.warning(f"Error checking unique records count: {str(e)}")
            
            return False

        # Modify the run_scraper function to periodically check for termination
        success = run_scraper(
            db_name=db_name,
            queue_collection=queue_collection,
            business_collection=business_collection,
            mongo_uri=MONGO_URI,
            headless=headless,
            debug=False,
            fast=False,
            termination_check=termination_check  # Pass our custom termination check
        )
        
        # Update task status based on completion
        if task.get('limit_reached', False) or unique_records_count >= record_limit:
            task.update({
                'status': 'completed',
                'message': f'Google Maps scraping completed successfully with {unique_records_count} unique records (limit reached)',
                'unique_records': unique_records_count
            })
        elif success:
            task.update({
                'status': 'completed',
                'message': f'Google Maps scraping completed successfully with {unique_records_count} unique records',
                'unique_records': unique_records_count
            })
        else:
            task.update({
                'status': 'failed',
                'error': 'Google Maps scraping failed'
            })
        
        # If email scraping is requested, start it after Google Maps scraping is complete
        if (task.get('limit_reached', False) or success) and run_es_auto:
            log.info(f"Starting email scraper for {db_name}.{business_collection}")
            
            # Generate a task ID for email scraper
            email_task_id = f"ES_{db_name}_{business_collection}_{threading.get_ident()}"
            
            # Initialize task data for email scraper
            es_task_data[email_task_id] = {
                'status': 'starting',
                'db_name': db_name,
                'collection': business_collection,
                'max_sites': 0,  # Process all pending records
                'headless': headless,
                'total_records': 0,  # Will be updated in the task
                'processed': 0,
                'found': 0,
                'checked_no_email': 0,
                'failed': 0,
                'skipped': 0,
                'emails_collected': 0,
                'start_time': datetime.now(UTC).isoformat(),
                'should_terminate': False,
                'num_instances': EMAIL_SCRAPER_INSTANCES  # Use multiple instances
            }
            
            # Update Google Maps task with email scraper task ID
            task.update({
                'email_task_id': email_task_id
            })
            
            # Start email scraper in a background thread
            es_thread = threading.Thread(
                target=run_email_scrape_task,
                args=(email_task_id, db_name, business_collection, 0, headless)
            )
            es_thread.daemon = True
            es_thread.start()
            
            # Store thread for termination
            es_threads[email_task_id] = es_thread
            
            # Update task with email scraper info
            task.update({
                'email_task_id': email_task_id,
                'email_status_url': f'/api/statusES/{email_task_id}'
            })
        
        log.info(f"Google Maps scraper finished with success={success}, unique_records={unique_records_count}")
    
    except Exception as e:
        log.error(f"Error in Google Maps scraper: {str(e)}")
        task.update({
            'status': 'failed',
            'error': str(e)
        })
    finally:
        # Clean up task threads
        if task_id in gm_threads and not gm_threads[task_id].is_alive():
            del gm_threads[task_id]
            log.info(f"Cleaned up Google Maps scraper thread for task {task_id}")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
