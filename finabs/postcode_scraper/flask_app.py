"""
flask_app.py - Flask integration
------------------------------
Flask application for integrating the postcode scraper.
"""
from flask import Flask, request, jsonify
import threading
from collections import defaultdict

from postcode_scraper.db_management.db_connection import connect_to_mongodb, get_or_create_database
from postcode_scraper.db_management.db_operations import load_subsectors_into_mongo
from postcode_scraper.scraping.scraper import create_worker
from postcode_scraper.utils.city_abbreviations import get_city_name, get_city_abbreviation
from postcode_scraper.utils.logging_config import setup_logging

app = Flask(__name__)
logger = setup_logging()

# Thread-safe shared primitives
page_lock = threading.Lock()
results_lock = threading.Lock()
next_page_num = 1
stop_scraping = False

# Data containers
all_postcodes = []
sector_to_subsectors = {}

# Background task status
task_status = {}

# Configuration
MONGO_URI = "mongodb://localhost:27017"
NUM_WORKERS = 4  # Hardcoded number of workers
DELAY = 0.5
TIMEOUT = 15
HEADLESS = True


@app.route('/api/scrape', methods=['GET'])
def start_scrape():
    """API endpoint to start a scraping task using URL parameters."""
    # Get parameters from URL
    city = request.args.get('city')
    keyword = request.args.get('keyword', '')
    
    if not city:
        return jsonify({'error': 'Missing required parameter: city'}), 400
    
    # Get city abbreviation for the prefix
    prefix = get_city_abbreviation(city)
    if not prefix:
        return jsonify({'error': f'Could not find abbreviation for city: {city}'}), 400
    
    # Generate a task ID
    task_id = f"{prefix}_{city}_{keyword}_{threading.get_ident()}"
    
    # Initialize task status
    task_status[task_id] = {
        'status': 'starting',
        'progress': 0,
        'postcodes_count': 0,
        'sectors_count': 0,
        'subsectors_count': 0,
        'city': city,
        'prefix': prefix,
        'keyword': keyword
    }
    
    # Start background task
    threading.Thread(
        target=run_scrape_task,
        args=(task_id, prefix, city, keyword)
    ).start()
    
    return jsonify({
        'task_id': task_id,
        'message': f'Scraping task started for {prefix} in {city} with keyword {keyword}',
        'status_url': f'/api/status/{task_id}'
    })


@app.route('/api/status/<task_id>', methods=['GET'])
def get_status(task_id):
    """API endpoint to get the status of a scraping task."""
    if task_id not in task_status:
        return jsonify({'error': 'Task not found'}), 404
    
    return jsonify(task_status[task_id])


def run_scrape_task(task_id, prefix, city, keyword):
    """Run a scraping task in the background."""
    global next_page_num, stop_scraping, all_postcodes, sector_to_subsectors
    
    # Reset global variables
    next_page_num = 1
    stop_scraping = False
    all_postcodes = []
    sector_to_subsectors = defaultdict(set)
    
    # Update task status
    task_status[task_id]['status'] = 'running'
    
    try:
        # Create and start worker threads
        threads = []
        for _ in range(NUM_WORKERS):
            worker = create_worker(
                prefix, 
                TIMEOUT, 
                DELAY, 
                HEADLESS,
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
        
        # Create database name based on city
        db_name = city.replace(" ", "_").lower()
        
        # Create collection name based on keyword if provided
        collection_name = "subsector_queue"
        if keyword:
            collection_name = f"{keyword.replace(' ', '_').lower()}_subsector_queue"
        
        # Load data into MongoDB
        client = connect_to_mongodb(MONGO_URI)
        db = get_or_create_database(client, db_name)
        load_subsectors_into_mongo(db, sector_to_subsectors, collection_name)
        
        # Update task status
        task_status[task_id].update({
            'status': 'completed',
            'progress': 100,
            'postcodes_count': len(all_postcodes),
            'sectors_count': len(sector_to_subsectors),
            'subsectors_count': sum(len(subs) for subs in sector_to_subsectors.values()),
            'database': db_name,
            'collection': collection_name
        })
        
    except Exception as e:
        logger.error(f"Error in scraping task {task_id}: {str(e)}")
        task_status[task_id].update({
            'status': 'failed',
            'error': str(e)
        })


if __name__ == '__main__':
    app.run(debug=True)
