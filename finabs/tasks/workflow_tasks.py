"""
tasks/workflow_tasks.py - Workflow Tasks
------------------------------
Background tasks for integrated workflow operations.
"""
import threading
import time
from collections import defaultdict
from datetime import datetime, UTC

from app import logger, ps_task_data, gm_task_data, es_task_data, ps_threads, gm_threads, es_threads
from config import MONGO_URI, HEADLESS
from persistence import save_workflows
from utils.city_abbreviations import get_city_abbreviation
from db_management.db_connection import connect_to_mongodb
from email_scraper.db_management.db_connection import setup_mongodb
from email_scraper.db_management.db_operations import get_pending_records
from tasks.postcode_tasks import run_postcode_scrape_task
from tasks.gmaps_tasks import run_gmaps_scrape_task_with_limit
from tasks.email_tasks import run_email_scrape_task

def run_integrated_workflow(workflow_id, city, keyword, workflows):
    """Run the integrated scraping workflow in the background."""
    logger.info(f"Starting integrated workflow {workflow_id} for {city} with keyword {keyword}")
    
    workflow = workflows[workflow_id]
    workflow['status'] = 'running'
    workflow['current_stage'] = 'checking_data'
    
    # Save workflow status update
    save_workflows(workflows)
    
    try:
        # Connect to MongoDB
        client = connect_to_mongodb(MONGO_URI)
        
        # Check if database exists
        db_exists = city in client.list_database_names()
        
        # Determine collection names
        queue_collection = f"{keyword.replace(' ', '_').lower()}_subsector_queue"
        business_collection = f"{keyword.replace(' ', '_').lower()}"
        
        # Check if collections exist and have data
        collection_exists = False
        has_data = False
        
        if db_exists:
            db = client[city]
            collection_exists = queue_collection in db.list_collection_names()
            
            if collection_exists:
                count = db[queue_collection].count_documents({})
                has_data = count > 0
        
        # Step 1: Run Postcode Scraper if needed
        if not (db_exists and collection_exists and has_data):
            workflow['current_stage'] = 'postcode_scraping'
            workflow['stages']['postcode']['status'] = 'running'
            
            # Save workflow status update
            save_workflows(workflows)
            
            # Get city abbreviation for the prefix
            prefix = get_city_abbreviation(city)
            if not prefix:
                workflow['status'] = 'failed'
                workflow['error'] = f'Could not find abbreviation for city: {city}'
                workflow['end_time'] = datetime.now(UTC).isoformat()
                
                # Save workflow status update
                save_workflows(workflows)
                return
            
            # Generate a task ID for postcode scraper
            ps_task_id = f"PS_{prefix}_{city}_{keyword}_{threading.get_ident()}"
            workflow['stages']['postcode']['task_id'] = ps_task_id
            
            # Initialize task data for postcode scraper
            ps_task_data[ps_task_id] = {
                'status': 'starting',
                'progress': 0,
                'postcodes_count': 0,
                'sectors_count': 0,
                'subsectors_count': 0,
                'city': city,
                'prefix': prefix,
                'keyword': keyword,
                'auto_run_gmaps': False,  # We'll handle this ourselves
                'next_page_num': 1,
                'stop_scraping': False,
                'all_postcodes': [],
                'sector_to_subsectors': defaultdict(set),
                'should_terminate': False
            }
            
            # Save workflow status update
            save_workflows(workflows)
            
            # Run postcode scraper
            ps_threads[ps_task_id] = []
            main_thread = threading.Thread(
                target=run_postcode_scrape_task,
                args=(ps_task_id, prefix, city, keyword, False)
            )
            main_thread.daemon = True
            main_thread.start()
            ps_threads[ps_task_id].append(main_thread)
            
            # Wait for postcode scraper to complete
            while ps_task_data[ps_task_id]['status'] not in ['completed', 'failed', 'terminated']:
                # Check if workflow should be terminated
                if workflow['should_terminate']:
                    ps_task_data[ps_task_id]['should_terminate'] = True
                    ps_task_data[ps_task_id]['stop_scraping'] = True
                    break
                
                # Update workflow with postcode scraper progress
                workflow['stages']['postcode']['progress'] = ps_task_data[ps_task_id]['progress']
                
                # Save workflow status update periodically
                if int(time.time()) % 10 == 0:  # Save every 10 seconds
                    save_workflows(workflows)
                
                time.sleep(1)
            
            # Update workflow with postcode scraper status
            workflow['stages']['postcode']['status'] = ps_task_data[ps_task_id]['status']
            
            # Save workflow status update
            save_workflows(workflows)
            
            # Check if postcode scraper failed or was terminated
            if ps_task_data[ps_task_id]['status'] in ['failed', 'terminated']:
                workflow['status'] = ps_task_data[ps_task_id]['status']
                workflow['error'] = ps_task_data[ps_task_id].get('error', 'Postcode scraper failed or was terminated')
                workflow['end_time'] = datetime.now(UTC).isoformat()
                
                # Save workflow status update
                save_workflows(workflows)
                return
        else:
            # Skip postcode scraper
            workflow['stages']['postcode']['status'] = 'skipped'
            workflow['stages']['postcode']['message'] = 'Data already exists'
            
            # Save workflow status update
            save_workflows(workflows)
        
        # Step 2: Run Google Maps Scraper
        workflow['current_stage'] = 'gmaps_scraping'
        workflow['stages']['gmaps']['status'] = 'running'
        
        # Generate a task ID for Google Maps scraper
        gm_task_id = f"GM_{city}_{queue_collection}_{threading.get_ident()}"
        workflow['stages']['gmaps']['task_id'] = gm_task_id
        
        # Initialize task data for Google Maps scraper
        gm_task_data[gm_task_id] = {
            'status': 'starting',
            'db_name': city,
            'queue_collection': queue_collection,
            'business_collection': business_collection,
            'should_terminate': False,
            'unique_target': 120,  # Target 120 unique records
            'unique_count': 0
        }
        
        # Save workflow status update
        save_workflows(workflows)
        
        # Run Google Maps scraper
        gm_thread = threading.Thread(
            target=run_gmaps_scrape_task_with_limit,
            args=(gm_task_id, city, queue_collection, business_collection, 120)
        )
        gm_thread.daemon = True
        gm_thread.start()
        gm_threads[gm_task_id] = gm_thread
        
        # Wait for Google Maps scraper to complete
        while gm_task_data[gm_task_id]['status'] not in ['completed', 'failed', 'terminated']:
            # Check if workflow should be terminated
            if workflow['should_terminate']:
                gm_task_data[gm_task_id]['should_terminate'] = True
                break
            
            # Update workflow with Google Maps scraper progress
            if 'progress' in gm_task_data[gm_task_id]:
                workflow['stages']['gmaps']['progress'] = gm_task_data[gm_task_id]['progress']
            
            # Update unique count in workflow
            if 'unique_count' in gm_task_data[gm_task_id]:
                workflow['stages']['gmaps']['unique_count'] = gm_task_data[gm_task_id]['unique_count']
            
            # Save workflow status update periodically
            if int(time.time()) % 10 == 0:  # Save every 10 seconds
                save_workflows(workflows)
            
            time.sleep(1)
        
        # Update workflow with Google Maps scraper status
        workflow['stages']['gmaps']['status'] = gm_task_data[gm_task_id]['status']
        
        # Save workflow status update
        save_workflows(workflows)
        
        # Check if Google Maps scraper failed or was terminated
        if gm_task_data[gm_task_id]['status'] in ['failed', 'terminated']:
            workflow['status'] = gm_task_data[gm_task_id]['status']
            workflow['error'] = gm_task_data[gm_task_id].get('error', 'Google Maps scraper failed or was terminated')
            workflow['end_time'] = datetime.now(UTC).isoformat()
            
            # Save workflow status update
            save_workflows(workflows)
            return
        
        # Step 3: Run Email Scraper
        workflow['current_stage'] = 'email_scraping'
        workflow['stages']['email']['status'] = 'running'
        
        # Generate a task ID for email scraper
        es_task_id = f"ES_{city}_{business_collection}_{threading.get_ident()}"
        workflow['stages']['email']['task_id'] = es_task_id
        
        # Connect to MongoDB to check for pending email records
        mongo_client, collection = setup_mongodb(MONGO_URI, city, business_collection)
        
        if mongo_client is None or collection is None:
            workflow['stages']['email']['status'] = 'failed'
            workflow['stages']['email']['error'] = 'Failed to connect to MongoDB'
            workflow['status'] = 'failed'
            workflow['error'] = 'Failed to connect to MongoDB for email scraping'
            workflow['end_time'] = datetime.now(UTC).isoformat()
            
            # Save workflow status update
            save_workflows(workflows)
            return
        
        # Get pending email records
        pending_records = get_pending_records(collection, 0)  # 0 means get all pending records
        num_pending = len(pending_records)
        
        if num_pending == 0:
            workflow['stages']['email']['status'] = 'skipped'
            workflow['stages']['email']['message'] = 'No pending email records found'
            workflow['status'] = 'completed'
            workflow['end_time'] = datetime.now(UTC).isoformat()
            
            # Save workflow status update
            save_workflows(workflows)
            mongo_client.close()
            return
        
        # Initialize task data for email scraper
        es_task_data[es_task_id] = {
            'status': 'starting',
            'db_name': city,
            'collection': business_collection,
            'max_sites': 0,  # Process all pending records
            'headless': HEADLESS,
            'total_records': num_pending,
            'processed': 0,
            'found': 0,
            'checked_no_email': 0,
            'failed': 0,
            'skipped': 0,
            'emails_collected': 0,
            'start_time': datetime.now(UTC).isoformat(),
            'should_terminate': False
        }
        
        # Save workflow status update
        save_workflows(workflows)
        
        # Run email scraper
        es_thread = threading.Thread(
            target=run_email_scrape_task,
            args=(es_task_id, city, business_collection, 0, HEADLESS)
        )
        es_thread.daemon = True
        es_thread.start()
        es_threads[es_task_id] = es_thread
        
        # Wait for email scraper to complete
        while es_task_data[es_task_id]['status'] not in ['completed', 'failed', 'terminated']:
            # Check if workflow should be terminated
            if workflow['should_terminate']:
                es_task_data[es_task_id]['should_terminate'] = True
                break
            
            # Update workflow with email scraper progress
            workflow['stages']['email']['processed'] = es_task_data[es_task_id]['processed']
            workflow['stages']['email']['found'] = es_task_data[es_task_id]['found']
            workflow['stages']['email']['total'] = es_task_data[es_task_id]['total_records']
            
            if es_task_data[es_task_id]['total_records'] > 0:
                progress = (es_task_data[es_task_id]['processed'] / es_task_data[es_task_id]['total_records']) * 100
                workflow['stages']['email']['progress'] = progress
            
            # Save workflow status update periodically
            if int(time.time()) % 10 == 0:  # Save every 10 seconds
                save_workflows(workflows)
            
            time.sleep(1)
        
        # Update workflow with email scraper status
        workflow['stages']['email']['status'] = es_task_data[es_task_id]['status']
        
        # Save workflow status update
        save_workflows(workflows)
        
        # Check if email scraper failed or was terminated
        if es_task_data[es_task_id]['status'] in ['failed', 'terminated']:
            workflow['status'] = es_task_data[es_task_id]['status']
            workflow['error'] = es_task_data[es_task_id].get('error', 'Email scraper failed or was terminated')
        else:
            workflow['status'] = 'completed'
        
        workflow['end_time'] = datetime.now(UTC).isoformat()
        
        # Save workflow status update
        save_workflows(workflows)
    
    except Exception as e:
        logger.error(f"Error in integrated workflow {workflow_id}: {str(e)}")
        workflow['status'] = 'failed'
        workflow['error'] = str(e)
        workflow['end_time'] = datetime.now(UTC).isoformat()
        
        # Save workflow status update
        save_workflows(workflows)
