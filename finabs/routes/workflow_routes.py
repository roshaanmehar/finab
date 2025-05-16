"""
routes/workflow_routes.py - Workflow Routes
------------------------------
Routes for integrated workflow operations.
"""
from flask import Blueprint, request, jsonify
import threading
from datetime import datetime, UTC
from collections import defaultdict

from app import app, logger, ps_task_data, gm_task_data, es_task_data, ps_threads, gm_threads, es_threads
from config import MONGO_URI, HEADLESS
from persistence import save_workflows
from tasks.workflow_tasks import run_integrated_workflow
from utils.city_abbreviations import get_city_abbreviation
from db_management.db_connection import connect_to_mongodb

workflow_bp = Blueprint('workflow', __name__)

@workflow_bp.route('/api/scrapeAll', methods=['GET'])
def start_integrated_scrape():
    """API endpoint to start an integrated scraping workflow that handles all steps automatically."""
    # Get parameters from URL
    city = request.args.get('city')
    keyword = request.args.get('keyword', 'restaurants')
    
    if not city:
        return jsonify({'error': 'Missing required parameter: city'}), 400
    
    # Generate a workflow ID
    workflow_id = f"WF_{city}_{keyword}_{threading.get_ident()}"
    
    # Initialize workflow status
    workflow_status = {
        'workflow_id': workflow_id,
        'city': city,
        'keyword': keyword,
        'status': 'starting',
        'current_stage': 'initializing',
        'stages': {
            'postcode': {'status': 'pending', 'task_id': None},
            'gmaps': {'status': 'pending', 'task_id': None},
            'email': {'status': 'pending', 'task_id': None}
        },
        'start_time': datetime.now(UTC).isoformat(),
        'should_terminate': False
    }
    
    # Store workflow status
    app.config.setdefault('workflows', {})[workflow_id] = workflow_status
    
    # Save workflows to file
    save_workflows(app.config['workflows'])
    
    # Start workflow in background thread
    workflow_thread = threading.Thread(
        target=run_integrated_workflow,
        args=(workflow_id, city, keyword, app.config['workflows'])
    )
    workflow_thread.daemon = True
    workflow_thread.start()
    
    return jsonify({
        'workflow_id': workflow_id,
        'message': f'Integrated scraping workflow started for {city} with keyword {keyword}',
        'status_url': f'/api/statusAll/{workflow_id}'
    })

@workflow_bp.route('/api/statusAll/<workflow_id>', methods=['GET'])
def get_integrated_status(workflow_id):
    """API endpoint to get the status of an integrated scraping workflow."""
    workflows = app.config.setdefault('workflows', {})
    
    if workflow_id not in workflows:
        return jsonify({'error': 'Workflow not found'}), 404
    
    workflow = workflows[workflow_id]
    
    # Calculate elapsed time
    if 'start_time' in workflow:
        start_time = datetime.fromisoformat(workflow['start_time'])
        if workflow['status'] == 'completed':
            end_time = datetime.fromisoformat(workflow.get('end_time', datetime.now(UTC).isoformat()))
        else:
            end_time = datetime.now(UTC)
        
        elapsed_seconds = (end_time - start_time).total_seconds()
        workflow['elapsed_time'] = elapsed_seconds
    
    return jsonify(workflow)

@workflow_bp.route('/api/terminateAll/<workflow_id>', methods=['POST'])
def terminate_integrated_workflow(workflow_id):
    """API endpoint to terminate a running integrated scraping workflow."""
    workflows = app.config.setdefault('workflows', {})
    
    if workflow_id not in workflows:
        return jsonify({'error': 'Workflow not found'}), 404
    
    workflow = workflows[workflow_id]
    
    # Check if workflow is already completed or failed
    if workflow['status'] in ['completed', 'failed', 'terminated']:
        return jsonify({
            'message': f'Workflow {workflow_id} is already in state: {workflow["status"]}',
            'status': workflow['status']
        })
    
    # Set termination flag
    workflow['should_terminate'] = True
    workflow['status'] = 'terminating'
    
    # Terminate any active tasks
    for stage, info in workflow['stages'].items():
        if info['status'] == 'running' and info['task_id']:
            task_id = info['task_id']
            
            if stage == 'postcode' and task_id in ps_task_data:
                ps_task_data[task_id]['should_terminate'] = True
                ps_task_data[task_id]['stop_scraping'] = True
            elif stage == 'gmaps' and task_id in gm_task_data:
                gm_task_data[task_id]['should_terminate'] = True
            elif stage == 'email' and task_id in es_task_data:
                es_task_data[task_id]['should_terminate'] = True
    
    # Update workflow status
    workflow['status'] = 'terminated'
    workflow['end_time'] = datetime.now(UTC).isoformat()
    
    # Save workflows to file
    save_workflows(app.config['workflows'])
    
    return jsonify({
        'message': f'Workflow {workflow_id} has been terminated',
        'status': 'terminated'
    })
