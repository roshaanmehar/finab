"""
app.py - Main Flask Application
------------------------------
Main Flask application entry point.
"""
from flask import Flask
from flask_cors import CORS
import threading
from collections import defaultdict

from utils.logging_config import setup_logging
from email_scraper.utils.logging_config import setup_email_logging
from persistence import load_workflows

# Create Flask app
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

# Set up logging
logger = setup_logging()
email_logger = setup_email_logging()

# Load persisted workflows
app.config['workflows'] = load_workflows()

# Background task status and data
ps_task_data = {}  # Postcode scraper tasks
gm_task_data = {}  # Google Maps scraper tasks
es_task_data = {}  # Email scraper tasks

# Store thread objects to allow termination
ps_threads = {}  # {task_id: [thread1, thread2, ...]}
gm_threads = {}  # {task_id: thread}
es_threads = {}  # {task_id: thread}

# Import routes after app is created to avoid circular imports
from routes.workflow_routes import workflow_bp
from routes.postcode_routes import postcode_bp
from routes.gmaps_routes import gmaps_bp
from routes.email_routes import email_bp
from routes.legacy_routes import legacy_bp

# Register blueprints
app.register_blueprint(workflow_bp)
app.register_blueprint(postcode_bp)
app.register_blueprint(gmaps_bp)
app.register_blueprint(email_bp)
app.register_blueprint(legacy_bp)

if __name__ == '__main__':
    app.run(debug=True)
