"""
Utilities for persisting data
"""
import pickle
import os
import logging

logger = logging.getLogger(__name__)

def save_workflows(app):
    """Save workflows to file"""
    try:
        with open(app.config['WORKFLOWS_FILE'], 'wb') as f:
            pickle.dump(app.config['workflows'], f)
    except Exception as e:
        logger.error(f"Error saving workflows: {e}")
