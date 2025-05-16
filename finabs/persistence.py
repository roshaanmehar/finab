"""
persistence.py - Persistence utilities
------------------------------
Utilities for persisting data across Flask app restarts.
"""
import pickle
import os
import threading
from typing import Dict, Any

# File to store workflows data
WORKFLOWS_FILE = 'workflows_data.pkl'
LOCK = threading.Lock()

def save_workflows(workflows: Dict[str, Any]) -> None:
    """Save workflows data to a file."""
    with LOCK:
        try:
            with open(WORKFLOWS_FILE, 'wb') as f:
                pickle.dump(workflows, f)
        except Exception as e:
            print(f"Error saving workflows: {e}")

def load_workflows() -> Dict[str, Any]:
    """Load workflows data from a file."""
    if os.path.exists(WORKFLOWS_FILE):
        with LOCK:
            try:
                with open(WORKFLOWS_FILE, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                print(f"Error loading workflows: {e}")
    return {}
