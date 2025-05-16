"""
run.py - Application Runner
------------------------------
Entry point for running the Flask application.
"""
from app import app

if __name__ == '__main__':
    app.run(debug=True)
