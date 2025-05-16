"""
config.py - Configuration
------------------------------
Configuration settings for the application.
"""

# MongoDB connection string
MONGO_URI = "mongodb+srv://roshaanatck:DOcnGUEEB37bQtcL@scraper-db-cluster.88kc14b.mongodb.net/?retryWrites=true&w=majority&appName=scraper-db-cluster"

# Scraper settings
NUM_WORKERS = 4  # Number of worker threads for postcode scraper
DELAY = 0.5      # Delay between requests (seconds)
TIMEOUT = 15     # Request timeout (seconds)
HEADLESS = True  # Run browsers in headless mode
