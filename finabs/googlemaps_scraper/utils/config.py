"""
config.py - Configuration settings
---------------------------------
Contains all the configuration settings for the Google Maps scraper.
"""

# ─────────────────── Tunables & Delays ──────────────
# Optimized delays - reduced but still human-like
SEARCH_DELAY_MIN, SEARCH_DELAY_MAX = 0.3, 0.7
CLICK_WAIT_MIN,   CLICK_WAIT_MAX   = 0.5, 1.0
CLOSE_WAIT_MIN,   CLOSE_WAIT_MAX   = 0.3, 0.6
SCROLL_WAIT_MIN,  SCROLL_WAIT_MAX  = 1.0, 1.5
SUBSECTOR_WAIT_MIN, SUBSECTOR_WAIT_MAX = 3.0, 5.0
PHONE_WAIT_TIME = 1.0  # Reduced wait time for phone numbers
ADDRESS_WAIT_TIME = 1.0  # Wait time for address to load
WEBSITE_WAIT_TIME = 1.0  # Wait time for website to load
CARD_PROCESSING_DELAY = 6.0  # Seconds to wait between each card processing

MAX_SCROLL_ATTEMPTS = 6   # Reduced from 8 to avoid getting stuck too long
RESULT_LIMIT = 120   # stop after this many cards
MAX_STALE_RETRIES = 3  # Maximum retries for stale element exceptions
PAGE_REFRESH_THRESHOLD = 3  # Refresh page after this many consecutive stale errors
MONGO_RETRY_ATTEMPTS = 3  # Number of times to retry MongoDB operations
MONGO_RETRY_DELAY = 1.0  # Seconds to wait between MongoDB retries
DRIVER_RESET_THRESHOLD = 10  # Reset driver after this many errors

SERVICE, CITY = "restaurants in", "leeds"  # Default city, can be overridden

# ───────────── CSS Selectors & XPaths ─────────────
# Card pane
CARD_PANE_CSS = "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div"
CARD_PANE_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div'

# Business name
NAME_CSS = "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div.TIHn2 > div > div.lMbq3e > div:nth-child(1) > h1"
NAME_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[2]/div/div[1]/div[1]/h1'

# Rating
RATING_CSS = "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div.TIHn2 > div > div.lMbq3e > div.LBgpqf > div > div.fontBodyMedium.dmRWX > div.F7nice > span:nth-child(1) > span:nth-child(1)"
RATING_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[1]/span[1]'

# Number of reviews
REVIEWS_CSS = "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div.TIHn2 > div > div.lMbq3e > div.LBgpqf > div > div.fontBodyMedium.dmRWX > div.F7nice > span:nth-child(2) > span > span"
REVIEWS_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[2]/div/div[1]/div[2]/div/div[1]/div[2]/span[2]/span/span'

# Address - multiple selectors for better reliability
ADDRESS_SELECTORS = [
    # Primary selectors
    "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div:nth-child(9) > div:nth-child(3) > button > div > div.rogA2c > div.Io6YTe.fontBodyMedium.kR99db.fdkmkc",
    '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[9]/div[3]/button/div/div[2]/div[1]',
    
    # Fallback selectors
    "button[data-item-id='address'] div.Io6YTe",
    "button[aria-label*='address'] div.Io6YTe",
    "button[data-tooltip*='address'] div.Io6YTe",
    "div[role='button'][data-item-id*='address'] div.Io6YTe",
    "div.rogA2c div.Io6YTe.fontBodyMedium",
    
    # Generic selectors
    "div.Io6YTe.fontBodyMedium:not(:empty)"
]

# Website - multiple selectors for better reliability
WEBSITE_SELECTORS = [
    # Primary selectors
    "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div:nth-child(9) > div:nth-child(8) > a > div > div.rogA2c.ITvuef > div.Io6YTe.fontBodyMedium.kR99db.fdkmkc",
    '//*[@id="QA0Szd"]/div/div/div[1]/div[3]/div/div[1]/div/div/div[2]/div[9]/div[8]/a/div/div[2]/div[1]',
    
    # Fallback selectors
    "a[data-item-id='authority']",
    "a[aria-label*='website']",
    "a[data-tooltip*='website']",
    "a[href^='https']:not([href*='google'])",
    
    # Generic selectors
    "div.m6QErb a[target='_blank']"
]

# Phone number - prioritized selectors (most reliable first)
PHONE_SELECTORS = [
    "button[data-item-id='phone:tel'] div.Io6YTe",
    "#QA0Szd > div > div > div.w6VYqd > div.bJzME.Hu9e2e.tTVLSc > div > div.e07Vkf.kA9KIf > div > div > div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde > div:nth-child(9) > div:nth-child(9) > button > div > div.rogA2c > div.Io6YTe.fontBodyMedium.kR99db.fdkmkc",
    "button[aria-label*='phone'] div.Io6YTe",
    "button[data-tooltip='Copy phone number'] div.Io6YTe"
]

# Tile name selector - to get name from tile before clicking
TILE_NAME_CSS = "div.qBF1Pd.fontHeadlineSmall"

# Fallback selectors (original ones)
FALLBACK_NAME = "h1.DUwDvf"
FALLBACK_STARS = "span.Aq14fc"
FALLBACK_REVIEWS = "span.z5jxId"
