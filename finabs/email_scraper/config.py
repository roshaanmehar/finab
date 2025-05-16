"""
config.py - Configuration settings
---------------------------------
Contains all the configuration settings for the email scraper.
"""
import re
from pathlib import Path

# ─────────────────── Paths ──────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# ─────────────────── Tunables & Delays ──────────────
WEBSITE_WAIT_MIN, WEBSITE_WAIT_MAX = 1.0, 2.5
CONTACT_WAIT_MIN, CONTACT_WAIT_MAX = 0.8, 1.5
MONGO_RETRY_ATTEMPTS = 3
MONGO_RETRY_DELAY = 2.0

DEFAULT_MONGO_URI = "mongodb+srv://roshaanatck:DOcnGUEEB37bQtcL@scraper-db-cluster.88kc14b.mongodb.net/?retryWrites=true&w=majority&appName=scraper-db-cluster"
DEFAULT_DB_NAME = "Manchester"
DEFAULT_COLLECTION_NAME = "restaurants"

# Reduce wait times for faster processing
WEBSITE_WAIT_MIN = 1.0  # Reduced from original value
WEBSITE_WAIT_MAX = 2.5  # Reduced from original value
CONTACT_WAIT_MIN = 0.5  # Reduced from original value
CONTACT_WAIT_MAX = 1.5  # Reduced from original value

# Add a timeout for page operations
PAGE_LOAD_TIMEOUT = 20  # seconds
SCRIPT_TIMEOUT = 10     # seconds

# Add a flag for parallel processing
ENABLE_PARALLEL_PROCESSING = True

# Batch size for atomic record acquisition
BATCH_SIZE = 10

# Domain error patterns that indicate a site is unreachable
DOMAIN_ERROR_PATTERNS = [
    "err_name_not_resolved",
    "err_connection_refused", 
    "err_connection_timed_out",
    "err_ssl_protocol_error",
    "err_connection_reset",
    "err_address_unreachable",
    "err_cert_authority_invalid",
    "err_cert_common_name_invalid"
]

# ─────────────────── User Agents ──────────────
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/124.0.0.0",
]

# ─────────────────── Regex Patterns ──────────────
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.(?:[a-zA-Z]{2,}|co\.uk|org\.uk|ac\.uk|gov\.uk|nhs\.uk|com\.au|net\.au|org\.au|ca|de|fr|es|it|nl|eu)")

# ─────────────────── Contact Paths ──────────────
CONTACT_PATHS = [
    "/contact", "/contact-us", "/about", "/about-us", "/get-in-touch", "/reach-us",
    "/enquiry", "/enquiries", "/support", "/help", "/connect", "/feedback",
    "/info/contact", "/company/contact", "/about/contact", "/kontakt", "/contacto", "/contatto",
    "/legal", "/privacy", "/imprint", "/impressum", "/terms", "/customer-service", "/contactus",
    "/contact.html", "/contact.php", "/contact-form", "/contact_us", "/contactinfo"
]

# ─────────────────── Cookie Button Patterns ──────────────
COOKIE_BUTTON_PATTERNS = [
    "accept", "accept all", "i accept", "agree", "i agree", "consent", "allow", "allow all",
    "allow cookies", "ok", "got it", "continue", "understood", "accept cookies",
    "accept & close", "accept and close", "save preferences", "save settings",
    "save and continue", "alle akzeptieren", "akzeptieren", "aceptar", "aceptar todo",
    "accetta", "accetta tutto", "j'accepte", "accepter", "manage settings", "confirm choices",
    "cookiebotdialogbodybuttonaccept", "onetrust-accept-btn-handler", "accept-cookies",
    "cookie-accept", "cookie-consent-accept", "gdpr-accept", "cc-accept", "cookie-agree",
    "cookie-banner__accept", "cookie-consent__accept", "cookie-banner-accept",
    "cookie_action_close_header", "wt-cli-accept-all-btn", "cmplz-accept", "cookie-notice-accept",
    "klaro-accept", "tarteaucitronPersonalize", "_cookiesjsr", "CybotCookiebotDialogBodyButtonAccept"
]

# ─────────────────── Circuit Breaker Settings ──────────────
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3
CIRCUIT_BREAKER_RESET_TIMEOUT = 1800  # 30 minutes in seconds
