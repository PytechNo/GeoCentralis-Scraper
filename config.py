"""
Central configuration for GeoCentralis Industrial Scraper.
"""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(BASE_DIR, "scraper.db")

# ── Paths ─────────────────────────────────────────────────────────────────────
CITIES_FILE = os.path.join(BASE_DIR, "listurlportail.txt")
DATA_DIR = os.path.join(BASE_DIR, "data")
EXPORT_DIR = os.path.join(DATA_DIR, "exports")

# ── WFS ───────────────────────────────────────────────────────────────────────
WFS_URL = "https://geoserver.geocentralis.com/geoserver/ows"
WFS_PAGE_SIZE = 2000
WFS_TIMEOUT = 30                                # per-page fetch timeout (seconds)
WFS_HITS_TIMEOUT = 15                           # hits-count request timeout (seconds)
WFS_MAX_RETRIES = 3                             # retries per WFS request on timeout
WFS_PREFETCH_THREADS = 3                        # parallel WFS pre-fetch threads
WFS_LAYER = "mat_uev_cr_s"                      # primary layer
WFS_FALLBACK_LAYERS = [
    "v_a_residentiel_1",
    "v_a_multiresidentiel_3",
    "v_a_non_residentiel_4",
    "v_a_agricole_2",
]

# ── Workers / Scraping ────────────────────────────────────────────────────────
# HTTP workers are lightweight (no Chrome), so we can use more of them.
# Each worker uses ~5 MB RAM instead of ~300 MB for Selenium.
DEFAULT_WORKERS = 20
DEFAULT_HEADLESS = True
MAX_RETRIES = 3
PROPERTY_BATCH_SIZE = 200                       # properties fetched per DB query
PROGRESS_BROADCAST_INTERVAL = 1                 # seconds between WS pushes
REQUEST_DELAY = 0.05                            # seconds between HTTP requests (per worker)

# ── API ───────────────────────────────────────────────────────────────────────
API_HOST = "127.0.0.1"
API_PORT = 8080
