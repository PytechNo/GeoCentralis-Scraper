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
WFS_TIMEOUT = 90
WFS_LAYER = "mat_uev_cr_s"                     # primary layer
WFS_FALLBACK_LAYERS = [
    "v_a_residentiel_1",
    "v_a_multiresidentiel_3",
    "v_a_non_residentiel_4",
    "v_a_agricole_2",
]

# ── Workers / Scraping ────────────────────────────────────────────────────────
DEFAULT_WORKERS = 4
DEFAULT_HEADLESS = True
MAX_RETRIES = 3
PROPERTY_BATCH_SIZE = 50                        # properties fetched per DB query
PROGRESS_BROADCAST_INTERVAL = 2                 # seconds between WS pushes

# ── API ───────────────────────────────────────────────────────────────────────
API_HOST = "127.0.0.1"
API_PORT = 8080
