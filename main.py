#!/usr/bin/env python3
"""
GeoCentralis Industrial Scraper – entry point.

Usage
  python main.py                     # start the dashboard on http://127.0.0.1:8080
  python main.py --port 9000         # custom port
  python main.py --auto-start -w 6   # auto-import cities + start scraping with 6 workers
"""

from __future__ import annotations

import argparse
import sys
import os

# ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from src import db


def main() -> None:
    parser = argparse.ArgumentParser(description="GeoCentralis Industrial Scraper")
    parser.add_argument("--host", default=config.API_HOST, help="API bind address")
    parser.add_argument("--port", type=int, default=config.API_PORT, help="API port")
    parser.add_argument("--auto-start", action="store_true", help="Auto-import cities and start scraping on boot")
    parser.add_argument("-w", "--workers", type=int, default=config.DEFAULT_WORKERS, help="Number of browser workers")
    parser.add_argument("--headless", action="store_true", default=config.DEFAULT_HEADLESS, help="Run browsers headless")
    parser.add_argument("--no-headless", action="store_true", help="Run browsers with visible UI")
    args = parser.parse_args()

    headless = not args.no_headless if args.no_headless else args.headless

    # init DB
    db.init_db()

    # auto-import cities if requested
    if args.auto_start:
        count = db.import_cities_from_file(config.CITIES_FILE)
        print(f"Imported {count} new cities")

    # start API server (this also starts the broadcast loop)
    import uvicorn
    from src.api import app, coordinator

    if args.auto_start:
        # schedule start after uvicorn boots
        import threading

        def _auto():
            import time
            time.sleep(3)
            coordinator.start(workers=args.workers, headless=headless)

        threading.Thread(target=_auto, daemon=True).start()

    print(f"\n{'='*60}")
    print(f"  GeoCentralis Industrial Scraper")
    print(f"  Dashboard: http://{args.host}:{args.port}")
    print(f"{'='*60}\n")

    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
