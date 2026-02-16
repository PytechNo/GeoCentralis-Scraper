"""
Coordinator – the brain of the scraper.

Responsibilities:
  • WFS pre-fetcher thread: continuously fetches WFS data for pending cities
  • Worker pool: launches and manages N BrowserWorker threads
  • Pause / resume / stop controls (via threading events)
  • Job lifecycle management
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Optional

import config
from src import db
from src.wfs_client import fetch_municipality_properties
from src.http_worker import HTTPWorker


class Coordinator:
    """Singleton-ish orchestrator.  Call ``start()`` / ``pause()`` / ``stop()``."""

    def __init__(self):
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._worker_threads: list[threading.Thread] = []
        self._wfs_threads: list[threading.Thread] = []
        self._monitor_thread: threading.Thread | None = None
        self._job_id: int | None = None
        self._running = False
        self._lock = threading.Lock()

    # ── public controls ───────────────────────────────────────────────────

    @property
    def running(self) -> bool:
        return self._running

    def start(self, workers: int = config.DEFAULT_WORKERS, headless: bool = True) -> dict:
        with self._lock:
            if self._running:
                return {"error": "Job already running"}

            self._stop_event.clear()
            self._pause_event.clear()

            # reset any cities stuck in 'scraping' from a previous crash
            self._reset_stale_cities()

            db.clear_workers()
            self._job_id = db.create_job(workers, headless)
            job = db.get_current_job()
            total_cities = db.get_dashboard_stats()["total_cities"]
            if self._job_id:
                db.update_job_status(self._job_id, "running")

            db.add_log("INFO", "coordinator", f"Starting job #{self._job_id} with {workers} workers (headless={headless})")

            # start WFS pre-fetchers (multiple threads to avoid one slow city blocking everything)
            self._wfs_threads = []
            for i in range(config.WFS_PREFETCH_THREADS):
                t = threading.Thread(target=self._wfs_prefetch_loop, daemon=True, name=f"wfs-prefetch-{i+1}")
                t.start()
                self._wfs_threads.append(t)

            # start browser workers (staggered)
            self._worker_threads = []
            for i in range(workers):
                w = HTTPWorker(
                    worker_id=i + 1,
                    job_id=self._job_id,
                    stop_event=self._stop_event,
                    pause_event=self._pause_event,
                )
                t = threading.Thread(target=w.run, daemon=True, name=f"worker-{i+1}")
                t.start()
                self._worker_threads.append(t)
                time.sleep(1)  # small stagger – HTTP workers are lightweight

            # start monitor thread
            self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True, name="monitor")
            self._monitor_thread.start()

            self._running = True
            return {"job_id": self._job_id, "workers": workers}

    def pause(self) -> dict:
        if not self._running:
            return {"error": "No job running"}
        self._pause_event.set()
        if self._job_id:
            db.update_job_status(self._job_id, "paused")
        db.add_log("INFO", "coordinator", "Job paused")
        return {"status": "paused"}

    def resume(self) -> dict:
        if not self._running:
            return {"error": "No job running"}
        self._pause_event.clear()
        if self._job_id:
            db.update_job_status(self._job_id, "running")
        db.add_log("INFO", "coordinator", "Job resumed")
        return {"status": "running"}

    def stop(self) -> dict:
        if not self._running:
            return {"error": "No job running"}
        db.add_log("INFO", "coordinator", "Stopping job…")
        self._stop_event.set()
        self._pause_event.clear()  # unblock paused workers so they see stop

        # wait for workers (with timeout)
        for t in self._worker_threads:
            t.join(timeout=30)
        for t in self._wfs_threads:
            t.join(timeout=10)

        if self._job_id:
            db.update_job_status(self._job_id, "cancelled")
            db.update_job_progress(self._job_id)

        self._running = False
        self._worker_threads = []
        db.add_log("INFO", "coordinator", "Job stopped")
        return {"status": "stopped"}

    # ── WFS pre-fetcher ───────────────────────────────────────────────────

    def _wfs_prefetch_loop(self) -> None:
        """Continuously pick pending cities and fetch their WFS data."""
        while not self._stop_event.is_set():
            try:
                city = db.get_next_city_for_wfs()
                if city is None:
                    # nothing to pre-fetch right now – sleep and retry
                    time.sleep(5)
                    continue

                mun_id = city["municipality_id"]
                city_id = city["id"]
                label = f"{city['mrc_name']}/{mun_id}"
                db.add_log("INFO", "wfs", f"Fetching WFS for {label}…")

                try:
                    props = fetch_municipality_properties(mun_id)
                    if props:
                        inserted = db.insert_properties(city_id, props)
                        db.mark_city_wfs_done(city_id, len(props))
                        db.add_log("INFO", "wfs", f"{label}: {inserted} properties inserted")
                    else:
                        db.mark_city_wfs_failed(city_id, "No properties found on any WFS layer")
                        db.add_log("WARN", "wfs", f"{label}: no properties found")
                except Exception as exc:
                    db.mark_city_wfs_failed(city_id, str(exc))
                    db.add_log("ERROR", "wfs", f"{label}: WFS error – {exc}")

                time.sleep(1)  # small sleep between cities

            except Exception as exc:
                db.add_log("ERROR", "wfs", f"Pre-fetcher error: {exc}")
                time.sleep(5)

    # ── monitor ───────────────────────────────────────────────────────────

    def _monitor_loop(self) -> None:
        """Watch for job completion."""
        while not self._stop_event.is_set():
            time.sleep(10)
            try:
                if not self._running:
                    break

                # check if all workers are done
                alive = any(t.is_alive() for t in self._worker_threads)
                wfs_alive = any(t.is_alive() for t in self._wfs_threads)
                if not alive and not wfs_alive:
                    # everyone finished
                    if self._job_id:
                        db.update_job_progress(self._job_id)
                        db.update_job_status(self._job_id, "completed")
                    db.add_log("INFO", "coordinator", "All workers finished – job complete")
                    self._running = False
                    break

                # refresh job progress
                if self._job_id:
                    db.update_job_progress(self._job_id)

            except Exception:
                pass

    # ── helpers ───────────────────────────────────────────────────────────

    def _reset_stale_cities(self) -> None:
        """If a previous run crashed, some cities may be stuck in 'scraping' or
        'fetching_wfs'.  Reset them so they can be re-claimed."""
        from src.db import _conn, _now
        with _conn() as c:
            c.execute("UPDATE cities SET status='wfs_done' WHERE status='scraping'")
            c.execute("UPDATE cities SET status='pending' WHERE status='fetching_wfs'")
            # also reset any properties stuck in 'scraping'
            c.execute("UPDATE properties SET status='pending' WHERE status='scraping'")
