"""
SQLite database layer for the GeoCentralis Industrial Scraper.

Provides schema management, CRUD operations for cities / properties / jobs /
workers / logs, and dashboard aggregation queries.

All public functions open their own connection (WAL mode) so they are safe to
call from any thread.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import config

# ── helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _conn():
    """Yield a short-lived connection with WAL journaling."""
    c = sqlite3.connect(config.DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("PRAGMA busy_timeout=10000")
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return dict(row)


def _rows_to_list(rows) -> list[dict]:
    return [dict(r) for r in rows]


# ── schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT    NOT NULL UNIQUE,
    municipality_id TEXT    NOT NULL,
    mrc_name        TEXT    NOT NULL,
    status          TEXT    DEFAULT 'pending',
    total_properties INTEGER DEFAULT 0,
    scraped_count   INTEGER DEFAULT 0,
    failed_count    INTEGER DEFAULT 0,
    wfs_fetched_at  TEXT,
    created_at      TEXT    DEFAULT (datetime('now')),
    updated_at      TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS properties (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id         INTEGER NOT NULL REFERENCES cities(id),
    matricule       TEXT    NOT NULL,
    adresse         TEXT,
    geometry        TEXT,
    status          TEXT    DEFAULT 'pending',
    sidebar_data    TEXT,
    modal_data      TEXT,
    evaluation_data TEXT,
    error_message   TEXT,
    attempts        INTEGER DEFAULT 0,
    scraped_at      TEXT,
    created_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(city_id, matricule)
);

CREATE TABLE IF NOT EXISTS jobs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    status              TEXT    DEFAULT 'idle',
    total_cities        INTEGER DEFAULT 0,
    completed_cities    INTEGER DEFAULT 0,
    workers_requested   INTEGER DEFAULT 4,
    headless            INTEGER DEFAULT 1,
    started_at          TEXT,
    finished_at         TEXT,
    created_at          TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS workers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id           INTEGER NOT NULL,
    job_id              INTEGER REFERENCES jobs(id),
    status              TEXT    DEFAULT 'idle',
    current_city_id     INTEGER REFERENCES cities(id),
    current_city_label  TEXT,
    current_matricule   TEXT,
    properties_scraped  INTEGER DEFAULT 0,
    properties_failed   INTEGER DEFAULT 0,
    city_total          INTEGER DEFAULT 0,
    started_at          TEXT,
    last_heartbeat      TEXT
);

CREATE TABLE IF NOT EXISTS logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    level       TEXT    DEFAULT 'INFO',
    source      TEXT,
    message     TEXT,
    created_at  TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_properties_city_status ON properties(city_id, status);
CREATE INDEX IF NOT EXISTS idx_cities_status ON cities(status);
CREATE INDEX IF NOT EXISTS idx_logs_created ON logs(created_at DESC);
"""


def init_db() -> None:
    """Create all tables if they don't exist."""
    with _conn() as c:
        c.executescript(_SCHEMA)


# ── cities ────────────────────────────────────────────────────────────────────

def parse_city_url(url: str) -> tuple[str, str]:
    """Return (mrc_name, municipality_id) from a portal URL."""
    path = urlparse(url.strip()).path.rstrip("/")
    parts = path.split("/")
    return parts[-2], parts[-1]


def import_cities_from_file(filepath: str) -> int:
    """Read a URL-per-line file, insert new cities. Returns count inserted."""
    with open(filepath, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    inserted = 0
    with _conn() as c:
        for url in urls:
            mrc, mun = parse_city_url(url)
            try:
                cur = c.execute(
                    "INSERT OR IGNORE INTO cities (url, municipality_id, mrc_name) VALUES (?,?,?)",
                    (url.rstrip("/"), mun, mrc),
                )
                if cur.rowcount:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass
    add_log("INFO", "system", f"Imported {inserted} new cities from {filepath}")
    return inserted


def get_all_cities() -> list[dict]:
    with _conn() as c:
        return _rows_to_list(c.execute(
            "SELECT * FROM cities ORDER BY id"
        ).fetchall())


def get_city(city_id: int) -> dict | None:
    with _conn() as c:
        return _row_to_dict(c.execute("SELECT * FROM cities WHERE id=?", (city_id,)).fetchone())


def get_next_city_for_wfs() -> dict | None:
    """Atomically claim the next pending city for WFS fetching."""
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM cities WHERE status='pending' ORDER BY id LIMIT 1"
        ).fetchone()
        if row:
            c.execute("UPDATE cities SET status='fetching_wfs', updated_at=? WHERE id=?", (_now(), row["id"]))
            return _row_to_dict(row)
    return None


def mark_city_wfs_done(city_id: int, total_properties: int) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE cities SET status='wfs_done', total_properties=?, wfs_fetched_at=?, updated_at=? WHERE id=?",
            (total_properties, _now(), _now(), city_id),
        )


def mark_city_wfs_failed(city_id: int) -> None:
    with _conn() as c:
        c.execute("UPDATE cities SET status='wfs_failed', updated_at=? WHERE id=?", (_now(), city_id))


def claim_city_for_scraping() -> dict | None:
    """Atomically claim a wfs_done city for scraping. Returns city dict or None."""
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM cities WHERE status='wfs_done' ORDER BY id LIMIT 1"
        ).fetchone()
        if row:
            c.execute("UPDATE cities SET status='scraping', updated_at=? WHERE id=?", (_now(), row["id"]))
            return _row_to_dict(row)
    return None


def update_city_counts(city_id: int) -> None:
    """Recompute scraped_count and failed_count from property rows."""
    with _conn() as c:
        row = c.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN status='scraped' THEN 1 ELSE 0 END), 0) AS scraped,
                COALESCE(SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END), 0) AS failed
            FROM properties WHERE city_id=?
        """, (city_id,)).fetchone()
        c.execute(
            "UPDATE cities SET scraped_count=?, failed_count=?, updated_at=? WHERE id=?",
            (row["scraped"], row["failed"], _now(), city_id),
        )


def mark_city_completed(city_id: int) -> None:
    update_city_counts(city_id)
    with _conn() as c:
        c.execute("UPDATE cities SET status='completed', updated_at=? WHERE id=?", (_now(), city_id))


def mark_city_scraping(city_id: int) -> None:
    with _conn() as c:
        c.execute("UPDATE cities SET status='scraping', updated_at=? WHERE id=?", (_now(), city_id))


def reset_city(city_id: int) -> None:
    """Reset a city and its properties for re-scraping."""
    with _conn() as c:
        c.execute("UPDATE properties SET status='pending', attempts=0, sidebar_data=NULL, modal_data=NULL, evaluation_data=NULL, error_message=NULL, scraped_at=NULL WHERE city_id=?", (city_id,))
        c.execute("UPDATE cities SET status='wfs_done', scraped_count=0, failed_count=0, updated_at=? WHERE id=?", (_now(), city_id))


def reset_failed_properties(city_id: int) -> int:
    """Reset only failed properties in a city for retry. Returns count reset."""
    with _conn() as c:
        c.execute("UPDATE properties SET status='pending', attempts=0, error_message=NULL WHERE city_id=? AND status='failed'", (city_id,))
        cnt = c.execute("SELECT changes()").fetchone()[0]
        update_city_counts(city_id)
        return cnt


# ── properties ────────────────────────────────────────────────────────────────

def insert_properties(city_id: int, props: list[dict]) -> int:
    """Bulk-insert properties for a city. Returns count inserted."""
    inserted = 0
    with _conn() as c:
        for p in props:
            try:
                cur = c.execute(
                    "INSERT OR IGNORE INTO properties (city_id, matricule, adresse, geometry) VALUES (?,?,?,?)",
                    (city_id, p["matricule"], p.get("adresse", ""), json.dumps(p.get("geometry"))),
                )
                if cur.rowcount:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass
    return inserted


def get_pending_properties(city_id: int, limit: int = 50) -> list[dict]:
    with _conn() as c:
        return _rows_to_list(c.execute(
            "SELECT * FROM properties WHERE city_id=? AND status='pending' ORDER BY id LIMIT ?",
            (city_id, limit),
        ).fetchall())


def get_pending_count(city_id: int) -> int:
    with _conn() as c:
        return c.execute(
            "SELECT COUNT(*) FROM properties WHERE city_id=? AND status='pending'",
            (city_id,),
        ).fetchone()[0]


def mark_property_scraping(prop_id: int) -> None:
    with _conn() as c:
        c.execute("UPDATE properties SET status='scraping', attempts=attempts+1 WHERE id=?", (prop_id,))


def mark_property_scraped(prop_id: int, sidebar_data: dict, modal_data: dict, evaluation_data: dict) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE properties SET status='scraped', sidebar_data=?, modal_data=?, evaluation_data=?, scraped_at=? WHERE id=?",
            (json.dumps(sidebar_data, ensure_ascii=False),
             json.dumps(modal_data, ensure_ascii=False),
             json.dumps(evaluation_data, ensure_ascii=False),
             _now(), prop_id),
        )


def mark_property_failed(prop_id: int, error_message: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE properties SET status='failed', error_message=? WHERE id=?",
            (error_message, prop_id),
        )


def get_city_properties(city_id: int, status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict]:
    with _conn() as c:
        if status:
            return _rows_to_list(c.execute(
                "SELECT * FROM properties WHERE city_id=? AND status=? ORDER BY id LIMIT ? OFFSET ?",
                (city_id, status, limit, offset),
            ).fetchall())
        return _rows_to_list(c.execute(
            "SELECT * FROM properties WHERE city_id=? ORDER BY id LIMIT ? OFFSET ?",
            (city_id, limit, offset),
        ).fetchall())


# ── jobs ──────────────────────────────────────────────────────────────────────

def create_job(workers_count: int = 4, headless: bool = True) -> int:
    with _conn() as c:
        c.execute(
            "INSERT INTO jobs (status, workers_requested, headless, started_at) VALUES ('running',?,?,?)",
            (workers_count, int(headless), _now()),
        )
        return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def get_current_job() -> dict | None:
    with _conn() as c:
        return _row_to_dict(c.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT 1"
        ).fetchone())


def update_job_status(job_id: int, status: str) -> None:
    with _conn() as c:
        extra = ""
        params: list = [status]
        if status in ("completed", "cancelled"):
            extra = ", finished_at=?"
            params.append(_now())
        params.append(job_id)
        c.execute(f"UPDATE jobs SET status=?{extra} WHERE id=?", params)


def update_job_progress(job_id: int) -> None:
    with _conn() as c:
        row = c.execute("SELECT COUNT(*) FROM cities WHERE status='completed'").fetchone()
        c.execute("UPDATE jobs SET completed_cities=? WHERE id=?", (row[0], job_id))


# ── workers ───────────────────────────────────────────────────────────────────

def register_worker(worker_id: int, job_id: int) -> int:
    with _conn() as c:
        c.execute(
            "INSERT INTO workers (worker_id, job_id, status, started_at, last_heartbeat) VALUES (?,?,'starting',?,?)",
            (worker_id, job_id, _now(), _now()),
        )
        return c.execute("SELECT last_insert_rowid()").fetchone()[0]


def update_worker_status(
    worker_id: int,
    *,
    status: str | None = None,
    city_id: int | None = None,
    city_label: str | None = None,
    matricule: str | None = None,
    scraped: int | None = None,
    failed: int | None = None,
    city_total: int | None = None,
) -> None:
    fields = ["last_heartbeat=?"]
    params: list[Any] = [_now()]
    if status is not None:
        fields.append("status=?"); params.append(status)
    if city_id is not None:
        fields.append("current_city_id=?"); params.append(city_id)
    if city_label is not None:
        fields.append("current_city_label=?"); params.append(city_label)
    if matricule is not None:
        fields.append("current_matricule=?"); params.append(matricule)
    if scraped is not None:
        fields.append("properties_scraped=?"); params.append(scraped)
    if failed is not None:
        fields.append("properties_failed=?"); params.append(failed)
    if city_total is not None:
        fields.append("city_total=?"); params.append(city_total)
    params.append(worker_id)
    with _conn() as c:
        c.execute(f"UPDATE workers SET {', '.join(fields)} WHERE worker_id=?", params)


def get_all_workers() -> list[dict]:
    with _conn() as c:
        return _rows_to_list(c.execute("SELECT * FROM workers ORDER BY worker_id").fetchall())


def clear_workers() -> None:
    with _conn() as c:
        c.execute("DELETE FROM workers")


# ── logs ──────────────────────────────────────────────────────────────────────

def add_log(level: str, source: str, message: str) -> None:
    try:
        with _conn() as c:
            c.execute("INSERT INTO logs (level, source, message) VALUES (?,?,?)", (level, source, message))
            # keep only last 5000
            c.execute("DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 5000)")
    except Exception:
        pass  # never crash on logging


def get_recent_logs(limit: int = 100) -> list[dict]:
    with _conn() as c:
        return _rows_to_list(c.execute(
            "SELECT * FROM logs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall())[::-1]


# ── dashboard aggregation ────────────────────────────────────────────────────

def get_dashboard_stats() -> dict:
    with _conn() as c:
        cities = c.execute("""
            SELECT
                COUNT(*)                                                        AS total_cities,
                COALESCE(SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END),0) AS completed_cities,
                COALESCE(SUM(CASE WHEN status='scraping'  THEN 1 ELSE 0 END),0) AS scraping_cities,
                COALESCE(SUM(CASE WHEN status IN ('pending','fetching_wfs') THEN 1 ELSE 0 END),0) AS pending_cities,
                COALESCE(SUM(CASE WHEN status='wfs_done'  THEN 1 ELSE 0 END),0) AS ready_cities,
                COALESCE(SUM(CASE WHEN status='wfs_failed' THEN 1 ELSE 0 END),0) AS failed_cities,
                COALESCE(SUM(total_properties),0) AS total_properties,
                COALESCE(SUM(scraped_count),0)    AS total_scraped,
                COALESCE(SUM(failed_count),0)     AS total_failed
            FROM cities
        """).fetchone()

        job = c.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()

        return {
            "total_cities":      cities["total_cities"],
            "completed_cities":  cities["completed_cities"],
            "scraping_cities":   cities["scraping_cities"],
            "pending_cities":    cities["pending_cities"],
            "ready_cities":      cities["ready_cities"],
            "failed_cities":     cities["failed_cities"],
            "total_properties":  cities["total_properties"],
            "total_scraped":     cities["total_scraped"],
            "total_failed":      cities["total_failed"],
            "job_status":        dict(job)["status"] if job else "idle",
            "job_id":            dict(job)["id"] if job else None,
            "job_started_at":    dict(job)["started_at"] if job else None,
        }


# ── export ────────────────────────────────────────────────────────────────────

def export_city_geojson(city_id: int) -> dict:
    """Build a GeoJSON FeatureCollection for a single city's scraped properties."""
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM properties WHERE city_id=? AND status='scraped'", (city_id,)
        ).fetchall()

    features = []
    for r in rows:
        r = dict(r)
        geom = json.loads(r["geometry"]) if r["geometry"] else None
        eval_data = json.loads(r["evaluation_data"]) if r["evaluation_data"] else {}
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "matricule": r["matricule"],
                "adresse": r["adresse"],
                **eval_data,
            },
        })
    return {"type": "FeatureCollection", "features": features}


def export_all_geojson() -> dict:
    with _conn() as c:
        rows = c.execute("SELECT * FROM properties WHERE status='scraped'").fetchall()

    features = []
    for r in rows:
        r = dict(r)
        geom = json.loads(r["geometry"]) if r["geometry"] else None
        eval_data = json.loads(r["evaluation_data"]) if r["evaluation_data"] else {}
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "matricule": r["matricule"],
                "adresse": r["adresse"],
                **eval_data,
            },
        })
    return {"type": "FeatureCollection", "features": features}
