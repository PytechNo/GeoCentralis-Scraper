"""
FastAPI server – REST API + WebSocket live feed + static dashboard.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import config
from src import db
from src.coordinator import Coordinator

app = FastAPI(title="GeoCentralis Scraper", version="2.0.0")
coordinator = Coordinator()

# ── serve dashboard ───────────────────────────────────────────────────────────

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


# mount any additional static assets (css/js/images) if needed
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ── REST endpoints ────────────────────────────────────────────────────────────

@app.get("/api/dashboard")
def api_dashboard():
    stats = db.get_dashboard_stats()
    # compute rate
    if stats.get("job_started_at"):
        from datetime import datetime, timezone
        try:
            started = datetime.fromisoformat(stats["job_started_at"])
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            stats["elapsed_seconds"] = max(int(elapsed), 0)
            if elapsed > 0 and stats["total_scraped"] > 0:
                rate = stats["total_scraped"] / (elapsed / 60)
                stats["rate_per_minute"] = round(rate, 1)
                remaining = stats["total_properties"] - stats["total_scraped"] - stats["total_failed"]
                stats["eta_seconds"] = int(remaining / (rate / 60)) if rate > 0 else 0
            else:
                stats["rate_per_minute"] = 0
                stats["eta_seconds"] = 0
        except Exception:
            stats["elapsed_seconds"] = 0
            stats["rate_per_minute"] = 0
            stats["eta_seconds"] = 0
    else:
        stats["elapsed_seconds"] = 0
        stats["rate_per_minute"] = 0
        stats["eta_seconds"] = 0
    return stats


@app.get("/api/cities")
def api_cities():
    return db.get_all_cities()


@app.get("/api/cities/{city_id}")
def api_city(city_id: int):
    city = db.get_city(city_id)
    if not city:
        return JSONResponse({"error": "Not found"}, 404)
    return city


@app.get("/api/cities/{city_id}/properties")
def api_city_properties(city_id: int, status: str | None = None, limit: int = 100, offset: int = 0):
    return db.get_city_properties(city_id, status=status, limit=limit, offset=offset)


@app.post("/api/cities/{city_id}/reset")
def api_reset_city(city_id: int):
    db.reset_city(city_id)
    return {"ok": True}


@app.post("/api/cities/{city_id}/retry-failed")
def api_retry_failed(city_id: int):
    count = db.reset_failed_properties(city_id)
    return {"reset": count}


@app.get("/api/workers")
def api_workers():
    return db.get_all_workers()


@app.get("/api/logs")
def api_logs(limit: int = 200):
    return db.get_recent_logs(limit)


@app.get("/api/jobs")
def api_jobs():
    job = db.get_current_job()
    return job if job else {"status": "idle"}


# ── controls ──────────────────────────────────────────────────────────────────

@app.post("/api/cities/import")
def api_import_cities():
    count = db.import_cities_from_file(config.CITIES_FILE)
    return {"imported": count, "total": len(db.get_all_cities())}


@app.post("/api/jobs/start")
def api_start(workers: int = 4, headless: bool = True):
    return coordinator.start(workers=workers, headless=headless)


@app.post("/api/jobs/pause")
def api_pause():
    return coordinator.pause()


@app.post("/api/jobs/resume")
def api_resume():
    return coordinator.resume()


@app.post("/api/jobs/stop")
def api_stop():
    return coordinator.stop()


@app.post("/api/jobs/reset_all")
def api_reset_all():
    # Attempt to stop gracefully
    if coordinator.running:
        coordinator.stop()
    
    # Force reset DB state
    db.reset_all_state()
    return {"status": "reset"}


@app.post("/api/jobs/wipe")
def api_wipe_all():
    # Stop any running process
    if coordinator.running:
        coordinator.stop()

    # Wipe database
    db.wipe_all_data()

    # Automatically re-import cities so we are ready to scrape
    count = db.import_cities_from_file(config.CITIES_FILE)

    return {"status": "wiped", "imported_cities": count}



# ── exports ───────────────────────────────────────────────────────────────────

@app.get("/api/export/{city_id}/geojson")
def api_export_city(city_id: int):
    return db.export_city_geojson(city_id)


@app.get("/api/export/all/geojson")
def api_export_all():
    return db.export_all_geojson()


# ── WebSocket live feed ───────────────────────────────────────────────────────

_clients: Set[WebSocket] = set()


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    _clients.add(websocket)
    try:
        while True:
            # keep-alive: client can send pings, we ignore them
            await websocket.receive_text()
    except WebSocketDisconnect:
        _clients.discard(websocket)
    except Exception:
        _clients.discard(websocket)


async def _broadcast_loop():
    """Push a state snapshot to all connected clients every N seconds."""
    while True:
        await asyncio.sleep(config.PROGRESS_BROADCAST_INTERVAL)
        if not _clients:
            continue
        try:
            payload = _build_ws_payload()
            dead: list[WebSocket] = []
            for ws in list(_clients):
                try:
                    await ws.send_text(json.dumps(payload, default=str))
                except Exception:
                    dead.append(ws)
            for ws in dead:
                _clients.discard(ws)
        except Exception:
            pass


def _build_ws_payload() -> dict:
    stats = db.get_dashboard_stats()
    # compute rate
    if stats.get("job_started_at"):
        from datetime import datetime, timezone
        try:
            started = datetime.fromisoformat(stats["job_started_at"])
            elapsed = (datetime.now(timezone.utc) - started).total_seconds()
            stats["elapsed_seconds"] = max(int(elapsed), 0)
            if elapsed > 0 and stats["total_scraped"] > 0:
                rate = stats["total_scraped"] / (elapsed / 60)
                stats["rate_per_minute"] = round(rate, 1)
                remaining = stats["total_properties"] - stats["total_scraped"] - stats["total_failed"]
                stats["eta_seconds"] = int(remaining / (rate / 60)) if rate > 0 else 0
            else:
                stats["rate_per_minute"] = 0
                stats["eta_seconds"] = 0
        except Exception:
            stats["elapsed_seconds"] = 0
            stats["rate_per_minute"] = 0
            stats["eta_seconds"] = 0
    else:
        stats["elapsed_seconds"] = 0
        stats["rate_per_minute"] = 0
        stats["eta_seconds"] = 0

    return {
        "stats": stats,
        "workers": db.get_all_workers(),
        "cities": db.get_all_cities(),
        "logs": db.get_recent_logs(80),
    }


@app.on_event("startup")
async def on_startup():
    db.init_db()
    asyncio.create_task(_broadcast_loop())
