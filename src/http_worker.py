"""
HTTP-based property scraper – replaces the Selenium browser worker.

Instead of launching Chrome and interacting with the Leaflet map, this worker
calls the GeoCentralis API endpoints directly via HTTP requests:

1. /fiche_role/unite-evaluation.json/  → resolve matricule → coordinates + date
2. /georole_web_2/info_ue/{mun}/{mat}/ → sidebar HTML (property summary)
3. /fiche_role/propriete/              → fiche détaillée HTML (full details)

All endpoints are public (no auth required).  This approach is:
  • Much faster (no browser overhead)
  • More reliable (no Chrome crashes, no map init issues)
  • Uses ~5 MB RAM per worker instead of ~300 MB
"""

from __future__ import annotations

import html
import json
import re
import threading
import time
import traceback
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config
from src import db

# ── Portal base URL ──────────────────────────────────────────────────────────
PORTAL_BASE = "https://portail.geocentralis.com"


class HTTPWorker:
    """A single HTTP-based scraper running in its own thread."""

    def __init__(
        self,
        worker_id: int,
        job_id: int,
        stop_event: threading.Event,
        pause_event: threading.Event,
    ):
        self.worker_id = worker_id
        self.job_id = job_id
        self.stop_event = stop_event
        self.pause_event = pause_event
        self.session: requests.Session | None = None

    # ── HTTP session lifecycle ────────────────────────────────────────────

    def _setup_session(self) -> None:
        """Create a requests.Session with retry logic and connection pooling."""
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "Accept": "application/json, text/html, */*; q=0.01",
            "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
            "X-Requested-With": "XMLHttpRequest",
        })
        adapter = HTTPAdapter(
            max_retries=Retry(total=3, backoff_factor=0.3, status_forcelist=[502, 503, 504]),
            pool_connections=10,
            pool_maxsize=10,
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        self.session = s

    def _ensure_session(self) -> None:
        if self.session is None:
            self._setup_session()

    # ── API calls ─────────────────────────────────────────────────────────

    def _resolve_matricule(self, matricule: str, municipality_id: str) -> dict | None:
        """Call the evaluation JSON endpoint to get coordinates, date, and matricule_complet.

        Returns dict with {matricule, lat, lng, matricule_complet, idMunicipalite, dateEvenement}
        or None on failure.
        """
        url = f"{PORTAL_BASE}/fiche_role/unite-evaluation.json/"
        today = time.strftime("%Y-%m-%d")
        params = {
            "idFeature": matricule,
            "idMunicipalite": municipality_id,
            "dateEvenement": today,
        }
        try:
            r = self.session.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            props = data.get("properties", {})
            if props and props.get("matricule"):
                return props
            return None
        except Exception as exc:
            self._log("WARN", f"Resolve matricule {matricule}: {type(exc).__name__}: {str(exc)[:100]}")
            return None

    def _fetch_sidebar(self, municipality_id: str, matricule: str, date_evenement: str) -> tuple[str, dict]:
        """Fetch the sidebar HTML from the info_ue endpoint.

        Returns (raw_html, parsed_data) where parsed_data is a dict of key-value pairs.
        """
        url = f"{PORTAL_BASE}/georole_web_2/info_ue/{municipality_id}/{matricule}/"
        params = {
            "matricule_complet": matricule,
            "date_evenement": date_evenement,
            "acces_public": "None",
            "id_module": "22",
        }
        try:
            r = self.session.get(url, params=params, timeout=20)
            r.raise_for_status()
            resp = r.json()
            raw_html = resp.get("html", "")
            ue_exists = resp.get("ue_exists", False)
            if not ue_exists or "Aucune correspondance" in raw_html:
                return "", {}
            parsed = self._parse_sidebar_html(raw_html)
            return raw_html, parsed
        except Exception as exc:
            self._log("WARN", f"Sidebar {matricule}: {type(exc).__name__}: {str(exc)[:100]}")
            return "", {}

    def _fetch_fiche(self, id_ue: str, date_evenement: str, matricule: str) -> tuple[str, dict]:
        """Fetch the fiche détaillée HTML.

        Returns (raw_html, parsed_data).
        """
        url = f"{PORTAL_BASE}/fiche_role/propriete/"
        params = {
            "id_ue": id_ue,
            "date_evenement": date_evenement,
            "matricule": matricule,
        }
        try:
            r = self.session.get(url, params=params, timeout=20)
            r.raise_for_status()
            raw_html = r.text
            if not raw_html or len(raw_html) < 100:
                return "", {}
            parsed = self._parse_fiche_html(raw_html)
            return raw_html, parsed
        except Exception as exc:
            self._log("WARN", f"Fiche {matricule}: {type(exc).__name__}: {str(exc)[:100]}")
            return "", {}

    # ── HTML parsing ──────────────────────────────────────────────────────

    @staticmethod
    def _clean_text(text: str) -> str:
        """Unescape HTML entities, collapse whitespace."""
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        text = text.rstrip(":\u00a0 ")
        return text

    def _parse_sidebar_html(self, raw: str) -> dict:
        """Parse the sidebar HTML (lineContainer1 divs) into a dict.

        Expected structure:
          <div class='lineContainer1'>
              <div class='left1'>Label:</div>
              <div class='right1'>Value</div>
          </div>
        """
        data: dict = {}
        owners: list[str] = []

        # Extract key-value pairs from lineContainer1 divs
        pattern = re.compile(
            r"class=['\"]left1['\"][^>]*>(.*?)</div>\s*"
            r"<div\s+class=['\"]right1['\"][^>]*>(.*?)</div>",
            re.DOTALL | re.IGNORECASE,
        )
        for m in pattern.finditer(raw):
            key = self._clean_text(re.sub(r"<[^>]+>", "", m.group(1)))
            val = self._clean_text(re.sub(r"<[^>]+>", "", m.group(2)))
            if not key or not val:
                continue
            if key == "Nom":
                owners.append(val)
            elif key not in data:
                data[key] = val

        # Extract hidden inputs
        for m in re.finditer(r'id="(\w+)"[^>]*value="([^"]*)"', raw):
            field_id, field_val = m.group(1), m.group(2)
            if field_val and field_id not in data:
                data[f"__{field_id}"] = field_val

        if owners:
            data["Propriétaires"] = owners
            data["Nom"] = "; ".join(owners)

        return data

    def _parse_fiche_html(self, raw: str) -> dict:
        """Parse the fiche détaillée HTML into a dict.

        The fiche has two layouts:
          • Normal rows: col-sm-5 (label) then col-sm-7 (value)
          • Building details: col-sm-7 (label) then col-sm-5 (value)
        We handle both patterns.
        """
        data: dict = {}
        owners: list[str] = []

        # Pattern 1: label in col-sm-5, value in col-sm-7 (normal rows)
        # Use [^§] trick to avoid matching across row boundaries
        pattern1 = re.compile(
            r'<div\s+class="row[^"]*"[^>]*>\s*'
            r'<div\s+class="col-sm-5"[^>]*>\s*<p[^>]*>(.*?)</p>\s*</div>\s*'
            r'<div\s+class="col-sm-7"[^>]*>\s*<p[^>]*>(.*?)</p>',
            re.DOTALL | re.IGNORECASE,
        )
        # Pattern 2: label in col-sm-7, value in col-sm-5 (building details)
        pattern2 = re.compile(
            r'<div\s+class="row[^"]*"[^>]*>\s*'
            r'<div\s+class="col-sm-7"[^>]*>\s*<p[^>]*>(.*?)</p>\s*</div>\s*'
            r'<div\s+class="col-sm-5"[^>]*>\s*<p[^>]*>(.*?)</p>',
            re.DOTALL | re.IGNORECASE,
        )

        for pattern in (pattern1, pattern2):
            for m in pattern.finditer(raw):
                key = self._clean_text(re.sub(r"<[^>]+>", "", m.group(1)))
                val = self._clean_text(re.sub(r"<[^>]+>", "", m.group(2)))
                if not key or not val:
                    continue
                if key == "Nom":
                    owners.append(val)
                elif key not in data:
                    data[key] = val

        # Also extract section headers (h3/h2 with evb-ficheData)
        for m in re.finditer(r'class="evb-ficheData[^"]*"[^>]*>(.*?)</span>', raw, re.DOTALL):
            val = self._clean_text(re.sub(r"<[^>]+>", "", m.group(1)))
            start = max(0, m.start() - 200)
            preceding = raw[start:m.start()]
            label_match = re.search(r'([A-ZÀ-Ÿ][^<:]*?)\s*:?\s*<span', preceding, re.IGNORECASE)
            if label_match and val:
                label = self._clean_text(label_match.group(1))
                if label and label not in data:
                    data[label] = val

        if owners:
            data["Propriétaires"] = owners
            data["Nom"] = "; ".join(owners)

        return data

    # ── single-property scrape ────────────────────────────────────────────

    def _scrape_one(self, prop: dict, municipality_id: str) -> bool:
        """Scrape a single property via HTTP.  Returns True on success."""
        if self.stop_event.is_set():
            return False

        prop_id = prop["id"]
        matricule = prop["matricule"]

        db.mark_property_scraping(prop_id)
        db.update_worker_status(self.worker_id, matricule=matricule)

        if self.stop_event.is_set():
            return False

        # Step 1: Resolve matricule → get coordinates, date, matricule_complet
        resolved = self._resolve_matricule(matricule, municipality_id)
        if not resolved:
            db.mark_property_failed(prop_id, "Matricule not found via evaluation API")
            return False

        matricule_complet = resolved.get("matricule_complet", matricule)
        date_evenement = resolved.get("dateEvenement", time.strftime("%Y-%m-%d"))
        # Clean the date (API returns "2025-07-08 23:59:59", we need "2025-07-08")
        date_evenement = date_evenement.split(" ")[0] if " " in date_evenement else date_evenement

        if self.stop_event.is_set():
            return False

        # Step 2: Fetch sidebar data
        sidebar_html, sidebar_data = self._fetch_sidebar(municipality_id, matricule_complet, date_evenement)
        if not sidebar_data:
            # Try with original matricule format
            sidebar_html, sidebar_data = self._fetch_sidebar(municipality_id, matricule, date_evenement)

        if not sidebar_data:
            # Store at least the resolved data
            minimal = {
                "matricule": matricule,
                "matricule_complet": matricule_complet,
                "lat": resolved.get("lat", ""),
                "lng": resolved.get("lng", ""),
            }
            db.mark_property_scraped(prop_id, minimal, {}, minimal)
            self._log("WARN", f"{matricule}: no sidebar data, saved minimal")
            return True

        if self.stop_event.is_set():
            return False

        # Step 3: Fetch fiche détaillée (if idUe available)
        modal_data = {}
        id_ue = sidebar_data.get("__idUe", "")
        if id_ue:
            _, modal_data = self._fetch_fiche(id_ue, date_evenement, matricule_complet or matricule)

        # Add coordinates to sidebar data
        sidebar_data["lat"] = resolved.get("lat", "")
        sidebar_data["lng"] = resolved.get("lng", "")
        sidebar_data["matricule_complet"] = matricule_complet

        # Remove internal fields from sidebar_data for clean storage
        clean_sidebar = {k: v for k, v in sidebar_data.items() if not k.startswith("__")}

        combined = {**clean_sidebar, **modal_data}
        db.mark_property_scraped(prop_id, clean_sidebar, modal_data, combined)
        return True

    # ── main loop ─────────────────────────────────────────────────────────

    def run(self) -> None:
        db.register_worker(self.worker_id, self.job_id)
        self._log("INFO", "Starting up (HTTP mode)…")

        try:
            # Tiny stagger to spread out initial connections
            if self.worker_id > 1:
                stagger = (self.worker_id - 1) * 0.1
                self._log("INFO", f"Waiting {stagger:.1f}s before starting (stagger)…")
                if self.stop_event.wait(stagger):
                    return

            self._setup_session()
            db.update_worker_status(self.worker_id, status="idle")

            while not self.stop_event.is_set():
                # ── pause gate ────────────────────────────────────────
                while self.pause_event.is_set() and not self.stop_event.is_set():
                    db.update_worker_status(self.worker_id, status="paused")
                    time.sleep(1)
                if self.stop_event.is_set():
                    break

                # ── claim a city ──────────────────────────────────────
                city = db.claim_city_for_scraping()
                if city is None:
                    stats = db.get_dashboard_stats()
                    if stats["pending_cities"] > 0 or stats["ready_cities"] > 0:
                        db.update_worker_status(self.worker_id, status="waiting")
                        time.sleep(3)
                        continue
                    else:
                        self._log("INFO", "No more cities to scrape")
                        break

                city_id = city["id"]
                municipality_id = city["municipality_id"]
                city_label = f"{city['mrc_name']}/{municipality_id}"
                total = city["total_properties"]

                self._log("INFO", f"Claimed city {city_label} ({total} properties)")
                db.update_worker_status(
                    self.worker_id,
                    status="running",
                    city_id=city_id,
                    city_label=city_label,
                    scraped=0,
                    failed=0,
                    city_total=total,
                )

                # ── scrape loop ───────────────────────────────────────
                scraped = 0
                failed = 0
                consecutive_failures = 0
                while not self.stop_event.is_set():
                    while self.pause_event.is_set() and not self.stop_event.is_set():
                        db.update_worker_status(self.worker_id, status="paused")
                        time.sleep(1)

                    batch = db.get_pending_properties(city_id, limit=config.PROPERTY_BATCH_SIZE)
                    if not batch:
                        break

                    for prop in batch:
                        if self.stop_event.is_set():
                            break
                        while self.pause_event.is_set() and not self.stop_event.is_set():
                            db.update_worker_status(self.worker_id, status="paused")
                            time.sleep(1)

                        try:
                            ok = self._scrape_one(prop, municipality_id)
                        except Exception as exc:
                            db.mark_property_failed(prop["id"], str(exc))
                            ok = False

                        if ok:
                            scraped += 1
                            consecutive_failures = 0
                        else:
                            failed += 1
                            consecutive_failures += 1

                        db.update_worker_status(
                            self.worker_id,
                            status="running",
                            scraped=scraped,
                            failed=failed,
                        )

                        if (scraped + failed) % 2 == 0:
                            db.update_city_counts(city_id)

                        # If many consecutive failures, back off
                        if consecutive_failures >= 20:
                            self._log("WARN", f"20 consecutive failures in {city_label} – pausing 10s")
                            consecutive_failures = 0
                            time.sleep(10)
                            # Recreate session in case of connection issues
                            self._setup_session()

                        # Minimal delay between requests
                        time.sleep(config.REQUEST_DELAY)

                # ── city done ─────────────────────────────────────────
                db.update_city_counts(city_id)
                pending = db.get_pending_count(city_id)
                if pending == 0:
                    db.mark_city_completed(city_id)
                    self._log("INFO", f"City {city_label} completed: {scraped} scraped, {failed} failed")
                else:
                    db.mark_city_scraping(city_id)
                    self._log("INFO", f"City {city_label} interrupted ({pending} remaining)")

        except Exception as exc:
            self._log("ERROR", f"Fatal: {traceback.format_exc()}")
        finally:
            if self.session:
                self.session.close()
            db.update_worker_status(self.worker_id, status="stopped")
            self._log("INFO", "Shut down")

    # ── logging helper ────────────────────────────────────────────────────

    def _log(self, level: str, msg: str) -> None:
        tag = f"worker-{self.worker_id}"
        db.add_log(level, tag, msg)
        print(f"[{tag}] {msg}")
