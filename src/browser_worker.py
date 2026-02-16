"""
Selenium browser worker – runs in its own thread, scrapes one city at a time.

The worker:
1. Claims a city from the DB (status = wfs_done)
2. Opens the portal URL in a headless Chrome instance
3. Iterates over pending properties, scraping sidebar + modal data
4. Writes results back to the DB row-by-row
5. When the city is done, claims the next one and reloads the portal
6. Respects stop / pause events from the coordinator
"""

from __future__ import annotations

import threading
import time
import traceback

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import config
from src import db


class BrowserWorker:
    """A single browser-backed scraper running in its own thread."""

    def __init__(
        self,
        worker_id: int,
        job_id: int,
        headless: bool,
        stop_event: threading.Event,
        pause_event: threading.Event,
    ):
        self.worker_id = worker_id
        self.job_id = job_id
        self.headless = headless
        self.stop_event = stop_event
        self.pause_event = pause_event
        self.driver = None
        self._current_portal: str | None = None  # URL currently loaded

    # ── browser lifecycle ─────────────────────────────────────────────────

    def _setup_driver(self) -> None:
        opts = webdriver.ChromeOptions()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        # ── Container-friendly flags (Proxmox / LXC / Docker) ──
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-sync")
        opts.add_argument("--disable-translate")
        opts.add_argument("--no-first-run")
        opts.add_argument("--disable-setuid-sandbox")
        opts.add_argument("--disable-hang-monitor")
        opts.add_argument("--js-flags=--max-old-space-size=512")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        self.driver = webdriver.Chrome(options=opts)
        self.driver.set_page_load_timeout(120)
        self.driver.set_script_timeout(30)
        self.driver.set_window_size(1280, 720)

    def _ensure_browser(self) -> None:
        """Start or restart Chrome if needed."""
        if self.driver is None:
            self._setup_driver()
            return
        try:
            _ = self.driver.current_url
        except Exception:
            self._log("WARN", "Browser dead – restarting")
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
            self._setup_driver()
            self._current_portal = None

    def _load_portal(self, url: str) -> bool:
        """Navigate to a city portal and wait for the map. Retries up to 3 times with fresh browser."""
        for attempt in range(3):
            self._ensure_browser()
            self._log("INFO", f"Loading portal {url}" + (f" (attempt {attempt+1}/3)" if attempt else ""))
            try:
                self.driver.get(url)
                WebDriverWait(self.driver, 60).until(
                    EC.presence_of_element_located((By.ID, "map"))
                )
                time.sleep(3)
                self._dismiss_warning_modal()
                self._current_portal = url
                # Initialize map feature selection (required before selectFeatureByAttribute works)
                self._init_map_selection()
                return True
            except Exception as exc:
                self._log("WARN", f"Portal load attempt {attempt+1}/3 failed: {type(exc).__name__}: {str(exc)[:120]}")
                # Kill browser completely and restart fresh for next attempt
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.driver = None
                self._current_portal = None
                if attempt < 2:
                    wait = 5 * (attempt + 1)  # 5s, 10s backoff
                    self._log("INFO", f"Waiting {wait}s before retry…")
                    time.sleep(wait)
                    try:
                        self._setup_driver()
                    except Exception as setup_exc:
                        self._log("ERROR", f"Failed to restart browser: {setup_exc}")
                        time.sleep(10)
        self._log("ERROR", f"Failed to load portal after 3 attempts: {url}")
        return False

    def _quit(self) -> None:
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass

    # ── modal / sidebar helpers (preserved from original codebase) ────────

    def _dismiss_warning_modal(self) -> None:
        try:
            btn = self.driver.find_element(
                By.CSS_SELECTOR, "button[data-dismiss='modal'].btn-primary"
            )
            btn.click()
            time.sleep(0.5)
        except Exception:
            try:
                for btn in self.driver.find_elements(
                    By.CSS_SELECTOR, "button[data-dismiss='modal']"
                ):
                    if "accepte" in btn.text.lower():
                        btn.click()
                        time.sleep(0.5)
                        break
            except Exception:
                pass

    def _init_map_selection(self) -> bool:
        """Initialize the map's EnableSelectFeatureByAttribute after portal load.

        The Leaflet plugin Leaflet.Evb.Map.SelectFeature.js adds selectFeatureByAttribute
        to L.Map, but it only works after EnableSelectFeatureByAttribute() is called with
        the correct AJAX URL and municipality ID.  The public sig-web portal only calls
        EnableSelectFeatureOnMap (click-based), so we must set up the attribute-based one.
        """
        js = """
        // Find the Leaflet map – prefer window.map (used by GeoCentralis portal)
        var map = window.map;
        if (!map) {
            for (var key in window) {
                try {
                    if (window[key] && window[key].getZoom && window[key].getCenter && window[key].addLayer) {
                        map = window[key]; break;
                    }
                } catch(e) {}
            }
            if (map) window.map = map;
        }
        if (!map) return {success: false, error: 'Leaflet map not found'};

        // Get municipality ID from hidden input on the page
        var idMunEl = document.getElementById('idMunicipaliteStartup');
        if (!idMunEl) return {success: false, error: 'idMunicipaliteStartup not found'};
        var munId = idMunEl.value;

        // Get current date for date_evenement
        var dateEvt = (typeof moment !== 'undefined') ? moment().format('YYYY-MM-DD')
                    : new Date().toISOString().split('T')[0];

        // Check if EnableSelectFeatureByAttribute exists
        if (typeof map.EnableSelectFeatureByAttribute !== 'function') {
            var methods = Object.getOwnPropertyNames(Object.getPrototypeOf(map)).filter(function(k) {
                return k.indexOf('elect') >= 0 || k.indexOf('nable') >= 0;
            }).join(', ');
            return {success: false, error: 'EnableSelectFeatureByAttribute not on map. Related: ' + (methods || 'none')};
        }

        try {
            map.EnableSelectFeatureByAttribute({
                url: '/fiche_role/unite-evaluation.json/',
                zoomToFeature: false,
                idMunicipalite: munId,
                dateEvenement: dateEvt,
                callback: function(properties) {
                    window._lastGeoResult = properties;
                    if (typeof postSelectFeatureOnMapSig === 'function') {
                        try { postSelectFeatureOnMapSig(properties); } catch(e) {}
                    }
                }
            });
            return {success: true, munId: munId};
        } catch(e) {
            return {success: false, error: 'EnableSelectFeatureByAttribute threw: ' + e.toString()};
        }
        """
        try:
            res = self.driver.execute_script(js)
            if res and res.get("success"):
                self._log("INFO", f"Map selection initialized (municipality: {res.get('munId')})")
                return True
            else:
                error = res.get("error", "unknown") if res else "null result"
                self._log("WARN", f"Map selection init failed: {error}")
        except Exception as exc:
            self._log("WARN", f"Map init error: {type(exc).__name__}: {str(exc)[:150]}")
        return False

    def _select_matricule(self, matricule: str) -> bool:
        js = f"""
        var map = window.map;
        if (!map) return {{success: false, error: 'window.map not set'}};
        if (!map.selectFeatureByAttribute) return {{success: false, error: 'selectFeatureByAttribute not initialized'}};

        window._lastGeoResult = null;
        try {{
            map.selectFeatureByAttribute('{matricule}', true, false);
            return {{success: true}};
        }} catch(e) {{ return {{success: false, error: e.toString()}}; }}
        """
        try:
            res = self.driver.execute_script(js)
            if res and res.get("success"):
                time.sleep(2)  # wait for AJAX response + sidebar population
                self._dismiss_warning_modal()
                return True
            else:
                error = res.get("error", "unknown") if res else "script returned null"
                # If not initialized, try re-init once
                if "not initialized" in str(error) or "not set" in str(error):
                    self._log("WARN", "Map selection not initialized – retrying init")
                    if self._init_map_selection():
                        try:
                            res2 = self.driver.execute_script(js)
                            if res2 and res2.get("success"):
                                time.sleep(2)
                                self._dismiss_warning_modal()
                                return True
                        except Exception:
                            pass
                self._log("WARN", f"selectMatricule({matricule}) failed: {error}")
        except Exception as exc:
            self._log("WARN", f"selectMatricule({matricule}) JS error: {type(exc).__name__}: {str(exc)[:150]}")
        return False

    def _get_property_via_ajax(self, matricule: str) -> dict | None:
        """Directly call the GeoCentralis API to get property data, bypassing the map UI."""
        js = f"""
        var idMunEl = document.getElementById('idMunicipaliteStartup');
        if (!idMunEl) return null;
        var dateEvt = (typeof moment !== 'undefined') ? moment().format('YYYY-MM-DD')
                    : new Date().toISOString().split('T')[0];
        var result = null;
        $.ajax({{
            type: 'GET',
            url: '/fiche_role/unite-evaluation.json/',
            data: {{
                idFeature: '{matricule}',
                idMunicipalite: idMunEl.value,
                dateEvenement: dateEvt
            }},
            async: false,
            dataType: 'json',
            success: function(response) {{
                if (response && response.properties) {{
                    result = response.properties;
                }} else {{
                    result = response;
                }}
            }},
            error: function() {{ result = null; }}
        }});
        return result;
        """
        try:
            data = self.driver.execute_script(js)
            if data and isinstance(data, dict):
                return data
        except Exception:
            pass
        return None

    def _extract_sidebar(self) -> dict | None:
        try:
            time.sleep(0.5)
            containers = self.driver.find_elements(By.CLASS_NAME, "lineContainer1")
            if not containers:
                return None
            data: dict = {}
            owners: list[str] = []
            for c in containers:
                try:
                    key = c.find_element(By.CLASS_NAME, "left1").text.strip().rstrip(":")
                    val = c.find_element(By.CLASS_NAME, "right1").text.strip()
                    if key and val:
                        if key == "Nom":
                            owners.append(val)
                        elif key not in data:
                            data[key] = val
                except Exception:
                    continue
            if owners:
                data["Propriétaires"] = owners
                data["Nom"] = "; ".join(owners)
            return data or None
        except Exception:
            return None

    def _click_detailed_fiche(self) -> bool:
        try:
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "btnVoirFicheDetaillee"))
            ).click()
            time.sleep(1)
            return True
        except Exception:
            return False

    def _extract_modal(self) -> dict | None:
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "modal-body"))
            )
            time.sleep(0.5)
            data: dict = {}
            owners: list[str] = []
            for row in self.driver.find_elements(
                By.CSS_SELECTOR, ".modal-body .row.margin-bottom-05"
            ):
                try:
                    labels = row.find_elements(By.CSS_SELECTOR, ".col-sm-5, .col-sm-7")
                    values = row.find_elements(By.CSS_SELECTOR, ".col-sm-7, .col-sm-5")
                    if len(labels) >= 1 and len(values) >= 2:
                        lbl = labels[0].text.strip().rstrip(":").rstrip()
                        val = values[1].text.strip() if len(values) > 1 else values[0].text.strip()
                        if lbl and val:
                            if lbl == "Nom":
                                owners.append(val)
                            elif lbl not in data:
                                data[lbl] = val
                    elif len(labels) == 2:
                        lbl = labels[0].text.strip().rstrip(":").rstrip()
                        val = labels[1].text.strip()
                        if lbl and val:
                            if lbl == "Nom":
                                owners.append(val)
                            elif lbl not in data:
                                data[lbl] = val
                except Exception:
                    continue
            # strong/text-lg fallback
            parent_rows_seen: list = []
            for strong in self.driver.find_elements(
                By.CSS_SELECTOR, ".modal-body .text-lg strong"
            ):
                try:
                    parent = strong.find_element(
                        By.XPATH, "./ancestor::div[contains(@class, 'row')]"
                    )
                    if parent in parent_rows_seen:
                        continue
                    parent_rows_seen.append(parent)
                    ps = parent.find_elements(By.CSS_SELECTOR, "p.text-lg")
                    if len(ps) >= 2:
                        lbl = ps[0].text.strip().rstrip(":").rstrip()
                        val = ps[1].text.strip()
                        if lbl and val:
                            if lbl == "Nom":
                                if val not in owners:
                                    owners.append(val)
                            elif lbl not in data:
                                data[lbl] = val
                except Exception:
                    continue
            if owners:
                data["Propriétaires"] = owners
                data["Nom"] = "; ".join(owners)
            return data or None
        except Exception:
            return None

    def _close_modal(self) -> bool:
        for _ in range(3):
            try:
                try:
                    self.driver.find_element(
                        By.ID, "CloseformModalPageFicheRoleDetaillee"
                    ).click()
                    time.sleep(0.8)
                except Exception:
                    try:
                        self.driver.find_element(
                            By.CSS_SELECTOR, ".modal-header .close"
                        ).click()
                        time.sleep(0.8)
                    except Exception:
                        self.driver.execute_script("""
                            document.querySelectorAll('.modal').forEach(m=>{m.style.display='none';m.classList.remove('in')});
                            document.querySelectorAll('.modal-backdrop').forEach(b=>b.remove());
                            document.body.classList.remove('modal-open');
                        """)
                        time.sleep(0.5)
                # verify closed
                try:
                    m = self.driver.find_element(By.CSS_SELECTOR, ".modal.in")
                    if not m.is_displayed():
                        return True
                except Exception:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    # ── single-property scrape ────────────────────────────────────────────

    def _scrape_one(self, prop: dict) -> bool:
        """Scrape a single property.  Returns True on success."""
        if self.stop_event.is_set():
            return False

        prop_id = prop["id"]
        matricule = prop["matricule"]

        db.mark_property_scraping(prop_id)
        db.update_worker_status(self.worker_id, matricule=matricule)

        if self.stop_event.is_set():
            return False

        # Check browser is still alive before trying
        try:
            current = self.driver.current_url
        except Exception:
            db.mark_property_failed(prop_id, "Browser dead")
            return False

        if not self._select_matricule(matricule):
            # Fallback: try direct AJAX call to get property data
            ajax_data = self._get_property_via_ajax(matricule)
            if ajax_data:
                self._log("INFO", f"Got {matricule} via direct AJAX (map select failed)")
                db.mark_property_scraped(prop_id, ajax_data, {}, ajax_data)
                return True
            db.mark_property_failed(prop_id, "Could not select on map and AJAX fallback failed")
            return False

        if self.stop_event.is_set():
            return False

        sidebar = self._extract_sidebar()
        if not sidebar:
            # Fallback: check if AJAX result was stored by the callback
            try:
                ajax_data = self.driver.execute_script("return window._lastGeoResult;")
                if ajax_data and isinstance(ajax_data, dict):
                    sidebar = ajax_data
                    self._log("INFO", f"Using stored AJAX result for {matricule} (sidebar empty)")
            except Exception:
                pass

        if not sidebar:
            # Last resort: direct AJAX call
            ajax_data = self._get_property_via_ajax(matricule)
            if ajax_data:
                self._log("INFO", f"Got {matricule} via direct AJAX (sidebar empty)")
                db.mark_property_scraped(prop_id, ajax_data, {}, ajax_data)
                return True
            db.mark_property_failed(prop_id, "No sidebar data and AJAX fallback failed")
            return False

        if self.stop_event.is_set():
            return False

        modal: dict = {}
        if self._click_detailed_fiche():
            if self.stop_event.is_set():
                return False
            modal = self._extract_modal() or {}
            self._close_modal()

        combined = {**sidebar, **modal}
        db.mark_property_scraped(prop_id, sidebar, modal, combined)
        return True

    # ── main loop ─────────────────────────────────────────────────────────

    def run(self) -> None:
        tag = f"worker-{self.worker_id}"
        db.register_worker(self.worker_id, self.job_id)
        self._log("INFO", "Starting up…")

        try:
            # Don't launch Chrome yet – wait until we actually claim a city
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
                    # check if there are still pending / fetching cities
                    stats = db.get_dashboard_stats()
                    if stats["pending_cities"] > 0 or stats["ready_cities"] > 0:
                        db.update_worker_status(self.worker_id, status="waiting")
                        time.sleep(3)
                        continue
                    else:
                        self._log("INFO", "No more cities to scrape")
                        break

                city_id = city["id"]
                city_label = f"{city['mrc_name']}/{city['municipality_id']}"
                total = city["total_properties"]

                self._log("INFO", f"Claimed city {city_label} ({total} properties)")
                db.update_worker_status(
                    self.worker_id,
                    status="loading",
                    city_id=city_id,
                    city_label=city_label,
                    scraped=0,
                    failed=0,
                    city_total=total,
                )

                # ── load portal ───────────────────────────────────────
                portal_url = city["url"]
                if not portal_url.endswith("/"):
                    portal_url += "/"
                if not self._load_portal(portal_url):
                    # Revert city to wfs_done so another worker can try it
                    from src.db import _conn, _now
                    with _conn() as c:
                        c.execute("UPDATE cities SET status='wfs_done', updated_at=? WHERE id=?", (_now(), city_id))
                    self._log("ERROR", f"Cannot load portal for {city_label} – releasing city")
                    continue

                db.update_worker_status(self.worker_id, status="running")

                # ── scrape loop ───────────────────────────────────────
                scraped = 0
                failed = 0
                consecutive_failures = 0
                while not self.stop_event.is_set():
                    # pause gate
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
                            self._ensure_browser()
                            ok = self._scrape_one(prop)
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

                        # periodic city count refresh (every 10 properties)
                        if (scraped + failed) % 10 == 0:
                            db.update_city_counts(city_id)

                        # if too many consecutive failures, browser is likely broken
                        # reload the portal to recover
                        if consecutive_failures >= 10:
                            self._log("WARN", f"10 consecutive failures – reloading portal for {city_label}")
                            consecutive_failures = 0
                            try:
                                self.driver.quit()
                            except Exception:
                                pass
                            self._setup_driver()
                            self._current_portal = None
                            if not self._load_portal(portal_url):
                                self._log("ERROR", f"Cannot reload portal – abandoning {city_label}")
                                break

                # ── city done ─────────────────────────────────────────
                db.update_city_counts(city_id)
                pending = db.get_pending_count(city_id)
                if pending == 0:
                    db.mark_city_completed(city_id)
                    self._log("INFO", f"City {city_label} completed: {scraped} scraped, {failed} failed")
                else:
                    # stopped mid-city: revert city to wfs_done so another worker can pick it up
                    db.mark_city_scraping(city_id)  # keep as scraping; coordinator will handle
                    self._log("INFO", f"City {city_label} interrupted ({pending} remaining)")

        except Exception as exc:
            self._log("ERROR", f"Fatal: {traceback.format_exc()}")
        finally:
            self._quit()
            db.update_worker_status(self.worker_id, status="stopped")
            self._log("INFO", "Shut down")

    # ── logging helper ────────────────────────────────────────────────────

    def _log(self, level: str, msg: str) -> None:
        tag = f"worker-{self.worker_id}"
        db.add_log(level, tag, msg)
        print(f"[{tag}] {msg}")
