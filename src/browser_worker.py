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
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        self.driver = webdriver.Chrome(options=opts)
        self.driver.set_window_size(1920, 1080)

    def _ensure_browser(self) -> None:
        """Restart browser if it died."""
        try:
            _ = self.driver.current_url
        except Exception:
            self._log("WARN", "Browser dead – restarting")
            try:
                self.driver.quit()
            except Exception:
                pass
            self._setup_driver()
            self._current_portal = None

    def _load_portal(self, url: str) -> bool:
        """Navigate to a city portal and wait for the map."""
        self._ensure_browser()
        self._log("INFO", f"Loading portal {url}")
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.ID, "map"))
            )
            time.sleep(3)
            self._dismiss_warning_modal()
            self._current_portal = url
            return True
        except Exception as exc:
            self._log("ERROR", f"Failed to load portal: {exc}")
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

    def _select_matricule(self, matricule: str) -> bool:
        js = f"""
        var map = null;
        for (var key in window) {{
            if (window[key] instanceof L.Map) {{ map = window[key]; break; }}
        }}
        if (map && map.selectFeatureByAttribute) {{
            try {{
                map.selectFeatureByAttribute('{matricule}', true, true);
                return {{success: true}};
            }} catch(e) {{ return {{success: false, error: e.toString()}}; }}
        }}
        return {{success: false, error: 'Map or function not found'}};
        """
        try:
            res = self.driver.execute_script(js)
            if res and res.get("success"):
                time.sleep(1.5)
                self._dismiss_warning_modal()
                return True
        except Exception:
            pass
        return False

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
        prop_id = prop["id"]
        matricule = prop["matricule"]

        db.mark_property_scraping(prop_id)
        db.update_worker_status(self.worker_id, matricule=matricule)

        if not self._select_matricule(matricule):
            db.mark_property_failed(prop_id, "Could not select on map")
            return False

        sidebar = self._extract_sidebar()
        if not sidebar:
            db.mark_property_failed(prop_id, "No sidebar data")
            return False

        modal: dict = {}
        if self._click_detailed_fiche():
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
            self._setup_driver()
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
                    if stats["pending_cities"] > 0 or stats["ready_cities"] > 0 or stats["scraping_cities"] > 1:
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
                    db.mark_city_wfs_failed(city_id, "Could not load portal UI")
                    self._log("ERROR", f"Cannot load portal for {city_label}")
                    continue

                db.update_worker_status(self.worker_id, status="running")

                # ── scrape loop ───────────────────────────────────────
                scraped = 0
                failed = 0
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
                        else:
                            failed += 1

                        db.update_worker_status(
                            self.worker_id,
                            status="running",
                            scraped=scraped,
                            failed=failed,
                        )

                        # periodic city count refresh (every 25 properties)
                        if (scraped + failed) % 25 == 0:
                            db.update_city_counts(city_id)

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
