"""
GeoCentralis Property Scraper - Multi-Worker Version
Uses multiple parallel browser instances to scrape evaluation data faster

CLI usage examples (PowerShell):
    & "C:\\Program Files\\Python313\\python.exe" scrape_from_wfs_list_multiworker.py --wfs-file "data_raw\\ALL_mat_uev_cr_s.geojson" --workers 4 --headless
    & "C:\\Program Files\\Python313\\python.exe" scrape_from_wfs_list_multiworker.py --wfs-file "data_raw\\ALL_mat_uev_cr_s.geojson" --workers 2 --limit 100
"""

import time
import argparse
import json
import threading
import queue
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class WorkerScraper:
    """Individual worker that runs in its own thread with its own browser"""
    
    def __init__(self, worker_id, portal_url, headless, task_queue, results_queue, stats_lock, stats):
        self.worker_id = worker_id
        self.portal_url = portal_url
        self.headless = headless
        self.task_queue = task_queue
        self.results_queue = results_queue
        self.stats_lock = stats_lock
        self.stats = stats
        self.driver = None
        
    def setup_driver(self):
        """Initialize Chrome WebDriver for this worker"""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_window_size(1920, 1080)
        
    def load_portal(self):
        """Load the portal and wait for map"""
        self.driver.get(self.portal_url)
        wait = WebDriverWait(self.driver, 30)
        wait.until(EC.presence_of_element_located((By.ID, "map")))
        time.sleep(3)
        
    def dismiss_warning_modal(self):
        """Dismiss legal notice modal"""
        try:
            accept_button = self.driver.find_element(By.CSS_SELECTOR, "button[data-dismiss='modal'].btn-primary")
            accept_button.click()
            time.sleep(0.5)
            return True
        except:
            try:
                buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[data-dismiss='modal']")
                for button in buttons:
                    if "accepte" in button.text.lower():
                        button.click()
                        time.sleep(0.5)
                        return True
            except:
                pass
        return False
        
    def click_property_by_matricule(self, matricule):
        """Select property on map using JavaScript"""
        js_code = f"""
        var map = null;
        for (var key in window) {{
            if (window[key] instanceof L.Map) {{
                map = window[key];
                break;
            }}
        }}
        
        if (map && map.selectFeatureByAttribute) {{
            try {{
                map.selectFeatureByAttribute('{matricule}', true, true);
                return {{success: true}};
            }} catch(e) {{
                return {{success: false, error: e.toString()}};
            }}
        }} else {{
            return {{success: false, error: 'Map or function not found'}};
        }}
        """
        
        try:
            result = self.driver.execute_script(js_code)
            if result.get('success'):
                time.sleep(1.5)
                self.dismiss_warning_modal()
                return True
            return False
        except:
            return False
            
    def extract_evaluation_data_from_sidebar(self):
        """Extract data from sidebar"""
        try:
            time.sleep(0.5)
            line_containers = self.driver.find_elements(By.CLASS_NAME, "lineContainer1")
            
            if not line_containers:
                return None
            
            data = {}
            owner_names = []
            
            for container in line_containers:
                try:
                    left = container.find_element(By.CLASS_NAME, "left1")
                    right = container.find_element(By.CLASS_NAME, "right1")
                    
                    key = left.text.strip().rstrip(':')
                    value = right.text.strip()
                    
                    if key and value:
                        if key == "Nom":
                            owner_names.append(value)
                        elif key not in data:
                            data[key] = value
                except:
                    continue
            
            if owner_names:
                data['Propriétaires'] = owner_names
                data['Nom'] = owner_names[0] if len(owner_names) == 1 else '; '.join(owner_names)
            
            return data if data else None
        except:
            return None
            
    def click_detailed_fiche_button(self):
        """Click button to open detailed modal"""
        try:
            wait = WebDriverWait(self.driver, 10)
            button = wait.until(EC.element_to_be_clickable((By.ID, "btnVoirFicheDetaillee")))
            button.click()
            time.sleep(1)
            return True
        except:
            return False
            
    def extract_modal_data(self):
        """Extract data from modal"""
        try:
            wait = WebDriverWait(self.driver, 10)
            modal_body = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "modal-body")))
            time.sleep(0.5)
            
            data = {}
            owner_names = []
            
            rows = self.driver.find_elements(By.CSS_SELECTOR, ".modal-body .row.margin-bottom-05")
            
            for row in rows:
                try:
                    labels = row.find_elements(By.CSS_SELECTOR, ".col-sm-5, .col-sm-7")
                    values = row.find_elements(By.CSS_SELECTOR, ".col-sm-7, .col-sm-5")
                    
                    if len(labels) >= 1 and len(values) >= 2:
                        label_elem = labels[0]
                        value_elem = values[1] if len(values) > 1 else values[0]
                        
                        label = label_elem.text.strip().rstrip(':').rstrip()
                        value = value_elem.text.strip()
                        
                        if label and value:
                            if label == "Nom":
                                owner_names.append(value)
                            elif label not in data:
                                data[label] = value
                    elif len(labels) == 2:
                        label = labels[0].text.strip().rstrip(':').rstrip()
                        value = labels[1].text.strip()
                        
                        if label and value:
                            if label == "Nom":
                                owner_names.append(value)
                            elif label not in data:
                                data[label] = value
                except:
                    continue
            
            strong_elements = self.driver.find_elements(By.CSS_SELECTOR, ".modal-body .text-lg strong")
            parent_rows = []
            for strong in strong_elements:
                try:
                    parent = strong.find_element(By.XPATH, "./ancestor::div[contains(@class, 'row')]")
                    if parent not in parent_rows:
                        parent_rows.append(parent)
                        all_p = parent.find_elements(By.CSS_SELECTOR, "p.text-lg")
                        if len(all_p) >= 2:
                            label = all_p[0].text.strip().rstrip(':').rstrip()
                            value = all_p[1].text.strip()
                            if label and value:
                                if label == "Nom":
                                    if value not in owner_names:
                                        owner_names.append(value)
                                elif label not in data:
                                    data[label] = value
                except:
                    continue
            
            if owner_names:
                data['Propriétaires'] = owner_names
                data['Nom'] = '; '.join(owner_names)
            
            return data if data else None
        except:
            return None
            
    def close_modal(self):
        """Close modal with multiple fallback methods"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                try:
                    close_button = self.driver.find_element(By.ID, "CloseformModalPageFicheRoleDetaillee")
                    close_button.click()
                    time.sleep(0.8)
                except:
                    try:
                        close_x = self.driver.find_element(By.CSS_SELECTOR, ".modal-header .close")
                        close_x.click()
                        time.sleep(0.8)
                    except:
                        try:
                            dismiss_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[data-dismiss='modal']")
                            for btn in dismiss_buttons:
                                if btn.is_displayed():
                                    btn.click()
                                    time.sleep(0.8)
                                    break
                        except:
                            try:
                                self.driver.execute_script("""
                                    var modals = document.querySelectorAll('.modal');
                                    modals.forEach(function(modal) {
                                        modal.style.display = 'none';
                                        modal.classList.remove('in');
                                    });
                                    var backdrops = document.querySelectorAll('.modal-backdrop');
                                    backdrops.forEach(function(backdrop) {
                                        backdrop.remove();
                                    });
                                    document.body.classList.remove('modal-open');
                                """)
                                time.sleep(0.5)
                            except:
                                pass
                
                try:
                    modal = self.driver.find_element(By.CSS_SELECTOR, ".modal.in")
                    if modal.is_displayed():
                        if attempt < max_attempts - 1:
                            time.sleep(0.5)
                            continue
                        else:
                            return False
                    else:
                        return True
                except:
                    return True
            except:
                if attempt < max_attempts - 1:
                    time.sleep(0.5)
                else:
                    return False
        
        return False
        
    def scrape_property(self, prop):
        """Scrape a single property"""
        matricule = prop['matricule']
        
        if not self.click_property_by_matricule(matricule):
            return None
        
        sidebar_data = self.extract_evaluation_data_from_sidebar()
        if not sidebar_data:
            return None
        
        modal_data = {}
        if self.click_detailed_fiche_button():
            modal_data = self.extract_modal_data() or {}
            self.close_modal()
        
        combined_data = {**sidebar_data, **modal_data}
        
        return {
            'matricule': matricule,
            'adresse': prop['adresse'],
            'geometry': prop['geometry'],
            'sidebar_data': sidebar_data,
            'modal_data': modal_data,
            'evaluation_data': combined_data
        }
        
    def run(self):
        """Main worker loop"""
        try:
            print(f"[Worker {self.worker_id}] Starting up...")
            self.setup_driver()
            self.load_portal()
            print(f"[Worker {self.worker_id}] Ready")
            
            while True:
                try:
                    prop = self.task_queue.get(timeout=2)
                    if prop is None:  # Poison pill
                        break
                    
                    result = self.scrape_property(prop)
                    
                    with self.stats_lock:
                        if result:
                            self.stats['successful'] += 1
                            self.results_queue.put(result)
                        else:
                            self.stats['failed'] += 1
                        
                        total = self.stats['successful'] + self.stats['failed']
                        if total % 10 == 0:
                            print(f"[Worker {self.worker_id}] Progress: {total} total ({self.stats['successful']} success, {self.stats['failed']} failed)")
                    
                    self.task_queue.task_done()
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    print(f"[Worker {self.worker_id}] Error: {e}")
                    self.task_queue.task_done()
                    
        finally:
            if self.driver:
                self.driver.quit()
            print(f"[Worker {self.worker_id}] Shut down")


class MultiWorkerCoordinator:
    """Coordinates multiple worker threads"""
    
    def __init__(self, wfs_file, num_workers=2, headless=False, limit=None, resume=True):
        self.wfs_file = wfs_file
        self.num_workers = num_workers
        self.headless = headless
        self.limit = limit
        self.resume = resume
        self.portal_url = "https://portail.geocentralis.com/public/sig-web/mrc-appalaches/31084/"
        
        self.properties = []
        self.results = []
        self.task_queue = queue.Queue()
        self.results_queue = queue.Queue()
        self.stats_lock = threading.Lock()
        self.stats = {'successful': 0, 'failed': 0}
        
    def load_matricules_from_wfs(self):
        """Load property matricules from WFS GeoJSON file"""
        print(f"Loading matricules from {self.wfs_file}...")
        try:
            with open(self.wfs_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            features = data.get('features', [])
            
            for feature in features:
                props = feature.get('properties', {})
                matricule = props.get('matricule')
                if matricule:
                    self.properties.append({
                        'matricule': matricule,
                        'adresse': props.get('adresse_immeuble', props.get('adresse', 'N/A')),
                        'geometry': feature.get('geometry')
                    })
            
            if self.limit:
                self.properties = self.properties[:self.limit]
            print(f"✓ Loaded {len(self.properties)} properties")
            return True
            
        except Exception as e:
            print(f"❌ Error loading WFS file: {e}")
            return False
            
    def load_progress_backup(self):
        """Load latest progress backup if available"""
        import glob
        backups = sorted(glob.glob("progress_backup_*.json"), key=lambda x: int(x.split('_')[-1].split('.')[0]), reverse=True)
        if backups:
            latest = backups[0]
            print(f"Resuming from backup: {latest}")
            try:
                with open(latest, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                scraped_matricules = set(r['matricule'] for r in self.results)
                self.properties = [p for p in self.properties if p['matricule'] not in scraped_matricules]
                print(f"✓ Skipping {len(scraped_matricules)} already-scraped properties")
                self.stats['successful'] = len(scraped_matricules)
            except Exception as e:
                print(f"❌ Error loading backup: {e}")
        else:
            print("No progress backup found; starting fresh.")
            
    def save_progress(self, count):
        """Save intermediate progress"""
        filename = f"data/results/progress_backup_{count}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Progress saved to {filename}")
        
    def save_results(self):
        """Save final results"""
        with open('data/results/all_properties_with_evaluation.json', 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        geojson = {
            "type": "FeatureCollection",
            "features": []
        }
        
        for result in self.results:
            feature = {
                "type": "Feature",
                "geometry": result['geometry'],
                "properties": {
                    "matricule": result['matricule'],
                    "adresse": result['adresse'],
                    **result['evaluation_data']
                }
            }
            geojson['features'].append(feature)
        
        with open('data/results/all_properties_with_evaluation.geojson', 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Saved {len(self.results)} properties")
        
    def collect_results(self):
        """Collect results from the results queue"""
        while True:
            try:
                result = self.results_queue.get(timeout=1)
                if result is None:
                    break
                self.results.append(result)
                
                if len(self.results) % 10 == 0:
                    self.save_progress(len(self.results))
            except queue.Empty:
                continue
                
    def run(self):
        """Main coordinator method"""
        print("="*80)
        print(f"GEOCENTRALIS MULTI-WORKER SCRAPER ({self.num_workers} workers)")
        print("="*80)
        
        if not self.load_matricules_from_wfs():
            return
            
        if self.resume:
            self.load_progress_backup()
            
        # Add all properties to task queue
        for prop in self.properties:
            self.task_queue.put(prop)
            
        # Add poison pills for workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)
            
        # Start result collector thread
        collector_thread = threading.Thread(target=self.collect_results)
        collector_thread.start()
        
        # Start worker threads
        workers = []
        start_time = time.time()
        
        for i in range(self.num_workers):
            worker = WorkerScraper(
                worker_id=i+1,
                portal_url=self.portal_url,
                headless=self.headless,
                task_queue=self.task_queue,
                results_queue=self.results_queue,
                stats_lock=self.stats_lock,
                stats=self.stats
            )
            thread = threading.Thread(target=worker.run)
            thread.start()
            workers.append(thread)
            time.sleep(2)  # Stagger worker startup
            
        # Wait for all workers to finish
        for thread in workers:
            thread.join()
            
        # Signal collector to stop
        self.results_queue.put(None)
        collector_thread.join()
        
        # Save final results
        self.save_results()
        
        elapsed = time.time() - start_time
        print("\n" + "="*80)
        print("SCRAPING COMPLETE")
        print("="*80)
        print(f"Total properties: {len(self.properties)}")
        print(f"Successful: {self.stats['successful']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Total time: {elapsed / 60:.1f} minutes")
        print(f"Output: data/results/all_properties_with_evaluation.json")
        print(f"Output: data/results/all_properties_with_evaluation.geojson")


def main():
    parser = argparse.ArgumentParser(description="Multi-worker scraper for GeoCentralis evaluation data")
    parser.add_argument('--wfs-file', required=True, help='Path to WFS GeoJSON file')
    parser.add_argument('--workers', type=int, default=2, help='Number of parallel workers (default: 2)')
    parser.add_argument('--headless', action='store_true', help='Run browsers in headless mode')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of properties to scrape')
    parser.add_argument('--no-resume', action='store_true', help='Do not resume from progress backup')
    args = parser.parse_args()

    coordinator = MultiWorkerCoordinator(
        wfs_file=args.wfs_file,
        num_workers=args.workers,
        headless=args.headless,
        limit=args.limit,
        resume=not args.no_resume
    )
    
    try:
        coordinator.run()
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
        coordinator.save_results()

if __name__ == "__main__":
    main()
