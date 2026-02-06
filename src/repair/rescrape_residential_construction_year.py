"""
Multi-worker scraper to re-scrape construction year for residential properties
Targets:
- "Logement" - 369 properties
- "Autres immeubles résidentiels" - 85 properties
Total: 454 residential properties missing construction year
"""

import json
import time
import threading
from queue import Queue
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from datetime import datetime

class ResidentialConstructionYearScraper:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.driver = None
        self.wait = None
        self.properties_scraped = 0
        self.construction_years_found = 0
        
    def setup_driver(self):
        """Initialize Chrome driver with options"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 15)
        print(f"[Worker {self.worker_id}] Browser initialized")
        
    def dismiss_warning_modal(self):
        """Dismiss the legal notice modal if it appears"""
        try:
            accept_button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "btnAccepterAvertissement"))
            )
            accept_button.click()
            time.sleep(0.5)
            print(f"[Worker {self.worker_id}] Dismissed legal notice")
        except (TimeoutException, NoSuchElementException):
            pass
    
    def search_property(self, matricule):
        """Search for a property by matricule"""
        try:
            search_input = self.wait.until(
                EC.presence_of_element_located((By.ID, "txtRecherche"))
            )
            search_input.clear()
            search_input.send_keys(matricule)
            
            search_button = self.driver.find_element(By.ID, "btnRechercher")
            search_button.click()
            
            time.sleep(1.5)
            return True
        except Exception as e:
            print(f"[Worker {self.worker_id}] Error searching for {matricule}: {e}")
            return False
    
    def click_detailed_fiche_button(self):
        """Click the 'Voir fiche du rôle détaillée' button to open modal"""
        try:
            button = self.wait.until(
                EC.element_to_be_clickable((By.ID, "btnVoirFicheDetaillee"))
            )
            button.click()
            time.sleep(1.5)
            return True
        except Exception as e:
            print(f"[Worker {self.worker_id}] Error clicking detailed fiche button: {e}")
            return False
    
    def extract_modal_data(self):
        """Extract all data from the modal dialog, focusing on construction year"""
        try:
            modal = self.wait.until(
                EC.presence_of_element_located((By.ID, "divFicheRoleDetaillee"))
            )
            
            data = {}
            
            # Extract all rows from the modal
            rows = modal.find_elements(By.CSS_SELECTOR, "div.row")
            
            for row in rows:
                try:
                    label_elem = row.find_element(By.CSS_SELECTOR, "label")
                    label_text = label_elem.text.strip().rstrip(':')
                    
                    value_elem = row.find_element(By.CSS_SELECTOR, "div[class*='col-']")
                    value_text = value_elem.text.strip()
                    
                    if label_text and value_text and label_text != value_text:
                        data[label_text] = value_text
                        
                        # Log when we find construction year
                        if label_text == "Année de construction":
                            print(f"[Worker {self.worker_id}] ✓ Found construction year: {value_text}")
                            
                except Exception:
                    continue
            
            return data
            
        except Exception as e:
            print(f"[Worker {self.worker_id}] Error extracting modal data: {e}")
            return None
    
    def close_modal(self):
        """Close the modal dialog with multiple fallback methods"""
        for attempt in range(3):
            try:
                # Method 1: Click close button
                close_button = self.driver.find_element(By.CSS_SELECTOR, "#divFicheRoleDetaillee button.close")
                close_button.click()
                time.sleep(0.5)
                
                # Verify modal is closed
                try:
                    self.driver.find_element(By.ID, "divFicheRoleDetaillee")
                    # Modal still visible, try next method
                    continue
                except NoSuchElementException:
                    return True
                    
            except Exception:
                pass
            
            # Method 2: JavaScript force close
            try:
                self.driver.execute_script("""
                    var modal = document.getElementById('divFicheRoleDetaillee');
                    if (modal) modal.style.display = 'none';
                    var backdrop = document.querySelector('.modal-backdrop');
                    if (backdrop) backdrop.remove();
                """)
                time.sleep(0.5)
                return True
            except Exception:
                pass
        
        return False
    
    def scrape_property(self, property_data):
        """Scrape construction year for a single property"""
        matricule = property_data['matricule']
        
        try:
            # Navigate to the portal
            self.driver.get("https://portail.geocentralis.com/public/sig-web/mrc-appalaches/31015/")
            time.sleep(2)
            
            # Dismiss warning modal
            self.dismiss_warning_modal()
            
            # Search for property
            if not self.search_property(matricule):
                return None
            
            # Click detailed fiche button to open modal
            if not self.click_detailed_fiche_button():
                return None
            
            # Extract modal data
            modal_data = self.extract_modal_data()
            
            # Close modal
            self.close_modal()
            
            if modal_data:
                self.properties_scraped += 1
                
                # Check if we found construction year
                if 'Année de construction' in modal_data and modal_data['Année de construction']:
                    self.construction_years_found += 1
                
                return {
                    'matricule': matricule,
                    'adresse': property_data.get('adresse', 'N/A'),
                    'utilisation_predominante': property_data.get('utilisation_predominante', 'N/A'),
                    'modal_data': modal_data,
                    'scraped_at': datetime.now().isoformat()
                }
            
            return None
            
        except Exception as e:
            print(f"[Worker {self.worker_id}] Error scraping {matricule}: {e}")
            return None
    
    def cleanup(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()
            print(f"[Worker {self.worker_id}] Browser closed")


class MultiWorkerCoordinator:
    def __init__(self, num_workers=4):
        self.num_workers = num_workers
        self.task_queue = Queue()
        self.results = []
        self.results_lock = threading.Lock()
        self.stats = {
            'total': 0,
            'scraped': 0,
            'with_construction_year': 0,
            'failed': 0
        }
        self.stats_lock = threading.Lock()
        
    def worker_thread(self, worker_id):
        """Worker thread function"""
        scraper = ResidentialConstructionYearScraper(worker_id)
        
        try:
            scraper.setup_driver()
            
            while True:
                try:
                    property_data = self.task_queue.get(timeout=5)
                    
                    if property_data is None:
                        break
                    
                    result = scraper.scrape_property(property_data)
                    
                    with self.results_lock:
                        if result:
                            self.results.append(result)
                            
                            with self.stats_lock:
                                self.stats['scraped'] += 1
                                if 'Année de construction' in result.get('modal_data', {}) and result['modal_data']['Année de construction']:
                                    self.stats['with_construction_year'] += 1
                        else:
                            with self.stats_lock:
                                self.stats['failed'] += 1
                    
                    # Progress update every 10 properties
                    if (self.stats['scraped'] + self.stats['failed']) % 10 == 0:
                        self.print_progress()
                    
                    self.task_queue.task_done()
                    
                except Exception as e:
                    print(f"[Worker {worker_id}] Thread error: {e}")
                    break
                    
        finally:
            scraper.cleanup()
            print(f"[Worker {worker_id}] Thread finished")
    
    def print_progress(self):
        """Print current progress"""
        with self.stats_lock:
            total_processed = self.stats['scraped'] + self.stats['failed']
            success_rate = (self.stats['scraped'] / total_processed * 100) if total_processed > 0 else 0
            construction_year_rate = (self.stats['with_construction_year'] / self.stats['scraped'] * 100) if self.stats['scraped'] > 0 else 0
            
            print(f"\n{'='*80}")
            print(f"PROGRESS: {total_processed}/{self.stats['total']} properties")
            print(f"Successfully scraped: {self.stats['scraped']} ({success_rate:.1f}%)")
            print(f"Construction years found: {self.stats['with_construction_year']} ({construction_year_rate:.1f}%)")
            print(f"Failed: {self.stats['failed']}")
            print(f"{'='*80}\n")
    
    def run(self, properties):
        """Run the multi-worker scraping process"""
        self.stats['total'] = len(properties)
        
        print(f"\n{'='*80}")
        print(f"STARTING RESIDENTIAL CONSTRUCTION YEAR RE-SCRAPE")
        print(f"{'='*80}")
        print(f"Total properties to scrape: {len(properties)}")
        print(f"Number of workers: {self.num_workers}")
        print(f"{'='*80}\n")
        
        # Add all properties to queue
        for prop in properties:
            self.task_queue.put(prop)
        
        # Add sentinel values for workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)
        
        # Start worker threads
        threads = []
        for i in range(self.num_workers):
            thread = threading.Thread(target=self.worker_thread, args=(i + 1,))
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        self.print_progress()
        
        return self.results


def filter_residential_properties(input_file):
    """Filter properties to get only residential units"""
    print(f"\nLoading properties from {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        all_properties = json.load(f)
    
    print(f"Total properties in file: {len(all_properties)}")
    
    # Filter for residential types
    residential_types = [
        "Logement",
        "Autres immeubles résidentiels"
    ]
    
    residential_properties = [
        prop for prop in all_properties 
        if prop.get('utilisation_predominante') in residential_types
    ]
    
    print(f"\nFiltered residential properties:")
    for res_type in residential_types:
        count = sum(1 for p in residential_properties if p.get('utilisation_predominante') == res_type)
        print(f"  - {res_type}: {count}")
    
    print(f"\nTotal residential properties to scrape: {len(residential_properties)}")
    
    return residential_properties


def main():
    print("="*80)
    print("RESIDENTIAL CONSTRUCTION YEAR RE-SCRAPER")
    print("="*80)
    
    # Filter residential properties
    residential_properties = filter_residential_properties('data/matricules/properties_without_construction_year.json')
    
    if not residential_properties:
        print("No residential properties found to scrape!")
        return
    
    # Run multi-worker scraper
    num_workers = 4
    coordinator = MultiWorkerCoordinator(num_workers=num_workers)
    results = coordinator.run(residential_properties)
    
    # Save results
    print("\n" + "="*80)
    print("SAVING RESULTS")
    print("="*80)
    
    # All results
    output_file = 'data/results/residential_rescrape_results_all.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved all results to {output_file}")
    
    # Results with construction year
    with_year = [r for r in results if 'Année de construction' in r.get('modal_data', {}) and r['modal_data']['Année de construction']]
    output_file_year = 'data/results/residential_rescrape_with_construction_year.json'
    with open(output_file_year, 'w', encoding='utf-8') as f:
        json.dump(with_year, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved {len(with_year)} properties WITH construction year to {output_file_year}")
    
    # Results still missing construction year
    still_missing = [r for r in results if 'Année de construction' not in r.get('modal_data', {}) or not r['modal_data']['Année de construction']]
    output_file_missing = 'data/results/residential_still_missing_construction_year.json'
    with open(output_file_missing, 'w', encoding='utf-8') as f:
        json.dump(still_missing, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved {len(still_missing)} properties STILL MISSING construction year to {output_file_missing}")
    
    # Final statistics
    print("\n" + "="*80)
    print("FINAL STATISTICS")
    print("="*80)
    print(f"Total residential properties targeted: {len(residential_properties)}")
    print(f"Successfully scraped: {len(results)} ({len(results)/len(residential_properties)*100:.1f}%)")
    print(f"Construction years found: {len(with_year)} ({len(with_year)/len(results)*100:.1f}% of scraped)")
    print(f"Still missing construction year: {len(still_missing)}")
    print(f"Failed to scrape: {len(residential_properties) - len(results)}")
    print("="*80)
    
    # Show some examples of found construction years
    if with_year:
        print("\n" + "="*80)
        print("EXAMPLES OF CONSTRUCTION YEARS FOUND")
        print("="*80)
        for i, prop in enumerate(with_year[:10], 1):
            print(f"{i}. Matricule: {prop['matricule']}")
            print(f"   Adresse: {prop.get('adresse', 'N/A')}")
            print(f"   Usage: {prop.get('utilisation_predominante', 'N/A')}")
            print(f"   Année de construction: {prop['modal_data'].get('Année de construction', 'N/A')}")
            print()


if __name__ == "__main__":
    main()
