"""
GeoCentralis Property Scraper - Using WFS List
Uses the matricule list from WFS data and scrapes evaluation data via browser automation

CLI usage examples (PowerShell):
    & "C:\\Program Files\\Python313\\python.exe" "c:\\My Web Sites\\Scraper\\scrape_from_wfs_list.py" --wfs-file "c:\\My Web Sites\\Scraper\\data_raw\\ALL_mat_uev_cr_s.geojson" --headless --limit 50
    & "C:\\Program Files\\Python313\\python.exe" "c:\\My Web Sites\\Scraper\\scrape_from_wfs_list.py" --wfs-file "c:\\My Web Sites\\Scraper\\data_raw\\ALL_residential_properties.geojson"
"""

import time
import argparse
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

class GeoCentralisWFSScraper:
    def __init__(self, wfs_file='ALL_residential_properties.geojson', headless: bool = False, limit: int | None = None, resume: bool = True):
        self.portal_url = "https://portail.geocentralis.com/public/sig-web/mrc-appalaches/31084/"
        self.driver = None
        self.wfs_file = wfs_file
        self.properties = []
        self.results = []
        self.successful = 0
        self.failed = 0
        self.headless = headless
        self.limit = limit
        self.resume = resume
    def load_progress_backup(self):
        """Load latest progress backup if available"""
        import glob
        backups = sorted(glob.glob("data/results/progress_backup_*.json"), key=lambda x: int(x.split('_')[-1].split('.')[0]), reverse=True)
        if backups:
            latest = backups[0]
            print(f"Resuming from backup: {latest}")
            try:
                with open(latest, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                scraped_matricules = set(r['matricule'] for r in self.results)
                # Remove already-scraped properties from self.properties
                self.properties = [p for p in self.properties if p['matricule'] not in scraped_matricules]
                print(f"✓ Skipping {len(scraped_matricules)} already-scraped properties")
            except Exception as e:
                print(f"❌ Error loading backup: {e}")
        else:
            print("No progress backup found; starting fresh.")
        
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
                    # Note: These layers don't have geometry or address, just matricule
                    self.properties.append({
                        'matricule': matricule,
                        'adresse': props.get('adresse_immeuble', props.get('adresse', 'N/A')),
                        'geometry': feature.get('geometry')
                    })
            
            if self.limit:
                self.properties = self.properties[: self.limit]
            print(f"✓ Loaded {len(self.properties)} properties")
            return True
            
        except Exception as e:
            print(f"❌ Error loading WFS file: {e}")
            return False
    
    def setup_driver(self):
        """Initialize Chrome WebDriver"""
        print("Setting up browser...")
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_window_size(1920, 1080)
        print("✓ Browser ready")
        
    def load_portal(self):
        """Load the portal and wait for map"""
        print(f"\nLoading portal: {self.portal_url}")
        self.driver.get(self.portal_url)
        
        # Wait for map to be present
        wait = WebDriverWait(self.driver, 30)
        wait.until(EC.presence_of_element_located((By.ID, "map")))
        time.sleep(5)  # Extra wait for map to initialize
        print("✓ Portal loaded")
        
    def dismiss_warning_modal(self):
        """Dismiss any warning/avertissement modal (legal notice) that might be blocking"""
        try:
            # Look for "J'accepte" button in the legal notice modal
            accept_button = self.driver.find_element(By.CSS_SELECTOR, "button[data-dismiss='modal'].btn-primary")
            accept_button.click()
            time.sleep(0.5)
            print("   ✓ Dismissed legal notice modal")
            return True
        except:
            try:
                # Alternative: try to find by text content
                buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[data-dismiss='modal']")
                for button in buttons:
                    if "accepte" in button.text.lower():
                        button.click()
                        time.sleep(0.5)
                        print("   ✓ Dismissed legal notice modal")
                        return True
            except:
                pass
            # Modal might not be present, which is fine
            return False
    
    def click_property_by_matricule(self, matricule):
        """Trigger property selection using the map's selectFeatureByAttribute function"""
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
                time.sleep(1.5)  # Wait for sidebar to update
                # Dismiss any warning modal that might be blocking
                self.dismiss_warning_modal()
                return True
            else:
                return False
        except Exception as e:
            return False
    
    def extract_evaluation_data_from_sidebar(self):
        """Extract all data from the sidebar after a property is selected"""
        try:
            # Wait for sidebar to have content
            time.sleep(0.5)
            
            # Find all line containers
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
                        # Special handling for owner names (Nom field can appear multiple times)
                        if key == "Nom":
                            owner_names.append(value)
                        elif key not in data:
                            data[key] = value
                except:
                    continue
            
            # Add all owner names as an array if we found any
            if owner_names:
                data['Propriétaires'] = owner_names
                # Keep the first name in 'Nom' for backwards compatibility
                data['Nom'] = owner_names[0] if len(owner_names) == 1 else '; '.join(owner_names)
            
            return data if data else None
            
        except Exception as e:
            return None
    
    def click_detailed_fiche_button(self):
        """Click the 'Voir fiche du rôle détaillée' button to open the modal"""
        try:
            wait = WebDriverWait(self.driver, 10)
            button = wait.until(EC.element_to_be_clickable((By.ID, "btnVoirFicheDetaillee")))
            button.click()
            time.sleep(1)  # Wait for modal to open
            return True
        except Exception as e:
            print(f"      ⚠ Could not click detailed fiche button: {e}")
            return False
    
    def extract_modal_data(self):
        """Extract all detailed information from the modal"""
        try:
            wait = WebDriverWait(self.driver, 10)
            
            # Wait for modal to be visible
            modal_body = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "modal-body")))
            time.sleep(0.5)  # Extra wait for content to render
            
            data = {}
            owner_names = []
            
            # Extract all rows with col-sm-5 (label) and col-sm-7 (value) or col-sm-7/col-sm-5 pattern
            rows = self.driver.find_elements(By.CSS_SELECTOR, ".modal-body .row.margin-bottom-05")
            
            for row in rows:
                try:
                    # Try different column patterns
                    labels = row.find_elements(By.CSS_SELECTOR, ".col-sm-5, .col-sm-7")
                    values = row.find_elements(By.CSS_SELECTOR, ".col-sm-7, .col-sm-5")
                    
                    if len(labels) >= 1 and len(values) >= 2:
                        # First element is label, second is value
                        label_elem = labels[0]
                        value_elem = values[1] if len(values) > 1 else values[0]
                        
                        label = label_elem.text.strip().rstrip(':').rstrip()
                        value = value_elem.text.strip()
                        
                        if label and value:
                            # Special handling for owner names (Nom field can appear multiple times)
                            if label == "Nom":
                                owner_names.append(value)
                            elif label not in data:
                                data[label] = value
                    elif len(labels) == 2:
                        # Two columns, first is label, second is value
                        label = labels[0].text.strip().rstrip(':').rstrip()
                        value = labels[1].text.strip()
                        
                        if label and value:
                            # Special handling for owner names
                            if label == "Nom":
                                owner_names.append(value)
                            elif label not in data:
                                data[label] = value
                except Exception as e:
                    continue
            
            # Also try extracting with text-lg paragraphs inside strong tags
            strong_elements = self.driver.find_elements(By.CSS_SELECTOR, ".modal-body .text-lg strong")
            parent_rows = []
            for strong in strong_elements:
                try:
                    # Get parent row
                    parent = strong.find_element(By.XPATH, "./ancestor::div[contains(@class, 'row')]")
                    if parent not in parent_rows:
                        parent_rows.append(parent)
                        # Try to find label and value within this row
                        all_p = parent.find_elements(By.CSS_SELECTOR, "p.text-lg")
                        if len(all_p) >= 2:
                            label = all_p[0].text.strip().rstrip(':').rstrip()
                            value = all_p[1].text.strip()
                            if label and value:
                                # Special handling for owner names
                                if label == "Nom":
                                    if value not in owner_names:
                                        owner_names.append(value)
                                elif label not in data:
                                    data[label] = value
                except:
                    continue
            
            # Add all owner names as an array if we found any
            if owner_names:
                data['Propriétaires'] = owner_names
                # Keep joined names in 'Nom' for backwards compatibility
                data['Nom'] = '; '.join(owner_names)
            
            return data if data else None
            
        except Exception as e:
            print(f"      ⚠ Error extracting modal data: {e}")
            return None
    
    def close_modal(self):
        """Close the modal dialog with multiple fallback methods and verification"""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                # Method 1: Try clicking the "Annuler" button by ID
                try:
                    close_button = self.driver.find_element(By.ID, "CloseformModalPageFicheRoleDetaillee")
                    close_button.click()
                    time.sleep(0.8)
                except:
                    # Method 2: Try clicking the X button
                    try:
                        close_x = self.driver.find_element(By.CSS_SELECTOR, ".modal-header .close")
                        close_x.click()
                        time.sleep(0.8)
                    except:
                        # Method 3: Try any button with data-dismiss="modal"
                        try:
                            dismiss_buttons = self.driver.find_elements(By.CSS_SELECTOR, "button[data-dismiss='modal']")
                            for btn in dismiss_buttons:
                                if btn.is_displayed():
                                    btn.click()
                                    time.sleep(0.8)
                                    break
                        except:
                            # Method 4: Force close with JavaScript
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
                
                # Verify modal is actually closed
                try:
                    modal = self.driver.find_element(By.CSS_SELECTOR, ".modal.in")
                    if modal.is_displayed():
                        # Modal still visible, try again
                        if attempt < max_attempts - 1:
                            print(f"      ⚠ Modal still visible, retrying... (attempt {attempt + 2}/{max_attempts})")
                            time.sleep(0.5)
                            continue
                        else:
                            print(f"      ⚠ Could not close modal after {max_attempts} attempts")
                            return False
                    else:
                        return True
                except:
                    # Modal not found or not visible, success!
                    return True
                    
            except Exception as e:
                if attempt < max_attempts - 1:
                    print(f"      ⚠ Error closing modal, retrying... (attempt {attempt + 2}/{max_attempts})")
                    time.sleep(0.5)
                else:
                    print(f"      ⚠ Could not close modal: {e}")
                    return False
        
        return False
    
    def scrape_property(self, prop, index, total):
        """Scrape a single property"""
        matricule = prop['matricule']
        adresse = prop['adresse']
        
        print(f"\n[{index + 1}/{total}] {adresse}")
        print(f"   Matricule: {matricule}")
        
        # Click the property on the map
        if not self.click_property_by_matricule(matricule):
            print(f"   ⚠ Could not select property")
            self.failed += 1
            return False
        
        # Extract data from sidebar
        sidebar_data = self.extract_evaluation_data_from_sidebar()
        
        if not sidebar_data:
            print(f"   ⚠ No data found in sidebar")
            self.failed += 1
            return False
        
        # Click the detailed fiche button to open modal
        modal_data = {}
        if self.click_detailed_fiche_button():
            # Extract detailed data from modal
            modal_data = self.extract_modal_data() or {}
            
            if modal_data:
                print(f"   ✓ Got detailed modal data ({len(modal_data)} fields)")
            else:
                print(f"   ⚠ No data extracted from modal")
            
            # Close modal
            self.close_modal()
        
        # Combine sidebar and modal data
        combined_data = {
            **sidebar_data,
            **modal_data
        }
        
        # Check for valuation fields
        has_valuation = any(key in combined_data for key in [
            'Valeur du terrain', 
            'Valeur du bâtiment', 
            'Valeur de l\'immeuble'
        ])
        
        if has_valuation:
            print(f"   ✓ Got evaluation data ({len(combined_data)} fields)")
            print(f"      Terrain: {combined_data.get('Valeur du terrain', 'N/A')}")
            print(f"      Bâtiment: {combined_data.get('Valeur du bâtiment', 'N/A')}")
            valeur_immeuble = combined_data.get("Valeur de l'immeuble", 'N/A')
            print(f"      Total: {valeur_immeuble}")
        else:
            print(f"   ⚠ No valuation data found")
        
        # Store result
        result = {
            'matricule': matricule,
            'adresse': adresse,
            'geometry': prop['geometry'],
            'sidebar_data': sidebar_data,
            'modal_data': modal_data,
            'evaluation_data': combined_data
        }
        self.results.append(result)
        self.successful += 1
        
        return True
    
    def scrape_all(self):
        """Main scraping loop"""
        print("\n" + "="*80)
        print("STARTING BULK SCRAPING")
        print("="*80)
        
        total = len(self.properties)
        start_time = time.time()
        
        for i, prop in enumerate(self.properties):
            self.scrape_property(prop, i, total)
            
            # Save progress every 10 properties
            if (i + 1) % 10 == 0:
                self.save_progress(i + 1)
                elapsed = time.time() - start_time
                avg_time = elapsed / (i + 1)
                remaining = (total - i - 1) * avg_time
                print(f"\n   Progress: {i + 1}/{total} ({self.successful} successful, {self.failed} failed)")
                print(f"   Estimated time remaining: {remaining / 60:.1f} minutes")
        
        # Final save
        self.save_results()
        
        elapsed = time.time() - start_time
        print("\n" + "="*80)
        print("SCRAPING COMPLETE")
        print("="*80)
        print(f"Total properties: {total}")
        print(f"Successful: {self.successful}")
        print(f"Failed: {self.failed}")
        print(f"Total time: {elapsed / 60:.1f} minutes")
        print(f"Output: all_properties_with_evaluation.json")
        print(f"Output: all_properties_with_evaluation.geojson")
    
    def save_progress(self, count):
        """Save intermediate progress"""
        filename = f"data/results/progress_backup_{count}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"   💾 Progress saved to {filename}")
    
    def save_results(self):
        """Save final results"""
        # Save as JSON
        with open('data/results/all_properties_with_evaluation.json', 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        # Save as GeoJSON
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
    
    def cleanup(self):
        """Close browser"""
        if self.driver:
            print("\nClosing browser...")
            self.driver.quit()

def main():
    parser = argparse.ArgumentParser(description="Scrape evaluation data using matricules from a WFS GeoJSON file")
    parser.add_argument('--wfs-file', required=True, help='Path to WFS GeoJSON (e.g., data_raw/ALL_mat_uev_cr_s.geojson)')
    parser.add_argument('--headless', action='store_true', help='Run Chrome in headless mode')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of properties to scrape (for testing)')
    parser.add_argument('--no-resume', action='store_true', help='Do not resume from progress backup; start fresh')
    args = parser.parse_args()

    print("="*80)
    print("GEOCENTRALIS PROPERTY SCRAPER - WFS MODE")
    print("="*80)

    scraper = GeoCentralisWFSScraper(wfs_file=args.wfs_file, headless=args.headless, limit=args.limit, resume=not args.no_resume)

    try:
        # Load properties from WFS file
        if not scraper.load_matricules_from_wfs():
            return

        # Resume from backup if enabled
        if scraper.resume:
            scraper.load_progress_backup()

        # Setup browser
        scraper.setup_driver()

        # Load portal
        scraper.load_portal()

        # Scrape all properties
        scraper.scrape_all()

    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
        scraper.save_results()

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        scraper.cleanup()

if __name__ == "__main__":
    main()
