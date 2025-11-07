import json
from typing import Dict, List

def load_json_file(filepath: str) -> List[Dict]:
    """Load a JSON file and return the data."""
    print(f"Loading {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"Loaded {len(data)} records from {filepath}")
    return data

def save_json_file(filepath: str, data: List[Dict]):
    """Save data to a JSON file."""
    print(f"Saving {len(data)} records to {filepath}...")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved successfully!")

def update_properties_with_rescrape(all_properties: List[Dict], rescrape_data: List[Dict]) -> tuple:
    """
    Update all_properties with data from rescrape_data based on matricule.
    
    Returns:
        tuple: (updated_properties, update_count)
    """
    # Create a lookup dictionary for rescrape data by matricule
    rescrape_lookup = {}
    for item in rescrape_data:
        matricule = item.get('matricule')
        if matricule:
            rescrape_lookup[matricule] = item
    
    print(f"Created lookup with {len(rescrape_lookup)} rescrape records")
    
    # Update all_properties with rescrape data
    update_count = 0
    for i, prop in enumerate(all_properties):
        matricule = prop.get('matricule')
        if matricule and matricule in rescrape_lookup:
            # Update the property with rescrape data
            rescrape_item = rescrape_lookup[matricule]
            
            # Update all fields from rescrape
            for key, value in rescrape_item.items():
                prop[key] = value
            
            update_count += 1
            
            if (i + 1) % 1000 == 0:
                print(f"Processed {i + 1} properties, updated {update_count} so far...")
    
    return all_properties, update_count

def main():
    # File paths
    all_properties_file = 'all_properties_with_evaluation.json'
    rescrape_file = 'residential_rescrape_results_all.json'
    output_file = 'all_properties_with_evaluation_updated.json'
    
    # Load both files
    all_properties = load_json_file(all_properties_file)
    rescrape_data = load_json_file(rescrape_file)
    
    # Update properties
    print("\nUpdating properties with rescrape data...")
    updated_properties, update_count = update_properties_with_rescrape(all_properties, rescrape_data)
    
    print(f"\nTotal properties in all_properties: {len(all_properties)}")
    print(f"Total properties in rescrape: {len(rescrape_data)}")
    print(f"Properties updated: {update_count}")
    
    # Save the updated data
    save_json_file(output_file, updated_properties)
    
    print(f"\n✓ Update complete! Updated file saved as: {output_file}")

if __name__ == "__main__":
    main()
