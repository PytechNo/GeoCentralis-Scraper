"""
Find and export matricules that are missing modal data

This script analyzes the all_properties_with_evaluation.json file and identifies
properties where modal data extraction failed or is incomplete.

Usage:
    python find_missing_modal_data.py
"""

import json
from pathlib import Path

def has_modal_data(property_record):
    """
    Check if a property has modal data
    Returns True if modal_data exists and has meaningful content
    """
    modal_data = property_record.get('modal_data', {})
    
    # Check if modal_data is empty or None
    if not modal_data:
        return False
    
    # Check if modal_data has at least some key fields
    # Modal should have fields like "Dossier n°", "Utilisation prédominante", etc.
    key_modal_fields = [
        'Utilisation prédominante',
        'Dossier n°',
        'Année de construction',
        'Condition d\'inscription'
    ]
    
    # If at least one key modal field exists, consider it has modal data
    has_key_field = any(field in modal_data for field in key_modal_fields)
    
    return has_key_field


def analyze_properties(input_file='data/results/all_properties_with_evaluation.json'):
    """
    Analyze properties and find those missing modal data
    """
    print(f"Loading properties from {input_file}...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            properties = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in '{input_file}': {e}")
        return
    
    print(f"✓ Loaded {len(properties)} properties")
    
    # Analyze properties
    missing_modal = []
    has_modal = []
    
    for prop in properties:
        matricule = prop.get('matricule', 'Unknown')
        
        if has_modal_data(prop):
            has_modal.append(matricule)
        else:
            missing_modal.append({
                'matricule': matricule,
                'adresse': prop.get('adresse', 'N/A'),
                'sidebar_fields': len(prop.get('sidebar_data', {})),
                'modal_fields': len(prop.get('modal_data', {}))
            })
    
    # Print summary
    print("\n" + "="*80)
    print("ANALYSIS SUMMARY")
    print("="*80)
    print(f"Total properties: {len(properties)}")
    print(f"Properties WITH modal data: {len(has_modal)} ({len(has_modal)/len(properties)*100:.1f}%)")
    print(f"Properties MISSING modal data: {len(missing_modal)} ({len(missing_modal)/len(properties)*100:.1f}%)")
    
    # Export missing matricules
    if missing_modal:
        print("\n" + "="*80)
        print("EXPORTING MISSING MODAL DATA")
        print("="*80)
        
        # Export as simple text file (one matricule per line)
        txt_file = 'data/matricules/matricules_missing_modal_data.txt'
        with open(txt_file, 'w', encoding='utf-8') as f:
            for item in missing_modal:
                f.write(f"{item['matricule']}\n")
        print(f"✓ Saved matricules to: {txt_file}")
        
        # Export as JSON with details
        json_file = 'data/matricules/matricules_missing_modal_data.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(missing_modal, f, ensure_ascii=False, indent=2)
        print(f"✓ Saved detailed info to: {json_file}")
        
        # Export as CSV
        csv_file = 'data/matricules/matricules_missing_modal_data.csv'
        with open(csv_file, 'w', encoding='utf-8') as f:
            f.write("matricule,adresse,sidebar_fields,modal_fields\n")
            for item in missing_modal:
                f.write(f"{item['matricule']},{item['adresse']},{item['sidebar_fields']},{item['modal_fields']}\n")
        print(f"✓ Saved CSV to: {csv_file}")
        
        # Show first 10 examples
        print("\n" + "="*80)
        print("FIRST 10 EXAMPLES OF MISSING MODAL DATA:")
        print("="*80)
        for i, item in enumerate(missing_modal[:10], 1):
            print(f"{i}. Matricule: {item['matricule']}")
            print(f"   Address: {item['adresse']}")
            print(f"   Sidebar fields: {item['sidebar_fields']}, Modal fields: {item['modal_fields']}")
            print()
    else:
        print("\n✓ All properties have modal data!")
    
    return missing_modal


def main():
    print("="*80)
    print("FIND PROPERTIES MISSING MODAL DATA")
    print("="*80)
    print()
    
    missing = analyze_properties()
    
    if missing:
        print("\n" + "="*80)
        print("NEXT STEPS")
        print("="*80)
        print("You can re-scrape the missing properties using:")
        print("  1. Edit scrape_from_wfs_list.py to accept a list of matricules")
        print("  2. Use matricules_missing_modal_data.txt as input")
        print("  3. Run scraper with only those matricules")
        print()
        print("Or manually review the CSV file to understand which properties failed.")


if __name__ == "__main__":
    main()
