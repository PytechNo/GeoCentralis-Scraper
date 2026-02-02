"""
Clean all_properties_with_evaluation.json by removing properties without modal data

This script:
1. Loads all_properties_with_evaluation.json
2. Filters out properties that don't have modal data
3. Saves a cleaned version with only complete properties
4. Creates a backup of the original file

Usage:
    python clean_properties_remove_no_modal.py
"""

import json
import shutil
from datetime import datetime

def has_modal_data(property_record):
    """
    Check if a property has meaningful modal data
    """
    modal_data = property_record.get('modal_data', {})
    
    # Check if modal_data is empty or None
    if not modal_data:
        return False
    
    # Check if modal_data has at least some key fields
    key_modal_fields = [
        'Utilisation prédominante',
        'Dossier n°',
        'Année de construction',
        'Condition d\'inscription'
    ]
    
    # If at least one key modal field exists, consider it has modal data
    has_key_field = any(field in modal_data for field in key_modal_fields)
    
    return has_key_field


def clean_properties(input_file='data/results/all_properties_with_evaluation.json', 
                     output_file='data/results/all_properties_with_evaluation_cleaned.json',
                     backup=True):
    """
    Remove properties without modal data from the file
    """
    print("="*80)
    print("CLEANING PROPERTIES - REMOVING ENTRIES WITHOUT MODAL DATA")
    print("="*80)
    print()
    
    # Load the file
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
    
    # Create backup if requested
    if backup:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"data/results/all_properties_with_evaluation_backup_{timestamp}.json"
        print(f"\nCreating backup: {backup_file}")
        shutil.copy2(input_file, backup_file)
        print(f"✓ Backup created")
    
    # Filter properties
    print(f"\nFiltering properties...")
    properties_with_modal = []
    properties_without_modal = []
    
    for prop in properties:
        if has_modal_data(prop):
            properties_with_modal.append(prop)
        else:
            properties_without_modal.append({
                'matricule': prop.get('matricule'),
                'adresse': prop.get('adresse')
            })
    
    # Print statistics
    print("\n" + "="*80)
    print("FILTERING RESULTS")
    print("="*80)
    print(f"Original count: {len(properties)}")
    print(f"Properties WITH modal data: {len(properties_with_modal)} ({len(properties_with_modal)/len(properties)*100:.1f}%)")
    print(f"Properties WITHOUT modal data (removed): {len(properties_without_modal)} ({len(properties_without_modal)/len(properties)*100:.1f}%)")
    
    # Save cleaned version
    print(f"\nSaving cleaned properties to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(properties_with_modal, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved {len(properties_with_modal)} properties with complete modal data")
    
    # Save removed properties list for reference
    removed_file = 'data/results/removed_properties_without_modal.json'
    print(f"\nSaving removed properties list to: {removed_file}")
    with open(removed_file, 'w', encoding='utf-8') as f:
        json.dump(properties_without_modal, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved {len(properties_without_modal)} removed properties")
    
    # Also create cleaned GeoJSON
    print(f"\nCreating cleaned GeoJSON...")
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    for result in properties_with_modal:
        if result.get('geometry'):  # Only add if geometry exists
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
    
    geojson_file = 'data/results/all_properties_with_evaluation_cleaned.geojson'
    with open(geojson_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved cleaned GeoJSON: {geojson_file} ({len(geojson['features'])} features)")
    
    # Summary
    print("\n" + "="*80)
    print("CLEANING COMPLETE")
    print("="*80)
    print(f"\nFiles created:")
    if backup:
        print(f"  ✓ {backup_file} (backup of original)")
    print(f"  ✓ {output_file} (cleaned JSON)")
    print(f"  ✓ {geojson_file} (cleaned GeoJSON)")
    print(f"  ✓ {removed_file} (list of removed properties)")
    
    print(f"\nTo replace the original file with the cleaned version:")
    print(f"  1. Review {output_file} to ensure it looks correct")
    print(f"  2. Rename or delete {input_file}")
    print(f"  3. Rename {output_file} to {input_file}")
    print(f"\nOr run: python -c \"import shutil; shutil.copy2('{output_file}', '{input_file}')\"")
    
    return properties_with_modal, properties_without_modal


def main():
    # Option 1: Create cleaned version (keeps original)
    clean_properties(
        input_file='all_properties_with_evaluation.json',
        output_file='all_properties_with_evaluation_cleaned.json',
        backup=True
    )


if __name__ == "__main__":
    main()
