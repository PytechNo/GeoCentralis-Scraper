"""
Merge rescrape_results_with_modal.json into all_properties_with_evaluation.json

This script:
1. Loads the cleaned all_properties_with_evaluation.json
2. Loads the re-scraped data from rescrape_results_with_modal.json
3. Merges them together (avoiding duplicates)
4. Saves the final complete dataset

Usage:
    python merge_rescrape_results.py
"""

import json
import shutil
from datetime import datetime

def merge_properties(
    main_file='all_properties_with_evaluation.json',
    rescrape_file='rescrape_results_with_modal.json',
    output_file='all_properties_with_evaluation_merged.json'
):
    """
    Merge re-scraped properties into the main file
    """
    print("="*80)
    print("MERGING RE-SCRAPED PROPERTIES WITH CLEANED DATA")
    print("="*80)
    print()
    
    # Load main file
    print(f"Loading main file: {main_file}...")
    try:
        with open(main_file, 'r', encoding='utf-8') as f:
            main_properties = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: File '{main_file}' not found")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in '{main_file}': {e}")
        return
    
    print(f"✓ Loaded {len(main_properties)} properties from main file")
    
    # Load rescrape file
    print(f"\nLoading rescrape file: {rescrape_file}...")
    try:
        with open(rescrape_file, 'r', encoding='utf-8') as f:
            rescrape_properties = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: File '{rescrape_file}' not found")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON in '{rescrape_file}': {e}")
        return
    
    print(f"✓ Loaded {len(rescrape_properties)} re-scraped properties")
    
    # Create backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"all_properties_backup_before_merge_{timestamp}.json"
    print(f"\nCreating backup: {backup_file}")
    shutil.copy2(main_file, backup_file)
    print(f"✓ Backup created")
    
    # Build a dictionary of existing matricules for quick lookup
    print(f"\nAnalyzing existing properties...")
    existing_matricules = {}
    for prop in main_properties:
        matricule = prop.get('matricule')
        if matricule:
            existing_matricules[matricule] = prop
    
    print(f"✓ Found {len(existing_matricules)} unique matricules in main file")
    
    # Merge re-scraped properties
    print(f"\nMerging re-scraped properties...")
    added = 0
    updated = 0
    skipped = 0
    
    for prop in rescrape_properties:
        matricule = prop.get('matricule')
        
        if not matricule:
            skipped += 1
            continue
        
        if matricule in existing_matricules:
            # Update existing property (replace with re-scraped version which has modal data)
            existing_matricules[matricule] = prop
            updated += 1
        else:
            # Add new property
            main_properties.append(prop)
            existing_matricules[matricule] = prop
            added += 1
    
    # Print merge statistics
    print("\n" + "="*80)
    print("MERGE RESULTS")
    print("="*80)
    print(f"Original properties: {len(main_properties) - added}")
    print(f"Re-scraped properties: {len(rescrape_properties)}")
    print(f"  - Added (new): {added}")
    print(f"  - Updated (replaced): {updated}")
    print(f"  - Skipped (no matricule): {skipped}")
    print(f"Final total: {len(main_properties)} properties")
    
    # Save merged file
    print(f"\nSaving merged properties to: {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(main_properties, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved {len(main_properties)} properties")
    
    # Create merged GeoJSON
    print(f"\nCreating merged GeoJSON...")
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }
    
    for result in main_properties:
        if result.get('geometry'):
            feature = {
                "type": "Feature",
                "geometry": result['geometry'],
                "properties": {
                    "matricule": result['matricule'],
                    "adresse": result['adresse'],
                    **result.get('evaluation_data', {})
                }
            }
            geojson['features'].append(feature)
    
    geojson_file = 'all_properties_with_evaluation_merged.geojson'
    with open(geojson_file, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved merged GeoJSON: {geojson_file} ({len(geojson['features'])} features)")
    
    # Verify all have modal data
    print(f"\nVerifying modal data...")
    with_modal = 0
    without_modal = 0
    
    key_modal_fields = ['Utilisation prédominante', 'Dossier n°', 'Année de construction', 'Condition d\'inscription']
    
    for prop in main_properties:
        modal_data = prop.get('modal_data', {})
        has_key_field = any(field in modal_data for field in key_modal_fields)
        
        if has_key_field:
            with_modal += 1
        else:
            without_modal += 1
    
    print(f"Properties WITH modal data: {with_modal} ({with_modal/len(main_properties)*100:.1f}%)")
    print(f"Properties WITHOUT modal data: {without_modal} ({without_modal/len(main_properties)*100:.1f}%)")
    
    # Summary
    print("\n" + "="*80)
    print("MERGE COMPLETE")
    print("="*80)
    print(f"\nFiles created:")
    print(f"  ✓ {backup_file} (backup before merge)")
    print(f"  ✓ {output_file} (merged JSON)")
    print(f"  ✓ {geojson_file} (merged GeoJSON)")
    
    print(f"\nTo replace the main file with the merged version:")
    print(f"  python -c \"import shutil; shutil.copy2('{output_file}', '{main_file}')\"")
    
    if without_modal > 0:
        print(f"\n⚠ WARNING: {without_modal} properties still don't have modal data")
        print(f"  You may want to re-run the rescraper on those properties")
    else:
        print(f"\n✓ SUCCESS: All {len(main_properties)} properties now have complete modal data!")
    
    return main_properties


def main():
    merged = merge_properties(
        main_file='all_properties_with_evaluation.json',
        rescrape_file='rescrape_results_with_modal.json',
        output_file='all_properties_with_evaluation_merged.json'
    )
    
    if merged:
        print("\n" + "="*80)
        print("NEXT STEPS")
        print("="*80)
        print("1. Review the merged file to ensure it looks correct")
        print("2. If satisfied, replace the original:")
        print("   python -c \"import shutil; shutil.copy2('all_properties_with_evaluation_merged.json', 'all_properties_with_evaluation.json'); shutil.copy2('all_properties_with_evaluation_merged.geojson', 'all_properties_with_evaluation.geojson')\"")
        print("3. Your final dataset will be complete with all modal data!")


if __name__ == "__main__":
    main()
