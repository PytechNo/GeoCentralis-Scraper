"""
Export properties without "Année de construction" (construction year)

This script finds all properties that don't have a construction year
and exports them to various formats for analysis.

Usage:
    python export_no_construction_year.py
"""

import json

def export_properties_without_year(input_file='data/results/all_properties_with_evaluation.json'):
    """
    Find and export properties without construction year
    """
    print("="*80)
    print("EXPORTING PROPERTIES WITHOUT CONSTRUCTION YEAR")
    print("="*80)
    print()
    
    # Load properties
    print(f"Loading properties from {input_file}...")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            properties = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found")
        return
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON: {e}")
        return
    
    print(f"✓ Loaded {len(properties)} properties")
    
    # Filter properties without construction year
    print(f"\nAnalyzing properties...")
    without_year = []
    with_year = 0
    
    for prop in properties:
        modal_data = prop.get('modal_data', {})
        eval_data = prop.get('evaluation_data', {})
        
        # Check both modal_data and evaluation_data
        has_year = ('Année de construction' in modal_data and modal_data['Année de construction']) or \
                   ('Année de construction' in eval_data and eval_data['Année de construction'])
        
        if not has_year:
            # Extract relevant info
            without_year.append({
                'matricule': prop.get('matricule'),
                'adresse': prop.get('adresse'),
                'utilisation_predominante': modal_data.get('Utilisation prédominante', 
                                                           eval_data.get('Utilisation prédominante', 'N/A')),
                'nombre_etages': modal_data.get('Nombre d\'étages', 
                                               eval_data.get('Nombre d\'étages', 'N/A')),
                'valeur_batiment': eval_data.get('Valeur du bâtiment', 'N/A'),
                'valeur_terrain': eval_data.get('Valeur du terrain', 'N/A'),
                'superficie': modal_data.get('Superficie', eval_data.get('Superficie totale', 'N/A')),
                'nom_proprietaire': modal_data.get('Nom', eval_data.get('Nom', 'N/A'))
            })
        else:
            with_year += 1
    
    # Print statistics
    print("\n" + "="*80)
    print("ANALYSIS RESULTS")
    print("="*80)
    print(f"Total properties: {len(properties)}")
    print(f"Properties WITH construction year: {with_year} ({with_year/len(properties)*100:.1f}%)")
    print(f"Properties WITHOUT construction year: {len(without_year)} ({len(without_year)/len(properties)*100:.1f}%)")
    
    if not without_year:
        print("\n✓ All properties have construction year!")
        return
    
    # Export as JSON
    json_file = 'data/matricules/properties_without_construction_year.json'
    print(f"\nExporting to JSON: {json_file}")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(without_year, f, ensure_ascii=False, indent=2)
    print(f"✓ Saved {len(without_year)} properties")
    
    # Export as CSV
    csv_file = 'data/matricules/properties_without_construction_year.csv'
    print(f"\nExporting to CSV: {csv_file}")
    with open(csv_file, 'w', encoding='utf-8') as f:
        # Header
        f.write("matricule,adresse,utilisation_predominante,nombre_etages,valeur_batiment,valeur_terrain,superficie,nom_proprietaire\n")
        # Rows
        for prop in without_year:
            f.write(f"{prop['matricule']},{prop['adresse']},{prop['utilisation_predominante']},{prop['nombre_etages']},{prop['valeur_batiment']},{prop['valeur_terrain']},{prop['superficie']},{prop['nom_proprietaire']}\n")
    print(f"✓ Saved CSV")
    
    # Export matricules only (TXT)
    txt_file = 'data/matricules/matricules_without_construction_year.txt'
    print(f"\nExporting matricules only: {txt_file}")
    with open(txt_file, 'w', encoding='utf-8') as f:
        for prop in without_year:
            f.write(f"{prop['matricule']}\n")
    print(f"✓ Saved matricules list")
    
    # Analyze by usage type
    print("\n" + "="*80)
    print("BREAKDOWN BY USAGE TYPE")
    print("="*80)
    
    usage_counts = {}
    for prop in without_year:
        usage = prop['utilisation_predominante']
        usage_counts[usage] = usage_counts.get(usage, 0) + 1
    
    for usage, count in sorted(usage_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{usage}: {count} ({count/len(without_year)*100:.1f}%)")
    
    # Show first 10 examples
    print("\n" + "="*80)
    print("FIRST 10 EXAMPLES")
    print("="*80)
    for i, prop in enumerate(without_year[:10], 1):
        print(f"\n{i}. Matricule: {prop['matricule']}")
        print(f"   Adresse: {prop['adresse']}")
        print(f"   Usage: {prop['utilisation_predominante']}")
        print(f"   Valeur bâtiment: {prop['valeur_batiment']}")
        print(f"   Valeur terrain: {prop['valeur_terrain']}")
    
    # Summary
    print("\n" + "="*80)
    print("EXPORT COMPLETE")
    print("="*80)
    print(f"\nFiles created:")
    print(f"  ✓ {json_file} - Full details in JSON format")
    print(f"  ✓ {csv_file} - Spreadsheet format")
    print(f"  ✓ {txt_file} - Simple matricule list")
    
    return without_year


def main():
    export_properties_without_year('all_properties_with_evaluation.json')


if __name__ == "__main__":
    main()
