"""
Check if construction year is captured in re-scraped data
"""

import json

# Load re-scraped data
with open('rescrape_results_with_modal.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total re-scraped properties: {len(data)}")
print("\n" + "="*80)
print("Checking for 'Année de construction' field...")
print("="*80)

# Check first property
if data:
    sample = data[0]
    print(f"\nSample property:")
    print(f"Matricule: {sample.get('matricule')}")
    print(f"Adresse: {sample.get('adresse')}")
    
    modal_data = sample.get('modal_data', {})
    print(f"\nModal data fields ({len(modal_data)} total):")
    for k, v in list(modal_data.items())[:20]:
        print(f"  - {k}: {v}")
    
    print(f"\n'Année de construction' in modal_data: {'Année de construction' in modal_data}")
    if 'Année de construction' in modal_data:
        print(f"  Value: {modal_data['Année de construction']}")
    
    eval_data = sample.get('evaluation_data', {})
    print(f"\n'Année de construction' in evaluation_data: {'Année de construction' in eval_data}")
    if 'Année de construction' in eval_data:
        print(f"  Value: {eval_data['Année de construction']}")

# Count how many have construction year
print("\n" + "="*80)
print("Analyzing all re-scraped properties...")
print("="*80)

with_year = 0
without_year = 0
year_values = []

for prop in data:
    modal_data = prop.get('modal_data', {})
    if 'Année de construction' in modal_data:
        with_year += 1
        year = modal_data['Année de construction']
        if year and year not in year_values:
            year_values.append(year)
    else:
        without_year += 1

print(f"\nProperties WITH 'Année de construction': {with_year} ({with_year/len(data)*100:.1f}%)")
print(f"Properties WITHOUT 'Année de construction': {without_year} ({without_year/len(data)*100:.1f}%)")

if year_values:
    print(f"\nSample construction years found: {year_values[:10]}")
