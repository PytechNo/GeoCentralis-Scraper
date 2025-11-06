"""
Run the full pipeline to fetch all current properties and scrape evaluation values.

Steps:
1) Fetch full current property list (matricules) from WFS layer evb:mat_uev_cr_s
2) Use the generated GeoJSON to scrape evaluation data via the portal UI

Usage (PowerShell):
  & "C:\\Program Files\\Python313\\python.exe" "c:\\My Web Sites\\Scraper\\run_full_pipeline.py" --municipality 31084 --headless --limit 50
  & "C:\\Program Files\\Python313\\python.exe" "c:\\My Web Sites\\Scraper\\run_full_pipeline.py" --municipality 31084 --headless

Notes:
- Headless mode recommended for unattended runs
- Use --limit to test small batches before full run
- Chrome + matching driver must be installed or available
"""

from __future__ import annotations
import argparse
import os
import subprocess
import sys
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable  # current Python

QUERY_SCRIPT = os.path.join(SCRIPT_DIR, 'query_all_properties_wfs.py')
SCRAPER_SCRIPT = os.path.join(SCRIPT_DIR, 'scrape_from_wfs_list.py')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'data_raw')
DEFAULT_WFS_FILE = os.path.join(OUTPUT_DIR, 'ALL_mat_uev_cr_s.geojson')


def run(cmd: list[str]) -> int:
    print("\n>>>", " ".join(cmd))
    return subprocess.call(cmd)


def main() -> None:
    parser = argparse.ArgumentParser(description='Run full WFS -> Evaluation scraping pipeline')
    parser.add_argument('--municipality', '-m', default='31084', help='Municipality ID (default 31084)')
    parser.add_argument('--wfs-file', default=DEFAULT_WFS_FILE, help='Path to WFS GeoJSON output (default ALL_mat_uev_cr_s.geojson)')
    parser.add_argument('--refresh-wfs', action='store_true', help='Force re-fetch WFS GeoJSON even if file exists')
    parser.add_argument('--headless', action='store_true', help='Run Chrome headless for scraping')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of properties to scrape (testing)')

    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1) Fetch WFS data if needed
    if args.refresh_wfs or not os.path.exists(args.wfs_file):
        print("\n[1/2] Fetching current property units from WFS (mat_uev_cr_s)...")
        rc = run([
            PYTHON,
            QUERY_SCRIPT,
            '--municipality', args.municipality,
            '--layers', 'mat_uev_cr_s',
            '--fetch-all',
            '--out-dir', OUTPUT_DIR,
            '--page-size', '2000',
            '--save-matricules',
        ])
        if rc != 0:
            print("WFS fetch failed; aborting.")
            sys.exit(rc)
    else:
        print(f"\n[1/2] Using existing WFS file: {args.wfs_file}")

    # 2) Run browser scraper using GeoJSON
    print("\n[2/2] Scraping evaluation data from the portal UI...")
    scraper_cmd = [
        PYTHON,
        SCRAPER_SCRIPT,
        '--wfs-file', args.wfs_file,
    ]
    if args.headless:
        scraper_cmd.append('--headless')
    if args.limit is not None:
        scraper_cmd += ['--limit', str(args.limit)]

    rc = run(scraper_cmd)
    if rc != 0:
        print("Scraping returned a non-zero exit code.")
        sys.exit(rc)

    print("\nPipeline complete. Outputs:")
    print("  - data_raw/ALL_mat_uev_cr_s.geojson (full current property units)")
    print("  - matricules/mat_uev_cr_s_matricules.(txt|json)")
    print("  - all_properties_with_evaluation.json + .geojson (results)")


if __name__ == '__main__':
    main()
