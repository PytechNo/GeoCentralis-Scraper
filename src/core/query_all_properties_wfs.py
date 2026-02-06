"""
Query all properties for a municipality from GeoCentralis GeoServer (WFS)

Summary
- Uses the public WFS endpoint: https://geoserver.geocentralis.com/geoserver/ows
- Filters by municipality with CQL: id_municipalite='<MUN_ID>'
- Tries multiple candidate layers and reports counts
- Optionally fetches ALL features with paging and saves GeoJSON files

Notes
- Public WFS exposes a subset (e.g., ~990) compared to what the map can show (~13k)
- The best public coverage usually comes from the four category layers:
  evb:v_a_residentiel_1, evb:v_a_multiresidentiel_3, evb:v_a_non_residentiel_4, evb:v_a_agricole_2
- Some layers like evb:mat_uev_cr_s may exist but can be large or restricted

Usage examples (Windows PowerShell)
  python query_all_properties_wfs.py --municipality 31084 --count-only
  python query_all_properties_wfs.py --municipality 31084 --fetch-all --out-dir data_raw
  python query_all_properties_wfs.py --municipality 31084 --layers v_a_residentiel_1 v_a_non_residentiel_4 --fetch-all

Outputs
- Per-layer GeoJSON files saved to --out-dir
- Optional combined file ALL_properties_combined.geojson when --combine is set
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Iterable

import requests

WFS_URL = "https://geoserver.geocentralis.com/geoserver/ows"
DEFAULT_PAGE_SIZE = 1000

# Canonical layer candidates (without prefix 'evb:')
CANDIDATE_LAYERS = [
    # Four category layers (broad public coverage)
    "v_a_residentiel_1",
    "v_a_multiresidentiel_3",
    "v_a_non_residentiel_4",
    "v_a_agricole_2",
    # Layers with evaluation/built/vacant variants
    "v_immeuble_construit_municipalite_eval",
    "v_terrain_vacant_municipalite_eval",
    # Potential matricule/unit layer (availability may vary)
    "mat_uev_cr_s",
]

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
})


def wfs_hits(type_name: str, cql_filter: str) -> int:
    """Return numberMatched (feature count) for a layer using resultType=hits."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": f"evb:{type_name}",
        "resultType": "hits",
        "CQL_FILTER": cql_filter,
    }
    r = SESSION.get(WFS_URL, params=params, timeout=45)
    r.raise_for_status()
    # numberMatched appears in WFS 2.0.0 JSON, else totalFeatures may appear
    try:
        data = r.json()
        return int(data.get("numberMatched", data.get("totalFeatures", 0)))
    except Exception:
        # Some servers return XML for hits; try to extract from text
        text = r.text
        for key in ("numberMatched=\"", "numberOfFeatures=\""):
            if key in text:
                try:
                    start = text.index(key) + len(key)
                    end = text.index("\"", start)
                    return int(text[start:end])
                except Exception:
                    pass
        return 0


def wfs_fetch_page(type_name: str, cql_filter: str, start: int, count: int) -> Dict:
    """Fetch a single page of features as GeoJSON (WFS 2.0.0), with graceful fallback to 1.0.0."""
    params_2 = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": f"evb:{type_name}",
        "outputFormat": "application/json",
        "startIndex": str(start),
        "count": str(count),
        "CQL_FILTER": cql_filter,
        "srsName": "EPSG:4326",
    }
    r = SESSION.get(WFS_URL, params=params_2, timeout=90)
    if r.status_code == 400:
        # Fallback to WFS 1.0.0 (no paging support). We'll request a small set when used for samples.
        params_1 = {
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": f"evb:{type_name}",
            "outputFormat": "application/json",
            "maxFeatures": str(count),
            "CQL_FILTER": cql_filter,
        }
        r1 = SESSION.get(WFS_URL, params=params_1, timeout=90)
        r1.raise_for_status()
        return r1.json()
    r.raise_for_status()
    return r.json()


def wfs_fetch_all(type_name: str, cql_filter: str, page_size: int = DEFAULT_PAGE_SIZE, sleep_sec: float = 0.2) -> Dict:
    """Fetch all features with paging and return a complete GeoJSON FeatureCollection."""
    features: List[Dict] = []
    start = 0
    while True:
        data = wfs_fetch_page(type_name, cql_filter, start=start, count=page_size)
        page_features = data.get("features", [])
        if not page_features:
            # If we never got any features and start==0, try a full 1.0.0 fetch as a last resort
            if start == 0:
                params_1 = {
                    "service": "WFS",
                    "version": "1.0.0",
                    "request": "GetFeature",
                    "typeName": f"evb:{type_name}",
                    "outputFormat": "application/json",
                    "maxFeatures": "50000",
                    "CQL_FILTER": cql_filter,
                }
                r1 = SESSION.get(WFS_URL, params=params_1, timeout=120)
                if r1.ok:
                    data1 = r1.json()
                    page_features = data1.get("features", [])
                    features.extend(page_features)
            break
        features.extend(page_features)
        start += len(page_features)
        if len(page_features) < page_size:
            break
        time.sleep(sleep_sec)
    return {
        "type": "FeatureCollection",
        "features": features,
    }


def has_matricule_property(sample_feature: Optional[Dict]) -> bool:
    if not sample_feature:
        return False
    props = sample_feature.get("properties") or {}
    return any(k.lower() == "matricule" or "matricul" in k.lower() for k in props.keys())


def save_geojson(fc: Dict, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)


def extract_matricules(features: Iterable[Dict]) -> List[str]:
    """Extract unique matricule strings from a list of features."""
    mats: List[str] = []
    for f in features or []:
        props = f.get("properties") or {}
        m = None
        for k, v in props.items():
            if isinstance(k, str) and (k.lower() == "matricule" or "matricul" in k.lower()):
                m = v
                break
        if m:
            mats.append(str(m))
    seen = set()
    unique: List[str] = []
    for m in mats:
        if m not in seen:
            seen.add(m)
            unique.append(m)
    return unique

def build_cql(municipality: str, layer: str, cql_extra: Optional[str]) -> str:
    base = f"id_municipalite='{municipality}'"
    # For unit/matricule layer, default to current records only
    if layer in ("mat_uev_cr_s", "v_mat_uev_cr_s"):
        base += " AND date_fin IS NULL"
    if cql_extra:
        base += f" AND ({cql_extra})"
    return base

def run(municipality: str, layers: List[str], out_dir: str, count_only: bool, fetch_all: bool, combine: bool, page_size: int, cql_extra: Optional[str], save_matricules: bool) -> None:
    print(f"Municipality: {municipality}")
    print(f"Layers to try: {', '.join(layers)}")
    print(f"Mode: {'COUNT-ONLY' if count_only else 'FETCH-ALL' if fetch_all else 'SAMPLE'}\n")

    layer_results: List[Tuple[str, int, bool]] = []  # (layer, hits, has_matricule)

    for layer in layers:
        cql = build_cql(municipality, layer, cql_extra)
        try:
            hits = wfs_hits(layer, cql)
        except Exception as e:
            print(f"- {layer}: error getting hits: {e}")
            continue

        sample_fc = {}
        has_mat = False
        if hits > 0:
            try:
                sample_fc = wfs_fetch_page(layer, cql, start=0, count=1)
                sample_feat = (sample_fc.get("features") or [None])[0]
                has_mat = has_matricule_property(sample_feat)
            except Exception as e:
                print(f"- {layer}: error fetching sample: {e}")

        layer_results.append((layer, hits, has_mat))
        print(f"- {layer:35} hits={hits:6d}  matricule_field={'YES' if has_mat else 'no'}")

    if count_only:
        return

    # Fetch all per-layer if requested
    fetched: List[Dict] = []
    ts = datetime.now().strftime("%Y%m%d")
    for layer, hits, has_mat in layer_results:
        if hits <= 0:
            continue
        if not fetch_all:
            continue
        try:
            print(f"\nFetching ALL features for {layer} (hits={hits})...")
            cql = build_cql(municipality, layer, cql_extra)
            fc = wfs_fetch_all(layer, cql, page_size=page_size)
            count = len(fc.get("features") or [])
            print(f"  -> got {count} features")
            # Derive a friendly filename
            name_map = {
                "v_a_residentiel_1": "ALL_residential_properties.geojson",
                "v_a_multiresidentiel_3": "ALL_multiresidential_properties.geojson",
                "v_a_non_residentiel_4": "ALL_nonresidential_properties.geojson",
                "v_a_agricole_2": "ALL_agricultural_properties.geojson",
                "v_immeuble_construit_municipalite_eval": "built_properties_eval.geojson",
                "v_terrain_vacant_municipalite_eval": "vacant_land_eval.geojson",
            }
            fname = name_map.get(layer, f"ALL_{layer}.geojson")
            out_path = os.path.join(out_dir, fname)
            save_geojson(fc, out_path)
            print(f"  -> saved {out_path}")
            if save_matricules:
                mats = extract_matricules(fc.get("features") or [])
                if mats:
                    mats_dir = os.path.join(os.getcwd(), "data", "matricules")
                    os.makedirs(mats_dir, exist_ok=True)
                    with open(os.path.join(mats_dir, f"{layer}_matricules.txt"), "w", encoding="utf-8") as f:
                        f.write("\n".join(mats))
                    with open(os.path.join(mats_dir, f"{layer}_matricules.json"), "w", encoding="utf-8") as f:
                        json.dump(mats, f, ensure_ascii=False, indent=2)
                    print(f"  -> saved {len(mats)} matricules to data/matricules/{layer}_matricules.(txt|json)")
            fetched.append(fc)
        except Exception as e:
            print(f"  !! failed to fetch {layer}: {e}")

    # Optionally combine all fetched features into one file
    if combine and fetched:
        combined = {"type": "FeatureCollection", "features": []}
        for fc in fetched:
            combined["features"].extend(fc.get("features") or [])
        print(f"\nCombining {len(fetched)} layers -> {len(combined['features'])} features total")
        out_path = os.path.join(out_dir, f"ALL_properties_combined_{ts}.geojson")
        save_geojson(combined, out_path)
        print(f"  -> saved {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query all public WFS properties for a municipality")
    parser.add_argument("--municipality", "-m", default="31015", help="Municipality ID (e.g., 31015)")
    parser.add_argument("--layers", nargs="*", default=CANDIDATE_LAYERS, help="List of layer names (without 'evb:') to try")
    parser.add_argument("--out-dir", default=os.path.join(os.getcwd(), "data", "raw"), help="Output directory for GeoJSON files")
    parser.add_argument("--count-only", action="store_true", help="Only print counts per layer; do not fetch features")
    parser.add_argument("--fetch-all", action="store_true", help="Fetch all features for each layer with hits > 0")
    parser.add_argument("--combine", action="store_true", help="Combine fetched layers into one GeoJSON file")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="WFS page size (count)")
    parser.add_argument("--cql-extra", default=None, help="Extra CQL appended with AND to base filter (e.g., date_fin IS NULL)")
    parser.add_argument("--save-matricules", action="store_true", help="When fetching, also save matricule lists per layer")

    args = parser.parse_args()

    try:
        run(
            municipality=args.municipality,
            layers=args.layers,
            out_dir=args.out_dir,
            count_only=args.count_only,
            fetch_all=args.fetch_all,
            combine=args.combine,
            page_size=args.page_size,
            cql_extra=args.cql_extra,
            save_matricules=args.save_matricules,
        )
    except requests.HTTPError as e:
        print(f"HTTP error: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
