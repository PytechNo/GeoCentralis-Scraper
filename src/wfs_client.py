"""
WFS client – fetches property matricules from GeoServer for any municipality.

Extracted and generalised from the original query_all_properties_wfs.py so that
the coordinator can call it for every city in the list.
"""

from __future__ import annotations

import time
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config
from src.db import add_log

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.8",
})

# Retry adapter for transient HTTP errors (5xx, connection resets)
_adapter = HTTPAdapter(
    max_retries=Retry(total=2, backoff_factor=0.5, status_forcelist=[502, 503, 504]),
    pool_maxsize=config.WFS_PREFETCH_THREADS + 2,
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)


# ── internal helpers ──────────────────────────────────────────────────────────

def _build_cql(municipality_id: str, layer: str) -> str:
    cql = f"id_municipalite='{municipality_id}'"
    if layer in ("mat_uev_cr_s", "v_mat_uev_cr_s"):
        cql += " AND date_fin IS NULL"
    return cql


def _wfs_hits(layer: str, cql: str) -> int:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": f"evb:{layer}",
        "resultType": "hits",
        "CQL_FILTER": cql,
    }
    last_exc = None
    for attempt in range(config.WFS_MAX_RETRIES):
        try:
            r = SESSION.get(config.WFS_URL, params=params, timeout=config.WFS_HITS_TIMEOUT)
            r.raise_for_status()
            try:
                data = r.json()
                return int(data.get("numberMatched", data.get("totalFeatures", 0)))
            except Exception:
                text = r.text
                for key in ('numberMatched="', 'numberOfFeatures="'):
                    if key in text:
                        start = text.index(key) + len(key)
                        end = text.index('"', start)
                        return int(text[start:end])
                return 0
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt < config.WFS_MAX_RETRIES - 1:
                wait = 2 ** attempt
                add_log("WARN", "wfs", f"Hits request failed (attempt {attempt+1}/{config.WFS_MAX_RETRIES}), retry in {wait}s: {exc}")
                time.sleep(wait)
    raise last_exc  # all retries exhausted


def _wfs_fetch_page(layer: str, cql: str, start: int, count: int) -> Dict:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": f"evb:{layer}",
        "outputFormat": "application/json",
        "startIndex": str(start),
        "count": str(count),
        "CQL_FILTER": cql,
        "srsName": "EPSG:4326",
    }
    last_exc = None
    for attempt in range(config.WFS_MAX_RETRIES):
        try:
            r = SESSION.get(config.WFS_URL, params=params, timeout=config.WFS_TIMEOUT)
            if r.status_code == 400:
                # fallback WFS 1.0.0
                params_1 = {
                    "service": "WFS",
                    "version": "1.0.0",
                    "request": "GetFeature",
                    "typeName": f"evb:{layer}",
                    "outputFormat": "application/json",
                    "maxFeatures": str(count),
                    "CQL_FILTER": cql,
                }
                r1 = SESSION.get(config.WFS_URL, params=params_1, timeout=config.WFS_TIMEOUT)
                r1.raise_for_status()
                return r1.json()
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            if attempt < config.WFS_MAX_RETRIES - 1:
                wait = 2 ** attempt
                add_log("WARN", "wfs", f"Page fetch failed (start={start}, attempt {attempt+1}/{config.WFS_MAX_RETRIES}), retry in {wait}s")
                time.sleep(wait)
    raise last_exc  # all retries exhausted


def _wfs_fetch_all(layer: str, cql: str, page_size: int) -> List[Dict]:
    """Fetch all features with pagination.  Returns list of GeoJSON features."""
    features: List[Dict] = []
    start = 0
    while True:
        data = _wfs_fetch_page(layer, cql, start, page_size)
        page = data.get("features", [])
        if not page:
            if start == 0:
                # last-resort full 1.0.0 fetch (with retry)
                params_1 = {
                    "service": "WFS",
                    "version": "1.0.0",
                    "request": "GetFeature",
                    "typeName": f"evb:{layer}",
                    "outputFormat": "application/json",
                    "maxFeatures": "50000",
                    "CQL_FILTER": cql,
                }
                for attempt in range(config.WFS_MAX_RETRIES):
                    try:
                        r1 = SESSION.get(config.WFS_URL, params=params_1, timeout=config.WFS_TIMEOUT * 2)
                        if r1.ok:
                            features.extend(r1.json().get("features", []))
                        break
                    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                        if attempt < config.WFS_MAX_RETRIES - 1:
                            time.sleep(2 ** attempt)
            break
        features.extend(page)
        start += len(page)
        if len(page) < page_size:
            break
        time.sleep(0.2)
    return features


def _extract_properties(features: List[Dict]) -> List[Dict]:
    """Convert GeoJSON features into lightweight property dicts."""
    results: List[Dict] = []
    seen = set()
    for f in features:
        props = f.get("properties", {})
        matricule = props.get("matricule")
        if not matricule or matricule in seen:
            continue
        seen.add(matricule)
        results.append({
            "matricule": str(matricule),
            "adresse": props.get("adresse_immeuble", props.get("adresse", "")),
            "geometry": f.get("geometry"),
        })
    return results


# ── public API ────────────────────────────────────────────────────────────────

def fetch_municipality_properties(municipality_id: str) -> List[Dict]:
    """
    Fetch all property matricules for a municipality.

    Tries the primary layer first, then falls back to category layers.
    Returns a list of ``{matricule, adresse, geometry}`` dicts.
    """
    # 1) try primary layer
    layer = config.WFS_LAYER
    cql = _build_cql(municipality_id, layer)
    try:
        hits = _wfs_hits(layer, cql)
        if hits > 0:
            add_log("INFO", "wfs", f"[{municipality_id}] {layer}: {hits} hits – fetching…")
            features = _wfs_fetch_all(layer, cql, config.WFS_PAGE_SIZE)
            props = _extract_properties(features)
            if props:
                add_log("INFO", "wfs", f"[{municipality_id}] Got {len(props)} unique matricules from {layer}")
                return props
    except Exception as exc:
        add_log("WARN", "wfs", f"[{municipality_id}] Primary layer failed: {exc}")

    # 2) fallback: combine category layers
    all_props: List[Dict] = []
    seen = set()
    for fb_layer in config.WFS_FALLBACK_LAYERS:
        fb_cql = _build_cql(municipality_id, fb_layer)
        try:
            hits = _wfs_hits(fb_layer, fb_cql)
            if hits <= 0:
                continue
            add_log("INFO", "wfs", f"[{municipality_id}] Fallback {fb_layer}: {hits} hits")
            features = _wfs_fetch_all(fb_layer, fb_cql, config.WFS_PAGE_SIZE)
            for p in _extract_properties(features):
                if p["matricule"] not in seen:
                    seen.add(p["matricule"])
                    all_props.append(p)
        except Exception as exc:
            add_log("WARN", "wfs", f"[{municipality_id}] Fallback {fb_layer} failed: {exc}")

    if all_props:
        add_log("INFO", "wfs", f"[{municipality_id}] Combined fallback: {len(all_props)} unique matricules")
    else:
        add_log("ERROR", "wfs", f"[{municipality_id}] No properties found on any layer")
    return all_props
