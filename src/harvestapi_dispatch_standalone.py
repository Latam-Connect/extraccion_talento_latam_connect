# -*- coding: utf-8 -*-
import os, time, json
from datetime import datetime
from typing import List, Dict, Any
from urllib.parse import urlparse
import requests
from dotenv import load_dotenv

load_dotenv()

APIFY_TOKEN: str = os.getenv("APIFY_TOKEN")
ACTOR_ID: str = os.getenv("APIFY_ACTOR_ID")
PROFILE_SCRAPER_MODE: str = "Profile details no email ($4 per 1k)"

APIFY_BASE = "https://api.apify.com/v2"
RUNS_ENDPOINT = f"/acts/{ACTOR_ID}/runs"

def normalize_linkedin_url(u: str) -> str:
    u = (u or "").strip()
    if not u: return u
    u = u.split("#", 1)[0].strip()
    p = urlparse(u)
    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()
    path = (p.path or "").rstrip("/")
    if not netloc and path:
        return u
    return f"{scheme}://{netloc}{path}"

def run_actor_async(token: str, body: Dict[str, Any]) -> str:
    r = requests.post(f"{APIFY_BASE}{RUNS_ENDPOINT}", params={"token": token}, json=body, timeout=(30, 60))
    r.raise_for_status()
    return (r.json().get("data") or {}).get("id")

def poll_run(token: str, run_id: str, timeout_total=3600, interval=10) -> Dict[str, Any]:
    start = time.time()
    while True:
        r = requests.get(f"{APIFY_BASE}/actor-runs/{run_id}", params={"token": token}, timeout=(30, 30))
        r.raise_for_status()
        data = r.json().get("data") or {}
        st = data.get("status")
        print(f"🛰️ Run {run_id} => {st}")
        if st in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}:
            return data
        if time.time() - start > timeout_total:
            raise TimeoutError(f"Timeout polling {run_id}")
        time.sleep(interval)

def fetch_dataset_items(token: str, ds_id: str) -> list:
    r = requests.get(f"{APIFY_BASE}/datasets/{ds_id}/items", params={"token": token, "clean": "true"}, timeout=(30, 120))
    try:
        return r.json()
    except Exception:
        items = []
        for line in r.text.splitlines():
            line = line.strip()
            if line:
                try: items.append(json.loads(line))
                except: pass
        return items

# 🔸 función reutilizable: recibe URLs y devuelve items
def harvest_for_urls(urls: List[str], token: str = None, mode: str = None) -> list:
    token = token or APIFY_TOKEN
    if not token or token == "PON_AQUI_TU_TOKEN":
        raise RuntimeError("Falta APIFY_TOKEN")
    urls = [normalize_linkedin_url(u) for u in urls if u]
    body = {
        "profileScraperMode": mode or PROFILE_SCRAPER_MODE,
        "urls": urls
    }
    run_id = run_actor_async(token, body)
    print(f"🚀 Lanzado run {run_id} (urls={len(urls)})")
    run_data = poll_run(token, run_id)
    print(f"📊 Run terminó: {run_data.get('status')}")
    ds = run_data.get("defaultDatasetId")
    return fetch_dataset_items(token, ds) if ds else []

# CLI opcional (por compatibilidad)
if __name__ == "__main__":
    import sys, json
    urls = [normalize_linkedin_url(u) for u in sys.argv[1:]]
    items = harvest_for_urls(urls)
    print(json.dumps({"items": items}, ensure_ascii=False))

