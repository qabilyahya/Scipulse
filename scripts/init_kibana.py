"""Crée les Data Views Kibana (idempotent). Usage : make init-kibana"""
import os
import time

import requests

KIBANA = os.getenv("KIBANA_HOST", "http://kibana:5601")
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

DATA_VIEWS = [
    {"id": "arxiv-papers-enriched", "title": "arxiv-papers-enriched", "name": "Gold — articles enrichis",
     "timeFieldName": "submitted_date"},
    {"id": "arxiv-papers", "title": "arxiv-papers", "name": "Gold partiel — ArXiv",
     "timeFieldName": "submitted_date"},
    {"id": "pipeline-monitoring", "title": "pipeline-monitoring", "name": "Monitoring pipeline",
     "timeFieldName": "@timestamp"},
]

for _ in range(30):  # attendre que Kibana soit prêt
    try:
        if requests.get(f"{KIBANA}/api/status", timeout=5).ok:
            break
    except requests.RequestException:
        pass
    time.sleep(5)

for dv in DATA_VIEWS:
    r = requests.get(f"{KIBANA}/api/data_views/data_view/{dv['id']}", headers=HEADERS, timeout=10)
    if r.ok:
        print(f"= {dv['name']} existe déjà")
        continue
    r = requests.post(f"{KIBANA}/api/data_views/data_view", headers=HEADERS, timeout=10,
                      json={"data_view": dv, "override": True})
    print(f"+ {dv['name']}" if r.ok else f"! {dv['name']} : {r.status_code} {r.text[:200]}")
