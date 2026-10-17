"""Crée les index Elasticsearch avec leurs mappings (idempotent). Usage : make init-es"""
import json
import os
from pathlib import Path

from elasticsearch import Elasticsearch

ES = Elasticsearch(os.getenv("ES_HOST", "http://localhost:9200"))
MAPPINGS = Path(__file__).resolve().parents[1] / "elasticsearch" / "mappings"
if not MAPPINGS.exists():
    MAPPINGS = Path("/opt/airflow/elasticsearch/mappings")

for f in sorted(MAPPINGS.glob("*.json")):
    index = f.stem
    if ES.indices.exists(index=index):
        print(f"= {index} existe déjà")
        continue
    ES.indices.create(index=index, body=json.loads(f.read_text()))
    print(f"+ {index} créé")
