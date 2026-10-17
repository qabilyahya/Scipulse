"""Crée les buckets Medallion dans MinIO (idempotent, avec attente du démarrage).

Remplace l'image minio/mc, retirée de Docker Hub en septembre 2026.
"""
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKETS = ["arxiv-raw", "hn-raw", "citations-raw", "arxiv-clean", "hn-clean", "citations-clean"]

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    region_name="us-east-1",
)

for attempt in range(30):
    try:
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        break
    except (EndpointConnectionError, ClientError) as exc:
        print(f"MinIO pas prêt ({attempt + 1}/30) : {exc}")
        time.sleep(2)
else:
    sys.exit("MinIO injoignable")

for b in BUCKETS:
    if b not in existing:
        s3.create_bucket(Bucket=b)
        print(f"+ {b}")
    else:
        print(f"= {b}")
print("Buckets Medallion OK")
