"""Accès au data lake MinIO (API S3). Toutes les lectures/écritures passent par ici."""
from __future__ import annotations

import io
import json
from collections.abc import Iterable, Iterator

import boto3
from botocore.client import Config

from scipulse.config import settings


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_user,
        aws_secret_access_key=settings.minio_password,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def put_jsonl(bucket: str, key: str, records: Iterable[dict]) -> int:
    """Écrit des enregistrements en JSON Lines (format Bronze). Retourne le nb de lignes."""
    buf = io.BytesIO()
    n = 0
    for rec in records:
        buf.write((json.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8"))
        n += 1
    if n:
        buf.seek(0)
        s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return n


def put_bytes(bucket: str, key: str, data: bytes) -> None:
    s3_client().put_object(Bucket=bucket, Key=key, Body=data)


def list_keys(bucket: str, prefix: str = "") -> Iterator[str]:
    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def read_jsonl(bucket: str, key: str) -> Iterator[dict]:
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"]
    for line in body.iter_lines():
        if line:
            yield json.loads(line)


def delete_prefix(bucket: str, prefix: str = "") -> int:
    """Supprime tous les objets d'un préfixe (réécriture idempotente de la couche Silver)."""
    client = s3_client()
    keys = [{"Key": k} for k in list_keys(bucket, prefix)]
    for i in range(0, len(keys), 1000):
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys[i:i + 1000]})
    return len(keys)
