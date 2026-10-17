"""Configuration centralisée, lue depuis les variables d'environnement (.env)."""
import os
from dataclasses import dataclass


def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


@dataclass(frozen=True)
class Buckets:
    """Buckets MinIO de l'architecture Medallion."""
    ARXIV_RAW: str = "arxiv-raw"
    HN_RAW: str = "hn-raw"
    CITATIONS_RAW: str = "citations-raw"
    ARXIV_CLEAN: str = "arxiv-clean"
    HN_CLEAN: str = "hn-clean"
    CITATIONS_CLEAN: str = "citations-clean"


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str = _env("MINIO_ENDPOINT", "http://localhost:9000")
    minio_user: str = _env("MINIO_ROOT_USER", "minioadmin")
    minio_password: str = _env("MINIO_ROOT_PASSWORD", "minioadmin123")
    es_host: str = _env("ES_HOST", "http://localhost:9200")
    logstash_http: str = _env("LOGSTASH_HTTP", "http://localhost:8090")
    spark_master: str = _env("SPARK_MASTER", "local[*]")
    arxiv_max_records: int = int(_env("ARXIV_MAX_RECORDS", "0") or 0)
    citations_source: str = _env("CITATIONS_SOURCE", "openalex")
    openalex_mailto: str = _env("OPENALEX_MAILTO", "")
    s2_api_key: str = _env("SEMANTIC_SCHOLAR_API_KEY", "")
    data_dir: str = _env("DATA_DIR", "/opt/airflow/data")


# Index Elasticsearch
ES_INDEX_ARXIV = "arxiv-papers"                 # Gold partiel (Module 1)
ES_INDEX_ENRICHED = "arxiv-papers-enriched"     # Gold final (Module 2, écrit par Spark)
ES_INDEX_MONITORING = "pipeline-monitoring"     # métriques pipeline (via Logstash)

settings = Settings()
buckets = Buckets()
