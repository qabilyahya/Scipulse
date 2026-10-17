"""Fonctions appelées par les DAGs Airflow. Les DAGs restent fins : toute la logique est ici."""
from __future__ import annotations

import pandas as pd

from scipulse.cleaning.arxiv import clean_arxiv
from scipulse.cleaning.citations import clean_citations
from scipulse.cleaning.hn import clean_hn
from scipulse.common.logging import get_logger
from scipulse.common.monitoring import emit
from scipulse.common.parquet import read_parquet, write_parquet
from scipulse.common.storage import delete_prefix, list_keys, read_jsonl
from scipulse.config import buckets, settings
from scipulse.ingestion import arxiv as arxiv_ing
from scipulse.ingestion import citations as cit_ing
from scipulse.ingestion import hn as hn_ing
from scipulse.quality.runner import validate

log = get_logger(__name__)


def _read_bronze(bucket: str, prefix: str = "") -> list[dict]:
    return [rec for key in list_keys(bucket, prefix) for rec in read_jsonl(bucket, key)]


def _write_silver(df: pd.DataFrame, bucket: str, source: str, partition_cols=None) -> int:
    delete_prefix(bucket)
    write_parquet(df, bucket, "", partition_cols=partition_cols)
    emit("transform", source=source, layer="silver", records=len(df))
    return len(df)


# ---------------- ArXiv ----------------
def arxiv_bronze() -> int:
    return arxiv_ing.upload_to_bronze(arxiv_ing.download_dump())


def arxiv_silver() -> int:
    raw = pd.DataFrame(_read_bronze(buckets.ARXIV_RAW))
    df = clean_arxiv(raw)
    validate(df, "arxiv", "silver")
    return _write_silver(df, buckets.ARXIV_CLEAN, "arxiv", partition_cols=["year"])


def arxiv_index() -> int:
    from scipulse.common.es_indexer import index_arxiv
    return index_arxiv(read_parquet(buckets.ARXIV_CLEAN))


# ---------------- Hacker News ----------------
def hn_poll(last_seen_id: int) -> int:
    items, new_max = hn_ing.poll_new_items(last_seen_id)
    hn_ing.write_bronze(items, tag="poll")
    return new_max


def hn_backfill(pages: int = 50) -> int:
    return hn_ing.write_bronze(hn_ing.backfill_arxiv_stories(pages=pages), tag="backfill")


def hn_silver() -> int:
    df = clean_hn(pd.DataFrame(_read_bronze(buckets.HN_RAW)))
    validate(df, "hn", "silver")
    return _write_silver(df, buckets.HN_CLEAN, "hn", partition_cols=["date"])


# ---------------- Citations ----------------
def citations_bronze(max_ids: int = 50_000) -> int:
    """Priorité aux articles mentionnés sur HN, puis aux plus récents."""
    arxiv = read_parquet(buckets.ARXIV_CLEAN, columns=["arxiv_id", "submitted_date"])
    try:
        hn_ids = set(read_parquet(buckets.HN_CLEAN, columns=["arxiv_id"])["arxiv_id"].dropna())
    except Exception:
        hn_ids = set()
    arxiv["on_hn"] = arxiv["arxiv_id"].isin(hn_ids)
    ids = arxiv.sort_values(["on_hn", "submitted_date"], ascending=False)["arxiv_id"].head(max_ids)
    return cit_ing.ingest(ids.tolist())


def citations_silver() -> int:
    source = settings.citations_source
    df = clean_citations(_read_bronze(buckets.CITATIONS_RAW, f"source={source}/"), source)
    validate(df, "citations", "silver")
    return _write_silver(df, buckets.CITATIONS_CLEAN, "citations")
