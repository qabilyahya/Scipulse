"""Indexation Python (Module 1 : Gold partiel arxiv-papers depuis Silver)."""
from __future__ import annotations

import pandas as pd
from elasticsearch.helpers import bulk

from elasticsearch import Elasticsearch
from scipulse.common.logging import get_logger
from scipulse.common.monitoring import emit
from scipulse.config import ES_INDEX_ARXIV, settings

log = get_logger(__name__)
FIELDS = ["arxiv_id", "title", "abstract", "authors", "categories", "primary_category",
          "category_group", "doi", "submitted_date", "update_date", "n_versions", "year"]


def _actions(df: pd.DataFrame, index: str):
    for rec in df[FIELDS].to_dict("records"):
        for k in ("submitted_date", "update_date"):
            rec[k] = rec[k].isoformat() if pd.notna(rec[k]) else None
        rec = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in rec.items()}
        yield {"_index": index, "_id": rec["arxiv_id"], "_source": rec}


def index_arxiv(df: pd.DataFrame, index: str = ES_INDEX_ARXIV) -> int:
    es = Elasticsearch(settings.es_host)
    ok, errors = bulk(es, _actions(df, index), chunk_size=2000, raise_on_error=False)
    log.info("ES %s : %s indexés, %s erreurs", index, ok, len(errors))
    emit("indexing", source="arxiv", layer="gold", records=ok, errors=len(errors))
    return ok
