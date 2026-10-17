"""Bronze citations : OpenAlex (par défaut) ou Semantic Scholar. Choix à justifier dans le README."""
from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone

import requests

from scipulse.cleaning.identifiers import arxiv_id_to_doi
from scipulse.common.logging import get_logger
from scipulse.common.monitoring import emit
from scipulse.common.storage import put_jsonl
from scipulse.config import buckets, settings

log = get_logger(__name__)

OPENALEX = "https://api.openalex.org/works"
S2_BATCH = "https://api.semanticscholar.org/graph/v1/paper/batch"
S2_FIELDS = "externalIds,citationCount,influentialCitationCount,fieldsOfStudy,year,tldr"


def _batched(it: Iterable[str], size: int) -> Iterator[list[str]]:
    batch: list[str] = []
    for x in it:
        batch.append(x)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def fetch_openalex(arxiv_ids: list[str]) -> list[dict]:
    """50 DOI par requête via le filtre doi:a|b|c."""
    dois = "|".join(arxiv_id_to_doi(i) for i in arxiv_ids)
    params = {"filter": f"doi:{dois}", "per-page": 50, "mailto": settings.openalex_mailto}
    r = requests.get(OPENALEX, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])


def fetch_semantic_scholar(arxiv_ids: list[str]) -> list[dict]:
    """Jusqu'à 500 ids par requête (POST batch)."""
    headers = {"x-api-key": settings.s2_api_key} if settings.s2_api_key else {}
    r = requests.post(S2_BATCH, params={"fields": S2_FIELDS}, headers=headers,
                      json={"ids": [f"ARXIV:{i}" for i in arxiv_ids]}, timeout=60)
    r.raise_for_status()
    return [p for p in r.json() if p]


def ingest(arxiv_ids: Iterable[str], source: str | None = None, sleep_s: float = 0.2) -> int:
    source = source or settings.citations_source
    fetch, size = (fetch_openalex, 50) if source == "openalex" else (fetch_semantic_scholar, 500)
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    total = 0
    for n, batch in enumerate(_batched(arxiv_ids, size)):
        try:
            records = fetch(batch)
        except requests.HTTPError as exc:
            log.warning("Batch %s en erreur (%s) — on continue", n, exc)
            time.sleep(5)
            continue
        total += put_jsonl(buckets.CITATIONS_RAW, f"source={source}/run={run}/part-{n:05d}.jsonl", records)
        time.sleep(sleep_s)  # politesse API
    log.info("Citations Bronze (%s) : %s enregistrements", source, total)
    emit("ingestion", source=f"citations_{source}", layer="bronze", records=total)
    return total
