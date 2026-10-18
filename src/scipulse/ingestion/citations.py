"""Bronze citations : OpenAlex (par défaut) ou Semantic Scholar. Choix à justifier dans le README."""
from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone

import requests

from scipulse.cleaning.identifiers import arxiv_id_to_doi, doi_to_arxiv_id, extract_arxiv_id_from_url
from scipulse.common.logging import get_logger
from scipulse.common.monitoring import emit
from scipulse.common.storage import put_jsonl
from scipulse.config import buckets, settings

log = get_logger(__name__)

OPENALEX = "https://api.openalex.org/works"
OPENALEX_FIELDS = "id,doi,title,publication_year,cited_by_count,counts_by_year,topics,referenced_works,locations"
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


def _match_arxiv_id(work: dict, wanted: set[str]) -> str | None:
    """Retrouve l'id ArXiv demandé dans un work OpenAlex.

    Le champ `doi` d'OpenAlex pointe souvent vers la version publiée (ex. 10.65215/…), pas vers le
    DOI arXiv : l'id ArXiv se trouve dans `locations` (arxiv.org/abs/…, doi.org/10.48550/arxiv.…).
    """
    candidates = [work.get("doi")]
    for loc in work.get("locations") or []:
        candidates += [loc.get("landing_page_url"), loc.get("pdf_url")]
    for c in candidates:
        aid = extract_arxiv_id_from_url(c) or doi_to_arxiv_id(c)
        if aid in wanted:
            return aid
    return None


def fetch_openalex(arxiv_ids: list[str]) -> list[dict]:
    """50 DOI par requête via le filtre doi:a|b|c ; chaque résultat est annoté avec `_arxiv_id`."""
    dois = "|".join(arxiv_id_to_doi(i) for i in arxiv_ids)
    params = {"filter": f"doi:{dois}", "per-page": 50, "select": OPENALEX_FIELDS,
              "mailto": settings.openalex_mailto}
    r = requests.get(OPENALEX, params=params, timeout=30)
    r.raise_for_status()
    wanted = set(arxiv_ids)
    results = r.json().get("results", [])
    for work in results:
        work["_arxiv_id"] = _match_arxiv_id(work, wanted)   # métadonnée d'ingestion (clé de jointure)
    return results


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
        except requests.RequestException as exc:   # HTTP, timeout, coupure réseau
            log.warning("Batch %s en erreur (%s) — on continue", n, exc)
            time.sleep(5)
            continue
        total += put_jsonl(buckets.CITATIONS_RAW, f"source={source}/run={run}/part-{n:05d}.jsonl", records)
        time.sleep(sleep_s)  # politesse API
    log.info("Citations Bronze (%s) : %s enregistrements", source, total)
    emit("ingestion", source=f"citations_{source}", layer="bronze", records=total)
    return total
