"""Bronze Hacker News : polling micro-batch (API Firebase) + backfill historique (API Algolia)."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

from scipulse.common.logging import get_logger
from scipulse.common.monitoring import emit
from scipulse.common.storage import put_jsonl
from scipulse.config import buckets

log = get_logger(__name__)

FIREBASE = "https://hacker-news.firebaseio.com/v0"
ALGOLIA = "https://hn.algolia.com/api/v1/search_by_date"


def _get_item(item_id: int) -> dict | None:
    r = requests.get(f"{FIREBASE}/item/{item_id}.json", timeout=10)
    return r.json() if r.ok else None


def poll_new_items(last_seen_id: int = 0, limit: int = 500) -> tuple[list[dict], int]:
    """Récupère les stories plus récentes que last_seen_id. Retourne (items, nouveau max id)."""
    ids = requests.get(f"{FIREBASE}/newstories.json", timeout=10).json()
    ids = [i for i in ids if i > last_seen_id][:limit]
    with ThreadPoolExecutor(max_workers=16) as pool:
        items = [it for it in pool.map(_get_item, ids) if it]
    fetched_at = datetime.now(timezone.utc).isoformat()
    for it in items:
        it["_fetched_at"] = fetched_at          # métadonnée d'ingestion (pas une transformation)
        it["_source"] = "firebase"
    return items, max(ids, default=last_seen_id)


def backfill_arxiv_stories(pages: int = 50, hits_per_page: int = 1000) -> list[dict]:
    """Historique des posts HN pointant vers arxiv.org (sinon la jointure HN×ArXiv serait vide)."""
    out: list[dict] = []
    before = int(datetime.now(timezone.utc).timestamp())
    for _ in range(pages):
        params = {
            "query": "arxiv.org",
            "restrictSearchableAttributes": "url",
            "tags": "story",
            "hitsPerPage": hits_per_page,
            "numericFilters": f"created_at_i<{before}",
        }
        hits = requests.get(ALGOLIA, params=params, timeout=30).json().get("hits", [])
        if not hits:
            break
        for h in hits:
            h["_source"] = "algolia"
        out.extend(hits)
        before = min(h["created_at_i"] for h in hits)
    return out


def write_bronze(items: list[dict], tag: str = "poll") -> int:
    if not items:
        return 0
    now = datetime.now(timezone.utc)
    key = f"date={now:%Y-%m-%d}/{tag}_{now:%H%M%S}.jsonl"
    n = put_jsonl(buckets.HN_RAW, key, items)
    log.info("HN Bronze : %s items -> %s", n, key)
    emit("ingestion", source="hn", layer="bronze", records=n, mode=tag)
    return n
