"""Bronze ArXiv : téléchargement du dump Kaggle et dépôt brut dans MinIO (aucune transformation)."""
from __future__ import annotations

import io
import json
import os
import zipfile
from datetime import date
from pathlib import Path

import requests

from scipulse.common.logging import get_logger
from scipulse.common.monitoring import emit
from scipulse.common.storage import put_bytes
from scipulse.config import buckets, settings

log = get_logger(__name__)

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"
DUMP_NAME = "arxiv-metadata-oai-snapshot.json"
CHUNK_LINES = 50_000


def download_dump(dest_dir: str | None = None) -> Path:
    """Télécharge le zip Kaggle (≈1,5 Go) s'il n'est pas déjà présent dans data/."""
    dest = Path(dest_dir or settings.data_dir) / "arxiv-metadata.zip"
    if dest.exists():
        log.info("Dump déjà présent : %s", dest)
        return dest
    auth = (os.environ["KAGGLE_USERNAME"], os.environ["KAGGLE_KEY"])
    tmp = dest.with_suffix(".zip.part")   # un téléchargement coupé n'est jamais pris pour complet
    log.info("Téléchargement du dump ArXiv depuis Kaggle…")
    with requests.get(KAGGLE_URL, auth=auth, stream=True, timeout=60) as r:
        r.raise_for_status()
        total, done = int(r.headers.get("content-length", 0)), 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                done += len(chunk)
                if done % (100 << 20) < (1 << 20):
                    log.info("… %s / %s Mo", done >> 20, total >> 20)
    zipfile.ZipFile(tmp).testzip()        # vérifie l'intégrité avant de valider
    tmp.rename(dest)
    return dest


def _in_scope(rec: dict, categories: tuple[str, ...], min_yymm: str, keep_ids: set[str]) -> bool:
    """Périmètre de veille : catégories ciblées + récence, OU article cité sur Hacker News."""
    aid = rec.get("id", "")
    if aid in keep_ids:
        return True
    cats = (rec.get("categories") or "").split()
    if categories and not any(c.startswith(categories) for c in cats):
        return False
    # Identifiant nouveau format YYMM.NNNNN → filtre de date sans parser les versions
    return not min_yymm or (aid[:4].isdigit() and "." in aid and aid[:4] >= min_yymm)


def upload_to_bronze(zip_path: str | Path, max_records: int | None = None,
                     keep_ids: set[str] | None = None) -> int:
    """Copie dans arxiv-raw/ les lignes du périmètre, TELLES QUELLES (aucune transformation du contenu).

    Périmètre : ARXIV_CATEGORIES + ARXIV_MIN_YYMM, plus tous les articles cités sur HN (hors plafond).
    """
    max_records = max_records if max_records is not None else settings.arxiv_max_records
    categories = tuple(c for c in os.getenv("ARXIV_CATEGORIES", "cs.,stat.ML").split(",") if c)
    min_yymm = os.getenv("ARXIV_MIN_YYMM", "2201")
    keep_ids = keep_ids or set()
    prefix = f"ingest_date={date.today().isoformat()}"
    total, capped, scanned, part, buf = 0, 0, 0, 0, io.BytesIO()

    def flush():
        nonlocal part, buf
        if buf.tell():
            put_bytes(buckets.ARXIV_RAW, f"{prefix}/part-{part:05d}.jsonl", buf.getvalue())
            part += 1
            buf = io.BytesIO()

    with zipfile.ZipFile(zip_path) as zf, zf.open(DUMP_NAME) as fh:
        for line in fh:
            scanned += 1
            rec = json.loads(line)
            if not _in_scope(rec, categories, min_yymm, keep_ids):
                continue
            cited_on_hn = rec.get("id") in keep_ids
            if max_records and capped >= max_records and not cited_on_hn:
                continue                          # plafond atteint : on ne garde que les articles cités sur HN
            buf.write(line)                       # ligne brute, octet pour octet
            total += 1
            capped += 0 if cited_on_hn else 1
            if total % CHUNK_LINES == 0:
                flush()
    flush()
    log.info("ArXiv Bronze : %s lignes retenues sur %s parcourues, %s fichiers", total, scanned, part)
    emit("ingestion", source="arxiv", layer="bronze", records=total, scanned=scanned, files=part)
    return total
