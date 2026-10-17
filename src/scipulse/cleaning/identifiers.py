"""Normalisation des clés de jointure : c'est le point de raccordement des 3 sources."""
from __future__ import annotations

import re

# Nouveau format (0704.0001, 2301.12345) et ancien format (hep-th/9901001, cs.LG/0101001)
_NEW_ID = r"\d{4}\.\d{4,5}"
_OLD_ID = r"[a-z\-]+(?:\.[A-Z]{2})?/\d{7}"
ARXIV_ID_RE = re.compile(rf"^(?P<id>{_NEW_ID}|{_OLD_ID})(?:v\d+)?$", re.IGNORECASE)
ARXIV_URL_RE = re.compile(
    rf"arxiv\.org/(?:abs|pdf|html)/(?P<id>{_NEW_ID}|{_OLD_ID})(?:v\d+)?(?:\.pdf)?",
    re.IGNORECASE,
)
ARXIV_DOI_PREFIX = "10.48550/arxiv."


def normalize_arxiv_id(raw: str | None) -> str | None:
    """'2301.12345v2' -> '2301.12345' ; 'arXiv:2301.12345' -> '2301.12345'."""
    if not isinstance(raw, str) or not raw:
        return None
    s = raw.strip()
    s = re.sub(r"^arxiv:", "", s, flags=re.IGNORECASE)
    m = ARXIV_ID_RE.match(s)
    return m.group("id").lower() if m else None


def extract_arxiv_id_from_url(url: str | None) -> str | None:
    """Extrait l'id ArXiv d'une URL Hacker News (abs/, pdf/, html/, avec version)."""
    if not isinstance(url, str) or not url:
        return None
    m = ARXIV_URL_RE.search(url)
    return m.group("id").lower() if m else None


def normalize_doi(raw: str | None) -> str | None:
    """'https://doi.org/10.1000/ABC' -> '10.1000/abc'."""
    if not isinstance(raw, str) or not raw:
        return None
    s = raw.strip().lower()
    s = re.sub(r"^(https?://)?(dx\.)?doi\.org/", "", s)
    s = re.sub(r"^doi:", "", s)
    return s if s.startswith("10.") else None


def arxiv_id_to_doi(arxiv_id: str) -> str:
    """Chaque article ArXiv possède un DOI DataCite : 10.48550/arxiv.<id>."""
    return f"{ARXIV_DOI_PREFIX}{arxiv_id.lower()}"


def doi_to_arxiv_id(doi: str | None) -> str | None:
    d = normalize_doi(doi)
    if d and d.startswith(ARXIV_DOI_PREFIX):
        return normalize_arxiv_id(d[len(ARXIV_DOI_PREFIX):])
    return None
