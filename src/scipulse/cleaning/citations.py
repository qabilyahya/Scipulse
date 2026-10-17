"""Silver citations : normalisation OpenAlex / Semantic Scholar vers un schéma commun."""
from __future__ import annotations

import pandas as pd

from scipulse.cleaning.identifiers import doi_to_arxiv_id, normalize_arxiv_id, normalize_doi

COLUMNS = ["arxiv_id", "doi", "cited_by_count", "counts_by_year", "fields", "referenced_works",
           "source_id", "publication_year"]


def _from_openalex(r: dict) -> dict:
    doi = normalize_doi(r.get("doi"))
    return {
        "arxiv_id": doi_to_arxiv_id(doi),
        "doi": doi,
        "cited_by_count": r.get("cited_by_count", 0),
        "counts_by_year": r.get("counts_by_year", []),          # vélocité de citation
        "fields": [t["display_name"] for t in (r.get("topics") or [])[:3]],
        "referenced_works": r.get("referenced_works", []),      # graphe (option B)
        "source_id": r.get("id"),
        "publication_year": r.get("publication_year"),
    }


def _from_s2(r: dict) -> dict:
    ext = r.get("externalIds") or {}
    return {
        "arxiv_id": normalize_arxiv_id(ext.get("ArXiv")),
        "doi": normalize_doi(ext.get("DOI")),
        "cited_by_count": r.get("citationCount", 0),
        "counts_by_year": [],
        "fields": r.get("fieldsOfStudy") or [],
        "referenced_works": [],
        "source_id": r.get("paperId"),
        "publication_year": r.get("year"),
    }


def clean_citations(records: list[dict], source: str) -> pd.DataFrame:
    parse = _from_openalex if source == "openalex" else _from_s2
    df = pd.DataFrame([parse(r) for r in records], columns=COLUMNS)
    df = df.dropna(subset=["arxiv_id"])
    df["cited_by_count"] = pd.to_numeric(df["cited_by_count"], errors="coerce").fillna(0).astype("int64")
    return df.drop_duplicates(subset="arxiv_id", keep="last").reset_index(drop=True)
