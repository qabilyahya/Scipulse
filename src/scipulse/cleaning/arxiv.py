"""Silver ArXiv : nettoyage guidé par les validations Great Expectations."""
from __future__ import annotations

import pandas as pd

from scipulse.cleaning.identifiers import normalize_arxiv_id, normalize_doi


def _first_version_date(versions) -> str | None:
    try:
        return versions[0]["created"]
    except (TypeError, IndexError, KeyError):
        return None


def clean_arxiv(raw: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["arxiv_id"] = raw["id"].astype(str).map(normalize_arxiv_id)
    df["title"] = raw["title"].fillna("").str.replace(r"\s+", " ", regex=True).str.strip()
    df["abstract"] = raw["abstract"].fillna("").str.replace(r"\s+", " ", regex=True).str.strip()
    df["authors"] = raw["authors_parsed"].map(
        lambda a: [" ".join(p for p in (x[1], x[0]) if p).strip() for x in a] if isinstance(a, list) else []
    )
    df["categories"] = raw["categories"].fillna("").str.split()
    df["primary_category"] = df["categories"].map(lambda c: c[0] if c else None)
    df["category_group"] = df["primary_category"].str.split(".").str[0]   # cs, math, physics…
    df["doi"] = raw.get("doi", pd.Series(dtype=str)).map(normalize_doi)
    df["submitted_date"] = pd.to_datetime(raw["versions"].map(_first_version_date), utc=True, errors="coerce")
    df["update_date"] = pd.to_datetime(raw["update_date"], utc=True, errors="coerce")
    df["n_versions"] = raw["versions"].map(lambda v: len(v) if isinstance(v, list) else 0)
    df["year"] = df["submitted_date"].dt.year

    # Règles issues de l'analyse qualité (voir notebooks/01_quality_eda.ipynb)
    df = df.dropna(subset=["arxiv_id", "submitted_date"])
    df = df[df["title"].str.len() > 0]
    df = df.drop_duplicates(subset="arxiv_id", keep="last")
    return df.reset_index(drop=True)
