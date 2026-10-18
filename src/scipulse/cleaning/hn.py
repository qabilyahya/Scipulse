"""Silver Hacker News : harmonisation Firebase/Algolia + extraction de l'id ArXiv."""
from __future__ import annotations

import pandas as pd

from scipulse.cleaning.identifiers import extract_arxiv_id_from_url


def _col(raw: pd.DataFrame, name: str, default=None) -> pd.Series:
    """Colonne si présente, sinon série remplie de `default` (un lot peut venir d'une seule API)."""
    return raw[name] if name in raw.columns else pd.Series(default, index=raw.index, dtype="object")


def _pick(raw: pd.DataFrame, is_algolia: pd.Series, firebase_col: str, algolia_col: str) -> pd.Series:
    return _col(raw, firebase_col).where(~is_algolia, _col(raw, algolia_col))


def clean_hn(raw: pd.DataFrame) -> pd.DataFrame:
    # Les deux API n'ont pas le même schéma : on harmonise
    is_algolia = _col(raw, "_source", "firebase") == "algolia"
    df = pd.DataFrame({
        "hn_id": pd.to_numeric(_pick(raw, is_algolia, "id", "objectID"), errors="coerce"),
        "title": _col(raw, "title"),
        "url": _col(raw, "url"),
        "score": pd.to_numeric(_pick(raw, is_algolia, "score", "points"), errors="coerce"),
        "num_comments": pd.to_numeric(_pick(raw, is_algolia, "descendants", "num_comments"), errors="coerce"),
        "time": pd.to_numeric(_pick(raw, is_algolia, "time", "created_at_i"), errors="coerce"),
        "type": _col(raw, "type", "story").fillna("story"),
        "deleted": _col(raw, "deleted", False).fillna(False).astype(bool),
        "dead": _col(raw, "dead", False).fillna(False).astype(bool),
    })
    df["posted_at"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    df["arxiv_id"] = df["url"].map(extract_arxiv_id_from_url)
    df[["score", "num_comments"]] = df[["score", "num_comments"]].fillna(0).astype("int64")

    df = df[~df["deleted"] & ~df["dead"] & (df["type"] == "story")]
    df = df.dropna(subset=["hn_id", "posted_at"])
    df["hn_id"] = df["hn_id"].astype("int64")
    # Un même post peut être vu plusieurs fois (polling) : on garde le score le plus récent/élevé
    df = df.sort_values("score").drop_duplicates(subset="hn_id", keep="last")
    df["year"] = df["posted_at"].dt.year
    return df.drop(columns=["deleted", "dead", "time"]).reset_index(drop=True)
