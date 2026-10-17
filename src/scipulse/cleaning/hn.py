"""Silver Hacker News : harmonisation Firebase/Algolia + extraction de l'id ArXiv."""
from __future__ import annotations

import pandas as pd

from scipulse.cleaning.identifiers import extract_arxiv_id_from_url


def clean_hn(raw: pd.DataFrame) -> pd.DataFrame:
    # Les deux API n'ont pas le même schéma : on harmonise
    is_algolia = raw.get("_source", pd.Series("firebase", index=raw.index)) == "algolia"
    df = pd.DataFrame({
        "hn_id": pd.to_numeric(raw.get("id").where(~is_algolia, raw.get("objectID")), errors="coerce"),
        "title": raw.get("title"),
        "url": raw.get("url"),
        "score": pd.to_numeric(raw.get("score").where(~is_algolia, raw.get("points")), errors="coerce"),
        "num_comments": pd.to_numeric(
            raw.get("descendants").where(~is_algolia, raw.get("num_comments")), errors="coerce"
        ),
        "time": pd.to_numeric(raw.get("time").where(~is_algolia, raw.get("created_at_i")), errors="coerce"),
        "type": raw.get("type", pd.Series("story", index=raw.index)).fillna("story"),
        "deleted": raw.get("deleted", pd.Series(False, index=raw.index)).fillna(False).astype(bool),
        "dead": raw.get("dead", pd.Series(False, index=raw.index)).fillna(False).astype(bool),
    })
    df["posted_at"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    df["arxiv_id"] = df["url"].map(extract_arxiv_id_from_url)
    df[["score", "num_comments"]] = df[["score", "num_comments"]].fillna(0).astype("int64")

    df = df[~df["deleted"] & ~df["dead"] & (df["type"] == "story")]
    df = df.dropna(subset=["hn_id", "posted_at"])
    df["hn_id"] = df["hn_id"].astype("int64")
    # Un même post peut être vu plusieurs fois (polling) : on garde le score le plus récent/élevé
    df = df.sort_values("score").drop_duplicates(subset="hn_id", keep="last")
    df["date"] = df["posted_at"].dt.strftime("%Y-%m-%d")
    return df.drop(columns=["deleted", "dead", "time"]).reset_index(drop=True)
