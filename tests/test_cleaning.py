import pandas as pd

from scipulse.cleaning.arxiv import clean_arxiv
from scipulse.cleaning.hn import clean_hn


def test_clean_arxiv_dedup_and_dates():
    raw = pd.DataFrame([
        {"id": "2301.00001", "title": " A  title ", "abstract": "x" * 50, "categories": "cs.LG stat.ML",
         "authors_parsed": [["Doe", "John", ""]], "doi": None,
         "versions": [{"version": "v1", "created": "Mon, 2 Jan 2023 10:00:00 GMT"}], "update_date": "2023-01-05"},
        {"id": "2301.00001", "title": "A title", "abstract": "x" * 50, "categories": "cs.LG",
         "authors_parsed": [["Doe", "John", ""]], "doi": None,
         "versions": [{"version": "v1", "created": "Mon, 2 Jan 2023 10:00:00 GMT"}], "update_date": "2023-01-06"},
    ])
    df = clean_arxiv(raw)
    assert len(df) == 1
    assert df.loc[0, "primary_category"] == "cs.LG"
    assert df.loc[0, "title"] == "A title"
    assert df.loc[0, "year"] == 2023
    assert df.loc[0, "authors"] == ["John Doe"]


def test_clean_hn_harmonizes_sources():
    raw = pd.DataFrame([
        {"_source": "firebase", "id": 1, "title": "t", "url": "https://arxiv.org/abs/2301.00001v2",
         "score": 10, "descendants": 3, "time": 1700000000, "type": "story"},
        {"_source": "algolia", "objectID": "2", "title": "t2", "url": "https://arxiv.org/pdf/2302.00002",
         "points": 50, "num_comments": 7, "created_at_i": 1700000100},
        {"_source": "firebase", "id": 3, "deleted": True, "time": 1700000000, "type": "story"},
    ])
    df = clean_hn(raw).sort_values("hn_id")
    assert df["hn_id"].tolist() == [1, 2]
    assert df["arxiv_id"].tolist() == ["2301.00001", "2302.00002"]
    assert df["score"].tolist() == [10, 50]
