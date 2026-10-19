"""Définition des suites Great Expectations (≥5 ArXiv, ≥5 HN, ≥3 citations).

Chaque suite est une liste (NomExpectation, kwargs) : lisible, versionnée, testable.
Elles s'appliquent sur les données Bronze aplaties (avant nettoyage) pour mesurer la qualité,
puis sur Silver pour vérifier que le nettoyage a corrigé les problèmes.
"""

ARXIV_SUITE = [
    ("ExpectColumnValuesToNotBeNull", {"column": "arxiv_id"}),
    ("ExpectColumnValuesToBeUnique", {"column": "arxiv_id"}),
    ("ExpectColumnValuesToMatchRegex", {"column": "arxiv_id",
                                        "regex": r"^(\d{4}\.\d{4,5}|[a-z\-]+(\.[a-z]{2})?/\d{7})$"}),
    ("ExpectColumnValuesToNotBeNull", {"column": "primary_category"}),
    ("ExpectColumnValuesToNotBeNull", {"column": "submitted_date"}),
    ("ExpectColumnValueLengthsToBeBetween", {"column": "abstract", "min_value": 20, "mostly": 0.999}),
    ("ExpectColumnValuesToBeBetween", {"column": "year", "min_value": 1991, "max_value": 2030}),
]

HN_SUITE = [
    ("ExpectColumnValuesToNotBeNull", {"column": "hn_id"}),
    ("ExpectColumnValuesToBeUnique", {"column": "hn_id"}),
    ("ExpectColumnValuesToBeBetween", {"column": "score", "min_value": 0}),
    ("ExpectColumnValuesToBeBetween", {"column": "num_comments", "min_value": 0}),
    ("ExpectColumnValuesToBeInSet", {"column": "type", "value_set": ["story"]}),
    ("ExpectColumnValuesToNotBeNull", {"column": "posted_at"}),
]

CITATIONS_SUITE = [
    ("ExpectColumnValuesToNotBeNull", {"column": "arxiv_id"}),
    ("ExpectColumnValuesToBeUnique", {"column": "arxiv_id"}),
    ("ExpectColumnValuesToBeBetween", {"column": "cited_by_count", "min_value": 0}),
    ("ExpectColumnValuesToMatchRegex", {"column": "doi", "regex": r"^10\.\d{4,9}/\S+$", "mostly": 0.95}),
]

SUITES = {"arxiv": ARXIV_SUITE, "hn": HN_SUITE, "citations": CITATIONS_SUITE}

