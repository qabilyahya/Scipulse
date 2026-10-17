import pytest

from scipulse.cleaning.identifiers import (
    arxiv_id_to_doi,
    doi_to_arxiv_id,
    extract_arxiv_id_from_url,
    normalize_arxiv_id,
    normalize_doi,
)


@pytest.mark.parametrize("raw,expected", [
    ("2301.12345", "2301.12345"),
    ("2301.12345v3", "2301.12345"),
    ("arXiv:0704.0001", "0704.0001"),
    ("hep-th/9901001v2", "hep-th/9901001"),
    ("not-an-id", None),
    (None, None),
])
def test_normalize_arxiv_id(raw, expected):
    assert normalize_arxiv_id(raw) == expected


@pytest.mark.parametrize("url,expected", [
    ("https://arxiv.org/abs/2301.12345", "2301.12345"),
    ("https://arxiv.org/pdf/2301.12345v2.pdf", "2301.12345"),
    ("http://www.arxiv.org/abs/cs/0101001", "cs/0101001"),
    ("https://example.com/paper", None),
    (None, None),
])
def test_extract_arxiv_id_from_url(url, expected):
    assert extract_arxiv_id_from_url(url) == expected


def test_normalize_doi():
    assert normalize_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    assert normalize_doi("doi:10.1000/x") == "10.1000/x"
    assert normalize_doi("garbage") is None


def test_doi_roundtrip():
    assert doi_to_arxiv_id(arxiv_id_to_doi("2301.12345")) == "2301.12345"
