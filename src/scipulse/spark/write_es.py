"""Écriture de la couche Gold dans Elasticsearch (seule écriture ES du Module 2)."""
from pyspark.sql import DataFrame

from scipulse.config import ES_INDEX_ENRICHED

GOLD_COLUMNS = [
    "arxiv_id", "title", "abstract", "authors", "categories", "primary_category", "category_group",
    "doi", "submitted_date", "year", "cited_by_count", "citation_velocity", "citations_last_2y",
    "fields", "hn_score", "hn_comments", "hn_posts", "hn_first_post", "cit_norm", "hn_norm", "recency",
    "impact_score", "is_hidden_gem", "is_hn_hype", "is_on_hn", "rank_in_category", "top_terms",
]


def write_gold(df: DataFrame, index: str = ES_INDEX_ENRICHED) -> None:
    cols = [c for c in GOLD_COLUMNS if c in df.columns]
    (
        df.select(*cols)
        .write.format("org.elasticsearch.spark.sql")
        .option("es.resource", index)
        .option("es.mapping.id", "arxiv_id")
        .option("es.write.operation", "upsert")
        .option("es.batch.size.entries", "2000")
        .mode("append")
        .save()
    )
