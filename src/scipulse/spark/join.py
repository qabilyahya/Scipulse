"""Jointure des 3 couches Silver (ArXiv × citations × HN) + analyse de couverture."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scipulse.common.spark import s3a
from scipulse.config import buckets


def load_silver(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    arxiv = spark.read.parquet(s3a(buckets.ARXIV_CLEAN))
    hn = spark.read.parquet(s3a(buckets.HN_CLEAN))
    cit = spark.read.parquet(s3a(buckets.CITATIONS_CLEAN))
    return arxiv, hn, cit


def aggregate_hn(hn: DataFrame) -> DataFrame:
    """Un article peut être posté plusieurs fois sur HN : on agrège par arxiv_id."""
    return (
        hn.where(F.col("arxiv_id").isNotNull())
        .groupBy("arxiv_id")
        .agg(
            F.sum("score").alias("hn_score"),
            F.sum("num_comments").alias("hn_comments"),
            F.count("*").alias("hn_posts"),
            F.min("posted_at").alias("hn_first_post"),
        )
    )


def join_sources(arxiv: DataFrame, hn: DataFrame, cit: DataFrame, broadcast_hn: bool = True) -> DataFrame:
    """ArXiv LEFT JOIN citations (sort-merge, gros volumes) LEFT JOIN HN agrégé (petit → broadcast).

    Comparer les plans avec .explain(mode="formatted") dans notebooks/02_spark_pipeline.ipynb.
    """
    hn_agg = aggregate_hn(hn)
    if broadcast_hn:
        hn_agg = F.broadcast(hn_agg)
    cit = cit.select("arxiv_id", "cited_by_count", "counts_by_year", "fields", "referenced_works")
    return (
        arxiv.join(cit, "arxiv_id", "left")
        .join(hn_agg, "arxiv_id", "left")
        .fillna({"cited_by_count": 0, "hn_score": 0, "hn_comments": 0, "hn_posts": 0})
    )


def match_coverage(df: DataFrame) -> dict:
    """Taux de correspondance : % d'articles ArXiv présents dans chaque source."""
    row = df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("cited_by_count") > 0, 1).otherwise(0)).alias("with_citations"),
        F.sum(F.when(F.col("hn_posts") > 0, 1).otherwise(0)).alias("with_hn"),
        F.sum(F.when((F.col("hn_posts") > 0) & (F.col("cited_by_count") > 0), 1).otherwise(0)).alias("with_both"),
    ).first().asDict()
    total = row["total"] or 1
    return {**row, **{f"pct_{k}": round(100 * row[k] / total, 2) for k in ("with_citations", "with_hn", "with_both")}}
