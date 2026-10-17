"""Indicateurs temporels (window functions)."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def yearly_growth_by_category(df: DataFrame) -> DataFrame:
    """Nombre de publications par (catégorie, année) et croissance vs année précédente."""
    w = Window.partitionBy("primary_category").orderBy("year")
    return (
        df.groupBy("primary_category", "year").agg(F.count("*").alias("n_papers"))
        .withColumn("prev_year_papers", F.lag("n_papers").over(w))
        .withColumn("yoy_growth",
                    (F.col("n_papers") - F.col("prev_year_papers")) / F.col("prev_year_papers"))
    )


def add_citation_velocity(df: DataFrame, current_year: int) -> DataFrame:
    """Citations par an depuis la publication + citations des 2 dernières années (OpenAlex)."""
    recent = F.aggregate(
        F.filter(F.coalesce(F.col("counts_by_year"), F.array()), lambda x: x["year"] >= current_year - 1),
        F.lit(0).cast("long"),
        lambda acc, x: acc + x["cited_by_count"],
    )
    return (
        df.withColumn("age_years", F.greatest(F.lit(1), F.lit(current_year) - F.col("year") + 1))
        .withColumn("citation_velocity", F.col("cited_by_count") / F.col("age_years"))
        .withColumn("citations_last_2y", recent)
    )


def add_category_rank(df: DataFrame) -> DataFrame:
    """Rang de l'article dans sa catégorie selon les citations."""
    w = Window.partitionBy("primary_category").orderBy(F.desc("cited_by_count"))
    return df.withColumn("rank_in_category", F.row_number().over(w))
