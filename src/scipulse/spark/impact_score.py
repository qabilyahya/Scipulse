"""Score d'impact composite — formule et pondérations à justifier dans docs/impact_score.md.

impact = W_CIT * cit_norm + W_HN * hn_norm + W_REC * recency

- cit_norm : percent_rank de log1p(citations) DANS la catégorie (évite que cs.LG écrase tout)
- hn_norm  : percent_rank de log1p(score + commentaires) sur les articles présents sur HN, 0 sinon
- recency  : décroissance exponentielle, demi-vie HALF_LIFE_YEARS
"""
from __future__ import annotations

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

W_CIT, W_HN, W_REC = 0.5, 0.3, 0.2
HALF_LIFE_YEARS = 2.0

# Seuils des cas d'usage analytiques (dashboard augmenté)
HIGH_PCT, LOW_PCT = 0.90, 0.30


def compute_impact(df: DataFrame, current_year: int, weights=(W_CIT, W_HN, W_REC)) -> DataFrame:
    w_cit, w_hn, w_rec = weights
    by_cat = Window.partitionBy("primary_category").orderBy("cit_log")
    on_hn = Window.partitionBy(F.col("hn_posts") > 0).orderBy("hn_log")

    df = (
        df.withColumn("cit_log", F.log1p("cited_by_count"))
        .withColumn("hn_log", F.log1p(F.col("hn_score") + F.col("hn_comments")))
        .withColumn("cit_norm", F.percent_rank().over(by_cat))
        .withColumn("hn_norm", F.when(F.col("hn_posts") > 0, F.percent_rank().over(on_hn)).otherwise(0.0))
        .withColumn("recency", F.exp(-math.log(2) * (F.lit(current_year) - F.col("year")) / HALF_LIFE_YEARS))
        .withColumn("impact_score",
                    F.round(100 * (w_cit * F.col("cit_norm") + w_hn * F.col("hn_norm") + w_rec * F.col("recency")), 2))
    )
    return (
        df.withColumn("is_hidden_gem", (F.col("cit_norm") >= HIGH_PCT) & (F.col("hn_posts") == 0))
        .withColumn("is_hn_hype", (F.col("hn_norm") >= HIGH_PCT) & (F.col("cit_norm") <= LOW_PCT))
        .withColumn("is_on_hn", F.col("hn_posts") > 0)
        .drop("cit_log", "hn_log")
    )
