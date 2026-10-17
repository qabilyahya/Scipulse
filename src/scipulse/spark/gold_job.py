"""Job Spark Gold : Silver × 3 → jointure → indicateurs → TF-IDF → score → Elasticsearch.

Lancement : spark-submit src/scipulse/spark/gold_job.py   (ou via le DAG scipulse_pipeline)
"""
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/opt/airflow/src")

from scipulse.common.logging import get_logger  # noqa: E402
from scipulse.common.monitoring import emit  # noqa: E402
from scipulse.common.spark import get_spark  # noqa: E402
from scipulse.spark.impact_score import compute_impact  # noqa: E402
from scipulse.spark.indicators import add_category_rank, add_citation_velocity  # noqa: E402
from scipulse.spark.join import join_sources, load_silver, match_coverage  # noqa: E402
from scipulse.spark.tfidf import add_top_terms, fit_tfidf  # noqa: E402
from scipulse.spark.write_es import write_gold  # noqa: E402

log = get_logger("gold_job")


def main() -> None:
    year = datetime.now(timezone.utc).year
    spark = get_spark("scipulse-gold")

    arxiv, hn, cit = load_silver(spark)
    df = join_sources(arxiv, hn, cit).cache()

    coverage = match_coverage(df)
    log.info("Match coverage : %s", coverage)
    emit("spark_join", source="all", layer="gold", **coverage)

    df = add_citation_velocity(df, year)
    df = add_category_rank(df)
    model, df = fit_tfidf(df)
    df = add_top_terms(df, model)
    df = compute_impact(df, year)

    write_gold(df)
    emit("indexing", source="all", layer="gold", records=df.count())
    spark.stop()


if __name__ == "__main__":
    main()
