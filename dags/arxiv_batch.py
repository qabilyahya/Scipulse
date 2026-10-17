"""DAG ArXiv (batch) : Bronze → Silver (GE + nettoyage) → Gold partiel (index arxiv-papers)."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {"owner": "scipulse", "retries": 1, "retry_delay": timedelta(minutes=5)}


@dag(
    dag_id="arxiv_batch",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["scipulse", "arxiv", "module1"],
)
def arxiv_batch():
    @task
    def bronze() -> int:
        from scipulse.tasks import arxiv_bronze
        return arxiv_bronze()

    @task
    def silver(_: int) -> int:
        from scipulse.tasks import arxiv_silver
        return arxiv_silver()

    @task
    def gold_partial(_: int) -> int:
        from scipulse.tasks import arxiv_index
        return arxiv_index()

    gold_partial(silver(bronze()))


arxiv_batch()
