"""DAG de bout en bout (Module 2) : HN Silver + citations Bronze/Silver → Spark Gold → ES."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {"owner": "scipulse", "retries": 1, "retry_delay": timedelta(minutes=10)}


@dag(
    dag_id="scipulse_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "spark", "module2"],
)
def scipulse_pipeline():
    @task
    def hn_silver() -> int:
        from scipulse.tasks import hn_silver as run
        return run()

    @task
    def citations_bronze(_: int) -> int:
        from scipulse.tasks import citations_bronze as run
        return run()

    @task
    def citations_silver(_: int) -> int:
        from scipulse.tasks import citations_silver as run
        return run()

    # BashOperator : plus simple à déboguer que SparkSubmitOperator (même commande qu'en local)
    spark_gold = BashOperator(
        task_id="spark_gold",
        bash_command="python /opt/airflow/src/scipulse/spark/gold_job.py",
    )

    hn = hn_silver()
    citations_silver(citations_bronze(hn)) >> spark_gold


scipulse_pipeline()

