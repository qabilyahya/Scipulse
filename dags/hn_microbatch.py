"""DAG Hacker News (micro-batch, toutes les 5 min) : polling API → Bronze hn-raw/."""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {"owner": "scipulse", "retries": 2, "retry_delay": timedelta(minutes=1)}


@dag(
    dag_id="hn_microbatch",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["scipulse", "hn", "module1"],
)
def hn_microbatch():
    @task
    def poll() -> int:
        from scipulse.tasks import hn_poll
        last = int(Variable.get("hn_last_seen_id", default_var=0))
        new_max = hn_poll(last)
        Variable.set("hn_last_seen_id", new_max)   # état : reprise sans doublon
        return new_max

    poll()


@dag(
    dag_id="hn_backfill",
    schedule=None,                     # lancement manuel, une fois
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["scipulse", "hn", "module1"],
)
def hn_backfill():
    @task
    def backfill() -> int:
        from scipulse.tasks import hn_backfill as run
        return run()

    backfill()


hn_microbatch()
hn_backfill()
