"""Monthly flights pipeline: BTS -> S3 -> Spark -> Snowflake -> dbt -> gate.

One DAG, one run per month (see docs/de_interview_notes.md Q13). Each run
owns exactly one data month — resolved as the run's logical date minus two
months, because BTS publishes flight data on a ~2 month lag.

Failure-mode defenses (notes Q1/Q2):
  - every task has an execution_timeout: a hung call is killed and retried,
    never left sleeping overnight
  - retries with exponential backoff for transient network/warehouse blips
  - dagrun_timeout bounds the whole run
  - every step is idempotent, so any retry or manual re-run is safe
  - the Great Expectations gate sits between dbt and the dashboard refresh:
    a failed gate means the refresh never fires
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

BUCKET = "swayam-airline-otp"
REPO = "/opt/airflow/repo"
MONTH = "{{ ti.xcom_pull(task_ids='resolve_month') }}"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="flights_pipeline",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,  # 2015-2024 history was backfilled explicitly
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    doc_md=__doc__,
    tags=["flights", "production"],
)
def flights_pipeline():

    @task(execution_timeout=timedelta(minutes=1))
    def resolve_month(data_interval_start=None) -> str:
        """The data month this run owns: logical date minus BTS's 2-month
        publication lag."""
        year = data_interval_start.year
        month = data_interval_start.month - 2
        if month < 1:
            year, month = year - 1, month + 12
        return f"{year}-{month:02d}"

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            f"cd {REPO} && python scripts/ingest_raw.py "
            f"--start {MONTH} --end {MONTH} --bucket {BUCKET}"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f"cd {REPO} && python scripts/backfill.py "
            f"--start {MONTH} --end {MONTH} "
            f"--raw-root s3a://{BUCKET}/raw/flights "
            f"--target s3a://{BUCKET}/lake/flights"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    snowflake_load = BashOperator(
        task_id="snowflake_load",
        bash_command=(
            f"cd {REPO} && python scripts/load_snowflake.py --month {MONTH}"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {REPO}/dbt_project && dbt build --profiles-dir .",
        execution_timeout=timedelta(minutes=30),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=f"cd {REPO} && python quality/run_checkpoint.py",
        execution_timeout=timedelta(minutes=15),
        retries=0,  # a failed gate is a data problem, not a transient blip
    )

    powerbi_refresh = BashOperator(
        task_id="powerbi_refresh",
        bash_command=f"cd {REPO} && python scripts/powerbi_push.py",
        # generous budget: the push honors Power BI's hourly-quota
        # Retry-After, which can be a ~55 minute sleep
        execution_timeout=timedelta(minutes=90),
        retries=1,
    )

    (
        resolve_month()
        >> ingest_raw
        >> spark_transform
        >> snowflake_load
        >> dbt_build
        >> quality_gate
        >> powerbi_refresh
    )


flights_pipeline()
