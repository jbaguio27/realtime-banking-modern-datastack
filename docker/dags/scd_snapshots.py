import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv

# dbt runtime paths inside airflow container
DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/banking_dbt"
DAGS_ENV_PATH = "/opt/airflow/dags/.env"

if os.path.exists(DAGS_ENV_PATH):
    load_dotenv(dotenv_path=DAGS_ENV_PATH, override=False)

# Runtime env passed to dbt commands
DBT_ENV = {
    **os.environ,
    "DBT_PARTIAL_PARSE": "false",
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DBT_LOG_PATH": f"{DBT_PROJECT_DIR}/logs",  # Forces logs into the project folder
    # Backward-compatible mapping: dbt-specific names first, then shared Snowflake env vars
    "DBT_SNOWFLAKE_ACCOUNT": os.getenv("DBT_SNOWFLAKE_ACCOUNT", os.getenv("SNOWFLAKE_ACCOUNT", "")),
    "DBT_SNOWFLAKE_USER": os.getenv("DBT_SNOWFLAKE_USER", os.getenv("SNOWFLAKE_USER", "")),
    "DBT_SNOWFLAKE_PASSWORD": os.getenv("DBT_SNOWFLAKE_PASSWORD", os.getenv("SNOWFLAKE_PASSWORD", "")),
    "DBT_SNOWFLAKE_ROLE": os.getenv("DBT_SNOWFLAKE_ROLE", os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")),
    "DBT_SNOWFLAKE_WAREHOUSE": os.getenv("DBT_SNOWFLAKE_WAREHOUSE", os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")),
    "DBT_SNOWFLAKE_DB": os.getenv("DBT_SNOWFLAKE_DB", os.getenv("SNOWFLAKE_DB", "BANKING")),
    "DBT_SNOWFLAKE_SCHEMA": os.getenv("DBT_SNOWFLAKE_SCHEMA", os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")),
    "DBT_THREADS": os.getenv("DBT_THREADS", "4"),
}

RUN_RESULTS_PATH = f"{DBT_PROJECT_DIR}/target/run_results.json"
QUALITY_WARN_THRESHOLD = int(os.getenv("DBT_QUALITY_WARN_THRESHOLD", "50"))


def summarize_quality_tests() -> None:
    """Summarize dbt test statuses and enforce warn threshold."""
    if not os.path.exists(RUN_RESULTS_PATH):
        raise FileNotFoundError(f"dbt run results not found: {RUN_RESULTS_PATH}")

    with open(RUN_RESULTS_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    results = payload.get("results", [])
    status_counts = {}
    for result in results:
        status = result.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    total = len(results)
    summary = ", ".join(f"{k}={v}" for k, v in sorted(status_counts.items()))
    print(f"dbt quality test summary: total={total}; {summary}")

    warn_count = status_counts.get("warn", 0)
    if warn_count > QUALITY_WARN_THRESHOLD:
        raise ValueError(
            f"Quality warn threshold breached: warn={warn_count}, "
            f"threshold={QUALITY_WARN_THRESHOLD}"
        )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "banking_realtime_dbt",
    default_args=default_args,
    description="Automated Snowflake Banking marts with tiered dbt tests",
    schedule=None,
    catchup=False,
) as dag:

    # 1. Clean & Staging
    # We run 'deps' here to ensure the Snowflake adapter is fully initialized
    run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_BIN} deps && {DBT_BIN} run --select staging",
        cwd=DBT_PROJECT_DIR,
        env=DBT_ENV,
    )

    # 2. Snapshots
    run_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot",
        cwd=DBT_PROJECT_DIR,
        env=DBT_ENV,
    )

    # 3. Build Dimensions and Facts
    run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_BIN} run --select marts",
        cwd=DBT_PROJECT_DIR,
        env=DBT_ENV,
    )

    # 4. Hard data integrity gate (must pass)
    run_critical_tests = BashOperator(
        task_id="dbt_test_critical",
        bash_command=f"{DBT_BIN} test --select tag:critical",
        cwd=DBT_PROJECT_DIR,
        env=DBT_ENV,
    )

    # 5. Soft data quality checks (warning-level visibility)
    run_quality_tests = BashOperator(
        task_id="dbt_test_quality",
        bash_command=f"{DBT_BIN} test --select tag:quality",
        cwd=DBT_PROJECT_DIR,
        env=DBT_ENV,
    )

    summarize_quality = PythonOperator(
        task_id="summarize_quality_results",
        python_callable=summarize_quality_tests,
    )

    # Lineage: Clean/Staging -> Snapshot -> Marts -> Critical -> Quality -> Summary
    run_staging >> run_snapshot >> run_marts >> run_critical_tests >> run_quality_tests >> summarize_quality
