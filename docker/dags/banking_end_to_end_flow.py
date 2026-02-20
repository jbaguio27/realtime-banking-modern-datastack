import json
import os
import socket
import time
from datetime import datetime, timedelta
from typing import Any
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# Runtime command paths
PYTHON_BIN = "/opt/airflow/dbt_venv/bin/python"

# Internal service endpoints (container network)
KAFKA_HOST = "kafka"
KAFKA_PORT = 9092
CONNECT_URL = "http://connect:8083"
MINIO_HOST = "minio"
MINIO_PORT = 9000

TASK_ENV = {
    **os.environ,
    # Use container DNS names for service-to-service communication.
    "POSTGRES_HOST": "banking-postgres",
    "POSTGRES_PORT": "5432",
    "KAFKA_BOOTSTRAP": f"{KAFKA_HOST}:{KAFKA_PORT}",
    "MINIO_ENDPOINT": "http://minio:9000",
}

STAGE_SLA_SECONDS = {
    "wait_for_kafka": 180,
    "wait_for_debezium_connect": 180,
    "ensure_debezium_connector": 180,
    "wait_for_minio": 180,
    "generate_fake_data_once": 120,
    "consume_kafka_to_minio": 300,
    "trigger_minio_to_snowflake": 600,
    "trigger_dbt_pipeline": 900,
    "trigger_superset_automation": 600,
}


def _stage_start_callback(context: dict[str, Any]) -> None:
    """Store start time for duration logging in success callback."""
    context["ti"].xcom_push(key="stage_started_at_epoch", value=time.time())


def _stage_success_callback(context: dict[str, Any]) -> None:
    """Print stage execution timing and SLA status."""
    ti = context["ti"]
    task_id = ti.task_id
    started_at = ti.xcom_pull(task_ids=task_id, key="stage_started_at_epoch")
    if started_at is None:
        print(f"[stage-timing] task={task_id} elapsed_seconds=unknown")
        return

    elapsed = round(time.time() - float(started_at), 2)
    sla_seconds = STAGE_SLA_SECONDS.get(task_id)
    if sla_seconds is None:
        print(f"[stage-timing] task={task_id} elapsed_seconds={elapsed}")
        return

    status = "within_sla" if elapsed <= sla_seconds else "sla_breached"
    print(
        f"[stage-timing] task={task_id} elapsed_seconds={elapsed} "
        f"sla_seconds={sla_seconds} status={status}"
    )


def _stage_failure_callback(context: dict[str, Any]) -> None:
    """Standard failure log for regular tasks."""
    ti = context["ti"]
    task_id = ti.task_id
    exc = context.get("exception")
    print(
        f"[stage-failure] parent_dag={ti.dag_id} parent_run_id={ti.run_id} "
        f"task={task_id} exception={exc!r}"
    )


def _trigger_failure_callback(context: dict[str, Any]) -> None:
    """Failure log for TriggerDagRunOperator tasks."""
    ti = context["ti"]
    task = context["task"]
    exc = context.get("exception")
    child_dag_id = (task.params or {}).get("trigger_dag_id")
    child_run_id = ti.xcom_pull(task_ids=ti.task_id, key="trigger_run_id")
    print(
        f"[trigger-failure] parent_dag={ti.dag_id} parent_run_id={ti.run_id} "
        f"task={ti.task_id} child_dag={child_dag_id} child_run_id={child_run_id} "
        f"exception={exc!r}"
    )


def wait_for_tcp(host: str, port: int, timeout_seconds: int = 180) -> None:
    """Wait until a TCP socket is reachable."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=3):
                return
        except OSError:
            time.sleep(3)
    raise TimeoutError(f"Timed out waiting for {host}:{port}")


def wait_for_http(url: str, timeout_seconds: int = 180) -> None:
    """Wait until HTTP endpoint responds with a non-server error status."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=5) as response:
                if 200 <= response.status < 500:
                    return
        except Exception:
            time.sleep(3)
    raise TimeoutError(f"Timed out waiting for {url}")


def _http_json(method: str, url: str, payload: dict | None = None) -> dict:
    """Execute an HTTP request and parse JSON body."""
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(url=url, method=method, data=body, headers=headers)
    with urlopen(request, timeout=10) as response:
        raw = response.read()
        return json.loads(raw.decode("utf-8")) if raw else {}


def ensure_debezium_connector(timeout_seconds: int = 180) -> None:
    """Create/update Debezium connector and wait until RUNNING."""
    connector_name = os.getenv("DEBEZIUM_CONNECTOR_NAME", "postgres-connector")
    connect_base = os.getenv("DEBEZIUM_CONNECT_URL", CONNECT_URL).rstrip("/")

    required_env = {
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
    }
    missing = [key for key, value in required_env.items() if not value]
    if missing:
        raise ValueError(f"Missing required env vars for Debezium connector: {', '.join(missing)}")

    connector_config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "banking-postgres",
        "database.port": "5432",
        "database.user": required_env["POSTGRES_USER"],
        "database.password": required_env["POSTGRES_PASSWORD"],
        "database.dbname": required_env["POSTGRES_DB"],
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    }

    upsert_url = f"{connect_base}/connectors/{connector_name}/config"
    status_url = f"{connect_base}/connectors/{connector_name}/status"

    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        try:
            _http_json("PUT", upsert_url, connector_config)
            status_payload = _http_json("GET", status_url)

            connector_state = status_payload.get("connector", {}).get("state")
            task_states = [
                task.get("state")
                for task in status_payload.get("tasks", [])
                if isinstance(task, dict)
            ]

            tasks_ok = not task_states or all(state == "RUNNING" for state in task_states)
            if connector_state == "RUNNING" and tasks_ok:
                print(
                    f"Debezium connector ready: name={connector_name}, "
                    f"connector_state={connector_state}, task_states={task_states or ['RUNNING']}"
                )
                return

            last_error = (
                f"Connector not ready yet: connector_state={connector_state}, "
                f"task_states={task_states}"
            )
            print(last_error)
        except Exception as exc:
            last_error = str(exc)
            print(f"Waiting for Debezium connector readiness: {last_error}")

        time.sleep(5)

    raise TimeoutError(
        f"Timed out ensuring Debezium connector '{connector_name}'. Last error: {last_error}"
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="banking_end_to_end_flow",
    default_args=default_args,
    description="End-to-end banking pipeline: generate -> consume -> load -> transform -> test",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "end-to-end", "cdc", "airflow"],
) as dag:
    # 1) Dependency checks
    wait_for_kafka = PythonOperator(
        task_id="wait_for_kafka",
        python_callable=wait_for_tcp,
        op_kwargs={"host": KAFKA_HOST, "port": KAFKA_PORT, "timeout_seconds": 180},
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_stage_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["wait_for_kafka"]),
    )

    wait_for_connect = PythonOperator(
        task_id="wait_for_debezium_connect",
        python_callable=wait_for_http,
        op_kwargs={"url": f"{CONNECT_URL}/connectors", "timeout_seconds": 180},
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_stage_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["wait_for_debezium_connect"]),
    )

    ensure_connector = PythonOperator(
        task_id="ensure_debezium_connector",
        python_callable=ensure_debezium_connector,
        op_kwargs={"timeout_seconds": 180},
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_stage_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["ensure_debezium_connector"]),
    )

    wait_for_minio = PythonOperator(
        task_id="wait_for_minio",
        python_callable=wait_for_tcp,
        op_kwargs={"host": MINIO_HOST, "port": MINIO_PORT, "timeout_seconds": 180},
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_stage_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["wait_for_minio"]),
    )

    # 2) Data ingestion
    generate_fake_data_once = BashOperator(
        task_id="generate_fake_data_once",
        bash_command=f"{PYTHON_BIN} faker_generator.py --once",
        cwd="/opt/airflow/data-generator",
        env=TASK_ENV,
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_stage_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["generate_fake_data_once"]),
    )

    consume_kafka_to_minio = BashOperator(
        task_id="consume_kafka_to_minio",
        bash_command=f"{PYTHON_BIN} kafka_to_minio.py --max-runtime-seconds 240 --max-idle-cycles 4",
        cwd="/opt/airflow/consumer",
        env=TASK_ENV,
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_stage_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["consume_kafka_to_minio"]),
    )

    # 3) Child pipeline orchestration
    trigger_minio_to_snowflake = TriggerDagRunOperator(
        task_id="trigger_minio_to_snowflake",
        trigger_dag_id="minio_to_snowflake_banking",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
        params={"trigger_dag_id": "minio_to_snowflake_banking"},
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_trigger_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["trigger_minio_to_snowflake"]),
    )

    trigger_dbt_pipeline = TriggerDagRunOperator(
        task_id="trigger_dbt_pipeline",
        trigger_dag_id="banking_realtime_dbt",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        params={"trigger_dag_id": "banking_realtime_dbt"},
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_trigger_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["trigger_dbt_pipeline"]),
    )

    trigger_superset_automation = TriggerDagRunOperator(
        task_id="trigger_superset_automation",
        trigger_dag_id="superset_business_automation",
        wait_for_completion=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
        params={"trigger_dag_id": "superset_business_automation"},
        on_execute_callback=_stage_start_callback,
        on_success_callback=_stage_success_callback,
        on_failure_callback=_trigger_failure_callback,
        sla=timedelta(seconds=STAGE_SLA_SECONDS["trigger_superset_automation"]),
    )

    (
        wait_for_kafka
        >> wait_for_connect
        >> ensure_connector
        >> wait_for_minio
        >> generate_fake_data_once
        >> consume_kafka_to_minio
        >> trigger_minio_to_snowflake
        >> trigger_dbt_pipeline
        >> trigger_superset_automation
    )
