import json
import os
import socket
import time
from datetime import datetime, timedelta
from http.cookiejar import CookieJar
from socket import timeout as SocketTimeout
from urllib.parse import quote_plus, urlencode
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

import snowflake.connector
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv

# Load DAG-local env file
dag_folder = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(dag_folder, ".env"))

DEFAULT_SUPERSET_URL = "http://superset:8088"
DEFAULT_SUPERSET_TIMEOUT_SECONDS = 90
DEFAULT_SUPERSET_RETRIES = 3
DEFAULT_SNOWFLAKE_ROLE = "ACCOUNTADMIN"
BI_SCHEMA = "ANALYTICS"


def _env_nonempty(*keys: str, default: str = "") -> str:
    """Return the first non-empty environment variable value."""
    for key in keys:
        val = os.getenv(key)
        if val is not None and val.strip() != "":
            return val
    return default


def _required_env(keys: list[str], context: str) -> None:
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise ValueError(f"Missing {context} env vars: {', '.join(missing)}")


def _snowflake_conn():
    required_keys = [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DB",
        "SNOWFLAKE_SCHEMA",
    ]
    _required_env(required_keys, "Snowflake")

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def wait_for_superset(timeout_seconds: int = 180) -> None:
    """Wait until Superset service is reachable via TCP."""
    host = os.getenv("SUPERSET_HOST", "superset")
    port = int(os.getenv("SUPERSET_PORT", "8088"))

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=3):
                print(f"Superset is reachable at {host}:{port}")
                return
        except OSError:
            time.sleep(3)
    raise TimeoutError(f"Timed out waiting for Superset at {host}:{port}")


def _superset_base_url() -> str:
    return _env_nonempty("SUPERSET_URL", default=DEFAULT_SUPERSET_URL).rstrip("/")


def _http_json(
    method: str,
    url: str,
    payload: dict | None = None,
    headers: dict | None = None,
    opener=None,
) -> dict:
    """Call Superset API endpoint and parse JSON with retry on timeout."""
    timeout_seconds = int(
        os.getenv("SUPERSET_API_TIMEOUT_SECONDS", str(DEFAULT_SUPERSET_TIMEOUT_SECONDS))
    )
    max_retries = int(os.getenv("SUPERSET_API_RETRIES", str(DEFAULT_SUPERSET_RETRIES)))
    data = None
    req_headers = headers.copy() if headers else {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    last_exc = None

    for attempt in range(1, max_retries + 1):
        req = Request(url=url, method=method, data=data, headers=req_headers)
        try:
            if opener is None:
                resp_ctx = urlopen(req, timeout=timeout_seconds)
            else:
                resp_ctx = opener.open(req, timeout=timeout_seconds)

            with resp_ctx as resp:
                body = resp.read()
            return json.loads(body.decode("utf-8")) if body else {}
        except HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Superset API {method} {url} failed: status={exc.code}, body={err_body}"
            ) from exc
        except (TimeoutError, SocketTimeout) as exc:
            last_exc = exc
            if attempt < max_retries:
                sleep_seconds = min(5 * attempt, 15)
                print(
                    f"Superset API timeout on attempt {attempt}/{max_retries}: "
                    f"{method} {url}. Retrying in {sleep_seconds}s..."
                )
                time.sleep(sleep_seconds)
            else:
                raise RuntimeError(
                    f"Superset API {method} {url} timed out after {max_retries} attempts "
                    f"(timeout={timeout_seconds}s)."
                ) from last_exc

    raise RuntimeError(f"Superset API {method} {url} failed unexpectedly.")


def _superset_auth_headers() -> tuple:
    """Authenticate to Superset and return API headers + cookie-aware opener."""
    username = _env_nonempty("SUPERSET_API_USERNAME", "SUPERSET_ADMIN_USERNAME", default="admin")
    password = _env_nonempty("SUPERSET_API_PASSWORD", "SUPERSET_ADMIN_PASSWORD", default="admin")
    cookie_jar = CookieJar()
    opener = build_opener(HTTPCookieProcessor(cookie_jar))

    login_payload = {
        "username": username,
        "password": password,
        "provider": "db",
        "refresh": True,
    }
    login_url = f"{_superset_base_url()}/api/v1/security/login"
    login_response = _http_json("POST", login_url, payload=login_payload, opener=opener)
    access_token = login_response.get("access_token")
    if not access_token:
        raise ValueError("Superset API login failed: missing access_token")

    base_headers = {
        "Authorization": f"Bearer {access_token}",
        "Referer": _superset_base_url(),
    }

    csrf_url = f"{_superset_base_url()}/api/v1/security/csrf_token/"
    csrf_response = _http_json("GET", csrf_url, headers=base_headers, opener=opener)
    csrf_token = csrf_response.get("result")
    if not csrf_token:
        raise ValueError("Superset CSRF token request failed: missing token")

    headers = {
        **base_headers,
        "X-CSRFToken": csrf_token,
    }
    return headers, opener


def _snowflake_sqlalchemy_uri() -> str:
    required_keys = [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DB",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_WAREHOUSE",
    ]
    _required_env(required_keys, "Snowflake for Superset connection")

    user = quote_plus(os.getenv("SNOWFLAKE_USER", ""))
    password = quote_plus(os.getenv("SNOWFLAKE_PASSWORD", ""))
    account = os.getenv("SNOWFLAKE_ACCOUNT", "")
    database = os.getenv("SNOWFLAKE_DB", "")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "")
    warehouse = quote_plus(os.getenv("SNOWFLAKE_WAREHOUSE", ""))
    role = quote_plus(os.getenv("SNOWFLAKE_ROLE", DEFAULT_SNOWFLAKE_ROLE))

    return (
        f"snowflake://{user}:{password}@{account}/{database}/{schema}"
        f"?warehouse={warehouse}&role={role}"
    )


def _ensure_superset_database(headers: dict, opener) -> int:
    """Find existing Superset database entry or create it."""
    target_name = os.getenv("SUPERSET_DB_NAME", "Snowflake Banking")
    list_url = f"{_superset_base_url()}/api/v1/database/?page=0&page_size=1000"
    list_response = _http_json("GET", list_url, headers=headers, opener=opener)
    for row in list_response.get("result", []):
        if row.get("database_name") == target_name:
            return int(row["id"])

    create_url = f"{_superset_base_url()}/api/v1/database/"
    payload = {
        "database_name": target_name,
        "engine": "snowflake",
        "configuration_method": "sqlalchemy_form",
        "sqlalchemy_uri": _snowflake_sqlalchemy_uri(),
        "expose_in_sqllab": True,
        "allow_file_upload": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
    }
    created = _http_json("POST", create_url, payload=payload, headers=headers, opener=opener)
    db_id = created.get("id")
    if db_id is None:
        # Some Superset versions return create response without id, so re-list.
        retry_list = _http_json("GET", list_url, headers=headers, opener=opener)
        for row in retry_list.get("result", []):
            if row.get("database_name") == target_name:
                return int(row["id"])
        raise ValueError("Superset database creation failed: missing database id in response")
    return int(db_id)


def _ensure_superset_datasets(headers: dict, database_id: int, opener) -> None:
    """Create BI datasets in Superset if they do not already exist."""
    analytics_schema = os.getenv("SUPERSET_BI_SCHEMA", BI_SCHEMA)
    required_tables = {
        "BI_BP1_TRANSACTION_RISK_DAILY",
        "BI_BP2_CUSTOMER_VALUE_SEGMENTS",
        "BI_BP3_LIQUIDITY_RISK_ACCOUNTS",
    }

    create_url = f"{_superset_base_url()}/api/v1/dataset/"
    for table_name in sorted(required_tables):
        payload = {
            "database": database_id,
            "schema": analytics_schema,
            "table_name": table_name,
        }
        try:
            _http_json("POST", create_url, payload=payload, headers=headers, opener=opener)
            print(f"Superset dataset created: {analytics_schema}.{table_name}")
        except RuntimeError as exc:
            msg = str(exc).lower()
            if "already exists" in msg or "duplicate" in msg:
                print(f"Superset dataset already exists: {analytics_schema}.{table_name}")
                continue
            raise


def provision_superset_assets() -> None:
    """Create Superset DB connection and BI datasets (if enabled)."""
    auto_provision = os.getenv("SUPERSET_AUTO_PROVISION", "true").strip().lower()
    if auto_provision not in {"1", "true", "yes", "on"}:
        print("Skipping Superset provisioning: SUPERSET_AUTO_PROVISION is disabled.")
        return

    headers, opener = _superset_auth_headers()
    database_id = _ensure_superset_database(headers, opener)
    _ensure_superset_datasets(headers, database_id, opener)
    print("Superset assets ensured: Snowflake database + BI datasets.")


def create_superset_reporting_views() -> None:
    """
    Materialize reporting-ready views for 3 business problems:
    1) Transaction reliability and operational risk trends
    2) Customer value and churn-risk behavior
    3) Liquidity/overdraft exposure at account level
    """
    sql_statements = [
        f"CREATE SCHEMA IF NOT EXISTS {BI_SCHEMA}",
        """
        CREATE OR REPLACE VIEW ANALYTICS.BI_BP1_TRANSACTION_RISK_DAILY AS
        SELECT
            CAST(transaction_time AS DATE) AS txn_date,
            transaction_type,
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN UPPER(COALESCE(status, 'UNKNOWN')) = 'FAILED' THEN 1 ELSE 0 END) AS failed_transactions,
            ROUND(
                100.0 * SUM(CASE WHEN UPPER(COALESCE(status, 'UNKNOWN')) = 'FAILED' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            ) AS failed_rate_pct,
            ROUND(SUM(amount), 2) AS total_amount,
            ROUND(AVG(amount), 2) AS avg_amount
        FROM ANALYTICS.FACT_TRANSACTIONS
        WHERE transaction_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
        GROUP BY 1, 2
        """,
        """
        CREATE OR REPLACE VIEW ANALYTICS.BI_BP2_CUSTOMER_VALUE_SEGMENTS AS
        WITH tx_90d AS (
            SELECT
                customer_id,
                COUNT(*) AS tx_count_90d,
                ROUND(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 2) AS inflow_90d,
                ROUND(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 2) AS outflow_90d,
                MAX(transaction_time) AS last_tx_time
            FROM ANALYTICS.FACT_TRANSACTIONS
            WHERE transaction_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
            GROUP BY customer_id
        ),
        active_customers AS (
            SELECT customer_id, first_name, last_name, email
            FROM ANALYTICS.DIM_CUSTOMERS
            WHERE is_current = TRUE
        )
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            COALESCE(t.tx_count_90d, 0) AS tx_count_90d,
            COALESCE(t.inflow_90d, 0) AS inflow_90d,
            COALESCE(t.outflow_90d, 0) AS outflow_90d,
            ROUND(COALESCE(t.inflow_90d, 0) - COALESCE(t.outflow_90d, 0), 2) AS net_flow_90d,
            t.last_tx_time,
            CASE
                WHEN COALESCE(t.tx_count_90d, 0) = 0 THEN 'Dormant'
                WHEN COALESCE(t.tx_count_90d, 0) >= 20 THEN 'High Activity'
                WHEN COALESCE(t.tx_count_90d, 0) >= 5 THEN 'Medium Activity'
                ELSE 'Low Activity'
            END AS activity_segment
        FROM active_customers c
        LEFT JOIN tx_90d t
            ON c.customer_id = t.customer_id
        """,
        """
        CREATE OR REPLACE VIEW ANALYTICS.BI_BP3_LIQUIDITY_RISK_ACCOUNTS AS
        WITH tx_30d AS (
            SELECT
                account_id,
                COUNT(*) AS tx_count_30d,
                MAX(transaction_time) AS last_tx_time
            FROM ANALYTICS.FACT_TRANSACTIONS
            WHERE transaction_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
            GROUP BY account_id
        )
        SELECT
            a.account_id,
            a.customer_id,
            a.account_type,
            a.currency,
            ROUND(a.balance, 2) AS current_balance,
            COALESCE(t.tx_count_30d, 0) AS tx_count_30d,
            t.last_tx_time,
            CASE
                WHEN a.balance < 0 THEN 'Overdrawn'
                WHEN a.balance < 100 THEN 'Low Balance'
                WHEN COALESCE(t.tx_count_30d, 0) = 0 THEN 'No Recent Activity'
                ELSE 'Healthy'
            END AS liquidity_risk_band
        FROM ANALYTICS.DIM_ACCOUNTS a
        LEFT JOIN tx_30d t
            ON a.account_id = t.account_id
        WHERE a.is_current = TRUE
        """,
    ]

    conn = _snowflake_conn()
    cur = conn.cursor()
    try:
        for sql in sql_statements:
            cur.execute(sql)
        print(
            "Created Superset BI views: "
            "ANALYTICS.BI_BP1_TRANSACTION_RISK_DAILY, "
            "ANALYTICS.BI_BP2_CUSTOMER_VALUE_SEGMENTS, "
            "ANALYTICS.BI_BP3_LIQUIDITY_RISK_ACCOUNTS"
        )
    finally:
        cur.close()
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="superset_business_automation",
    default_args=default_args,
    description="Build Superset business views in Snowflake",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["superset", "snowflake", "analytics", "banking"],
) as dag:
    wait_superset = PythonOperator(
        task_id="wait_for_superset",
        python_callable=wait_for_superset,
        op_kwargs={"timeout_seconds": 180},
    )

    build_reporting_views = PythonOperator(
        task_id="build_reporting_views",
        python_callable=create_superset_reporting_views,
    )

    provision_superset = PythonOperator(
        task_id="provision_superset_assets",
        python_callable=provision_superset_assets,
    )

    wait_superset >> build_reporting_views >> provision_superset
