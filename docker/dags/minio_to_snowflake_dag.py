import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import boto3
import snowflake.connector
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

dag_folder = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(dag_folder, ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

TABLES = ["customers", "accounts", "transactions"]
DOWNLOAD_WORKERS = int(os.getenv("MINIO_DOWNLOAD_WORKERS", "8"))
PUT_PARALLEL = int(os.getenv("SNOWFLAKE_PUT_PARALLEL", "8"))


def _validate_runtime_config() -> None:
    required = [
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_BUCKET",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DB",
        "SNOWFLAKE_SCHEMA",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")


def _download_object(s3_client, key: str, destination: str, transfer_cfg: TransferConfig) -> str:
    """Download one object from MinIO to local path."""
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    s3_client.download_file(BUCKET, key, destination, Config=transfer_cfg)
    return destination


def download_from_minio():
    """
    Download table-partitioned parquet files from MinIO to table folders.
    Returns a mapping: {table: [local_file_path, ...]}.
    """
    _validate_runtime_config()

    if os.path.exists(LOCAL_DIR):
        shutil.rmtree(LOCAL_DIR)
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    paginator = s3.get_paginator("list_objects_v2")
    transfer_cfg = TransferConfig(max_concurrency=DOWNLOAD_WORKERS, use_threads=True)
    downloaded: dict[str, list[str]] = {table: [] for table in TABLES}
    futures = []

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
        for table in TABLES:
            prefix = f"{table}/"
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")
                    if not key or key.endswith("/") or not key.lower().endswith(".parquet"):
                        continue
                    relative = key[len(prefix) :].replace("/", "__")
                    local_path = os.path.join(LOCAL_DIR, table, relative)
                    futures.append(
                        (
                            table,
                            key,
                            executor.submit(_download_object, s3, key, local_path, transfer_cfg),
                        )
                    )

        for table, key, future in futures:
            path = future.result()
            downloaded[table].append(path)
            print(f"Downloaded key={key} -> {path}")

    total_files = sum(len(paths) for paths in downloaded.values())
    if total_files == 0:
        print("No parquet files found in MinIO for configured tables.")
        return None

    for table in TABLES:
        print(f"Prepared {len(downloaded[table])} files for table={table}")
    return downloaded


def load_to_snowflake(**kwargs):
    """
    Batch PUT parquet files per table and COPY into Snowflake in one load command each.
    """
    _validate_runtime_config()

    ti = kwargs["ti"]
    downloaded = ti.xcom_pull(task_ids="download_minio")
    if not downloaded:
        print("No new data to load to Snowflake.")
        return

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cur = conn.cursor()

    try:
        for table in TABLES:
            table_files = downloaded.get(table, [])
            if not table_files:
                print(f"No local parquet files for table={table}, skipping.")
                continue

            table_dir = os.path.join(LOCAL_DIR, table)
            if not os.path.exists(table_dir):
                print(f"Missing local table directory for table={table}, skipping.")
                continue

            cur.execute(f"REMOVE @%{table}")
            cur.execute(
                f"PUT 'file://{table_dir}/*.parquet' @%{table} "
                f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL={PUT_PARALLEL}"
            )
            print(f"Staged {len(table_files)} files to @%{table} with batched PUT.")

            copy_sql = f"""
            COPY INTO {table} (v)
            FROM (SELECT $1 FROM @%{table})
            FILE_FORMAT = (TYPE = 'PARQUET' BINARY_AS_TEXT = FALSE)
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE;
            """
            cur.execute(copy_sql)
            print(f"Loaded and purged staged files for table={table}.")
    except Exception as exc:
        print(f"Error during Snowflake load: {exc!r}")
        raise
    finally:
        cur.close()
        conn.close()
        if os.path.exists(LOCAL_DIR):
            shutil.rmtree(LOCAL_DIR)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="ETL: MinIO Parquet -> Snowflake VARIANT Landing",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["banking", "snowflake", "minio"],
) as dag:
    task_download = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task_load = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    task_download >> task_load
