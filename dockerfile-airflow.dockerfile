FROM apache/airflow:3.1.7

ARG AIRFLOW_VERSION=3.1.7
ARG PYTHON_MAJOR_MINOR=3.12
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt"

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv globally
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# dbt is installed in an isolated venv used by scd_snapshots.py
RUN uv venv /opt/airflow/dbt_venv \
    && uv pip install --no-cache-dir \
        --python /opt/airflow/dbt_venv/bin/python \
        dbt-core==1.11.6 \
        dbt-snowflake==1.11.2 \
        "snowflake-connector-python[secure-local-storage]" \
        boto3 \
        python-dotenv \
        psycopg2-binary \
        faker \
        kafka-python-ng \
        pandas \
        fastparquet \
    && ln -sf /opt/airflow/dbt_venv/bin/dbt /usr/local/bin/dbt \
    && chown -R airflow:0 /opt/airflow/dbt_venv

# Airflow runtime deps for PythonOperator DAGs (minio_to_snowflake_dag.py)
# installed with Airflow constraints using uv.
RUN uv pip install --system --no-cache-dir \
    --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    boto3 \
    python-dotenv \
    "snowflake-connector-python[secure-local-storage]"

# Runtime dependencies for local generator/consumer scripts executed by Airflow.
RUN uv pip install --system --no-cache-dir \
    psycopg2-binary \
    faker \
    kafka-python-ng \
    pandas \
    fastparquet

USER airflow

ENV DBT_PROFILES_DIR=/opt/airflow/banking_dbt
ENV PATH="/opt/airflow/dbt_venv/bin:/home/airflow/.local/bin:$PATH"
