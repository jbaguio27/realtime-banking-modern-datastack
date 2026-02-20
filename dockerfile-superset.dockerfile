FROM apache/superset:latest

USER root
RUN /app/.venv/bin/python -m ensurepip && \
    /app/.venv/bin/python -m pip install --no-cache-dir \
    "sqlalchemy<2" \
    "snowflake-connector-python<4" \
    "snowflake-sqlalchemy<1.6"
USER superset
