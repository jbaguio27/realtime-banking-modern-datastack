# Airflow DAG Env Guide

Files:

- `docker/dags/.env` for active DAG runtime values
- `docker/dags/.env.example` template for cloners

Quick start:

```powershell
Copy-Item docker/dags/.env.example docker/dags/.env
```

This env is used by DAGs for:

- MinIO settings
- Snowflake credentials/settings
- Superset API provisioning settings
