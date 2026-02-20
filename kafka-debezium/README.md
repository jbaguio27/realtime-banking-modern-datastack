# Debezium Connector Env Guide

Files:

- `kafka-debezium/.env` for active local values
- `kafka-debezium/.env.example` template for cloners

Quick start:

```powershell
Copy-Item kafka-debezium/.env.example kafka-debezium/.env
python kafka-debezium/generate_and_post_connector.py
```

Notes:

- Script requires `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB`.
- Connector REST target is `http://localhost:8083/connectors` by default.
