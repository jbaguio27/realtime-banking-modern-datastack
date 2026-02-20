# Kafka To MinIO Env Guide

Files:

- `consumer/.env` for active local values
- `consumer/.env.example` template for cloners

Quick start:

```powershell
Copy-Item consumer/.env.example consumer/.env
python consumer/kafka_to_minio.py
```

Notes:

- Defaults in `.env.example` are for host-run (`localhost`).
- If running inside Docker network, use `kafka:9092` and `http://minio:9000`.
