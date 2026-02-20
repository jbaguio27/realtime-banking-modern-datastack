# Data Generator Env Guide

This folder uses one canonical env file:

- `data-generator/.env` for active values
- `data-generator/.env.example` as the clonable template

Quick start:

```powershell
Copy-Item data-generator/.env.example data-generator/.env
python data-generator/faker_generator.py --once --iterations 1
```

Notes:

- The `.env.example` profile is tuned to produce signal for dashboard BP1/BP2/BP3.
- You can run with a different file via `DATA_GENERATOR_ENV_FILE=<path>`.
