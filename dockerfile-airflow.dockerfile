FROM apache/airflow:3.1.7

USER root

# 1. Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 2. Setup build directory
WORKDIR /opt/airflow/custom_build

# 3. Copy files (as root, which is default)
COPY pyproject.toml README.md ./
COPY banking_modern_datastack/ ./banking_modern_datastack/

# 4. Install as ROOT
# This ensures no "Permission Denied" when creating .egg-info or writing to site-packages
RUN uv pip install --no-cache --system .

# 5. Clean up the build directory to keep the image small
WORKDIR /opt/airflow
RUN rm -rf /opt/airflow/custom_build

# 6. Switch to airflow user for security at runtime
USER airflow