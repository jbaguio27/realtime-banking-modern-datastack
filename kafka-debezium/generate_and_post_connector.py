"""Create a Debezium Postgres connector via Kafka Connect REST API."""

import json
import os
from typing import Any

import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - fallback for minimal local smoke runs
    def load_dotenv(*_args: Any, **_kwargs: Any) -> None:
        return None

CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "postgres-connector"
CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector"
DATABASE_HOST = "banking-postgres"
DATABASE_PORT = "5432"
TOPIC_PREFIX = "banking_server"
TABLE_INCLUDE_LIST = "public.customers,public.accounts,public.transactions"
PLUGIN_NAME = "pgoutput"
SLOT_NAME = "banking_slot"
PUBLICATION_AUTOCREATE_MODE = "filtered"
TOMBSTONES_ON_DELETE = "false"
DECIMAL_HANDLING_MODE = "double"
REQUEST_HEADERS = {"Content-Type": "application/json"}

STATUS_CREATED = 201
STATUS_ALREADY_EXISTS = 409

SUCCESS_MESSAGE = "Connector created successfully!"
ALREADY_EXISTS_MESSAGE = "Connector already exists."
ERROR_MESSAGE_TEMPLATE = "Failed to create connector ({status_code}): {response_text}"

REQUIRED_ENV_VARS = (
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
)


def get_required_env(env_name: str) -> str:
    """Return a required env var value or raise a clear validation error."""
    env_value = os.getenv(env_name)
    if env_value:
        return env_value

    raise ValueError(f"Missing required environment variable: {env_name}")


def validate_required_environment() -> None:
    """Validate required env vars before building connector config."""
    for env_name in REQUIRED_ENV_VARS:
        get_required_env(env_name)


def build_connector_config() -> dict[str, Any]:
    """Build connector payload from validated environment values."""
    return {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": CONNECTOR_CLASS,
            "database.hostname": DATABASE_HOST,
            "database.port": DATABASE_PORT,
            "database.user": get_required_env("POSTGRES_USER"),
            "database.password": get_required_env("POSTGRES_PASSWORD"),
            "database.dbname": get_required_env("POSTGRES_DB"),
            "topic.prefix": TOPIC_PREFIX,
            "table.include.list": TABLE_INCLUDE_LIST,
            "plugin.name": PLUGIN_NAME,
            "slot.name": SLOT_NAME,
            "publication.autocreate.mode": PUBLICATION_AUTOCREATE_MODE,
            "tombstones.on.delete": TOMBSTONES_ON_DELETE,
            "decimal.handling.mode": DECIMAL_HANDLING_MODE,
        },
    }


def build_response_message(status_code: int, response_text: str) -> str:
    """Map API response to the user-facing message."""
    if status_code == STATUS_CREATED:
        return SUCCESS_MESSAGE
    if status_code == STATUS_ALREADY_EXISTS:
        return ALREADY_EXISTS_MESSAGE
    return ERROR_MESSAGE_TEMPLATE.format(status_code=status_code, response_text=response_text)


def create_connector(payload: dict[str, Any]) -> requests.Response:
    """Send connector creation request to Kafka Connect."""
    return requests.post(CONNECT_URL, headers=REQUEST_HEADERS, data=json.dumps(payload))


def main() -> None:
    """Load env, validate config, call API, and print result."""
    try:
        load_dotenv()
        validate_required_environment()
        connector_config = build_connector_config()
        response = create_connector(connector_config)
        print(build_response_message(response.status_code, response.text))
    except ValueError as exc:
        print(str(exc))
    except requests.RequestException as exc:
        print(f"Failed to create connector request: {exc}")


if __name__ == "__main__":
    main()
