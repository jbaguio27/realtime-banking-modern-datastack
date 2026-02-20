import importlib.util
import os
import pathlib
import sys
import types
import unittest

# Provide a tiny stub so importing the module does not require installed python-dotenv.
dotenv_stub = types.ModuleType("dotenv")
dotenv_stub.load_dotenv = lambda *args, **kwargs: None
sys.modules.setdefault("dotenv", dotenv_stub)

MODULE_PATH = pathlib.Path("kafka-debezium/generate_and_post_connector.py")
SPEC = importlib.util.spec_from_file_location("generate_and_post_connector", MODULE_PATH)
connector_module = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(connector_module)


class TestConnectorHelpers(unittest.TestCase):
    def test_get_required_env_returns_value(self):
        with patch_env("POSTGRES_USER", "demo_user"):
            self.assertEqual(connector_module.get_required_env("POSTGRES_USER"), "demo_user")

    def test_get_required_env_raises_for_missing(self):
        with patch_env("POSTGRES_USER", None):
            with self.assertRaises(ValueError) as ctx:
                connector_module.get_required_env("POSTGRES_USER")
        self.assertIn("POSTGRES_USER", str(ctx.exception))

    def test_build_connector_config_uses_environment_values(self):
        with patch_env("POSTGRES_USER", "u"), patch_env("POSTGRES_PASSWORD", "p"), patch_env("POSTGRES_DB", "db"):
            payload = connector_module.build_connector_config()

        self.assertEqual(payload["name"], "postgres-connector")
        self.assertEqual(payload["config"]["database.user"], "u")
        self.assertEqual(payload["config"]["database.password"], "p")
        self.assertEqual(payload["config"]["database.dbname"], "db")

    def test_build_response_message_success(self):
        self.assertEqual(
            connector_module.build_response_message(201, ""),
            "Connector created successfully!",
        )

    def test_build_response_message_already_exists(self):
        self.assertEqual(
            connector_module.build_response_message(409, "ignored"),
            "Connector already exists.",
        )

    def test_build_response_message_error(self):
        self.assertEqual(
            connector_module.build_response_message(500, "boom"),
            "Failed to create connector (500): boom",
        )


class patch_env:
    def __init__(self, key: str, value: str | None):
        self.key = key
        self.value = value
        self.original = None
        self.had_original = False

    def __enter__(self):
        self.had_original = self.key in os.environ
        self.original = os.environ.get(self.key)
        if self.value is None:
            os.environ.pop(self.key, None)
        else:
            os.environ[self.key] = self.value

    def __exit__(self, exc_type, exc, tb):
        if self.had_original:
            os.environ[self.key] = self.original if self.original is not None else ""
        else:
            os.environ.pop(self.key, None)


if __name__ == "__main__":
    unittest.main()
