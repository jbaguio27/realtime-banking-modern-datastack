import importlib.util
import pathlib
import sys
import types
import unittest
from decimal import Decimal

# Provide a tiny stub so importing the generator does not require installed python-dotenv.
dotenv_stub = types.ModuleType("dotenv")
dotenv_stub.load_dotenv = lambda *args, **kwargs: None
sys.modules.setdefault("dotenv", dotenv_stub)


MODULE_PATH = pathlib.Path("data-generator/faker_generator.py")
SPEC = importlib.util.spec_from_file_location("faker_generator", MODULE_PATH)
faker_generator = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(faker_generator)


class TestFakerGeneratorHelpers(unittest.TestCase):
    def test_choose_subset_empty(self):
        self.assertEqual(faker_generator.choose_subset([], 0.5), set())

    def test_choose_subset_ensure_one(self):
        items = [1, 2, 3]
        subset = faker_generator.choose_subset(items, 0.01, ensure_one=True)
        self.assertGreaterEqual(len(subset), 1)
        self.assertTrue(subset.issubset(set(items)))

    def test_random_money_scale(self):
        val = faker_generator.random_money(Decimal("1.00"), Decimal("1.99"))
        self.assertGreaterEqual(val, Decimal("1.00"))
        self.assertLessEqual(val, Decimal("1.99"))
        self.assertEqual(val, val.quantize(Decimal("0.01")))

    def test_build_unique_email(self):
        email = faker_generator.build_unique_email("alice@example.com", now_ms=1234567890)
        self.assertTrue(email.startswith("alice."))
        self.assertTrue(email.endswith("@example.com"))

    def test_validate_config_fails_bad_ratios(self):
        bad_cfg = faker_generator.GeneratorConfig(failed_tx_ratio=0.8, pending_tx_ratio=0.5)
        with self.assertRaises(ValueError):
            faker_generator.validate_config(bad_cfg)


if __name__ == "__main__":
    unittest.main()
