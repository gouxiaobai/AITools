import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from services.recommendation.portfolio import num_from_prop_any, resolve_cash_config_fields


class CashPortfolioTest(unittest.TestCase):
    def test_num_from_prop_any_supports_formula_and_rollup(self) -> None:
        page = {
            "properties": {
                "\u73b0\u91d1\u516c\u5f0f": {"type": "formula", "formula": {"type": "number", "number": 123.45}},
                "\u73b0\u91d1\u6c47\u603b": {"type": "rollup", "rollup": {"type": "number", "number": -67.89}},
                "\u7eaf\u6570\u5b57": {"type": "number", "number": 0},
            }
        }

        self.assertEqual(num_from_prop_any(page, "\u73b0\u91d1\u516c\u5f0f"), 123.45)
        self.assertEqual(num_from_prop_any(page, "\u73b0\u91d1\u6c47\u603b"), -67.89)
        self.assertEqual(num_from_prop_any(page, "\u7eaf\u6570\u5b57"), 0.0)

    def test_resolve_cash_config_fields_accepts_formula_cash(self) -> None:
        cash_db = {
            "properties": {
                "\u53ef\u6d41\u52a8\u73b0\u91d1": {"type": "formula"},
                "\u540d\u79f0": {"type": "title"},
            }
        }

        fields = resolve_cash_config_fields(cash_db, pref_name="\u53ef\u6d41\u52a8\u73b0\u91d1")
        self.assertEqual(fields["cash"], "\u53ef\u6d41\u52a8\u73b0\u91d1")


if __name__ == "__main__":
    unittest.main()
