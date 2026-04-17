import argparse
import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from app.cli import build_parser
from core.config import Cfg
from core.notion_client import NotionClient
from services.risk.gates import GateThreshold, evaluate_release_gate
from services.selection.selector import SnapshotSlice, score_snapshot_slice
from stock_pipeline import main


class ArchitectureCompatTest(unittest.TestCase):
    def test_cli_parser_has_critical_commands(self) -> None:
        parser = build_parser()
        self.assertIsInstance(parser, argparse.ArgumentParser)
        help_text = parser.format_help()
        for cmd in [
            "param-recommend",
            "param-apply",
            "param-monitor",
            "select-stock",
            "param-risk-guard",
            "sync-market-universe",
            "snapshot-market-daily",
            "manual-filter-set",
        ]:
            self.assertIn(cmd, help_text)

    def test_stock_pipeline_main_callable(self) -> None:
        self.assertTrue(callable(main))

    def test_core_symbols_available(self) -> None:
        self.assertTrue(isinstance(Cfg.__name__, str))
        self.assertTrue(isinstance(NotionClient.__name__, str))

    def test_gate_service(self) -> None:
        proposal = {"validation": {"stability": 0.5}, "hit_rate": 0.6, "dd_mean": 0.1, "experiment_id": "exp1"}
        ok, _, _ = evaluate_release_gate(
            proposal=proposal,
            expected_experiment_id="exp1",
            threshold=GateThreshold(min_stability=0.3, min_hit_rate=0.4, max_dd_mean=0.2, require_experiment=True),
        )
        self.assertTrue(ok)

    def test_selection_service(self) -> None:
        rows = [
            {"stock_id": "1", "stock_code": "600001", "stock_name": "A", "market": "SH", "ret_1d": 0.01, "hit_flag": 1, "max_drawdown": 0.05},
            {"stock_id": "1", "stock_code": "600001", "stock_name": "A", "market": "SH", "ret_1d": 0.02, "hit_flag": 1, "max_drawdown": 0.04},
            {"stock_id": "2", "stock_code": "600002", "stock_name": "B", "market": "SH", "ret_1d": -0.01, "hit_flag": 0, "max_drawdown": 0.08},
        ]
        out = score_snapshot_slice(SnapshotSlice(rows=rows, strategy_filter=[], market_filter=[], start_date="", end_date=""), top_n=1)
        self.assertEqual(len(out["selected"]), 1)
        self.assertIn("rebalance_plan", out)


if __name__ == "__main__":
    unittest.main()
