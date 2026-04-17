import argparse
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from core.config import Cfg
from services.recommendation.collector import collect_recommendations_from_context
from services.recommendation.context import prepare_recommendation_context_obj
from services.recommendation.data_prep import TradePoint
from services.recommendation.snapshot_writer import build_stock_prices, emit_snapshot
from services.recommendation.types import RecommendationContext


class _FakeClient:
    def __init__(self, stock_db: dict, stock_rows: list[dict]) -> None:
        self._stock_db = stock_db
        self._stock_rows = stock_rows

    def get_database(self, database_id: str) -> dict:
        _ = database_id
        return self._stock_db

    def query_database_all(self, database_id: str) -> list[dict]:
        _ = database_id
        return self._stock_rows


class _FakeParamStore:
    call_count = 0

    def __init__(self, db_path: str) -> None:
        _ = db_path

    def get_active_param_set(self, strategy_id: str, market: str, symbol_scope: str) -> dict:
        _ = strategy_id
        _ = market
        _ = symbol_scope
        _FakeParamStore.call_count += 1
        return {"version": 1, "params": {"allow_small_sample": True, "min_confidence": "LOW"}}

    def close(self) -> None:
        return None


class RecommendationChainTest(unittest.TestCase):
    def setUp(self) -> None:
        self._old_sqlite_path = os.environ.get("SQLITE_PATH")
        self.tmp_db = os.path.join(tempfile.gettempdir(), f"reco_chain_{os.getpid()}.db")
        os.environ["SQLITE_PATH"] = self.tmp_db

    def tearDown(self) -> None:
        if self._old_sqlite_path is None:
            os.environ.pop("SQLITE_PATH", None)
        else:
            os.environ["SQLITE_PATH"] = self._old_sqlite_path
        if os.path.exists(self.tmp_db):
            os.remove(self.tmp_db)

    def test_context_collector_snapshot_chain(self) -> None:
        stock_db = {"properties": {"Title": {"type": "title"}, "Code": {"type": "rich_text"}, "Price": {"type": "number"}, "Cost": {"type": "number"}}}
        stock_rows = [
            {
                "id": "S1",
                "properties": {
                    "Title": {"type": "title", "title": [{"plain_text": "StockA"}]},
                    "Code": {"type": "rich_text", "rich_text": [{"plain_text": "600001"}]},
                    "Price": {"type": "number", "number": 12.0},
                    "Cost": {"type": "number", "number": 10.0},
                },
            }
        ]
        cfg = Cfg("stock", "trade", "div", "annual", "buy", "t", "snap", "cash")
        args = argparse.Namespace(data_source="trade", refresh_prices=False, strategy_set="baseline", allow_small_sample=True, min_confidence="LOW", param_market="", param_scope="*")
        points = {"S1": [TradePoint(date="2026-01-01", price=10.0, shares=100.0, direction="BUY", realized=1.0, stock_id="S1") for _ in range(25)]}
        fake_client = _FakeClient(stock_db, stock_rows)

        with patch("services.recommendation.context.resolve_stock_fields_runtime", return_value={"title": "Title", "stock_code": "Code", "current_price": "Price", "current_cost": "Cost"}), patch(
            "services.recommendation.context.load_trade_points_for_recommendation", return_value=points
        ):
            ctx = prepare_recommendation_context_obj(fake_client, cfg, args)
        self.assertIsInstance(ctx, RecommendationContext)
        self.assertEqual(len(ctx.stock_points["S1"]), 25)

        _FakeParamStore.call_count = 0
        with patch("services.recommendation.collector.ParamStore", _FakeParamStore):
            recs = collect_recommendations_from_context(ctx, args)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["strategy_id"], "BASELINE")
        self.assertIn("reliability_score", recs[0])
        self.assertIn("risk_regime", recs[0])
        self.assertIn("degradation_flag", recs[0])
        self.assertIn("execution_feasibility", recs[0])
        self.assertIn("suggest_position_band", recs[0])
        self.assertEqual(_FakeParamStore.call_count, 1)

        stock_prices = build_stock_prices("trade", ctx.stock_points, ctx.stock_kbars)
        out = emit_snapshot(recs=recs, stock_prices=stock_prices, snapshot_date="2026-04-17", dry_run=True)
        self.assertEqual(out["input_rows"], 1)
        self.assertEqual(out["upserted"], 0)


if __name__ == "__main__":
    unittest.main()
