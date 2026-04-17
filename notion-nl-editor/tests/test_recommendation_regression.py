import argparse
import os
import sys
import unittest
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from core.config import Cfg
from services.recommendation.runner import RecommendationRunner
from services.recommendation.snapshot_sync import snapshot_daily


class _ClientForWriteback:
    def __init__(self) -> None:
        self.updated = []

    def update_page(self, page_id: str, props: dict) -> None:
        self.updated.append((page_id, props))


class RecommendationRegressionTest(unittest.TestCase):
    def test_recommend_writeback_dry_run_and_non_dry_run(self) -> None:
        client = _ClientForWriteback()
        runner = RecommendationRunner(client, Cfg("a", "b", "c", "d", "e", "f", "g", "h"))

        stock_rows = [
            {
                "id": "S1",
                "properties": {
                    "Code": {"type": "rich_text", "rich_text": [{"plain_text": "600001"}]},
                    "Title": {"type": "title", "title": [{"plain_text": "StockA"}]},
                },
            }
        ]
        stock_fields = {
            "stock_code": "Code",
            "title": "Title",
            "out_action": "Action",
            "out_buy": "Buy",
            "out_sell": "Sell",
            "out_stop": "Stop",
            "out_pos": "Pos",
            "out_buy_shares": "BuyShares",
            "out_sell_shares": "SellShares",
            "out_holding_shares": "HoldingShares",
            "out_market_value": "MarketValue",
            "out_unrealized_pnl": "Unrealized",
            "out_conf": "Conf",
            "out_mode": "Mode",
            "out_reason": "Reason",
            "out_time": "Time",
        }
        db_props = {k: {"type": "number"} for k in ["Buy", "Sell", "Stop", "Pos", "BuyShares", "SellShares", "HoldingShares", "MarketValue", "Unrealized"]}
        db_props.update(
            {
                "Action": {"type": "select"},
                "Conf": {"type": "select"},
                "Mode": {"type": "select"},
                "Reason": {"type": "rich_text"},
                "Time": {"type": "date"},
            }
        )
        recs = [
            {
                "stock_id": "S1",
                "strategy_id": "BASELINE",
                "action": "BUY",
                "buy_price": 10.0,
                "sell_price": 11.0,
                "stop_price": 9.5,
                "position_delta": 0.1,
                "confidence": "HIGH",
                "mode": "FULL_MODEL",
                "reason": "ok",
            }
        ]

        runner._write_back_recommendations(stock_rows, stock_fields, db_props, recs, "2026-04-17", dry_run=True)
        self.assertEqual(len(client.updated), 0)

        runner._write_back_recommendations(stock_rows, stock_fields, db_props, recs, "2026-04-17", dry_run=False)
        self.assertEqual(len(client.updated), 1)

    def test_snapshot_daily_dry_run_and_non_dry_run(self) -> None:
        cfg = Cfg("a", "b", "c", "d", "e", "f", "g", "h")
        args = argparse.Namespace(
            data_source="kline",
            strategy_set="baseline",
            allow_small_sample=True,
            min_confidence="LOW",
            param_market="",
            param_scope="*",
            snapshot_date="2026-04-17",
            dry_run=True,
        )
        ctx_tuple = (
            {},
            {},
            [],
            {"S1": []},
            {"S1": [type("Bar", (), {"close": 10.0})(), type("Bar", (), {"close": 10.5})()]},
            {},
        )
        recs = [
            {
                "stock_id": "S1",
                "stock_code": "600001",
                "stock_name": "A",
                "strategy_id": "BASELINE",
                "mode": "FULL_MODEL",
                "action": "BUY",
                "confidence": "HIGH",
                "sample_count": 20,
                "buy_price": 10.0,
                "sell_price": 11.0,
                "stop_price": 9.5,
                "position_delta": 0.1,
            }
        ]
        with patch("services.recommendation.snapshot_sync.prepare_recommendation_context", return_value=ctx_tuple), patch(
            "services.recommendation.snapshot_sync.collect_recommendations", return_value=recs
        ), patch("services.recommendation.snapshot_sync.emit_snapshot", return_value={"dry_run": True}) as mock_emit:
            snapshot_daily(client=object(), cfg=cfg, args=args)
            self.assertTrue(mock_emit.call_args.kwargs["dry_run"])

        args.dry_run = False
        with patch("services.recommendation.snapshot_sync.prepare_recommendation_context", return_value=ctx_tuple), patch(
            "services.recommendation.snapshot_sync.collect_recommendations", return_value=recs
        ), patch("services.recommendation.snapshot_sync.emit_snapshot", return_value={"dry_run": False}) as mock_emit:
            snapshot_daily(client=object(), cfg=cfg, args=args)
            self.assertFalse(mock_emit.call_args.kwargs["dry_run"])


if __name__ == "__main__":
    unittest.main()
