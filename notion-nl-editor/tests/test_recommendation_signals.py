import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from services.recommendation.data_prep import TradePoint
from services.recommendation.signals import get_registered_strategy_ids, recommend_by_strategy_kline, recommend_by_strategy_trade
from stores.kline_store import KBar


def _bars(n: int = 30) -> list[KBar]:
    out: list[KBar] = []
    price = 10.0
    for i in range(n):
        price += 0.1
        out.append(
            KBar(
                symbol="600001.SH",
                trade_date=f"2026-01-{i+1:02d}",
                open=price - 0.05,
                high=price + 0.1,
                low=price - 0.1,
                close=price,
                vol=1000.0 + i,
                amount=10000.0 + i,
                adj="raw",
            )
        )
    return out


def _points(n: int = 30) -> list[TradePoint]:
    out: list[TradePoint] = []
    price = 10.0
    for i in range(n):
        price += 0.1
        out.append(
            TradePoint(
                date=f"2026-01-{i+1:02d}",
                price=price,
                shares=100.0,
                direction="BUY" if i % 2 == 0 else "SELL",
                realized=1.0,
                stock_id="S1",
            )
        )
    return out


class RecommendationSignalsTest(unittest.TestCase):
    def test_registry_contains_default_strategies(self) -> None:
        ids = set(get_registered_strategy_ids())
        self.assertIn("baseline", ids)
        self.assertIn("chan", ids)
        self.assertIn("atr_wave", ids)

    def test_contract_trade_and_kline(self) -> None:
        required = {"action", "buy_price", "sell_price", "stop_price", "position_delta", "confidence", "mode", "reason", "sample_count", "strategy_id"}
        for sid in get_registered_strategy_ids():
            trade_rec = recommend_by_strategy_trade(sid, 12.0, 11.0, _points(), True, "LOW", {})
            kline_rec = recommend_by_strategy_kline(sid, 12.0, 11.0, _bars(), True, "LOW", {})
            self.assertTrue(required.issubset(set(trade_rec.keys())))
            self.assertTrue(required.issubset(set(kline_rec.keys())))
            self.assertEqual(trade_rec["strategy_id"], sid.upper())
            self.assertEqual(kline_rec["strategy_id"], sid.upper())
            self.assertIn(trade_rec["confidence"], {"LOW", "MEDIUM", "HIGH"})
            self.assertIn(kline_rec["confidence"], {"LOW", "MEDIUM", "HIGH"})

    def test_extreme_input_missing_price(self) -> None:
        rec = recommend_by_strategy_kline("baseline", None, None, _bars(), True, "LOW", {})
        self.assertEqual(rec["action"], "HOLD")
        self.assertEqual(rec["confidence"], "LOW")


if __name__ == "__main__":
    unittest.main()
