import os
import sys
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from services.recommendation.backtest_runner import _apply_trade_constraints
from services.recommendation.types import RecommendationRequest


class BacktestConstraintsTest(unittest.TestCase):
    def test_constraints_block_extreme_move(self) -> None:
        req = RecommendationRequest(limit_move_pct=0.098, halt_move_pct=0.18, min_trade_lot=100, cost_bps=3.0, slippage_bps=2.0)
        rec = {"position_delta": 0.2}
        blocked = _apply_trade_constraints(raw_ret=0.2, action="BUY", rec=rec, current_price=10.0, req=req)
        self.assertEqual(blocked, 0.0)

    def test_constraints_apply_friction(self) -> None:
        req = RecommendationRequest(limit_move_pct=0.2, halt_move_pct=0.3, min_trade_lot=100, cost_bps=10.0, slippage_bps=5.0)
        rec = {"position_delta": 0.2}
        out = _apply_trade_constraints(raw_ret=0.03, action="BUY", rec=rec, current_price=10.0, req=req)
        self.assertLess(out, 0.03)
        self.assertGreater(out, 0.0)


if __name__ == "__main__":
    unittest.main()
