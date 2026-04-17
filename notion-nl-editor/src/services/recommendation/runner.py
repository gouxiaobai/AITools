import argparse
from dataclasses import asdict
import os

from core.config import Cfg
from core.notion_client import NotionClient
from services.recommendation.backtest_runner import BacktestRunner
from services.recommendation.recommend_runner import RecommendRunner
from services.recommendation.types import RecommendationRequest


class RecommendationRunner:
    def __init__(self, client: NotionClient, cfg: Cfg) -> None:
        self._recommend_runner = RecommendRunner(client, cfg)
        self._backtest_runner = BacktestRunner(client, cfg)

    @staticmethod
    def request_from_args(args: argparse.Namespace) -> RecommendationRequest:
        raw = {
            "dry_run": bool(getattr(args, "dry_run", False)),
            "asof_date": str(getattr(args, "asof_date", "") or ""),
            "snapshot_date": str(getattr(args, "snapshot_date", "") or ""),
            "emit_snapshot": bool(getattr(args, "emit_snapshot", False)),
            "strategy_set": str(getattr(args, "strategy_set", "baseline,chan,atr_wave") or "baseline,chan,atr_wave"),
            "allow_small_sample": bool(getattr(args, "allow_small_sample", True)),
            "min_confidence": str(getattr(args, "min_confidence", "MEDIUM") or "MEDIUM"),
            "data_source": str(getattr(args, "data_source", "kline") or "kline"),
            "adj": str(getattr(args, "adj", "raw") or "raw"),
            "start_date": str(getattr(args, "start_date", "") or ""),
            "end_date": str(getattr(args, "end_date", "") or ""),
            "force": bool(getattr(args, "force", False)),
            "refresh_prices": bool(getattr(args, "refresh_prices", False)),
            "timeout": int(getattr(args, "timeout", 8) or 8),
            "param_market": str(getattr(args, "param_market", "") or ""),
            "param_scope": str(getattr(args, "param_scope", "*") or "*"),
            "window": int(getattr(args, "window", 60) or 60),
            "execution_delay_days": int(getattr(args, "execution_delay_days", os.getenv("BACKTEST_EXEC_DELAY_DAYS", 1)) or 1),
            "cost_bps": float(getattr(args, "cost_bps", os.getenv("BACKTEST_COST_BPS", 3.0)) or 3.0),
            "slippage_bps": float(getattr(args, "slippage_bps", os.getenv("BACKTEST_SLIPPAGE_BPS", 2.0)) or 2.0),
            "min_trade_lot": int(getattr(args, "min_trade_lot", os.getenv("BACKTEST_MIN_TRADE_LOT", 100)) or 100),
            "limit_move_pct": float(getattr(args, "limit_move_pct", os.getenv("BACKTEST_LIMIT_MOVE_PCT", 0.098)) or 0.098),
            "halt_move_pct": float(getattr(args, "halt_move_pct", os.getenv("BACKTEST_HALT_MOVE_PCT", 0.18)) or 0.18),
        }
        return RecommendationRequest(**raw)

    @staticmethod
    def to_namespace(req: RecommendationRequest) -> argparse.Namespace:
        return argparse.Namespace(**asdict(req))

    def recommend_prices(self, req: RecommendationRequest) -> int:
        args = self.to_namespace(req)
        return self._recommend_runner.recommend_prices(req, args)

    def backtest_recommendation(self, req: RecommendationRequest) -> int:
        args = self.to_namespace(req)
        return self._backtest_runner.backtest_recommendation(req, args)

    # compatibility for existing tests/callers
    def _write_back_recommendations(self, *args, **kwargs) -> None:
        self._recommend_runner._write_back_recommendations(*args, **kwargs)
