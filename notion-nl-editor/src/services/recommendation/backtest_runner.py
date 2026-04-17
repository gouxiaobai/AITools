import argparse
import json
import math
import os
import random
from collections import defaultdict
from typing import Any, Dict, List

from core.config import Cfg
from core.notion_client import NotionClient
from core.runtime import sqlite_path
from param_store import ParamStore
from services.recommendation.common import parse_strategy_set, prop_text_any, resolve_recommend_data_source, safe_mean, safe_stdev
from services.recommendation.context import load_kbars_for_stocks
from services.recommendation.data_prep import build_trade_points, market_from_rule, resolve_stock_fields_runtime, resolve_trade_fields
from services.recommendation.engine import ExecutionContext
from services.recommendation.signals import recommend_by_strategy_kline, recommend_by_strategy_trade
from services.recommendation.types import RecommendationRequest


def _max_drawdown(returns: List[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for value in returns:
        equity *= (1.0 + value)
        peak = max(peak, equity)
        drawdown = (peak - equity) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, drawdown)
    return max_dd


def _returns_metrics(returns: List[float]) -> Dict[str, float]:
    if not returns:
        return {"count": 0.0, "mean": 0.0, "vol": 0.0, "sharpe_like": 0.0, "max_drawdown": 0.0}
    mean_ret = safe_mean(returns)
    vol = safe_stdev(returns)
    sharpe_like = (mean_ret / vol * math.sqrt(len(returns))) if vol > 0 else 0.0
    return {
        "count": float(len(returns)),
        "mean": mean_ret,
        "vol": vol,
        "sharpe_like": sharpe_like,
        "max_drawdown": _max_drawdown(returns),
    }


def _trade_move_with_delay(values: List[float], idx: int, delay_days: int) -> float:
    base_idx = idx + max(0, delay_days)
    nxt_idx = base_idx + 1
    if base_idx < 0 or nxt_idx >= len(values):
        return 0.0
    prev = values[base_idx]
    curr = values[nxt_idx]
    if prev <= 0:
        return 0.0
    return (curr - prev) / prev


def _execution_feasible(
    move: float,
    action: str,
    position_delta: float,
    current_price: float,
    req: RecommendationRequest,
) -> bool:
    if abs(move) >= max(0.0, float(req.halt_move_pct)):
        return False
    limit_pct = max(0.0, float(req.limit_move_pct))
    if action == "BUY" and move >= limit_pct:
        return False
    if action == "SELL" and move <= -limit_pct:
        return False
    min_lot = max(1, int(req.min_trade_lot))
    target_notional = abs(float(position_delta) * 10000.0)
    if current_price <= 0:
        return False
    shares = int(target_notional / current_price)
    if shares < min_lot:
        return False
    return True


def _apply_trade_constraints(raw_ret: float, action: str, rec: Dict[str, Any], current_price: float, req: RecommendationRequest) -> float:
    if action not in {"BUY", "SELL"}:
        return 0.0
    if not _execution_feasible(raw_ret, action, float(rec.get("position_delta", 0.0) or 0.0), current_price, req):
        return 0.0
    signed = raw_ret if action == "BUY" else -raw_ret
    friction = (float(req.cost_bps) + float(req.slippage_bps)) / 10000.0
    if signed > 0:
        return max(0.0, signed - friction)
    return signed - friction


def _benchmark_payload(returns: List[float], rng_seed: str) -> Dict[str, Dict[str, float]]:
    if not returns:
        empty = _returns_metrics([])
        return {"index_proxy": empty, "momentum": empty, "random": empty}
    index_proxy = list(returns)
    momentum: List[float] = []
    for i in range(1, len(returns)):
        prev = returns[i - 1]
        curr = returns[i]
        momentum.append(curr if prev >= 0 else -curr)
    rnd = random.Random(rng_seed)
    random_ret = [r if rnd.random() >= 0.5 else -r for r in returns]
    return {
        "index_proxy": _returns_metrics(index_proxy),
        "momentum": _returns_metrics(momentum),
        "random": _returns_metrics(random_ret),
    }


class BacktestRunner:
    def __init__(self, client: NotionClient, cfg: Cfg) -> None:
        self.client = client
        self.cfg = cfg

    def backtest_recommendation(self, req: RecommendationRequest, args: argparse.Namespace) -> int:
        if resolve_recommend_data_source(args) == "trade":
            return self._backtest_recommendation_trade(req)

        stock_db = self.client.get_database(self.cfg.stock_master_id)
        stock_fields = resolve_stock_fields_runtime(stock_db)
        stock_rows = self.client.query_database_all(self.cfg.stock_master_id)
        stock_kbars, stock_symbols = load_kbars_for_stocks(stock_rows, stock_fields, args)
        selected_strategies = parse_strategy_set(req.strategy_set)
        mode_returns: Dict[str, List[float]] = defaultdict(list)
        strategy_returns: Dict[str, List[float]] = defaultdict(list)
        strategy_mode_returns: Dict[str, List[float]] = defaultdict(list)
        strategy_hold_counts: Dict[str, int] = defaultdict(int)
        strategy_total_counts: Dict[str, int] = defaultdict(int)
        baseline_returns: List[float] = []
        all_strategy_returns: List[float] = []
        override_market = (req.param_market or "").upper()
        market_rule = os.getenv("SNAPSHOT_MARKET_RULE", "")
        scope = (req.param_scope or "*").strip() or "*"
        store = ParamStore(sqlite_path())
        ctx = ExecutionContext(param_cache={})

        row_by_id = {r.get("id", ""): r for r in stock_rows}
        try:
            for stock_id, bars in stock_kbars.items():
                valid = [bar for bar in bars if bar.close > 0]
                if len(valid) < 8:
                    continue
                close_values = [bar.close for bar in valid]
                row = row_by_id.get(stock_id, {})
                code_raw = prop_text_any(row, stock_fields["stock_code"]) if row else ""
                market = override_market or market_from_rule(code_raw or stock_symbols.get(stock_id, ""), market_rule)
                for idx in range(5, len(valid) - 1 - max(0, int(req.execution_delay_days))):
                    hist = valid[max(0, idx - req.window) : idx]
                    curr = valid[idx]
                    if curr.close <= 0:
                        continue
                    move = _trade_move_with_delay(close_values, idx, int(req.execution_delay_days))
                    baseline_returns.append(move)
                    for sid in selected_strategies:
                        sid_upper = sid.upper()
                        cache_key = f"{sid_upper}|{market}|{scope}"
                        active = ctx.get_cached_param(cache_key)
                        if active is None:
                            active = store.get_active_param_set(strategy_id=sid_upper, market=market, symbol_scope=scope)
                            ctx.put_cached_param(cache_key, active)
                        p_cfg = active.get("params", {})
                        rec = recommend_by_strategy_kline(
                            strategy_id=sid,
                            current_price=curr.close,
                            current_cost=None,
                            bars=hist,
                            allow_small_sample=bool(p_cfg.get("allow_small_sample", req.allow_small_sample)),
                            min_confidence=str(p_cfg.get("min_confidence", req.min_confidence)),
                            param_cfg=p_cfg,
                        )
                        strategy_ret = _apply_trade_constraints(
                            raw_ret=move,
                            action=str(rec["action"]).upper(),
                            rec=rec,
                            current_price=curr.close,
                            req=req,
                        )
                        sid_upper = rec["strategy_id"]
                        strategy_total_counts[sid_upper] += 1
                        if rec["action"] == "HOLD":
                            strategy_hold_counts[sid_upper] += 1
                        strategy_returns[sid_upper].append(strategy_ret)
                        strategy_mode_returns[f"{sid_upper}:{rec['mode']}"].append(strategy_ret)
                        mode_returns[rec["mode"]].append(strategy_ret)
                        all_strategy_returns.append(strategy_ret)
            strategy_metrics: Dict[str, Dict[str, float]] = {}
            for sid, arr in strategy_returns.items():
                metrics = _returns_metrics(arr)
                total = max(strategy_total_counts.get(sid, 0), 1)
                metrics["hold_ratio"] = strategy_hold_counts.get(sid, 0) / total
                strategy_metrics[sid] = metrics
        finally:
            store.close()

        out = {
            "baseline": _returns_metrics(baseline_returns),
            "strategy_all": _returns_metrics(all_strategy_returns),
            "strategy_by_mode": {k: _returns_metrics(v) for k, v in mode_returns.items()},
            "strategy_metrics": strategy_metrics,
            "strategy_mode_metrics": {k: _returns_metrics(v) for k, v in strategy_mode_returns.items()},
            "benchmarks": _benchmark_payload(baseline_returns, rng_seed=f"{market_rule}|{scope}|kline"),
            "execution_constraints": {
                "delay_days": int(req.execution_delay_days),
                "cost_bps": float(req.cost_bps),
                "slippage_bps": float(req.slippage_bps),
                "min_trade_lot": int(req.min_trade_lot),
                "limit_move_pct": float(req.limit_move_pct),
                "halt_move_pct": float(req.halt_move_pct),
            },
            "param_versions": {k: int(v.get("version", 0)) for k, v in ctx.param_cache.items()},
            "param_market": override_market or "AUTO",
            "data_source": "kline",
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    def _backtest_recommendation_trade(self, req: RecommendationRequest) -> int:
        trade_db = self.client.get_database(self.cfg.std_trades_id)
        trade_fields = resolve_trade_fields(trade_db)
        for key in ["date", "direction", "shares", "price", "stock"]:
            if not trade_fields[key]:
                raise RuntimeError(f"Missing required standard-trade field mapping: {key}")
        trade_rows = self.client.query_database_all(self.cfg.std_trades_id)
        stock_points = build_trade_points(trade_rows, trade_fields)

        selected_strategies = parse_strategy_set(req.strategy_set)
        mode_returns: Dict[str, List[float]] = defaultdict(list)
        strategy_returns: Dict[str, List[float]] = defaultdict(list)
        strategy_mode_returns: Dict[str, List[float]] = defaultdict(list)
        strategy_hold_counts: Dict[str, int] = defaultdict(int)
        strategy_total_counts: Dict[str, int] = defaultdict(int)
        baseline_returns: List[float] = []
        all_strategy_returns: List[float] = []
        market = (req.param_market or "SH").upper()
        scope = (req.param_scope or "*").strip() or "*"
        store = ParamStore(sqlite_path())
        ctx = ExecutionContext(param_cache={})

        try:
            for points in stock_points.values():
                valid = [point for point in points if point.price is not None and point.price > 0]
                if len(valid) < 8:
                    continue
                price_values = [float(point.price or 0.0) for point in valid]
                for idx in range(5, len(valid) - 1 - max(0, int(req.execution_delay_days))):
                    hist = valid[max(0, idx - req.window) : idx]
                    curr = valid[idx]
                    if not curr.price:
                        continue
                    move = _trade_move_with_delay(price_values, idx, int(req.execution_delay_days))
                    baseline_ret = move if curr.direction == "BUY" else (-move if curr.direction == "SELL" else 0.0)
                    baseline_returns.append(baseline_ret)
                    for sid in selected_strategies:
                        sid_upper = sid.upper()
                        active = ctx.get_cached_param(sid_upper)
                        if active is None:
                            active = store.get_active_param_set(strategy_id=sid_upper, market=market, symbol_scope=scope)
                            ctx.put_cached_param(sid_upper, active)
                        p_cfg = active.get("params", {})
                        rec = recommend_by_strategy_trade(
                            strategy_id=sid,
                            current_price=curr.price,
                            current_cost=None,
                            points=hist,
                            allow_small_sample=bool(p_cfg.get("allow_small_sample", req.allow_small_sample)),
                            min_confidence=str(p_cfg.get("min_confidence", req.min_confidence)),
                            param_cfg=p_cfg,
                        )
                        strategy_ret = _apply_trade_constraints(
                            raw_ret=move,
                            action=str(rec["action"]).upper(),
                            rec=rec,
                            current_price=float(curr.price or 0.0),
                            req=req,
                        )
                        sid_upper = rec["strategy_id"]
                        strategy_total_counts[sid_upper] += 1
                        if rec["action"] == "HOLD":
                            strategy_hold_counts[sid_upper] += 1
                        strategy_returns[sid_upper].append(strategy_ret)
                        strategy_mode_returns[f"{sid_upper}:{rec['mode']}"].append(strategy_ret)
                        mode_returns[rec["mode"]].append(strategy_ret)
                        all_strategy_returns.append(strategy_ret)
            strategy_metrics: Dict[str, Dict[str, float]] = {}
            for sid, arr in strategy_returns.items():
                metrics = _returns_metrics(arr)
                total = max(strategy_total_counts.get(sid, 0), 1)
                metrics["hold_ratio"] = strategy_hold_counts.get(sid, 0) / total
                strategy_metrics[sid] = metrics
        finally:
            store.close()

        out = {
            "baseline": _returns_metrics(baseline_returns),
            "strategy_all": _returns_metrics(all_strategy_returns),
            "strategy_by_mode": {k: _returns_metrics(v) for k, v in mode_returns.items()},
            "strategy_metrics": strategy_metrics,
            "strategy_mode_metrics": {k: _returns_metrics(v) for k, v in strategy_mode_returns.items()},
            "benchmarks": _benchmark_payload(baseline_returns, rng_seed=f"{market}|{scope}|trade"),
            "execution_constraints": {
                "delay_days": int(req.execution_delay_days),
                "cost_bps": float(req.cost_bps),
                "slippage_bps": float(req.slippage_bps),
                "min_trade_lot": int(req.min_trade_lot),
                "limit_move_pct": float(req.limit_move_pct),
                "halt_move_pct": float(req.halt_move_pct),
            },
            "param_versions": {k: int(v.get("version", 0)) for k, v in ctx.param_cache.items()},
            "param_market": market,
            "data_source": "trade",
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0
