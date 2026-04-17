import argparse
import os
from typing import Any, Dict, List, Optional

from core.notion_props import p_number, p_title
from core.runtime import sqlite_path
from param_store import ParamStore
from services.recommendation.common import parse_strategy_set, prop_text_any, resolve_recommend_data_source, safe_mean, safe_stdev
from services.recommendation.data_prep import TradePoint, market_from_rule
from services.recommendation.engine import ExecutionContext
from services.recommendation.portfolio import is_account_row
from services.recommendation.signals import recommend_by_strategy_kline, recommend_by_strategy_trade
from services.recommendation.types import RecommendationContext
from stores.kline_store import KBar


def _recent_returns_from_points(points: List[TradePoint]) -> List[float]:
    prices = [p.price for p in points if p.price is not None and p.price > 0]
    out: List[float] = []
    for i in range(1, len(prices)):
        prev = prices[i - 1]
        curr = prices[i]
        if prev > 0:
            out.append((curr - prev) / prev)
    return out


def _recent_returns_from_bars(bars: List[KBar]) -> List[float]:
    closes = [b.close for b in bars if b.close > 0]
    out: List[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        curr = closes[i]
        if prev > 0:
            out.append((curr - prev) / prev)
    return out


def _risk_regime(returns: List[float]) -> str:
    if not returns:
        return "UNKNOWN"
    vol = safe_stdev(returns[-20:])
    trend = safe_mean(returns[-20:])
    if vol >= 0.05:
        return "HIGH_VOL"
    if abs(trend) <= 0.003:
        return "RANGE"
    return "TREND"


def _reliability_score(rec: Dict[str, Any], returns: List[float], regime: str) -> float:
    conf_score = {"LOW": 0.35, "MEDIUM": 0.65, "HIGH": 0.85}.get(str(rec.get("confidence", "")).upper(), 0.35)
    sample_count = int(rec.get("sample_count", 0) or 0)
    sample_score = min(1.0, sample_count / 40.0)
    vol = safe_stdev(returns[-20:]) if returns else 0.0
    vol_score = max(0.0, 1.0 - vol / 0.08)
    regime_penalty = 0.08 if regime == "HIGH_VOL" else 0.0
    score = conf_score * 0.5 + sample_score * 0.3 + vol_score * 0.2 - regime_penalty
    return round(max(0.0, min(1.0, score)), 6)


def _enrich_recommendation(rec: Dict[str, Any], returns: List[float]) -> None:
    regime = _risk_regime(returns)
    reliability = _reliability_score(rec, returns, regime)
    signal_strength = round(min(1.0, abs(float(rec.get("position_delta", 0.0) or 0.0)) / 0.15), 6)
    conf = str(rec.get("confidence", "LOW")).upper()
    conf_margin = {"LOW": 0.2, "MEDIUM": 0.12, "HIGH": 0.08}.get(conf, 0.2)
    confidence_center = {"LOW": 0.45, "MEDIUM": 0.62, "HIGH": 0.78}.get(conf, 0.45)
    confidence_interval = {
        "lower": round(max(0.0, confidence_center - conf_margin), 6),
        "upper": round(min(1.0, confidence_center + conf_margin), 6),
    }
    degrade = reliability < 0.45 or conf == "LOW" or "fallback" in str(rec.get("reason", "")).lower()
    execution_feasibility = "OBSERVE_ONLY" if degrade else ("CAUTION" if regime == "HIGH_VOL" else "TRADEABLE")

    action = str(rec.get("action", "HOLD")).upper()
    base = abs(float(rec.get("position_delta", 0.0) or 0.0))
    band = {"min": 0.0, "max": 0.0}
    if action == "BUY":
        band = {"min": round(base * 0.6, 6), "max": round(base, 6)}
    elif action == "SELL":
        band = {"min": round(-base, 6), "max": round(-base * 0.6, 6)}

    rec["signal_strength"] = signal_strength
    rec["confidence_interval"] = confidence_interval
    rec["risk_regime"] = regime
    rec["reliability_score"] = reliability
    rec["degradation_flag"] = bool(degrade)
    rec["execution_feasibility"] = execution_feasibility
    rec["suggest_position_band"] = band


def collect_recommendations(
    stock_rows: List[Dict[str, Any]],
    stock_fields: Dict[str, Optional[str]],
    stock_points: Dict[str, List[TradePoint]],
    stock_kbars: Dict[str, List[KBar]],
    stock_symbols: Dict[str, str],
    args: argparse.Namespace,
) -> List[Dict[str, Any]]:
    selected_strategies = parse_strategy_set(getattr(args, "strategy_set", "baseline,chan,atr_wave"))
    recs: List[Dict[str, Any]] = []
    market_rule = os.getenv("SNAPSHOT_MARKET_RULE", "")
    override_market = (getattr(args, "param_market", "") or "").strip().upper()
    scope = (getattr(args, "param_scope", "") or "*").strip() or "*"
    data_source = resolve_recommend_data_source(args)
    store = ParamStore(sqlite_path())
    ctx = ExecutionContext(param_cache={})
    try:
        for row in stock_rows:
            if is_account_row(row, stock_fields):
                continue
            stock_id = row.get("id", "")
            title = p_title(row, stock_fields["title"]) if stock_fields["title"] else stock_id
            code_raw = prop_text_any(row, stock_fields["stock_code"]) if stock_fields.get("stock_code") else ""
            symbol = stock_symbols.get(stock_id, "")
            current_price = p_number(row, stock_fields["current_price"]) if stock_fields["current_price"] else None
            current_cost = p_number(row, stock_fields["current_cost"]) if stock_fields["current_cost"] else None
            kbars = stock_kbars.get(stock_id, [])
            stock_returns = _recent_returns_from_points(stock_points.get(stock_id, [])) if data_source == "trade" else _recent_returns_from_bars(kbars)
            if data_source == "kline" and current_price is None and kbars:
                current_price = kbars[-1].close
            market_code = code_raw or symbol
            market = override_market or market_from_rule(market_code, market_rule)

            for sid in selected_strategies:
                key = (sid.upper(), market.upper(), scope)
                cache_key = f"{key[0]}|{key[1]}|{key[2]}"
                active = ctx.get_cached_param(cache_key)
                if active is None:
                    active = store.get_active_param_set(strategy_id=sid, market=market, symbol_scope=scope)
                    ctx.put_cached_param(cache_key, active)
                param_cfg = active.get("params", {})
                if data_source == "trade":
                    rec = recommend_by_strategy_trade(
                        strategy_id=sid,
                        current_price=current_price,
                        current_cost=current_cost,
                        points=stock_points.get(stock_id, []),
                        allow_small_sample=bool(param_cfg.get("allow_small_sample", args.allow_small_sample)),
                        min_confidence=str(param_cfg.get("min_confidence", args.min_confidence)),
                        param_cfg=param_cfg,
                    )
                else:
                    rec = recommend_by_strategy_kline(
                        strategy_id=sid,
                        current_price=current_price,
                        current_cost=current_cost,
                        bars=kbars,
                        allow_small_sample=bool(param_cfg.get("allow_small_sample", args.allow_small_sample)),
                        min_confidence=str(param_cfg.get("min_confidence", args.min_confidence)),
                        param_cfg=param_cfg,
                    )
                rec["stock_id"] = stock_id
                rec["stock_name"] = title
                rec["stock_code"] = code_raw
                rec["symbol"] = symbol
                rec["market"] = market
                rec["data_source"] = data_source
                rec["param_version"] = active.get("version", 0)
                rec["param_snapshot"] = param_cfg
                _enrich_recommendation(rec, stock_returns)
                recs.append(rec)
    finally:
        store.close()
    return recs


def collect_recommendations_from_context(ctx: RecommendationContext, args: argparse.Namespace) -> List[Dict[str, Any]]:
    return collect_recommendations(
        stock_rows=ctx.stock_rows,
        stock_fields=ctx.stock_fields,
        stock_points=ctx.stock_points,
        stock_kbars=ctx.stock_kbars,
        stock_symbols=ctx.stock_symbols,
        args=args,
    )
