import datetime as dt
import os
from typing import Any, Dict, List
from uuid import uuid4

from core.runtime import sqlite_path
from services.recommendation.common import coerce_text
from services.recommendation.data_prep import market_from_rule
from stores.snapshot_store import SnapshotStore


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


def _series_returns(prices: List[float]) -> List[float]:
    out: List[float] = []
    for i in range(1, len(prices)):
        prev = prices[i - 1]
        curr = prices[i]
        if prev > 0:
            out.append((curr - prev) / prev)
    return out


def _ret_1d(prices: List[float]) -> float:
    if len(prices) < 2:
        return 0.0
    prev = prices[-2]
    curr = prices[-1]
    if prev <= 0:
        return 0.0
    return float((curr - prev) / prev)


def _hit_flag(action: str, ret_1d: float) -> int:
    if action == "BUY":
        return 1 if ret_1d > 0 else 0
    if action == "SELL":
        return 1 if ret_1d < 0 else 0
    return 1 if abs(ret_1d) <= 0.002 else 0


def build_snapshot_rows(
    recs: List[Dict[str, Any]],
    stock_prices: Dict[str, List[float]],
    snapshot_date: str,
    market_rule: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    now = dt.datetime.now().isoformat(timespec="seconds")
    rows: List[Dict[str, Any]] = []
    for rec in recs:
        stock_id = rec.get("stock_id", "")
        prices = stock_prices.get(stock_id, [])
        returns = _series_returns(prices)
        ret_1d = _ret_1d(prices)
        stock_code = coerce_text(rec.get("stock_code", ""))
        row = {
            "snapshot_date": snapshot_date,
            "strategy_id": coerce_text(rec.get("strategy_id", "")).upper(),
            "stock_id": stock_id,
            "stock_code": stock_code,
            "stock_name": coerce_text(rec.get("stock_name", "")),
            "market": market_from_rule(stock_code, market_rule),
            "strategy_mode": coerce_text(rec.get("mode", "")),
            "ret_1d": float(ret_1d),
            "hit_flag": int(_hit_flag(coerce_text(rec.get("action", "")), ret_1d)),
            "max_drawdown": float(_max_drawdown(returns)),
            "confidence": coerce_text(rec.get("confidence", "")),
            "sample_count": int(rec.get("sample_count", 0) or 0),
            "action": coerce_text(rec.get("action", "")),
            "buy_price": float(rec["buy_price"]) if rec.get("buy_price") is not None else None,
            "sell_price": float(rec["sell_price"]) if rec.get("sell_price") is not None else None,
            "stop_price": float(rec["stop_price"]) if rec.get("stop_price") is not None else None,
            "position_delta": float(rec.get("position_delta", 0.0) or 0.0),
            "run_id": run_id,
            "created_at": now,
            "updated_at": now,
        }
        if row["snapshot_date"] and row["strategy_id"] and row["stock_id"]:
            rows.append(row)
    return rows


def build_stock_prices(
    data_source: str,
    stock_points: Dict[str, List[Any]],
    stock_kbars: Dict[str, List[Any]],
) -> Dict[str, List[float]]:
    prices: Dict[str, List[float]] = {}
    if data_source == "trade":
        for sid, points in stock_points.items():
            prices[sid] = [point.price for point in points if point.price is not None and point.price > 0]
    else:
        for sid, bars in stock_kbars.items():
            prices[sid] = [bar.close for bar in bars if bar.close > 0]
    return prices


def emit_snapshot(recs: List[Dict[str, Any]], stock_prices: Dict[str, List[float]], snapshot_date: str, dry_run: bool) -> Dict[str, Any]:
    run_id = uuid4().hex[:12]
    rows = build_snapshot_rows(
        recs=recs,
        stock_prices=stock_prices,
        snapshot_date=snapshot_date,
        market_rule=os.getenv("SNAPSHOT_MARKET_RULE", ""),
        run_id=run_id,
    )
    db_path = sqlite_path()
    written = 0
    if not dry_run:
        store = SnapshotStore(db_path)
        try:
            written = store.upsert_many(rows)
        finally:
            store.close()
    return {
        "snapshot_date": snapshot_date,
        "sqlite_path": db_path,
        "run_id": run_id,
        "input_rows": len(rows),
        "upserted": written,
        "dry_run": bool(dry_run),
    }
