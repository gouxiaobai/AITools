import argparse
import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from core.runtime import split_csv, sqlite_path, today_or
from services.recommendation.market_sync import KlineProvider
from services.research.experiments import build_history_payload
from services.selection.selector import SnapshotSlice, score_snapshot_slice
from stores.kline_store import KlineStore
from stores.snapshot_store import SnapshotStore


def _to_ts_code(raw_code: str) -> Optional[str]:
    code = (raw_code or "").strip().upper()
    if not code:
        return None
    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) == 6:
        suffix = "SH" if digits.startswith(("5", "6", "9")) else "SZ"
        return f"{digits}.{suffix}"
    if "." in code:
        left, right = code.split(".", 1)
        if left.isdigit() and len(left) == 6 and right in {"SH", "SZ", "SS", "BJ"}:
            mapped = "SH" if right == "SS" else right
            return f"{left}.{mapped}"
    if code.startswith(("SH", "SZ", "BJ")) and len(code) == 8 and code[2:].isdigit():
        return f"{code[2:]}.{code[:2]}"
    return None


def _market_from_ts_code(ts_code: str) -> str:
    code = (ts_code or "").upper()
    if code.endswith(".SH"):
        return "SH"
    if code.endswith(".SZ"):
        return "SZ"
    if code.endswith(".BJ"):
        return "BJ"
    return ""


def _stock_code_from_ts(ts_code: str) -> str:
    code = (ts_code or "").upper()
    if "." in code:
        left, _ = code.split(".", 1)
        if left.isdigit() and len(left) == 6:
            return left
    return ""


def _normalize_stock_code(raw: str) -> str:
    digits = "".join(ch for ch in (raw or "") if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else digits


def _default_kline_start(end_date: str, lookback_days: int = 180) -> str:
    end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    return (end_dt - dt.timedelta(days=max(lookback_days, 30))).isoformat()


def _series_returns(prices: List[float]) -> List[float]:
    out: List[float] = []
    for idx in range(1, len(prices)):
        prev = prices[idx - 1]
        curr = prices[idx]
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
    return (curr - prev) / prev


def _max_drawdown(returns: List[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for val in returns:
        equity *= (1.0 + val)
        peak = max(peak, equity)
        dd = (peak - equity) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd


def _manual_filter_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        code = _normalize_stock_code(str(row.get("stock_code", "")))
        if not code:
            continue
        out[code] = {
            "decision": str(row.get("decision", "")).lower(),
            "reason": str(row.get("reason", "")),
            "operator": str(row.get("operator", "")),
        }
    return out


def _build_selection_from_candidates(candidates: List[Dict[str, Any]], top_n: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    selected = candidates[: max(1, int(top_n))]
    score_sum = sum(max(0.0, float(item.get("score", 0.0) or 0.0)) for item in selected)
    plan: List[Dict[str, Any]] = []
    for row in selected:
        score = max(0.0, float(row.get("score", 0.0) or 0.0))
        target_weight = (score / score_sum) if score_sum > 0 else (1.0 / float(len(selected) or 1))
        plan.append(
            {
                "stock_id": row.get("stock_id", ""),
                "stock_code": row.get("stock_code", ""),
                "stock_name": row.get("stock_name", ""),
                "target_weight": round(target_weight, 6),
                "score": round(float(row.get("score", 0.0) or 0.0), 6),
            }
        )
    return selected, plan


def history_query(args: argparse.Namespace) -> int:
    start_date = today_or(getattr(args, "start_date", ""))
    end_date = today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else start_date
    strategies = [x.upper() for x in split_csv(getattr(args, "strategies", ""))]
    markets = [x.upper() for x in split_csv(getattr(args, "markets", ""))]
    store = SnapshotStore(sqlite_path())
    try:
        rows = store.query_range(start_date, end_date, strategies=strategies or None, markets=markets or None)
    finally:
        store.close()
    print(json.dumps(build_history_payload(rows), ensure_ascii=False, indent=2))
    return 0


def sync_market_universe(args: argparse.Namespace) -> int:
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing TUSHARE_TOKEN for market universe sync.")
    list_status = str(getattr(args, "list_status", "L") or "L").upper()
    markets = [x.upper() for x in split_csv(getattr(args, "markets", "SH,SZ,BJ"))]
    now = dt.datetime.now().isoformat(timespec="seconds")

    kstore = KlineStore(sqlite_path())
    provider = KlineProvider(token=token, store=kstore)
    try:
        rows = provider.fetch_stock_basic(list_status=list_status)
    finally:
        kstore.close()

    prepared: List[Dict[str, Any]] = []
    for row in rows:
        ts_code = str(row.get("ts_code", "")).upper()
        market = _market_from_ts_code(ts_code)
        if markets and market not in markets:
            continue
        stock_code = _stock_code_from_ts(ts_code)
        if not stock_code:
            continue
        status = str(row.get("list_status", "")).upper()
        prepared.append(
            {
                "ts_code": ts_code,
                "stock_code": stock_code,
                "stock_name": str(row.get("name", "") or ""),
                "market": market,
                "list_status": status,
                "list_date": str(row.get("list_date", "") or ""),
                "delist_date": str(row.get("delist_date", "") or ""),
                "is_active": 1 if status == "L" else 0,
                "updated_at": now,
            }
        )

    store = SnapshotStore(sqlite_path())
    try:
        upserted = store.upsert_market_universe(prepared)
    finally:
        store.close()

    out = {
        "list_status": list_status,
        "market_filter": markets,
        "fetched": len(rows),
        "upserted": upserted,
        "sqlite_path": sqlite_path(),
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def snapshot_market_daily(args: argparse.Namespace) -> int:
    snapshot_date = today_or(getattr(args, "snapshot_date", ""))
    end_date = today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else snapshot_date
    start_date = today_or(getattr(args, "start_date", "")) if getattr(args, "start_date", "") else _default_kline_start(end_date)
    markets = [x.upper() for x in split_csv(getattr(args, "markets", "SH,SZ,BJ"))]
    strategy_id = str(getattr(args, "strategy_id", "MARKET_SCAN") or "MARKET_SCAN").upper()
    min_bars = max(20, int(getattr(args, "min_bars", 60) or 60))
    adj = str(getattr(args, "adj", os.getenv("KLINE_DEFAULT_ADJ", "raw")) or "raw").strip().lower()
    if adj not in {"raw", "qfq", "hfq"}:
        raise RuntimeError("adj must be one of raw/qfq/hfq")

    store = SnapshotStore(sqlite_path())
    try:
        universe = store.query_market_universe(markets=markets or None, active_only=not bool(getattr(args, "include_inactive", False)))
    finally:
        store.close()
    if not universe:
        raise RuntimeError("No market universe found. Run sync-market-universe first.")

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    allow_sync_missing = bool(getattr(args, "sync_missing", True))
    if allow_sync_missing and not token:
        raise RuntimeError("Missing TUSHARE_TOKEN for syncing missing K-line data.")

    kstore = KlineStore(sqlite_path())
    provider = KlineProvider(token=token, store=kstore) if allow_sync_missing else None
    run_id = uuid4().hex[:12]
    now = dt.datetime.now().isoformat(timespec="seconds")
    rows: List[Dict[str, Any]] = []
    synced_missing = 0
    try:
        for item in universe:
            ts_code = str(item.get("ts_code", "")).upper()
            stock_code = str(item.get("stock_code", "")).upper()
            stock_name = str(item.get("stock_name", "") or stock_code)
            market = str(item.get("market", "")).upper()
            bars = kstore.query_bars(ts_code, start_date=start_date, end_date=end_date, adj=adj)
            if len(bars) < min_bars and provider is not None:
                provider.sync_symbol(ts_code=ts_code, start_date=start_date, end_date=end_date, adj=adj, force=False)
                bars = kstore.query_bars(ts_code, start_date=start_date, end_date=end_date, adj=adj)
                synced_missing += 1
            if len(bars) < min_bars:
                continue
            prices = [bar.close for bar in bars if bar.close > 0]
            if len(prices) < min_bars:
                continue
            returns = _series_returns(prices)
            ret_1d = _ret_1d(prices)
            momentum_5 = sum(returns[-5:]) / float(min(len(returns), 5) or 1)
            drawdown = _max_drawdown(returns)
            action = "BUY" if momentum_5 > 0 else "HOLD"
            confidence = "HIGH" if momentum_5 > 0.015 else ("MEDIUM" if momentum_5 > 0.004 else "LOW")
            rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "strategy_id": strategy_id,
                    "stock_id": ts_code,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "market": market,
                    "strategy_mode": "MARKET_SCAN",
                    "ret_1d": float(ret_1d),
                    "hit_flag": 1 if ret_1d > 0 else 0,
                    "max_drawdown": float(drawdown),
                    "confidence": confidence,
                    "sample_count": len(returns),
                    "action": action,
                    "buy_price": float(prices[-1]) if prices else None,
                    "sell_price": None,
                    "stop_price": None,
                    "position_delta": float(max(0.0, min(momentum_5 * 5.0, 0.2))),
                    "run_id": run_id,
                    "created_at": now,
                    "updated_at": now,
                }
            )
    finally:
        kstore.close()

    upserted = 0
    if not bool(getattr(args, "dry_run", False)):
        store = SnapshotStore(sqlite_path())
        try:
            upserted = store.upsert_many(rows)
        finally:
            store.close()

    out = {
        "snapshot_date": snapshot_date,
        "strategy_id": strategy_id,
        "market_filter": markets,
        "universe_total": len(universe),
        "qualified_rows": len(rows),
        "upserted": upserted,
        "synced_missing_symbols": synced_missing,
        "start_date": start_date,
        "end_date": end_date,
        "adj": adj,
        "min_bars": min_bars,
        "dry_run": bool(getattr(args, "dry_run", False)),
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def manual_filter_set(args: argparse.Namespace) -> int:
    stock_code = _normalize_stock_code(str(getattr(args, "stock_code", "")))
    if len(stock_code) != 6:
        raise RuntimeError("--stock-code must resolve to a 6-digit code.")
    decision = str(getattr(args, "decision", "") or "").strip().lower()
    operator = str(getattr(args, "operator", "") or os.getenv("OPERATOR", "local_user")).strip() or "local_user"
    reason = str(getattr(args, "reason", "") or "").strip()
    now = dt.datetime.now().isoformat(timespec="seconds")

    store = SnapshotStore(sqlite_path())
    try:
        if decision == "clear":
            affected = store.delete_manual_filter(stock_code)
            out = {"stock_code": stock_code, "decision": "clear", "affected": affected}
        else:
            if decision not in {"include", "exclude", "watch"}:
                raise RuntimeError("--decision must be include/exclude/watch/clear")
            store.upsert_manual_filters(
                [
                    {
                        "stock_code": stock_code,
                        "decision": decision,
                        "reason": reason,
                        "operator": operator,
                        "updated_at": now,
                    }
                ]
            )
            out = {
                "stock_code": stock_code,
                "decision": decision,
                "reason": reason,
                "operator": operator,
                "updated_at": now,
            }
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def manual_filter_list(args: argparse.Namespace) -> int:
    _ = args
    store = SnapshotStore(sqlite_path())
    try:
        rows = store.query_manual_filters()
    finally:
        store.close()
    print(json.dumps({"count": len(rows), "rows": rows}, ensure_ascii=False, indent=2))
    return 0


def select_stock(args: argparse.Namespace) -> int:
    start_date = today_or(getattr(args, "start_date", "")) if getattr(args, "start_date", "") else (dt.date.today() - dt.timedelta(days=60)).isoformat()
    end_date = today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else dt.date.today().isoformat()
    strategies = [x.upper() for x in split_csv(getattr(args, "strategies", ""))]
    markets = [x.upper() for x in split_csv(getattr(args, "markets", ""))]
    top_n = max(1, int(getattr(args, "top_n", 10) or 10))
    min_samples = max(1, int(getattr(args, "min_samples", 5) or 5))
    manual_mode = str(getattr(args, "manual_filter_mode", "strict") or "strict").strip().lower()
    if manual_mode not in {"off", "strict", "overlay"}:
        raise RuntimeError("--manual-filter-mode must be one of off/strict/overlay")

    store = SnapshotStore(sqlite_path())
    try:
        rows = store.query_range(start_date=start_date, end_date=end_date, strategies=strategies or None, markets=markets or None)
        manual_map = _manual_filter_map(store.query_manual_filters())
    finally:
        store.close()
    rows = [row for row in rows if int(row.get("sample_count", 0) or 0) >= min_samples]
    score_out = score_snapshot_slice(
        SnapshotSlice(rows=rows, strategy_filter=[], market_filter=[], start_date="", end_date=""),
        top_n=top_n,
    )
    candidates = [dict(item) for item in score_out.get("candidates", [])]
    for row in candidates:
        code = _normalize_stock_code(str(row.get("stock_code", "")))
        manual = manual_map.get(code, {})
        row["manual_decision"] = manual.get("decision", "")
        row["manual_reason"] = manual.get("reason", "")
        row["manual_operator"] = manual.get("operator", "")

    filtered_candidates = candidates
    if manual_mode in {"strict", "overlay"}:
        filtered_candidates = [x for x in candidates if x.get("manual_decision", "") != "exclude"]

    if manual_mode == "strict":
        include_rows = [x for x in filtered_candidates if x.get("manual_decision", "") == "include"]
        include_codes = {str(x.get("stock_code", "")) for x in include_rows}
        others = [x for x in filtered_candidates if str(x.get("stock_code", "")) not in include_codes]
        filtered_candidates = include_rows + others
    selected, rebalance_plan = _build_selection_from_candidates(filtered_candidates, top_n=top_n)

    print(
        json.dumps(
            {
                "start_date": start_date,
                "end_date": end_date,
                "strategy_filter": strategies,
                "market_filter": markets,
                "min_samples": min_samples,
                "top_n": top_n,
                "manual_filter_mode": manual_mode,
                "candidate_count": len(filtered_candidates),
                "selected_count": len(selected),
                "selected": selected,
                "rebalance_plan": rebalance_plan,
                "candidates": filtered_candidates,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


__all__ = [
    "history_query",
    "manual_filter_list",
    "manual_filter_set",
    "select_stock",
    "snapshot_market_daily",
    "sync_market_universe",
]
