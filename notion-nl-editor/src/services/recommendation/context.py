import argparse
import datetime as dt
import os
from typing import Any, Dict, List, Optional, Tuple

from core.config import Cfg
from core.notion_client import NotionClient
from core.runtime import sqlite_path, today_or
from services.recommendation.common import coerce_text, prop_text_any, resolve_recommend_data_source
from services.recommendation.data_prep import TradePoint, build_trade_points, resolve_stock_fields_runtime, resolve_trade_fields
from services.recommendation.market_sync import KlineProvider, sync_prices
from services.recommendation.types import RecommendationContext
from stores.kline_store import KBar, KlineStore


def _to_tushare_ts_code(raw_code: str) -> Optional[str]:
    code = (raw_code or "").strip().upper()
    if not code:
        return None
    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) == 6:
        suffix = "SH" if digits.startswith(("5", "6", "9")) else "SZ"
        return f"{digits}.{suffix}"
    if "." in code:
        left, right = code.split(".", 1)
        if left.isdigit() and len(left) == 6 and right in {"SH", "SZ", "SS"}:
            return f"{left}.{'SH' if right == 'SS' else right}"
    if code.startswith(("SH", "SZ")) and len(code) == 8 and code[2:].isdigit():
        return f"{code[2:]}.{code[:2]}"
    return None


def _default_kline_start(end_date: str, lookback_days: int = 420) -> str:
    end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    start_dt = end_dt - dt.timedelta(days=max(lookback_days, 30))
    return start_dt.isoformat()


def load_trade_points_for_recommendation(client: NotionClient, cfg: Cfg) -> Dict[str, List[TradePoint]]:
    trade_db = client.get_database(cfg.std_trades_id)
    trade_fields = resolve_trade_fields(trade_db)
    for key in ["date", "direction", "shares", "price", "stock"]:
        if not trade_fields[key]:
            raise RuntimeError(f"Missing required standard-trade field mapping: {key}")
    trade_rows = client.query_database_all(cfg.std_trades_id)
    return build_trade_points(trade_rows, trade_fields)


def load_kbars_for_stocks(
    stock_rows: List[Dict[str, Any]],
    stock_fields: Dict[str, Optional[str]],
    args: argparse.Namespace,
) -> Tuple[Dict[str, List[KBar]], Dict[str, str]]:
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    adj = coerce_text(getattr(args, "adj", os.getenv("KLINE_DEFAULT_ADJ", "raw"))).strip().lower() or "raw"
    if adj not in {"raw", "qfq", "hfq"}:
        raise RuntimeError("adj must be one of raw/qfq/hfq")
    end_date = today_or(getattr(args, "end_date", ""))
    start_date = today_or(getattr(args, "start_date", "")) if getattr(args, "start_date", "") else _default_kline_start(end_date)
    force = bool(getattr(args, "force", False))

    store = KlineStore(sqlite_path())
    provider = KlineProvider(token=token, store=store)
    by_stock: Dict[str, List[KBar]] = {}
    by_stock_symbol: Dict[str, str] = {}
    try:
        for row in stock_rows:
            stock_id = row.get("id", "")
            code_raw = prop_text_any(row, stock_fields["stock_code"]) if stock_fields.get("stock_code") else ""
            ts_code = _to_tushare_ts_code(code_raw)
            if not stock_id or not ts_code:
                continue
            bars = store.query_bars(ts_code, start_date=start_date, end_date=end_date, adj=adj)
            if (not bars) or force:
                provider.sync_symbol(ts_code=ts_code, start_date=start_date, end_date=end_date, adj=adj, force=force)
                bars = store.query_bars(ts_code, start_date=start_date, end_date=end_date, adj=adj)
            by_stock[stock_id] = bars
            by_stock_symbol[stock_id] = ts_code
    finally:
        store.close()
    return by_stock, by_stock_symbol


def prepare_recommendation_context(
    client: NotionClient,
    cfg: Cfg,
    args: argparse.Namespace,
) -> Tuple[Dict[str, Any], Dict[str, Optional[str]], List[Dict[str, Any]], Dict[str, List[TradePoint]], Dict[str, List[KBar]], Dict[str, str]]:
    stock_db = client.get_database(cfg.stock_master_id)
    stock_fields = resolve_stock_fields_runtime(stock_db)
    stock_rows = client.query_database_all(cfg.stock_master_id)
    if getattr(args, "refresh_prices", False):
        sync_prices(client, cfg, argparse.Namespace(dry_run=bool(getattr(args, "dry_run", False)), timeout=getattr(args, "timeout", 8)))
        stock_rows = client.query_database_all(cfg.stock_master_id)

    data_source = resolve_recommend_data_source(args)
    stock_points: Dict[str, List[TradePoint]] = {}
    stock_kbars: Dict[str, List[KBar]] = {}
    stock_symbols: Dict[str, str] = {}
    if data_source == "trade":
        stock_points = load_trade_points_for_recommendation(client, cfg)
    else:
        stock_kbars, stock_symbols = load_kbars_for_stocks(stock_rows, stock_fields, args)
    return stock_db, stock_fields, stock_rows, stock_points, stock_kbars, stock_symbols


def prepare_recommendation_context_obj(
    client: NotionClient,
    cfg: Cfg,
    args: argparse.Namespace,
) -> RecommendationContext:
    stock_db, stock_fields, stock_rows, stock_points, stock_kbars, stock_symbols = prepare_recommendation_context(client, cfg, args)
    return RecommendationContext(
        stock_db=stock_db,
        stock_fields=stock_fields,
        stock_rows=stock_rows,
        stock_points=stock_points,
        stock_kbars=stock_kbars,
        stock_symbols=stock_symbols,
    )
