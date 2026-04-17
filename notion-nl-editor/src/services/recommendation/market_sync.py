import argparse
import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import requests

from core.config import Cfg
from core.notion_client import NotionClient
from core.notion_props import find_title_property_name, get_prop, p_rich, rt_plain, text_prop, title_prop
from core.runtime import split_csv, sqlite_path, today_or
from stores.kline_store import KBar, KlineStore


class KlineProvider:
    def __init__(self, token: str, store: KlineStore) -> None:
        self.token = token.strip()
        self.store = store
        self.session = requests.Session()
        self.base = "https://api.waditu.com"
        if not self.token:
            raise RuntimeError("Missing TUSHARE_TOKEN for kline mode.")

    def _post(self, api_name: str, params: Dict[str, Any], fields: str) -> List[Dict[str, Any]]:
        payload = {"api_name": api_name, "token": self.token, "params": params, "fields": fields}
        resp = self.session.post(self.base, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Tushare {api_name} failed: {data.get('msg', 'unknown')}")
        items = data.get("data", {})
        fields_arr = items.get("fields", [])
        rows = items.get("items", [])
        out: List[Dict[str, Any]] = []
        for row in rows:
            obj: Dict[str, Any] = {}
            for idx, key in enumerate(fields_arr):
                obj[str(key)] = row[idx] if idx < len(row) else None
            out.append(obj)
        return out

    def _fetch_daily_raw(self, ts_code: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        return self._post(
            "daily",
            {"ts_code": ts_code, "start_date": start_date.replace("-", ""), "end_date": end_date.replace("-", "")},
            "ts_code,trade_date,open,high,low,close,vol,amount",
        )

    def _fetch_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, float]:
        rows = self._post(
            "adj_factor",
            {"ts_code": ts_code, "start_date": start_date.replace("-", ""), "end_date": end_date.replace("-", "")},
            "ts_code,trade_date,adj_factor",
        )
        return {str(row.get("trade_date", "")): float(row.get("adj_factor", 0.0) or 0.0) for row in rows}

    def fetch_stock_basic(self, list_status: str = "L", exchange: str = "") -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"list_status": (list_status or "L").upper()}
        if exchange:
            params["exchange"] = exchange.upper()
        return self._post(
            "stock_basic",
            params,
            "ts_code,symbol,name,area,industry,market,list_status,list_date,delist_date",
        )

    def sync_symbol(self, ts_code: str, start_date: str, end_date: str, adj: str, force: bool = False) -> Dict[str, Any]:
        _ = force
        daily_rows = self._fetch_daily_raw(ts_code, start_date, end_date)
        if not daily_rows:
            self.store.record_sync(ts_code, start_date, end_date, 0, "ok", "")
            return {"symbol": ts_code, "bars": 0, "status": "ok"}
        adj_map: Dict[str, float] = {}
        if adj in {"qfq", "hfq"}:
            adj_map = self._fetch_adj_factor(ts_code, start_date, end_date)
        bars: List[KBar] = []
        base_factor = 0.0
        if adj_map:
            factors = [x for x in adj_map.values() if x > 0]
            if factors:
                base_factor = max(factors) if adj == "qfq" else min(factors)
        for row in daily_rows:
            trade_raw = str(row.get("trade_date", ""))
            trade_date = f"{trade_raw[:4]}-{trade_raw[4:6]}-{trade_raw[6:8]}" if len(trade_raw) == 8 else ""
            open_v = float(row.get("open", 0.0) or 0.0)
            high_v = float(row.get("high", 0.0) or 0.0)
            low_v = float(row.get("low", 0.0) or 0.0)
            close_v = float(row.get("close", 0.0) or 0.0)
            if adj_map and base_factor > 0 and trade_raw in adj_map and adj_map[trade_raw] > 0:
                factor = float(adj_map[trade_raw]) / base_factor
                open_v, high_v, low_v, close_v = open_v * factor, high_v * factor, low_v * factor, close_v * factor
            if not trade_date or close_v <= 0:
                continue
            bars.append(
                KBar(
                    symbol=ts_code,
                    trade_date=trade_date,
                    open=open_v,
                    high=high_v,
                    low=low_v,
                    close=close_v,
                    vol=float(row.get("vol", 0.0) or 0.0),
                    amount=float(row.get("amount", 0.0) or 0.0),
                    adj=adj,
                )
            )
        bars.sort(key=lambda x: x.trade_date)
        upserted = self.store.upsert_bars(bars, source="tushare")
        self.store.record_sync(ts_code, start_date, end_date, upserted, "ok", "")
        return {"symbol": ts_code, "bars": upserted, "status": "ok"}


def _find_prop_name(db_props: Dict[str, Dict[str, Any]], candidates: List[str], expected_types: Optional[List[str]] = None) -> Optional[str]:
    for name in candidates:
        info = db_props.get(name)
        if not info:
            continue
        if expected_types and info.get("type") not in expected_types:
            continue
        return name
    return None


def _find_prop_by_keywords(db_props: Dict[str, Dict[str, Any]], keywords: List[str], expected_types: Optional[List[str]] = None) -> Optional[str]:
    for name, info in db_props.items():
        if expected_types and info.get("type") not in expected_types:
            continue
        if any(keyword and (keyword in name) for keyword in keywords):
            return name
    return None


def _resolve_stock_fields_runtime(stock_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = stock_db.get("properties", {})
    fields = {
        "title": find_title_property_name(stock_db),
        "stock_code": _find_prop_name(props, ["股票代码", "代码"], ["rich_text", "title"]),
        "current_price": _find_prop_name(props, ["当前市价", "最新价", "现价", "市价"], ["number"]),
    }
    if not fields.get("stock_code"):
        fields["stock_code"] = _find_prop_by_keywords(props, ["股票代码", "代码"], ["rich_text", "title"])
    if not fields.get("current_price"):
        fields["current_price"] = _find_prop_by_keywords(props, ["当前市价", "最新价", "现价", "市价"], ["number"])
    return fields


def _prop_text_any(page: Dict[str, Any], key: Optional[str]) -> str:
    if not key:
        return ""
    prop = get_prop(page, key)
    typ = prop.get("type")
    if typ == "title":
        return rt_plain(prop.get("title", []))
    if typ == "rich_text":
        return rt_plain(prop.get("rich_text", []))
    if typ == "select":
        select = prop.get("select")
        return select.get("name", "") if select else ""
    if typ == "status":
        status = prop.get("status")
        return status.get("name", "") if status else ""
    if typ == "date":
        date_obj = prop.get("date")
        return date_obj.get("start", "") if date_obj else ""
    if typ == "number":
        num = prop.get("number")
        return "" if num is None else str(num)
    return ""


def _normalize_cn_symbol(raw_code: str) -> Optional[str]:
    code = (raw_code or "").strip().upper()
    if not code:
        return None
    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) == 6:
        if digits.startswith(("5", "6", "9")):
            return f"sh{digits}"
        return f"sz{digits}"
    if code.startswith(("SH", "SZ")) and len(code) == 8 and code[2:].isdigit():
        return code.lower()
    if code.endswith((".SH", ".SZ")) and len(code) == 9:
        return f"{code[-2:].lower()}{code[:6]}"
    if code.endswith((".SS", ".SZ")) and len(code) == 9:
        suffix = "sh" if code.endswith(".SS") else "sz"
        return f"{suffix}{code[:6]}"
    return None


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


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _write_prop_value(db_props: Dict[str, Dict[str, Any]], prop_name: str, value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    info = db_props.get(prop_name)
    if not info:
        return None
    typ = info.get("type")
    if typ == "number":
        return {"number": float(value)}
    if typ == "select":
        return {"select": {"name": _coerce_text(value)}}
    if typ == "status":
        return {"status": {"name": _coerce_text(value)}}
    if typ == "rich_text":
        return text_prop(_coerce_text(value))
    if typ == "date":
        return {"date": {"start": _coerce_text(value)}}
    if typ == "title":
        return title_prop(_coerce_text(value))
    return None


def _parse_tencent_quote_line(line: str) -> Tuple[Optional[str], Optional[float]]:
    if "=" not in line:
        return None, None
    left, right = line.split("=", 1)
    symbol = left.strip().removeprefix("v_")
    payload = right.strip().strip(";").strip('"')
    parts = payload.split("~")
    if len(parts) < 5:
        return symbol, None
    for idx in [3, 4]:
        try:
            price = float(parts[idx])
            if price > 0:
                return symbol, price
        except Exception:
            continue
    return symbol, None


def _fetch_realtime_prices_tencent(symbols: List[str], timeout: int = 8) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not symbols:
        return out
    for idx in range(0, len(symbols), 60):
        batch = symbols[idx : idx + 60]
        url = f"https://qt.gtimg.cn/q={','.join(batch)}"
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        text = resp.content.decode("gbk", errors="ignore")
        for item in text.split(";"):
            part = item.strip()
            if not part:
                continue
            symbol, price = _parse_tencent_quote_line(part)
            if symbol and price is not None:
                out[symbol] = price
    return out


def sync_prices(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db = client.get_database(cfg.stock_master_id)
    fields = _resolve_stock_fields_runtime(stock_db)
    if not fields.get("stock_code"):
        raise RuntimeError("Unable to locate stock code field (rich_text/title required).")
    if not fields.get("current_price"):
        raise RuntimeError("Unable to locate current price field (number required).")

    stock_rows = client.query_database_all(cfg.stock_master_id)
    db_props = stock_db.get("properties", {})

    symbol_to_row: Dict[str, Tuple[str, str]] = {}
    for row in stock_rows:
        row_id = row.get("id", "")
        code_raw = p_rich(row, fields["stock_code"]) if fields["stock_code"] else ""
        symbol = _normalize_cn_symbol(code_raw)
        if not symbol:
            continue
        symbol_to_row[symbol] = (row_id, code_raw)

    prices = _fetch_realtime_prices_tencent(sorted(symbol_to_row.keys()), timeout=int(getattr(args, "timeout", 8)))
    updated = 0
    skipped = 0
    for symbol, (row_id, _) in symbol_to_row.items():
        price = prices.get(symbol)
        if price is None:
            skipped += 1
            continue
        if not bool(getattr(args, "dry_run", False)):
            payload = _write_prop_value(db_props, fields["current_price"], price)
            if payload is not None:
                client.update_page(row_id, {fields["current_price"]: payload})
        updated += 1

    result = {
        "total_symbols": len(symbol_to_row),
        "price_fetched": len(prices),
        "updated": updated,
        "skipped": skipped,
        "dry_run": bool(getattr(args, "dry_run", False)),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def sync_kline(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db = client.get_database(cfg.stock_master_id)
    stock_fields = _resolve_stock_fields_runtime(stock_db)
    stock_rows = client.query_database_all(cfg.stock_master_id)
    end_date = today_or(getattr(args, "end_date", ""))
    start_date = today_or(getattr(args, "start_date", "")) if getattr(args, "start_date", "") else _default_kline_start(end_date)
    adj = _coerce_text(getattr(args, "adj", os.getenv("KLINE_DEFAULT_ADJ", "raw"))).strip().lower() or "raw"
    if adj not in {"raw", "qfq", "hfq"}:
        raise RuntimeError("adj must be one of raw/qfq/hfq")

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    store = KlineStore(sqlite_path())
    provider = KlineProvider(token=token, store=store)
    symbols = split_csv(getattr(args, "symbols", ""))
    ts_codes: List[str] = []
    if symbols:
        for raw in symbols:
            ts_code = _to_tushare_ts_code(raw)
            if ts_code:
                ts_codes.append(ts_code)
    else:
        for row in stock_rows:
            code_raw = _prop_text_any(row, stock_fields["stock_code"]) if stock_fields.get("stock_code") else ""
            ts_code = _to_tushare_ts_code(code_raw)
            if ts_code:
                ts_codes.append(ts_code)

    uniq_codes: List[str] = []
    for code in ts_codes:
        if code not in uniq_codes:
            uniq_codes.append(code)

    details: List[Dict[str, Any]] = []
    ok = 0
    fail = 0
    try:
        for ts_code in uniq_codes:
            try:
                result = provider.sync_symbol(
                    ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    adj=adj,
                    force=bool(getattr(args, "force", False)),
                )
                details.append(result)
                ok += 1
            except Exception as exc:
                fail += 1
                err = str(exc)
                details.append({"symbol": ts_code, "bars": 0, "status": "error", "error": err})
                store.record_sync(ts_code, start_date, end_date, 0, "error", err)
    finally:
        store.close()

    out = {
        "symbols_total": len(uniq_codes),
        "ok": ok,
        "failed": fail,
        "start_date": start_date,
        "end_date": end_date,
        "adj": adj,
        "sqlite_path": sqlite_path(),
        "details": details,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if fail == 0 else 2
