import math
import os
from typing import Any, Dict, List, Optional

from core.config import Cfg
from core.notion_client import NotionClient
from core.notion_props import get_prop, p_number, p_title
from services.recommendation.common import find_prop_name, prop_text_any


def account_row_code() -> str:
    return (os.getenv("ACCOUNT_ROW_CODE", "ACCOUNT") or "ACCOUNT").strip().upper()


def is_account_row(row: Dict[str, Any], stock_fields: Dict[str, Optional[str]]) -> bool:
    target = account_row_code()
    code = prop_text_any(row, stock_fields.get("stock_code"))
    title = p_title(row, stock_fields.get("title")) if stock_fields.get("title") else ""
    return (code or "").strip().upper() == target or (title or "").strip().upper() == target


def resolve_cash_config_fields(cash_db: Dict[str, Any], pref_name: str) -> Dict[str, Optional[str]]:
    props = cash_db.get("properties", {})
    return {
        "cash": find_prop_name(props, [pref_name, "可流动现金", "现金", "cash"], ["number", "formula", "rollup", "rich_text", "title"]),
    }


def num_from_prop_any(page: Dict[str, Any], key: Optional[str]) -> Optional[float]:
    if not key:
        return None
    prop = get_prop(page, key)
    if not prop:
        return None
    typ = prop.get("type")
    if typ == "number":
        num = prop.get("number")
        return float(num) if num is not None else None
    if typ == "formula":
        formula = prop.get("formula", {})
        if formula.get("type") == "number":
            num = formula.get("number")
            return float(num) if num is not None else None
        return None
    if typ == "rollup":
        rollup = prop.get("rollup", {})
        if rollup.get("type") == "number":
            num = rollup.get("number")
            return float(num) if num is not None else None
        return None
    if typ in {"rich_text", "title"}:
        text = prop_text_any(page, key).replace(",", "").strip()
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None
    return None


def load_cash_from_config_db(client: NotionClient, cfg: Cfg) -> float:
    db_id = (cfg.cash_config_id or "").strip()
    if not db_id:
        raise RuntimeError("Missing DB_CASH_CONFIG_ID.")
    pref = (os.getenv("CASH_FIELD_NAME", "可流动现金") or "可流动现金").strip()
    db = client.get_database(db_id)
    fields = resolve_cash_config_fields(db, pref_name=pref)
    key = fields.get("cash")
    if not key:
        raise RuntimeError(f"Cash config DB missing field: {pref}")
    rows = client.query_database_all(db_id)
    if not rows:
        raise RuntimeError("Cash config DB has no records.")
    val = num_from_prop_any(rows[0], key)
    if val is not None:
        return float(val)
    fallback = os.getenv("TOTAL_CASH_FALLBACK", "").replace(",", "").strip()
    if fallback:
        try:
            v = float(fallback)
            if v > 0:
                return v
        except Exception:
            pass
    raise RuntimeError("Cash config value invalid and TOTAL_CASH_FALLBACK not set.")


def read_cash_config_formula_summary(client: NotionClient, cfg: Cfg) -> Dict[str, Optional[float]]:
    db_id = (cfg.cash_config_id or "").strip()
    if not db_id:
        return {}
    pref_asset = (os.getenv("CASH_TOTAL_ASSET_FIELD_NAME", "总资产") or "总资产").strip()
    pref_mkt = (os.getenv("CASH_MARKET_VALUE_FIELD_NAME", "总持仓市值") or "总持仓市值").strip()
    pref_unr = (os.getenv("CASH_UNREALIZED_FIELD_NAME", "总浮动盈亏") or "总浮动盈亏").strip()
    pref_total_pnl = (os.getenv("CASH_TOTAL_PNL_FIELD_NAME", "总盈亏") or "总盈亏").strip()
    pref_realized = (os.getenv("CASH_REALIZED_FIELD_NAME", "已实现盈亏") or "已实现盈亏").strip()

    db = client.get_database(db_id)
    rows = client.query_database_all(db_id)
    if not rows:
        return {}
    row = rows[0]
    props = db.get("properties", {})

    def pick(candidates: List[str]) -> Optional[str]:
        return find_prop_name(props, candidates, ["number", "formula", "rollup", "rich_text", "title"])

    keys = {
        "total_asset": pick([pref_asset, "总资产", "total_asset"]),
        "market_value_total": pick([pref_mkt, "总持仓市值", "持仓市值合计", "market_value_total"]),
        "unrealized_pnl_total": pick([pref_unr, "总浮动盈亏", "未实现盈亏合计", "unrealized_pnl_total"]),
        "total_pnl": pick([pref_total_pnl, "总盈亏", "total_pnl"]),
        "realized_pnl_total": pick([pref_realized, "已实现盈亏", "realized_pnl_total"]),
    }

    out: Dict[str, Optional[float]] = {}
    for key, prop_name in keys.items():
        if not prop_name:
            out[key] = None
            continue
        prop = get_prop(row, prop_name)
        typ = prop.get("type")
        val: Optional[float] = None
        if typ == "number":
            num = prop.get("number")
            val = float(num) if num is not None else None
        elif typ == "formula":
            f = prop.get("formula", {})
            if f.get("type") == "number" and f.get("number") is not None:
                val = float(f.get("number"))
        elif typ == "rollup":
            r = prop.get("rollup", {})
            if r.get("type") == "number" and r.get("number") is not None:
                val = float(r.get("number"))
        elif typ in {"rich_text", "title"}:
            val = num_from_prop_any(row, prop_name)
        out[key] = val
    return out


def build_reconcile_result(code_summary: Dict[str, float], notion_summary: Dict[str, Optional[float]]) -> Dict[str, Any]:
    threshold = float(os.getenv("CASH_RECONCILE_THRESHOLD", "1.0") or 1.0)
    checks = []
    max_delta = 0.0
    keys = ["total_asset", "market_value_total", "unrealized_pnl_total", "realized_pnl_total", "total_pnl"]
    for key in keys:
        code_v = float(code_summary.get(key, 0.0) or 0.0)
        notion_v = notion_summary.get(key)
        if notion_v is None:
            continue
        delta = abs(code_v - float(notion_v))
        max_delta = max(max_delta, delta)
        checks.append({"key": key, "code": code_v, "notion": float(notion_v), "delta": delta})
    has_reference = len(checks) > 0
    ok = (max_delta <= threshold) if has_reference else True
    return {
        "has_reference": has_reference,
        "threshold": threshold,
        "max_delta": max_delta,
        "ok": ok,
        "checks": checks,
    }


def build_latest_price_map(
    stock_rows: List[Dict[str, Any]],
    stock_fields: Dict[str, Optional[str]],
    stock_kbars: Dict[str, List[Any]],
) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for row in stock_rows:
        if is_account_row(row, stock_fields):
            continue
        sid = row.get("id", "")
        if not sid:
            continue
        px = p_number(row, stock_fields["current_price"]) if stock_fields.get("current_price") else None
        if px is None:
            bars = stock_kbars.get(sid, [])
            if bars:
                px = bars[-1].close
        if px is not None and float(px) > 0:
            out[sid] = float(px)
    return out


def build_position_valuation(
    holding_shares_by_stock: Dict[str, float],
    holding_avg_cost_by_stock: Dict[str, float],
    latest_price_by_stock: Dict[str, float],
) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for sid, shares in holding_shares_by_stock.items():
        shares_f = float(shares or 0.0)
        if shares_f <= 0:
            continue
        avg = float(holding_avg_cost_by_stock.get(sid, 0.0) or 0.0)
        px = float(latest_price_by_stock.get(sid, 0.0) or 0.0)
        market_value = shares_f * px if px > 0 else 0.0
        unrealized = shares_f * (px - avg) if px > 0 else 0.0
        out[sid] = {
            "holding_shares_now": float(shares_f),
            "avg_cost_now": float(avg),
            "last_price_now": float(px),
            "market_value_now": float(market_value),
            "unrealized_pnl_now": float(unrealized),
            "priced_flag": 1.0 if px > 0 else 0.0,
        }
    return out


def calc_account_summary(
    cash: float,
    position_valuation_by_stock: Dict[str, Dict[str, float]],
    realized_pnl_total: float,
) -> Dict[str, float]:
    market_value_total = 0.0
    invested_cost_total = 0.0
    unrealized_pnl_total = 0.0
    priced_positions = 0
    unpriced_positions = 0
    for item in position_valuation_by_stock.values():
        market_value_total += float(item.get("market_value_now", 0.0) or 0.0)
        unrealized_pnl_total += float(item.get("unrealized_pnl_now", 0.0) or 0.0)
        shares = float(item.get("holding_shares_now", 0.0) or 0.0)
        avg = float(item.get("avg_cost_now", 0.0) or 0.0)
        invested_cost_total += shares * avg
        if float(item.get("priced_flag", 0.0) or 0.0) > 0:
            priced_positions += 1
        else:
            unpriced_positions += 1
    total_asset = cash + market_value_total
    total_pnl = float(realized_pnl_total) + unrealized_pnl_total
    return {
        "cash": float(cash),
        "market_value_total": float(market_value_total),
        "invested_cost_total": float(invested_cost_total),
        "realized_pnl_total": float(realized_pnl_total),
        "unrealized_pnl_total": float(unrealized_pnl_total),
        "total_asset": float(total_asset),
        "total_pnl": float(total_pnl),
        "priced_positions": float(priced_positions),
        "unpriced_positions": float(unpriced_positions),
    }


def round_lot_a(share_count: float) -> int:
    if share_count <= 0:
        return 0
    return int(math.floor(share_count / 100.0) * 100)


def attach_position_sizing(
    recs: List[Dict[str, Any]],
    sizing_base_asset: float,
    holding_shares_by_stock: Dict[str, float],
) -> None:
    for rec in recs:
        sid = str(rec.get("stock_id", ""))
        action = str(rec.get("action", "")).upper()
        pos_delta = float(rec.get("position_delta", 0.0) or 0.0)
        buy_price = rec.get("buy_price")
        sell_price = rec.get("sell_price")
        holding_shares = float(holding_shares_by_stock.get(sid, 0.0) or 0.0)
        holding_lot = round_lot_a(holding_shares)

        target_value_delta = float(sizing_base_asset) * pos_delta
        suggest_buy_shares = 0
        suggest_sell_shares = 0
        estimated_trade_value = 0.0
        sizing_note = ""

        exec_price_buy = float(buy_price) if buy_price is not None else None
        exec_price_sell = float(sell_price) if sell_price is not None else None

        if action == "BUY":
            if exec_price_buy and exec_price_buy > 0 and target_value_delta > 0:
                suggest_buy_shares = round_lot_a(target_value_delta / exec_price_buy)
                estimated_trade_value = float(suggest_buy_shares) * exec_price_buy
            else:
                sizing_note = "BUY 无有效买入价或仓位变化<=0，建议股数=0"
        elif action == "SELL":
            if exec_price_sell and exec_price_sell > 0 and target_value_delta < 0:
                raw_shares = round_lot_a(abs(target_value_delta) / exec_price_sell)
                suggest_sell_shares = min(raw_shares, holding_lot)
                estimated_trade_value = float(suggest_sell_shares) * exec_price_sell
                if raw_shares > holding_lot:
                    sizing_note = "卖出建议已按当前持仓上限截断"
            else:
                sizing_note = "SELL 无有效卖出价或仓位变化>=0，建议股数=0"
        else:
            sizing_note = "HOLD 不建议下单股数"

        rec["holding_shares_now"] = float(holding_shares)
        rec["target_value_delta"] = float(target_value_delta)
        rec["exec_price_buy"] = exec_price_buy
        rec["exec_price_sell"] = exec_price_sell
        rec["suggest_buy_shares"] = int(suggest_buy_shares)
        rec["suggest_sell_shares"] = int(suggest_sell_shares)
        rec["estimated_trade_value"] = float(estimated_trade_value)
        rec["sizing_note"] = sizing_note
