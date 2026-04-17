from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from core.notion_props import find_title_property_name, get_prop, p_date, p_formula_number, p_number, p_relation_ids, p_select, p_title
from services.recommendation.common import find_prop_name


def guess_market(stock_code: str) -> str:
    code = (stock_code or "").strip().upper()
    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) == 6:
        if digits.startswith(("5", "6", "9")):
            return "SH"
        return "SZ"
    if code.startswith(("SH", "SZ", "HK", "US")):
        return code[:2]
    if code.endswith(".HK"):
        return "HK"
    if code.endswith(".US"):
        return "US"
    return "OTHER"


def market_from_rule(stock_code: str, raw_rule: str) -> str:
    if not raw_rule:
        return guess_market(stock_code)
    code = (stock_code or "").strip().upper()
    digits = "".join(ch for ch in code if ch.isdigit())
    for token in [x.strip() for x in raw_rule.split(",") if x.strip()]:
        if ":" not in token:
            continue
        key, market = token.split(":", 1)
        key = key.strip().upper()
        market = market.strip().upper()
        if not key or not market:
            continue
        if code.startswith(key) or digits.startswith(key):
            return market
    return guess_market(stock_code)


def _find_prop_by_keywords(
    db_props: Dict[str, Dict[str, Any]],
    keywords: List[str],
    expected_types: Optional[List[str]] = None,
) -> Optional[str]:
    for name, info in db_props.items():
        typ = info.get("type")
        if expected_types and typ not in expected_types:
            continue
        if any(k and (k in name) for k in keywords):
            return name
    return None


def resolve_stock_fields_runtime(stock_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = stock_db.get("properties", {})
    fields = {
        "title": find_title_property_name(stock_db),
        "stock_code": find_prop_name(props, ["股票代码", "代码"], ["rich_text", "title"]),
        "current_price": find_prop_name(props, ["当前市价", "最新价", "现价", "市价"], ["number"]),
        "current_cost": find_prop_name(props, ["当前持仓成本", "持仓成本", "成本价"], ["number"]),
        "out_action": find_prop_name(props, ["建议动作"], ["select", "status", "rich_text", "title"]),
        "out_buy": find_prop_name(props, ["建议买入价"], ["number"]),
        "out_sell": find_prop_name(props, ["建议卖出价"], ["number"]),
        "out_stop": find_prop_name(props, ["建议止损价"], ["number"]),
        "out_pos": find_prop_name(props, ["建议仓位变化"], ["number"]),
        "out_buy_shares": find_prop_name(props, ["建议买入股数"], ["number"]),
        "out_sell_shares": find_prop_name(props, ["建议卖出股数"], ["number"]),
        "out_holding_shares": find_prop_name(props, ["当前持仓股数", "持仓股数"], ["number"]),
        "out_market_value": find_prop_name(props, ["持仓市值", "市值"], ["number"]),
        "out_unrealized_pnl": find_prop_name(props, ["浮动盈亏", "未实现盈亏"], ["number"]),
        "out_conf": find_prop_name(props, ["建议置信度"], ["select", "status", "rich_text", "title"]),
        "out_mode": find_prop_name(props, ["建议模式"], ["select", "status", "rich_text", "title"]),
        "out_reason": find_prop_name(props, ["建议原因", "触发原因"], ["rich_text", "title"]),
        "out_time": find_prop_name(props, ["建议更新时间"], ["date", "rich_text"]),
    }

    if not fields.get("stock_code"):
        fields["stock_code"] = _find_prop_by_keywords(props, ["股票代码", "代码"], ["rich_text", "title"])
    if not fields.get("current_price"):
        fields["current_price"] = _find_prop_by_keywords(props, ["当前市价", "最新价", "现价", "市价"], ["number"])
    if not fields.get("current_cost"):
        fields["current_cost"] = _find_prop_by_keywords(props, ["持仓成本", "成本"], ["number"])
    if not fields.get("out_action"):
        fields["out_action"] = _find_prop_by_keywords(props, ["建议动作"], ["select", "status", "rich_text", "title"])
    if not fields.get("out_buy"):
        fields["out_buy"] = _find_prop_by_keywords(props, ["建议买入价"], ["number"])
    if not fields.get("out_sell"):
        fields["out_sell"] = _find_prop_by_keywords(props, ["建议卖出价"], ["number"])
    if not fields.get("out_stop"):
        fields["out_stop"] = _find_prop_by_keywords(props, ["建议止损价"], ["number"])
    if not fields.get("out_pos"):
        fields["out_pos"] = _find_prop_by_keywords(props, ["建议仓位变化"], ["number"])
    if not fields.get("out_buy_shares"):
        fields["out_buy_shares"] = _find_prop_by_keywords(props, ["建议买入股数"], ["number"])
    if not fields.get("out_sell_shares"):
        fields["out_sell_shares"] = _find_prop_by_keywords(props, ["建议卖出股数"], ["number"])
    if not fields.get("out_holding_shares"):
        fields["out_holding_shares"] = _find_prop_by_keywords(props, ["持仓股数"], ["number"])
    if not fields.get("out_market_value"):
        fields["out_market_value"] = _find_prop_by_keywords(props, ["持仓市值", "市值"], ["number"])
    if not fields.get("out_unrealized_pnl"):
        fields["out_unrealized_pnl"] = _find_prop_by_keywords(props, ["浮动盈亏", "未实现盈亏"], ["number"])
    if not fields.get("out_conf"):
        fields["out_conf"] = _find_prop_by_keywords(props, ["建议置信度"], ["select", "status", "rich_text", "title"])
    if not fields.get("out_mode"):
        fields["out_mode"] = _find_prop_by_keywords(props, ["建议模式"], ["select", "status", "rich_text", "title"])
    if not fields.get("out_reason"):
        fields["out_reason"] = _find_prop_by_keywords(props, ["建议原因", "触发原因"], ["rich_text", "title"])
    if not fields.get("out_time"):
        fields["out_time"] = _find_prop_by_keywords(props, ["建议更新时间"], ["date", "rich_text"])

    return fields


def resolve_trade_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    fields = {
        "date": find_prop_name(props, ["日期", "交易日期", "下单日期", "鏃ユ湡"], ["date"]),
        "direction": find_prop_name(props, ["方向", "交易方向", "买卖方向", "鏂瑰悜"], ["select", "status"]),
        "shares": find_prop_name(props, ["股数", "数量", "成交数量", "鑲℃暟"], ["number"]),
        "price": find_prop_name(props, ["价格", "成交价", "成交价格", "浠锋牸"], ["number"]),
        "stock": find_prop_name(props, ["股票", "标的", "证券", "鑲＄エ"], ["relation"]),
        "realized": find_prop_name(props, ["单笔已实现收益"], ["formula", "number"]),
    }
    if not fields.get("date"):
        fields["date"] = _find_prop_by_keywords(props, ["日期"], ["date"])
    if not fields.get("direction"):
        fields["direction"] = _find_prop_by_keywords(props, ["方向"], ["select", "status"])
    if not fields.get("shares"):
        fields["shares"] = _find_prop_by_keywords(props, ["股", "数量"], ["number"])
    if not fields.get("price"):
        fields["price"] = _find_prop_by_keywords(props, ["价"], ["number"])
    if not fields.get("stock"):
        fields["stock"] = _find_prop_by_keywords(props, ["股票", "标的", "证券"], ["relation"])
    return fields


def resolve_trade_cost_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    return {
        "fee": find_prop_name(props, ["手续费", "费用"], ["number"]),
        "tax": find_prop_name(props, ["税费", "印花税"], ["number"]),
    }


@dataclass
class TradePoint:
    date: str
    price: Optional[float]
    shares: float
    direction: str
    realized: Optional[float]
    stock_id: str


def build_trade_points(
    trade_rows: List[Dict[str, Any]],
    field_map: Dict[str, Optional[str]],
) -> Dict[str, List[TradePoint]]:
    stock_points: Dict[str, List[TradePoint]] = defaultdict(list)
    if not field_map.get("stock"):
        return stock_points
    for row in trade_rows:
        stock_ids = p_relation_ids(row, field_map["stock"])
        if not stock_ids:
            continue
        date_s = p_date(row, field_map["date"]) if field_map.get("date") else ""
        direction = p_select(row, field_map["direction"]) if field_map.get("direction") else ""
        shares = p_number(row, field_map["shares"]) if field_map.get("shares") else None
        price = p_number(row, field_map["price"]) if field_map.get("price") else None

        realized: Optional[float] = None
        if field_map.get("realized"):
            if get_prop(row, field_map["realized"]).get("type") == "formula":
                realized = p_formula_number(row, field_map["realized"])
            else:
                realized = p_number(row, field_map["realized"])

        point = TradePoint(
            date=date_s or "",
            price=float(price) if price is not None else None,
            shares=float(shares) if shares is not None else 0.0,
            direction=direction or "",
            realized=realized,
            stock_id=stock_ids[0],
        )
        stock_points[stock_ids[0]].append(point)

    for sid in stock_points:
        stock_points[sid].sort(key=lambda x: x.date)
    return stock_points


def replay_positions_from_trades(
    trade_rows: List[Dict[str, Any]],
    trade_fields: Dict[str, Optional[str]],
    cost_fields: Dict[str, Optional[str]],
) -> tuple[Dict[str, float], Dict[str, float], float]:
    rows = sorted(trade_rows, key=lambda r: (p_date(r, trade_fields["date"]) if trade_fields.get("date") else "", str(r.get("id", ""))))
    holding_shares_by_stock: Dict[str, float] = defaultdict(float)
    holding_avg_cost_by_stock: Dict[str, float] = defaultdict(float)
    realized_pnl_total = 0.0
    for row in rows:
        stock_ids = p_relation_ids(row, trade_fields["stock"]) if trade_fields.get("stock") else []
        if not stock_ids:
            continue
        sid = stock_ids[0]
        direction = p_select(row, trade_fields["direction"]).strip().upper() if trade_fields.get("direction") else ""
        shares = float(p_number(row, trade_fields["shares"]) or 0.0) if trade_fields.get("shares") else 0.0
        price = float(p_number(row, trade_fields["price"]) or 0.0) if trade_fields.get("price") else 0.0
        fee = float(p_number(row, cost_fields["fee"]) or 0.0) if cost_fields.get("fee") else 0.0
        tax = float(p_number(row, cost_fields["tax"]) or 0.0) if cost_fields.get("tax") else 0.0
        if shares <= 0 or price <= 0:
            continue
        old_shares = float(holding_shares_by_stock.get(sid, 0.0))
        old_avg = float(holding_avg_cost_by_stock.get(sid, 0.0))
        if direction == "BUY":
            total_cost = old_shares * old_avg + shares * price + fee + tax
            new_shares = old_shares + shares
            holding_shares_by_stock[sid] = new_shares
            holding_avg_cost_by_stock[sid] = (total_cost / new_shares) if new_shares > 0 else 0.0
        elif direction == "SELL":
            sell_shares = min(shares, old_shares)
            if sell_shares <= 0:
                continue
            matched_cost = sell_shares * old_avg
            proceeds = sell_shares * price - fee - tax
            realized_pnl_total += proceeds - matched_cost
            remain = old_shares - sell_shares
            holding_shares_by_stock[sid] = remain
            holding_avg_cost_by_stock[sid] = old_avg if remain > 0 else 0.0
    return dict(holding_shares_by_stock), dict(holding_avg_cost_by_stock), float(realized_pnl_total)
