import argparse
import datetime as dt
import json
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


class NotionClient:
    def __init__(self, token: str, version: str) -> None:
        self.base = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": version,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        resp = requests.request(method, url, headers=self.headers, json=payload, timeout=30)
        if resp.status_code >= 400:
            raise RuntimeError(f"Notion API error {resp.status_code} {path}: {resp.text}")
        if not resp.text:
            return {}
        return resp.json()

    def get_database(self, database_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/databases/{database_id}")

    def query_database_all(self, database_id: str, filter_obj: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            payload: Dict[str, Any] = {"page_size": 100}
            if filter_obj:
                payload["filter"] = filter_obj
            if cursor:
                payload["start_cursor"] = cursor
            data = self._request("POST", f"/databases/{database_id}/query", payload)
            rows.extend(data.get("results", []))
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
        return rows

    def create_page(self, database_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/pages", {"parent": {"database_id": database_id}, "properties": properties})

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PATCH", f"/pages/{page_id}", {"properties": properties})


def rt_plain(rt: Optional[List[Dict[str, Any]]]) -> str:
    if not rt:
        return ""
    return "".join(x.get("plain_text", "") for x in rt).strip()


def get_prop(page: Dict[str, Any], key: str) -> Dict[str, Any]:
    return page.get("properties", {}).get(key, {})


def p_title(page: Dict[str, Any], key: str) -> str:
    return rt_plain(get_prop(page, key).get("title", []))


def p_rich(page: Dict[str, Any], key: str) -> str:
    return rt_plain(get_prop(page, key).get("rich_text", []))


def p_number(page: Dict[str, Any], key: str) -> Optional[float]:
    return get_prop(page, key).get("number")


def p_date(page: Dict[str, Any], key: str) -> str:
    date_obj = get_prop(page, key).get("date")
    if not date_obj:
        return ""
    return date_obj.get("start", "")


def p_select(page: Dict[str, Any], key: str) -> str:
    obj = get_prop(page, key).get("select")
    return obj.get("name", "") if obj else ""


def p_relation_ids(page: Dict[str, Any], key: str) -> List[str]:
    rel = get_prop(page, key).get("relation", [])
    return [x.get("id") for x in rel if x.get("id")]


def p_formula_number(page: Dict[str, Any], key: str) -> Optional[float]:
    formula = get_prop(page, key).get("formula", {})
    if formula.get("type") == "number":
        return formula.get("number")
    return None


def find_title_property_name(db: Dict[str, Any]) -> str:
    for k, info in db.get("properties", {}).items():
        if info.get("type") == "title":
            return k
    raise RuntimeError("No title property in database.")


def text_prop(text: str) -> Dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": text[:2000]}}]}


def title_prop(text: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": text[:2000]}}]}


@dataclass
class Cfg:
    stock_master_id: str
    std_trades_id: str
    std_dividend_id: str
    annual_id: str
    buy_wide_id: str
    t_record_id: str


def load_cfg() -> Cfg:
    return Cfg(
        stock_master_id=os.getenv("DB_STOCK_MASTER_ID", "9ff0bf7d-9ae4-41c8-9440-729daaa2a95d"),
        std_trades_id=os.getenv("DB_STD_TRADES_ID", "33c225a4-e273-810f-ae9f-d44f9d44d528"),
        std_dividend_id=os.getenv("DB_STD_DIVIDEND_ID", "33c225a4-e273-8112-9444-f798532e60cf"),
        annual_id=os.getenv("DB_ANNUAL_ID", "33c225a4-e273-8162-8804-dfde58582535"),
        buy_wide_id=os.getenv("DB_BUY_WIDE_ID", "0d485b47-e903-4fd3-901e-1bb4d09200f1"),
        t_record_id=os.getenv("DB_T_RECORD_ID", "93dde4b0-5d6f-4c49-a825-e49ae95be420"),
    )


def audit(client: NotionClient, cfg: Cfg, as_json: bool) -> int:
    std_rows = client.query_database_all(cfg.std_trades_id)
    stock_rows = client.query_database_all(cfg.stock_master_id)
    div_rows = client.query_database_all(cfg.std_dividend_id)
    annual_db = client.get_database(cfg.annual_id)
    annual_rows = client.query_database_all(cfg.annual_id)

    missing_stock_code = [r for r in stock_rows if not p_rich(r, "股票代码")]
    div_missing_stock = [r for r in div_rows if len(p_relation_ids(r, "股票")) == 0]
    div_missing_amount = [r for r in div_rows if p_number(r, "金额") is None]
    div_missing_date = [r for r in div_rows if not p_date(r, "日期")]

    annual_props = annual_db.get("properties", {})
    annual_manual_fields = []
    for k in ["做T收益", "分红收益", "已实现收益"]:
        if annual_props.get(k, {}).get("type") == "number":
            annual_manual_fields.append(k)

    checklist = [
        {
            "缺口项": "交易流水（标准）承载逐笔交易",
            "严重级别": "P0" if len(std_rows) == 0 else "P2",
            "影响范围": f"交易流水当前 {len(std_rows)} 条",
            "修复动作": "使用 add-trade 录入新交易，历史数据先 migrate-preview 再 migrate-apply",
            "验收条件": "交易流水条数 > 0，且新增交易全部带日期/方向/股数/价格/股票关联",
        },
        {
            "缺口项": "股票主档代码完整性",
            "严重级别": "P1" if missing_stock_code else "P3",
            "影响范围": f"缺代码股票 {len(missing_stock_code)}/{len(stock_rows)}",
            "修复动作": "补齐 股票代码 字段；新建股票时名称+代码同时填",
            "验收条件": "股票主档缺代码数量 = 0",
        },
        {
            "缺口项": "分红数据覆盖与关联",
            "严重级别": "P1" if (div_missing_stock or div_missing_amount or div_missing_date) else "P3",
            "影响范围": (
                f"缺股票关联 {len(div_missing_stock)}; 缺金额 {len(div_missing_amount)}; 缺日期 {len(div_missing_date)}"
            ),
            "修复动作": "补齐分红记录的 股票/金额/日期，统一入 分红汇总（标准）",
            "验收条件": "分红记录关键字段缺失数量 = 0",
        },
        {
            "缺口项": "年度收益汇总自动化",
            "严重级别": "P1" if annual_manual_fields else "P3",
            "影响范围": f"手填依赖字段: {','.join(annual_manual_fields) or '无'}; 年度行数 {len(annual_rows)}",
            "修复动作": "执行 sync-annual 按标准交易与分红重算年度表",
            "验收条件": "sync-annual 后年度字段由脚本重算，不依赖手工录入",
        },
    ]

    if as_json:
        print(json.dumps(checklist, ensure_ascii=False, indent=2))
        return 0

    print("Stock结构缺口审计结果")
    print("=" * 80)
    for item in checklist:
        print(f"- 缺口项: {item['缺口项']}")
        print(f"  严重级别: {item['严重级别']}")
        print(f"  影响范围: {item['影响范围']}")
        print(f"  修复动作: {item['修复动作']}")
        print(f"  验收条件: {item['验收条件']}")
    return 0


def stock_index(client: NotionClient, cfg: Cfg) -> Tuple[Dict[str, str], Dict[str, str]]:
    rows = client.query_database_all(cfg.stock_master_id)
    by_name: Dict[str, str] = {}
    by_code: Dict[str, str] = {}
    for r in rows:
        pid = r.get("id")
        name = p_title(r, "股票")
        code = p_rich(r, "股票代码")
        if name and pid and name not in by_name:
            by_name[name] = pid
        if code and pid and code not in by_code:
            by_code[code] = pid
    return by_name, by_code


def extract_candidates(client: NotionClient, cfg: Cfg) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    by_name, _ = stock_index(client, cfg)

    buy_db = client.get_database(cfg.buy_wide_id)
    title_prop_name = find_title_property_name(buy_db)
    number_cols = [k for k, info in buy_db.get("properties", {}).items() if info.get("type") == "number"]

    for row in client.query_database_all(cfg.buy_wide_id):
        src_title = p_title(row, title_prop_name)
        row_id = row.get("id", "")
        for col in number_cols:
            val = p_number(row, col)
            if val is None:
                continue
            stock_id = by_name.get(col, "")
            candidates.append(
                {
                    "记录": f"{src_title or '历史记录'} | {col}",
                    "source_table": "old_buy_record",
                    "source_row_id": row_id,
                    "source_title": src_title,
                    "source_stock_col": col,
                    "source_value": float(val),
                    "import_status": "pending_shares",
                    "stock_id": stock_id,
                }
            )

    for row in client.query_database_all(cfg.t_record_id):
        row_id = row.get("id", "")
        title = p_title(row, "交易")
        date_s = p_date(row, "日期")
        shares = p_number(row, "股数")
        buy_price = p_number(row, "买入价")
        sell_price = p_number(row, "卖出价")
        fee = p_number(row, "手续费")
        tax = p_number(row, "印花税")
        note = p_rich(row, "备注")
        rel = p_relation_ids(row, "股票")

        is_empty = not any(
            [
                title,
                date_s,
                shares is not None,
                buy_price is not None,
                sell_price is not None,
                fee is not None,
                tax is not None,
                note,
                len(rel) > 0,
            ]
        )
        if is_empty:
            continue

        source_value = 0.0
        if shares is not None and buy_price is not None and sell_price is not None:
            source_value = float((sell_price - buy_price) * shares)

        candidates.append(
            {
                "记录": f"{title or '做T历史记录'} | {date_s or 'no-date'}",
                "source_table": "old_t_record",
                "source_row_id": row_id,
                "source_title": title,
                "source_stock_col": "做T交易记录",
                "source_value": source_value,
                "import_status": "pending_shares",
                "stock_id": rel[0] if rel else "",
            }
        )
    return candidates


def existing_source_keys(client: NotionClient, cfg: Cfg) -> set:
    keys = set()
    for row in client.query_database_all(cfg.std_trades_id):
        st = p_select(row, "source_table")
        rid = p_rich(row, "source_row_id")
        scol = p_rich(row, "source_stock_col")
        if st and rid and scol:
            keys.add(f"{st}|{rid}|{scol}")
    return keys


def migrate_preview(client: NotionClient, cfg: Cfg, sample: int) -> int:
    candidates = extract_candidates(client, cfg)
    existed = existing_source_keys(client, cfg)
    todo = [x for x in candidates if f"{x['source_table']}|{x['source_row_id']}|{x['source_stock_col']}" not in existed]

    print(f"候选记录: {len(candidates)}")
    print(f"已存在去重键: {len(existed)}")
    print(f"待导入: {len(todo)}")
    for i, item in enumerate(todo[:sample], start=1):
        print(
            f"{i}. {item['记录']} | source={item['source_table']} | col={item['source_stock_col']} | value={item['source_value']}"
        )
    return 0


def migrate_apply(client: NotionClient, cfg: Cfg, limit: int) -> int:
    candidates = extract_candidates(client, cfg)
    existed = existing_source_keys(client, cfg)
    todo = [x for x in candidates if f"{x['source_table']}|{x['source_row_id']}|{x['source_stock_col']}" not in existed]
    if limit > 0:
        todo = todo[:limit]

    inserted = 0
    for item in todo:
        props: Dict[str, Any] = {
            "记录": title_prop(item["记录"]),
            "source_table": {"select": {"name": item["source_table"]}},
            "source_row_id": text_prop(item["source_row_id"]),
            "source_title": text_prop(item["source_title"]),
            "source_stock_col": text_prop(item["source_stock_col"]),
            "source_value": {"number": item["source_value"]},
            "import_status": {"select": {"name": item["import_status"]}},
        }
        if item.get("stock_id"):
            props["股票"] = {"relation": [{"id": item["stock_id"]}]}

        client.create_page(cfg.std_trades_id, props)
        inserted += 1
        if inserted % 20 == 0:
            print(f"progress {inserted}/{len(todo)}")
        time.sleep(0.12)

    print(f"导入完成: {inserted} 条")
    return 0


def add_trade(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    if args.direction not in {"BUY", "SELL"}:
        raise ValueError("direction must be BUY or SELL")

    try:
        dt.datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError("--date must be YYYY-MM-DD") from e

    by_name, by_code = stock_index(client, cfg)
    stock_id = by_code.get(args.stock) or by_name.get(args.stock)
    if not stock_id:
        raise ValueError(f"stock '{args.stock}' not found in 股票主档 title or 股票代码")

    title = f"{args.date} {args.direction} {args.stock} {args.shares}@{args.price}"
    props: Dict[str, Any] = {
        "记录": title_prop(title),
        "日期": {"date": {"start": args.date}},
        "方向": {"select": {"name": args.direction}},
        "股数": {"number": float(args.shares)},
        "价格": {"number": float(args.price)},
        "手续费": {"number": float(args.fee)},
        "税费": {"number": float(args.tax)},
        "股票": {"relation": [{"id": stock_id}]},
        "source_table": {"select": {"name": "manual"}},
        "import_status": {"select": {"name": "ready"}},
    }
    if args.strategy:
        props["策略"] = {"select": {"name": args.strategy}}
    if args.note:
        props["备注"] = {"rich_text": [{"type": "text", "text": {"content": args.note[:2000]}}]}

    page = client.create_page(cfg.std_trades_id, props)
    print(f"新增交易成功: id={page.get('id')}")
    return 0


def validate_manual_entries(client: NotionClient, cfg: Cfg) -> int:
    rows = client.query_database_all(cfg.std_trades_id)
    required = ["日期", "方向", "股数", "价格", "股票", "记录"]
    failures: List[Tuple[str, List[str]]] = []
    checked = 0

    for r in rows:
        source = p_select(r, "source_table")
        if source and source != "manual":
            continue
        checked += 1
        missing: List[str] = []
        if not p_date(r, "日期"):
            missing.append("日期")
        if p_select(r, "方向") not in {"BUY", "SELL"}:
            missing.append("方向")
        if p_number(r, "股数") is None:
            missing.append("股数")
        if p_number(r, "价格") is None:
            missing.append("价格")
        if len(p_relation_ids(r, "股票")) == 0:
            missing.append("股票")
        if not p_title(r, "记录"):
            missing.append("记录")
        if missing:
            failures.append((r.get("id", ""), missing))

    print(f"检查记录数(manual/未标注): {checked}")
    print(f"不合规记录数: {len(failures)}")
    for rid, missing in failures[:20]:
        print(f"- {rid}: 缺失 {','.join(missing)}")
    return 0 if not failures else 2


def parse_year(date_text: str) -> str:
    if not date_text:
        return ""
    return date_text[:4]


def annual_sync(client: NotionClient, cfg: Cfg, dry_run: bool) -> int:
    tx_rows = client.query_database_all(cfg.std_trades_id)
    div_rows = client.query_database_all(cfg.std_dividend_id)
    annual_rows = client.query_database_all(cfg.annual_id)

    totals: Dict[str, Dict[str, float]] = {}

    def init_year(y: str) -> None:
        if y not in totals:
            totals[y] = {"已实现收益": 0.0, "做T收益": 0.0, "分红收益": 0.0}

    for row in tx_rows:
        y = parse_year(p_date(row, "日期"))
        if not y:
            continue
        init_year(y)
        realized = p_formula_number(row, "单笔已实现收益")
        t_profit = p_formula_number(row, "单笔做T收益")
        if realized is not None:
            totals[y]["已实现收益"] += float(realized)
        if t_profit is not None:
            totals[y]["做T收益"] += float(t_profit)

    for row in div_rows:
        y = parse_year(p_date(row, "日期"))
        if not y:
            continue
        init_year(y)
        amount = p_number(row, "金额")
        if amount is not None:
            totals[y]["分红收益"] += float(amount)

    annual_title = "年份"
    annual_index: Dict[str, str] = {}
    for row in annual_rows:
        year = p_title(row, annual_title)
        if year:
            annual_index[year] = row.get("id", "")

    years = sorted(totals.keys())
    print(f"将同步年份: {', '.join(years) if years else '(none)'}")
    for y in years:
        vals = totals[y]
        print(f"- {y}: 已实现={vals['已实现收益']:.2f}, 做T={vals['做T收益']:.2f}, 分红={vals['分红收益']:.2f}")

    if dry_run:
        print("dry-run: 未写入年度收益汇总（标准）")
        return 0

    for y in years:
        vals = totals[y]
        props = {
            "已实现收益": {"number": round(vals["已实现收益"], 6)},
            "做T收益": {"number": round(vals["做T收益"], 6)},
            "分红收益": {"number": round(vals["分红收益"], 6)},
        }
        if y in annual_index and annual_index[y]:
            client.update_page(annual_index[y], props)
        else:
            create_props = {
                "年份": title_prop(y),
                "已实现收益": {"number": round(vals["已实现收益"], 6)},
                "做T收益": {"number": round(vals["做T收益"], 6)},
                "分红收益": {"number": round(vals["分红收益"], 6)},
            }
            client.create_page(cfg.annual_id, create_props)
    print(f"年度汇总同步完成: {len(years)} 年")
    return 0


@dataclass
class TradePoint:
    date: str
    price: Optional[float]
    shares: float
    direction: str
    realized: Optional[float]
    stock_id: str


def _find_prop_name(
    db_props: Dict[str, Dict[str, Any]],
    candidates: List[str],
    expected_types: Optional[List[str]] = None,
) -> Optional[str]:
    for name in candidates:
        info = db_props.get(name)
        if not info:
            continue
        if expected_types and info.get("type") not in expected_types:
            continue
        return name
    return None


def _resolve_stock_fields(stock_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = stock_db.get("properties", {})
    return {
        "title": find_title_property_name(stock_db),
        "stock_code": _find_prop_name(props, ["股票代码", "鑲＄エ浠ｇ爜"], ["rich_text"]),
        "current_price": _find_prop_name(props, ["当前市价", "最新价", "现价"], ["number"]),
        "current_cost": _find_prop_name(props, ["当前持仓成本", "持仓成本", "成本价"], ["number"]),
        "out_action": _find_prop_name(props, ["建议动作"], ["select", "status", "rich_text"]),
        "out_buy": _find_prop_name(props, ["建议买入价"], ["number"]),
        "out_sell": _find_prop_name(props, ["建议卖出价"], ["number"]),
        "out_stop": _find_prop_name(props, ["建议止损价"], ["number"]),
        "out_pos": _find_prop_name(props, ["建议仓位变化"], ["number"]),
        "out_conf": _find_prop_name(props, ["建议置信度"], ["select", "status", "rich_text"]),
        "out_mode": _find_prop_name(props, ["建议模式"], ["select", "status", "rich_text"]),
        "out_reason": _find_prop_name(props, ["建议原因", "触发原因"], ["rich_text", "title"]),
        "out_time": _find_prop_name(props, ["建议更新时间"], ["date", "rich_text"]),
    }


def _resolve_trade_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    return {
        "date": _find_prop_name(props, ["日期"], ["date"]),
        "direction": _find_prop_name(props, ["方向"], ["select", "status"]),
        "shares": _find_prop_name(props, ["股数"], ["number"]),
        "price": _find_prop_name(props, ["价格"], ["number"]),
        "stock": _find_prop_name(props, ["股票"], ["relation"]),
        "realized": _find_prop_name(props, ["单笔已实现收益"], ["formula", "number"]),
    }


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


def _resolve_stock_fields_runtime(stock_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    fields = _resolve_stock_fields(stock_db)
    props = stock_db.get("properties", {})

    if not fields.get("stock_code"):
        fields["stock_code"] = _find_prop_by_keywords(props, ["股票代码", "代码"], ["rich_text", "title"])
    if not fields.get("current_price"):
        fields["current_price"] = _find_prop_by_keywords(props, ["当前市价", "最新价", "现价", "市价"], ["number"])
    if not fields.get("current_cost"):
        fields["current_cost"] = _find_prop_by_keywords(props, ["持仓成本", "成本"], ["number"])
    if not fields.get("out_action"):
        fields["out_action"] = _find_prop_by_keywords(props, ["建议动作"], ["select", "status", "rich_text"])
    if not fields.get("out_buy"):
        fields["out_buy"] = _find_prop_by_keywords(props, ["建议买入价"], ["number"])
    if not fields.get("out_sell"):
        fields["out_sell"] = _find_prop_by_keywords(props, ["建议卖出价"], ["number"])
    if not fields.get("out_stop"):
        fields["out_stop"] = _find_prop_by_keywords(props, ["建议止损价"], ["number"])
    if not fields.get("out_pos"):
        fields["out_pos"] = _find_prop_by_keywords(props, ["建议仓位变化"], ["number"])
    if not fields.get("out_conf"):
        fields["out_conf"] = _find_prop_by_keywords(props, ["建议置信度"], ["select", "status", "rich_text"])
    if not fields.get("out_mode"):
        fields["out_mode"] = _find_prop_by_keywords(props, ["建议模式"], ["select", "status", "rich_text"])
    if not fields.get("out_reason"):
        fields["out_reason"] = _find_prop_by_keywords(props, ["建议原因", "触发原因"], ["rich_text", "title"])
    if not fields.get("out_time"):
        fields["out_time"] = _find_prop_by_keywords(props, ["建议更新时间"], ["date", "rich_text"])

    return fields


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
    for i in range(0, len(symbols), 60):
        batch = symbols[i : i + 60]
        url = f"https://qt.gtimg.cn/q={','.join(batch)}"
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        text = resp.content.decode("gbk", errors="ignore")
        for item in text.split(";"):
            item = item.strip()
            if not item:
                continue
            symbol, price = _parse_tencent_quote_line(item)
            if symbol and price is not None:
                out[symbol] = price
    return out


def sync_prices(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db = client.get_database(cfg.stock_master_id)
    fields = _resolve_stock_fields_runtime(stock_db)
    if not fields.get("stock_code"):
        raise RuntimeError("无法定位股票代码字段（需要 rich_text/title 类型）")
    if not fields.get("current_price"):
        raise RuntimeError("无法定位当前市价字段（需要 number 类型）")

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

    prices = _fetch_realtime_prices_tencent(sorted(symbol_to_row.keys()), timeout=args.timeout)
    updated = 0
    skipped = 0
    for symbol, (row_id, _) in symbol_to_row.items():
        price = prices.get(symbol)
        if price is None:
            skipped += 1
            continue
        if not args.dry_run:
            payload = _write_prop_value(db_props, fields["current_price"], price)
            if payload is not None:
                client.update_page(row_id, {fields["current_price"]: payload})
        updated += 1

    result = {
        "total_symbols": len(symbol_to_row),
        "price_fetched": len(prices),
        "updated": updated,
        "skipped": skipped,
        "dry_run": bool(args.dry_run),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _safe_mean(values: List[float]) -> float:
    return float(statistics.mean(values)) if values else 0.0


def _safe_stdev(values: List[float]) -> float:
    return float(statistics.pstdev(values)) if len(values) > 1 else 0.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _coerce_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def _write_prop_value(db_props: Dict[str, Dict[str, Any]], prop_name: str, value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    prop_info = db_props.get(prop_name)
    if not prop_info:
        return None
    typ = prop_info.get("type")
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


def _build_trade_points(
    trade_rows: List[Dict[str, Any]],
    field_map: Dict[str, Optional[str]],
) -> Dict[str, List[TradePoint]]:
    stock_points: Dict[str, List[TradePoint]] = defaultdict(list)
    if not field_map["stock"]:
        return stock_points

    for row in trade_rows:
        stock_ids = p_relation_ids(row, field_map["stock"])
        if not stock_ids:
            continue
        date_s = p_date(row, field_map["date"]) if field_map["date"] else ""
        direction = p_select(row, field_map["direction"]) if field_map["direction"] else ""
        shares = p_number(row, field_map["shares"]) if field_map["shares"] else None
        price = p_number(row, field_map["price"]) if field_map["price"] else None

        realized: Optional[float] = None
        if field_map["realized"]:
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


def _confidence_from_history(sample_count: int, returns: List[float], realized_values: List[float]) -> float:
    sample_score = min(sample_count / 40.0, 1.0) * 0.35
    vol = _safe_stdev(returns)
    vol_score = (1.0 - min(vol / 0.06, 1.0)) * 0.25
    realized_avg = _safe_mean(realized_values)
    realized_score = 0.15 if realized_avg > 0 else 0.05
    trend_consistency = (1.0 - min(abs(_safe_mean(returns)) / 0.12, 1.0)) * 0.15
    data_score = 0.10 if sample_count > 0 else 0.0
    return _clamp(sample_score + vol_score + realized_score + trend_consistency + data_score, 0.0, 1.0)


def _confidence_level(score: float) -> str:
    if score >= 0.75:
        return "HIGH"
    if score >= 0.55:
        return "MEDIUM"
    return "LOW"


def _level_rank(level: str) -> int:
    return {"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get(level, 0)


def _recommend_from_points(
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
) -> Dict[str, Any]:
    if current_price is None or current_price <= 0:
        return {
            "action": "HOLD",
            "buy_price": None,
            "sell_price": None,
            "stop_price": None,
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "行情或价格缺失，无法生成有效建议",
            "sample_count": len(points),
        }

    valid_points = [p for p in points if p.price is not None and p.price > 0]
    sample_count = len(valid_points)
    mode = "FULL_MODEL" if sample_count >= 20 else "TREND_FALLBACK"
    if sample_count < 20 and not allow_small_sample:
        return {
            "action": "HOLD",
            "buy_price": round(current_price, 4),
            "sell_price": round(current_price, 4),
            "stop_price": round(current_price * 0.98, 4),
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "样本不足，且未开启小样本建议",
            "sample_count": sample_count,
        }

    prices = [p.price for p in valid_points if p.price is not None]
    returns: List[float] = []
    for i in range(1, len(prices)):
        prev = prices[i - 1]
        curr = prices[i]
        if prev > 0:
            returns.append((curr - prev) / prev)

    realized_values = [p.realized for p in valid_points if p.realized is not None]
    conf_score = _confidence_from_history(sample_count, returns[-30:], realized_values[-30:])
    conf_level = _confidence_level(conf_score)
    if mode == "TREND_FALLBACK" and conf_level == "HIGH":
        conf_level = "MEDIUM"

    vol = _safe_stdev(returns[-20:]) if returns else 0.02
    if vol > 0.10:
        return {
            "action": "HOLD",
            "buy_price": round(current_price * 0.99, 4),
            "sell_price": round(current_price * 1.01, 4),
            "stop_price": round(current_price * 0.97, 4),
            "position_delta": 0.0,
            "confidence": conf_level,
            "mode": mode,
            "reason": "波动异常放大，触发不交易条件",
            "sample_count": sample_count,
        }

    ma_window = min(20, len(prices))
    moving_avg = _safe_mean(prices[-ma_window:]) if ma_window > 0 else current_price
    trend = (current_price / moving_avg - 1.0) if moving_avg > 0 else 0.0
    band = _clamp(max(vol * 1.2, 0.01), 0.01, 0.12)

    buy_price = current_price * (1.0 - band)
    sell_price = current_price * (1.0 + band)
    stop_price = current_price * (1.0 - band * 1.8)

    if current_cost is not None and current_cost > 0:
        buy_price = min(buy_price, current_cost * 0.995)
        sell_price = max(sell_price, current_cost * 1.005)

    action = "HOLD"
    reason = "信号中性，维持观望"
    if trend > 0.015:
        action = "BUY"
        reason = "趋势向上，建议逢回落分批买入"
    elif trend < -0.015:
        action = "SELL"
        reason = "趋势转弱，建议逢反弹减仓"

    expected_up = max(0.0, (sell_price - current_price) / current_price)
    expected_down = max(0.0, (current_price - stop_price) / current_price)
    if expected_up <= expected_down * 0.8:
        action = "HOLD"
        reason = "预期风险收益比劣于维持仓位，触发不交易条件"

    if _level_rank(conf_level) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"建议置信度 {conf_level} 低于阈值 {min_confidence}"

    pos_base = {"HIGH": 0.15, "MEDIUM": 0.10, "LOW": 0.05}[conf_level]
    if mode == "TREND_FALLBACK":
        pos_base *= 0.5
        reason = f"{reason}; 样本不足，趋势回退模型"

    pos_delta = 0.0
    if action == "BUY":
        pos_delta = pos_base
    elif action == "SELL":
        pos_delta = -pos_base

    if mode == "TREND_FALLBACK" and conf_level == "LOW" and action == "BUY":
        action = "HOLD"
        pos_delta = 0.0
        reason = "低置信度下不新开仓，维持观望"

    return {
        "action": action,
        "buy_price": round(buy_price, 4),
        "sell_price": round(sell_price, 4),
        "stop_price": round(stop_price, 4),
        "position_delta": round(pos_delta, 4),
        "confidence": conf_level,
        "mode": mode,
        "reason": reason,
        "sample_count": sample_count,
    }


def recommend_prices(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db = client.get_database(cfg.stock_master_id)
    trade_db = client.get_database(cfg.std_trades_id)
    stock_fields = _resolve_stock_fields_runtime(stock_db)
    trade_fields = _resolve_trade_fields(trade_db)

    for key in ["date", "direction", "shares", "price", "stock"]:
        if not trade_fields[key]:
            raise RuntimeError(f"Missing required standard-trade field mapping: {key}")

    stock_rows = client.query_database_all(cfg.stock_master_id)
    if getattr(args, "refresh_prices", False):
        sync_prices(
            client,
            cfg,
            argparse.Namespace(dry_run=bool(getattr(args, "dry_run", False)), timeout=getattr(args, "timeout", 8)),
        )
        stock_rows = client.query_database_all(cfg.stock_master_id)
    trade_rows = client.query_database_all(cfg.std_trades_id)
    stock_points = _build_trade_points(trade_rows, trade_fields)

    db_props = stock_db.get("properties", {})
    asof_date = args.asof_date or dt.date.today().isoformat()

    recs: List[Dict[str, Any]] = []
    for row in stock_rows:
        stock_id = row.get("id", "")
        title = p_title(row, stock_fields["title"]) if stock_fields["title"] else stock_id
        current_price = p_number(row, stock_fields["current_price"]) if stock_fields["current_price"] else None
        current_cost = p_number(row, stock_fields["current_cost"]) if stock_fields["current_cost"] else None
        rec = _recommend_from_points(
            current_price=current_price,
            current_cost=current_cost,
            points=stock_points.get(stock_id, []),
            allow_small_sample=args.allow_small_sample,
            min_confidence=args.min_confidence,
        )
        rec["stock_id"] = stock_id
        rec["stock_name"] = title
        recs.append(rec)

        if args.dry_run:
            continue

        props: Dict[str, Any] = {}
        write_map = [
            (stock_fields["out_action"], rec["action"]),
            (stock_fields["out_buy"], rec["buy_price"]),
            (stock_fields["out_sell"], rec["sell_price"]),
            (stock_fields["out_stop"], rec["stop_price"]),
            (stock_fields["out_pos"], rec["position_delta"]),
            (stock_fields["out_conf"], rec["confidence"]),
            (stock_fields["out_mode"], rec["mode"]),
            (stock_fields["out_reason"], rec["reason"]),
            (stock_fields["out_time"], asof_date),
        ]
        for prop_name, value in write_map:
            if not prop_name:
                continue
            payload = _write_prop_value(db_props, prop_name, value)
            if payload is not None:
                props[prop_name] = payload
        if props:
            client.update_page(stock_id, props)

    print(json.dumps(recs, ensure_ascii=False, indent=2))
    return 0


def _max_drawdown(returns: List[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in returns:
        equity *= (1.0 + r)
        peak = max(peak, equity)
        dd = (peak - equity) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd


def _returns_metrics(returns: List[float]) -> Dict[str, float]:
    if not returns:
        return {"count": 0.0, "mean": 0.0, "vol": 0.0, "sharpe_like": 0.0, "max_drawdown": 0.0}
    mean_r = _safe_mean(returns)
    vol = _safe_stdev(returns)
    sharpe_like = (mean_r / vol * math.sqrt(len(returns))) if vol > 0 else 0.0
    return {
        "count": float(len(returns)),
        "mean": mean_r,
        "vol": vol,
        "sharpe_like": sharpe_like,
        "max_drawdown": _max_drawdown(returns),
    }


def backtest_recommendation(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    trade_db = client.get_database(cfg.std_trades_id)
    trade_fields = _resolve_trade_fields(trade_db)
    for key in ["date", "direction", "shares", "price", "stock"]:
        if not trade_fields[key]:
            raise RuntimeError(f"Missing required standard-trade field mapping: {key}")

    trade_rows = client.query_database_all(cfg.std_trades_id)
    stock_points = _build_trade_points(trade_rows, trade_fields)

    mode_returns: Dict[str, List[float]] = defaultdict(list)
    baseline_returns: List[float] = []
    all_strategy_returns: List[float] = []

    for points in stock_points.values():
        valid = [p for p in points if p.price is not None and p.price > 0]
        if len(valid) < 8:
            continue
        for idx in range(5, len(valid) - 1):
            hist = valid[max(0, idx - args.window) : idx]
            curr = valid[idx]
            nxt = valid[idx + 1]
            if not curr.price or not nxt.price:
                continue
            rec = _recommend_from_points(
                current_price=curr.price,
                current_cost=None,
                points=hist,
                allow_small_sample=args.allow_small_sample,
                min_confidence=args.min_confidence,
            )

            move = (nxt.price - curr.price) / curr.price
            if rec["action"] == "BUY":
                strategy_ret = move
            elif rec["action"] == "SELL":
                strategy_ret = -move
            else:
                strategy_ret = 0.0

            actual_dir = curr.direction
            if actual_dir == "BUY":
                baseline_ret = move
            elif actual_dir == "SELL":
                baseline_ret = -move
            else:
                baseline_ret = 0.0

            mode_returns[rec["mode"]].append(strategy_ret)
            baseline_returns.append(baseline_ret)
            all_strategy_returns.append(strategy_ret)

    out = {
        "baseline": _returns_metrics(baseline_returns),
        "strategy_all": _returns_metrics(all_strategy_returns),
        "strategy_by_mode": {k: _returns_metrics(v) for k, v in mode_returns.items()},
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Stock page pipeline for audit, migration and automation.")
    p.add_argument("--token", default=os.getenv("NOTION_TOKEN"))
    p.add_argument("--notion-version", default=os.getenv("NOTION_VERSION", "2022-06-28"))

    sub = p.add_subparsers(dest="cmd", required=True)

    s_audit = sub.add_parser("audit", help="输出结构缺口审计清单")
    s_audit.add_argument("--json", action="store_true")

    s_mp = sub.add_parser("migrate-preview", help="预览历史数据迁移结果")
    s_mp.add_argument("--sample", type=int, default=20)

    s_ma = sub.add_parser("migrate-apply", help="执行历史数据迁移到交易流水（标准）")
    s_ma.add_argument("--limit", type=int, default=0, help="0=全部，其它=仅导入前N条")

    s_add = sub.add_parser("add-trade", help="新增一笔标准交易")
    s_add.add_argument("--date", required=True, help="YYYY-MM-DD")
    s_add.add_argument("--direction", required=True, choices=["BUY", "SELL"])
    s_add.add_argument("--stock", required=True, help="股票名称或股票代码")
    s_add.add_argument("--shares", required=True, type=float)
    s_add.add_argument("--price", required=True, type=float)
    s_add.add_argument("--fee", type=float, default=0.0)
    s_add.add_argument("--tax", type=float, default=0.0)
    s_add.add_argument("--strategy", default="")
    s_add.add_argument("--note", default="")

    sub.add_parser("validate-manual", help="校验手工录入是否满足必填约束")

    s_ys = sub.add_parser("sync-annual", help="按交易+分红重算年度收益汇总")
    s_ys.add_argument("--dry-run", action="store_true")

    s_rec = sub.add_parser("recommend-prices", help="计算并回写每只股票的下一次交易建议价位")
    s_rec.add_argument("--dry-run", action="store_true", help="仅输出建议，不回写Notion")
    s_rec.add_argument("--asof-date", default="", help="建议更新时间，默认今天 YYYY-MM-DD")
    s_rec.add_argument("--allow-small-sample", action="store_true", default=True)
    s_rec.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    s_rec.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    s_rec.add_argument("--refresh-prices", action="store_true", help="先自动拉取实时当前市价，再计算建议")
    s_rec.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")

    s_bt = sub.add_parser("backtest-recommendation", help="回测建议模型，输出FULL_MODEL与TREND_FALLBACK绩效")
    s_bt.add_argument("--window", type=int, default=60, help="历史窗口长度（按交易事件）")
    s_bt.add_argument("--allow-small-sample", action="store_true", default=True)
    s_bt.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    s_bt.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")

    s_sp = sub.add_parser("sync-prices", help="自动拉取实时行情并回写当前市价")
    s_sp.add_argument("--dry-run", action="store_true", help="仅拉取并输出统计，不写入Notion")
    s_sp.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")

    return p


def main() -> int:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(script_dir)
    load_dotenv(os.path.join(root, ".env"))

    parser = build_parser()
    args = parser.parse_args()

    if not args.token:
        print("Missing NOTION_TOKEN.", file=sys.stderr)
        return 1

    client = NotionClient(token=args.token, version=args.notion_version)
    cfg = load_cfg()

    if args.cmd == "audit":
        return audit(client, cfg, as_json=args.json)
    if args.cmd == "migrate-preview":
        return migrate_preview(client, cfg, sample=args.sample)
    if args.cmd == "migrate-apply":
        return migrate_apply(client, cfg, limit=args.limit)
    if args.cmd == "add-trade":
        return add_trade(client, cfg, args)
    if args.cmd == "validate-manual":
        return validate_manual_entries(client, cfg)
    if args.cmd == "sync-annual":
        return annual_sync(client, cfg, dry_run=args.dry_run)
    if args.cmd == "recommend-prices":
        return recommend_prices(client, cfg, args)
    if args.cmd == "backtest-recommendation":
        return backtest_recommendation(client, cfg, args)
    if args.cmd == "sync-prices":
        return sync_prices(client, cfg, args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
