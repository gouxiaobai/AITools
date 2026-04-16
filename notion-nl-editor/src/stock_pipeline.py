import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

import requests
from requests import exceptions as req_exc
from param_store import ParamStore, SCHEMA_VERSION


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
        self.session = requests.Session()
        self.max_retries = int(os.getenv("NOTION_HTTP_RETRIES", "4"))
        self.retry_backoff = float(os.getenv("NOTION_HTTP_RETRY_BACKOFF", "0.8"))

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        attempts = max(self.max_retries, 1)
        for i in range(attempts):
            try:
                resp = self.session.request(method, url, headers=self.headers, json=payload, timeout=30)
                if resp.status_code >= 400:
                    raise RuntimeError(f"Notion API error {resp.status_code} {path}: {resp.text}")
                if not resp.text:
                    return {}
                return resp.json()
            except (req_exc.SSLError, req_exc.ConnectionError, req_exc.Timeout) as e:
                if i == attempts - 1:
                    raise RuntimeError(f"Notion request failed after {attempts} attempts {path}: {e}") from e
                time.sleep(self.retry_backoff * (2**i))

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
    strategy_snapshot_id: str
    cash_config_id: str


def load_cfg() -> Cfg:
    return Cfg(
        stock_master_id=os.getenv("DB_STOCK_MASTER_ID", "9ff0bf7d-9ae4-41c8-9440-729daaa2a95d"),
        std_trades_id=os.getenv("DB_STD_TRADES_ID", "33c225a4-e273-810f-ae9f-d44f9d44d528"),
        std_dividend_id=os.getenv("DB_STD_DIVIDEND_ID", "33c225a4-e273-8112-9444-f798532e60cf"),
        annual_id=os.getenv("DB_ANNUAL_ID", "33c225a4-e273-8162-8804-dfde58582535"),
        buy_wide_id=os.getenv("DB_BUY_WIDE_ID", "0d485b47-e903-4fd3-901e-1bb4d09200f1"),
        t_record_id=os.getenv("DB_T_RECORD_ID", "93dde4b0-5d6f-4c49-a825-e49ae95be420"),
        strategy_snapshot_id=os.getenv("DB_STRATEGY_SNAPSHOT_ID", ""),
        cash_config_id=os.getenv("DB_CASH_CONFIG_ID", ""),
    )


def _sqlite_path() -> str:
    raw = os.getenv("SQLITE_PATH", "./data/strategy_snapshots.db")
    return os.path.abspath(raw)


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _today_or(raw: str) -> str:
    date_text = (raw or "").strip()
    if not date_text:
        return dt.date.today().isoformat()
    dt.datetime.strptime(date_text, "%Y-%m-%d")
    return date_text


def _guess_market(stock_code: str) -> str:
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


def _market_from_rule(stock_code: str, raw_rule: str) -> str:
    if not raw_rule:
        return _guess_market(stock_code)
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
    return _guess_market(stock_code)


def _prop_text_any(page: Dict[str, Any], key: str) -> str:
    prop = get_prop(page, key)
    typ = prop.get("type")
    if typ == "title":
        return rt_plain(prop.get("title", []))
    if typ == "rich_text":
        return rt_plain(prop.get("rich_text", []))
    if typ == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if typ == "status":
        sel = prop.get("status")
        return sel.get("name", "") if sel else ""
    if typ == "date":
        date_obj = prop.get("date")
        if date_obj:
            return date_obj.get("start", "")
        return ""
    if typ == "number":
        num = prop.get("number")
        return "" if num is None else str(num)
    return ""


class SnapshotStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        _ensure_parent_dir(db_path)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_daily_snapshot (
                snapshot_date TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                stock_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                market TEXT NOT NULL,
                strategy_mode TEXT NOT NULL,
                ret_1d REAL NOT NULL,
                hit_flag INTEGER NOT NULL,
                max_drawdown REAL NOT NULL,
                confidence TEXT NOT NULL,
                sample_count INTEGER NOT NULL,
                action TEXT NOT NULL,
                buy_price REAL,
                sell_price REAL,
                stop_price REAL,
                position_delta REAL NOT NULL,
                run_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (snapshot_date, strategy_id, stock_id)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshot_date_strategy ON strategy_daily_snapshot (snapshot_date, strategy_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshot_date_market ON strategy_daily_snapshot (snapshot_date, market)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshot_stock_strategy ON strategy_daily_snapshot (stock_id, strategy_id)"
        )
        self.conn.commit()

    def upsert_many(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO strategy_daily_snapshot (
                snapshot_date, strategy_id, stock_id, stock_code, stock_name, market, strategy_mode,
                ret_1d, hit_flag, max_drawdown, confidence, sample_count, action,
                buy_price, sell_price, stop_price, position_delta, run_id, created_at, updated_at
            ) VALUES (
                :snapshot_date, :strategy_id, :stock_id, :stock_code, :stock_name, :market, :strategy_mode,
                :ret_1d, :hit_flag, :max_drawdown, :confidence, :sample_count, :action,
                :buy_price, :sell_price, :stop_price, :position_delta, :run_id, :created_at, :updated_at
            )
            ON CONFLICT(snapshot_date, strategy_id, stock_id) DO UPDATE SET
                stock_code=excluded.stock_code,
                stock_name=excluded.stock_name,
                market=excluded.market,
                strategy_mode=excluded.strategy_mode,
                ret_1d=excluded.ret_1d,
                hit_flag=excluded.hit_flag,
                max_drawdown=excluded.max_drawdown,
                confidence=excluded.confidence,
                sample_count=excluded.sample_count,
                action=excluded.action,
                buy_price=excluded.buy_price,
                sell_price=excluded.sell_price,
                stop_price=excluded.stop_price,
                position_delta=excluded.position_delta,
                run_id=excluded.run_id,
                updated_at=excluded.updated_at
        """
        with self.conn:
            self.conn.executemany(sql, rows)
        return len(rows)

    def query_range(
        self,
        start_date: str,
        end_date: str,
        strategies: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT * FROM strategy_daily_snapshot
            WHERE snapshot_date >= ? AND snapshot_date <= ?
        """
        args: List[Any] = [start_date, end_date]
        if strategies:
            marks = ",".join(["?"] * len(strategies))
            sql += f" AND strategy_id IN ({marks})"
            args.extend([x.upper() for x in strategies])
        if markets:
            marks = ",".join(["?"] * len(markets))
            sql += f" AND market IN ({marks})"
            args.extend([x.upper() for x in markets])
        sql += " ORDER BY snapshot_date ASC, strategy_id ASC, stock_code ASC"
        cur = self.conn.execute(sql, args)
        return [dict(row) for row in cur.fetchall()]


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
    for k in ["T收益", "分红收益", "已实现收益"]:
        if annual_props.get(k, {}).get("type") == "number":
            annual_manual_fields.append(k)

    checklist = [
        {
            "item": "标准交易流水覆盖",
            "severity": "P0" if len(std_rows) == 0 else "P2",
            "impact": f"标准交易记录 {len(std_rows)} 条",
            "fix": "先 migrate-preview，再 migrate-apply；新增交易使用 add-trade",
            "acceptance": "标准交易流水可持续增长，关键字段完整",
        },
        {
            "item": "股票代码完整性",
            "severity": "P1" if missing_stock_code else "P3",
            "impact": f"缺少股票代码 {len(missing_stock_code)}/{len(stock_rows)}",
            "fix": "补齐股票代码字段",
            "acceptance": "股票主档缺代码数量为 0",
        },
        {
            "item": "分红记录完整性",
            "severity": "P1" if (div_missing_stock or div_missing_amount or div_missing_date) else "P3",
            "impact": f"缺股票关联 {len(div_missing_stock)}; 缺金额 {len(div_missing_amount)}; 缺日期 {len(div_missing_date)}",
            "fix": "补齐分红记录关键字段",
            "acceptance": "分红关键字段缺失数为 0",
        },
        {
            "item": "年度收益自动化",
            "severity": "P1" if annual_manual_fields else "P3",
            "impact": f"手填字段: {','.join(annual_manual_fields) or '无'}; 年度记录 {len(annual_rows)}",
            "fix": "执行 sync-annual 按标准交易与分红重算",
            "acceptance": "年度收益由脚本重算，不依赖手工录入",
        },
    ]

    if as_json:
        print(json.dumps(checklist, ensure_ascii=False, indent=2))
        return 0

    print("Stock audit checklist")
    print("=" * 80)
    for item in checklist:
        print(f"- Item: {item['item']}")
        print(f"  Severity: {item['severity']}")
        print(f"  Impact: {item['impact']}")
        print(f"  Fix: {item['fix']}")
        print(f"  Acceptance: {item['acceptance']}")
    return 0
def stock_index(client: NotionClient, cfg: Cfg) -> Tuple[Dict[str, str], Dict[str, str]]:
    stock_db = client.get_database(cfg.stock_master_id)
    fields = _resolve_stock_fields_runtime(stock_db)
    rows = client.query_database_all(cfg.stock_master_id)
    by_name: Dict[str, str] = {}
    by_code: Dict[str, str] = {}
    for r in rows:
        pid = r.get("id")
        name = p_title(r, fields["title"]) if fields.get("title") else ""
        code = _prop_text_any(r, fields["stock_code"]) if fields.get("stock_code") else ""
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
                    "璁板綍": f"{src_title or '鍘嗗彶璁板綍'} | {col}",
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
        title = p_title(row, "浜ゆ槗")
        date_s = p_date(row, "鏃ユ湡")
        shares = p_number(row, "鑲℃暟")
        buy_price = p_number(row, "买入价")
        sell_price = p_number(row, "卖出价")
        fee = p_number(row, "手续费")
        tax = p_number(row, "印花税")
        note = p_rich(row, "澶囨敞")
        rel = p_relation_ids(row, "鑲＄エ")

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
                "璁板綍": f"{title or '鍋歍鍘嗗彶璁板綍'} | {date_s or 'no-date'}",
                "source_table": "old_t_record",
                "source_row_id": row_id,
                "source_title": title,
                "source_stock_col": "鍋歍浜ゆ槗璁板綍",
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

    print(f"鍊欓€夎褰? {len(candidates)}")
    print(f"宸插瓨鍦ㄥ幓閲嶉敭: {len(existed)}")
    print(f"寰呭鍏? {len(todo)}")
    for i, item in enumerate(todo[:sample], start=1):
        print(
            f"{i}. {item['璁板綍']} | source={item['source_table']} | col={item['source_stock_col']} | value={item['source_value']}"
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
            "璁板綍": title_prop(item["璁板綍"]),
            "source_table": {"select": {"name": item["source_table"]}},
            "source_row_id": text_prop(item["source_row_id"]),
            "source_title": text_prop(item["source_title"]),
            "source_stock_col": text_prop(item["source_stock_col"]),
            "source_value": {"number": item["source_value"]},
            "import_status": {"select": {"name": item["import_status"]}},
        }
        if item.get("stock_id"):
            props["鑲＄エ"] = {"relation": [{"id": item["stock_id"]}]}

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
        raise ValueError(f"stock '{args.stock}' not found in stock master by title/code")

    title = f"{args.date} {args.direction} {args.stock} {args.shares}@{args.price}"
    trade_db = client.get_database(cfg.std_trades_id)
    write_fields = _resolve_trade_write_fields(trade_db)
    db_props = trade_db.get("properties", {})
    props: Dict[str, Any] = {}

    write_map = [
        (write_fields.get("title"), title),
        (write_fields.get("date"), args.date),
        (write_fields.get("direction"), args.direction),
        (write_fields.get("shares"), float(args.shares)),
        (write_fields.get("price"), float(args.price)),
        (write_fields.get("fee"), float(args.fee)),
        (write_fields.get("tax"), float(args.tax)),
        (write_fields.get("source_table"), "manual"),
        (write_fields.get("import_status"), "ready"),
    ]
    for prop_name, value in write_map:
        if not prop_name:
            continue
        payload = _write_prop_value(db_props, prop_name, value)
        if payload is not None:
            props[prop_name] = payload
    if write_fields.get("stock"):
        props[write_fields["stock"]] = {"relation": [{"id": stock_id}]}
    if args.strategy and write_fields.get("strategy"):
        payload = _write_prop_value(db_props, write_fields["strategy"], args.strategy)
        if payload is not None:
            props[write_fields["strategy"]] = payload
    if args.note and write_fields.get("note"):
        payload = _write_prop_value(db_props, write_fields["note"], args.note[:2000])
        if payload is not None:
            props[write_fields["note"]] = payload

    page = client.create_page(cfg.std_trades_id, props)
    print(f"新增交易成功: id={page.get('id')}")
    return 0


def validate_manual_entries(client: NotionClient, cfg: Cfg) -> int:
    trade_db = client.get_database(cfg.std_trades_id)
    rows = client.query_database_all(cfg.std_trades_id)
    fields = _resolve_trade_write_fields(trade_db)
    failures: List[Tuple[str, List[str]]] = []
    checked = 0

    for r in rows:
        source_field = fields.get("source_table")
        source = p_select(r, source_field) if source_field else ""
        if source and source != "manual":
            continue
        checked += 1
        missing: List[str] = []
        if not fields.get("date") or not p_date(r, fields["date"]):
            missing.append("日期")
        if not fields.get("direction") or p_select(r, fields["direction"]) not in {"BUY", "SELL"}:
            missing.append("方向")
        if not fields.get("shares") or p_number(r, fields["shares"]) is None:
            missing.append("股数")
        if not fields.get("price") or p_number(r, fields["price"]) is None:
            missing.append("价格")
        if not fields.get("stock") or len(p_relation_ids(r, fields["stock"])) == 0:
            missing.append("股票")
        if not fields.get("title"):
            missing.append("标题")
        elif get_prop(r, fields["title"]).get("type") == "title":
            if not p_title(r, fields["title"]):
                missing.append("标题")
        elif not _prop_text_any(r, fields["title"]):
            missing.append("标题")
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
            totals[y] = {"已实现收益": 0.0, "T收益": 0.0, "分红收益": 0.0}

    for row in tx_rows:
        y = parse_year(p_date(row, "日期"))
        if not y:
            continue
        init_year(y)
        realized = p_formula_number(row, "单笔已实现收益")
        t_profit = p_formula_number(row, "单笔T收益")
        if realized is not None:
            totals[y]["已实现收益"] += float(realized)
        if t_profit is not None:
            totals[y]["T收益"] += float(t_profit)

    for row in div_rows:
        y = parse_year(p_date(row, "日期"))
        if not y:
            continue
        init_year(y)
        amount = p_number(row, "金额")
        if amount is not None:
            totals[y]["分红收益"] += float(amount)

    annual_index: Dict[str, str] = {}
    for row in annual_rows:
        year = p_title(row, "年份")
        if year:
            annual_index[year] = row.get("id", "")

    years = sorted(totals.keys())
    print(f"将同步年份: {', '.join(years) if years else '(none)'}")

    if dry_run:
        print("dry-run: 未写入年度收益汇总")
        return 0

    for y in years:
        vals = totals[y]
        props = {
            "已实现收益": {"number": round(vals["已实现收益"], 6)},
            "T收益": {"number": round(vals["T收益"], 6)},
            "分红收益": {"number": round(vals["分红收益"], 6)},
        }
        if y in annual_index and annual_index[y]:
            client.update_page(annual_index[y], props)
        else:
            create_props = {
                "年份": title_prop(y),
                "已实现收益": {"number": round(vals["已实现收益"], 6)},
                "T收益": {"number": round(vals["T收益"], 6)},
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


@dataclass
class KBar:
    symbol: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    vol: float
    amount: float
    adj: str


class KlineStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        _ensure_parent_dir(db_path)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kline_daily (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                vol REAL NOT NULL,
                amount REAL NOT NULL,
                adj TEXT NOT NULL,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, trade_date, adj)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kline_sync_log (
                symbol TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                bars INTEGER NOT NULL,
                status TEXT NOT NULL,
                error TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(symbol, start_date, end_date)
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_symbol_date ON kline_daily(symbol, trade_date)")
        self.conn.commit()

    def upsert_bars(self, bars: List[KBar], source: str) -> int:
        if not bars:
            return 0
        now = dt.datetime.now().isoformat(timespec="seconds")
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO kline_daily(symbol, trade_date, open, high, low, close, vol, amount, adj, source, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, trade_date, adj) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    vol=excluded.vol,
                    amount=excluded.amount,
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        b.symbol,
                        b.trade_date,
                        float(b.open),
                        float(b.high),
                        float(b.low),
                        float(b.close),
                        float(b.vol),
                        float(b.amount),
                        b.adj,
                        source,
                        now,
                    )
                    for b in bars
                ],
            )
        return len(bars)

    def query_bars(self, symbol: str, start_date: str, end_date: str, adj: str) -> List[KBar]:
        cur = self.conn.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, vol, amount, adj
            FROM kline_daily
            WHERE symbol=? AND adj=? AND trade_date>=? AND trade_date<=?
            ORDER BY trade_date ASC
            """,
            (symbol, adj, start_date, end_date),
        )
        out: List[KBar] = []
        for row in cur.fetchall():
            out.append(
                KBar(
                    symbol=str(row["symbol"]),
                    trade_date=str(row["trade_date"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    vol=float(row["vol"]),
                    amount=float(row["amount"]),
                    adj=str(row["adj"]),
                )
            )
        return out

    def record_sync(self, symbol: str, start_date: str, end_date: str, bars: int, status: str, error: str = "") -> None:
        now = dt.datetime.now().isoformat(timespec="seconds")
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO kline_sync_log(symbol, start_date, end_date, bars, status, error, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, start_date, end_date) DO UPDATE SET
                    bars=excluded.bars,
                    status=excluded.status,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (symbol, start_date, end_date, int(bars), status, error[:500], now),
            )


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
            for i, key in enumerate(fields_arr):
                obj[str(key)] = row[i] if i < len(row) else None
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
        return {str(r.get("trade_date", "")): float(r.get("adj_factor", 0.0) or 0.0) for r in rows}

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
            tdate_raw = str(row.get("trade_date", ""))
            tdate = f"{tdate_raw[:4]}-{tdate_raw[4:6]}-{tdate_raw[6:8]}" if len(tdate_raw) == 8 else ""
            o = float(row.get("open", 0.0) or 0.0)
            h = float(row.get("high", 0.0) or 0.0)
            l = float(row.get("low", 0.0) or 0.0)
            c = float(row.get("close", 0.0) or 0.0)
            if adj_map and base_factor > 0 and tdate_raw in adj_map and adj_map[tdate_raw] > 0:
                factor = float(adj_map[tdate_raw]) / base_factor
                o, h, l, c = o * factor, h * factor, l * factor, c * factor
            if not tdate or c <= 0:
                continue
            bars.append(
                KBar(
                    symbol=ts_code,
                    trade_date=tdate,
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    vol=float(row.get("vol", 0.0) or 0.0),
                    amount=float(row.get("amount", 0.0) or 0.0),
                    adj=adj,
                )
            )
        bars.sort(key=lambda x: x.trade_date)
        upserted = self.store.upsert_bars(bars, source="tushare")
        self.store.record_sync(ts_code, start_date, end_date, upserted, "ok", "")
        return {"symbol": ts_code, "bars": upserted, "status": "ok"}

    def load_or_sync(self, ts_code: str, start_date: str, end_date: str, adj: str, force: bool = False) -> List[KBar]:
        bars = self.store.query_bars(ts_code, start_date, end_date, adj=adj)
        if bars and not force:
            return bars
        self.sync_symbol(ts_code=ts_code, start_date=start_date, end_date=end_date, adj=adj, force=force)
        return self.store.query_bars(ts_code, start_date, end_date, adj=adj)

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
        "stock_code": _find_prop_name(props, ["股票代码", "代码"], ["rich_text", "title"]),
        "current_price": _find_prop_name(props, ["当前市价", "最新价", "现价", "市价"], ["number"]),
        "current_cost": _find_prop_name(props, ["当前持仓成本", "持仓成本", "成本价"], ["number"]),
        "out_action": _find_prop_name(props, ["建议动作"], ["select", "status", "rich_text", "title"]),
        "out_buy": _find_prop_name(props, ["建议买入价"], ["number"]),
        "out_sell": _find_prop_name(props, ["建议卖出价"], ["number"]),
        "out_stop": _find_prop_name(props, ["建议止损价"], ["number"]),
        "out_pos": _find_prop_name(props, ["建议仓位变化"], ["number"]),
        "out_buy_shares": _find_prop_name(props, ["建议买入股数"], ["number"]),
        "out_sell_shares": _find_prop_name(props, ["建议卖出股数"], ["number"]),
        "out_holding_shares": _find_prop_name(props, ["当前持仓股数", "持仓股数"], ["number"]),
        "out_market_value": _find_prop_name(props, ["持仓市值", "市值"], ["number"]),
        "out_unrealized_pnl": _find_prop_name(props, ["浮动盈亏", "未实现盈亏"], ["number"]),
        "out_conf": _find_prop_name(props, ["建议置信度"], ["select", "status", "rich_text", "title"]),
        "out_mode": _find_prop_name(props, ["建议模式"], ["select", "status", "rich_text", "title"]),
        "out_reason": _find_prop_name(props, ["建议原因", "触发原因"], ["rich_text", "title"]),
        "out_time": _find_prop_name(props, ["建议更新时间"], ["date", "rich_text"]),
    }


def _resolve_trade_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    fields = {
        "date": _find_prop_name(props, ["日期", "交易日期", "下单日期", "鏃ユ湡"], ["date"]),
        "direction": _find_prop_name(props, ["方向", "交易方向", "买卖方向", "鏂瑰悜"], ["select", "status"]),
        "shares": _find_prop_name(props, ["股数", "数量", "成交数量", "鑲℃暟"], ["number"]),
        "price": _find_prop_name(props, ["价格", "成交价", "成交价格", "浠锋牸"], ["number"]),
        "stock": _find_prop_name(props, ["股票", "标的", "证券", "鑲＄エ"], ["relation"]),
        "realized": _find_prop_name(props, ["单笔已实现收益"], ["formula", "number"]),
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


def _resolve_trade_write_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    title_name = find_title_property_name(trade_db)
    out = {
        "title": title_name,
        "date": _find_prop_name(props, ["日期"], ["date"]),
        "direction": _find_prop_name(props, ["方向"], ["select", "status"]),
        "shares": _find_prop_name(props, ["股数"], ["number"]),
        "price": _find_prop_name(props, ["价格"], ["number"]),
        "fee": _find_prop_name(props, ["手续费", "费用"], ["number"]),
        "tax": _find_prop_name(props, ["税费", "印花税"], ["number"]),
        "stock": _find_prop_name(props, ["股票"], ["relation"]),
        "strategy": _find_prop_name(props, ["策略"], ["select", "status", "rich_text", "title"]),
        "note": _find_prop_name(props, ["备注"], ["rich_text", "title"]),
        "source_table": _find_prop_name(props, ["source_table"], ["select", "status"]),
        "import_status": _find_prop_name(props, ["import_status"], ["select", "status"]),
    }
    return out


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


def _account_row_code() -> str:
    return (os.getenv("ACCOUNT_ROW_CODE", "ACCOUNT") or "ACCOUNT").strip().upper()


def _is_account_row(row: Dict[str, Any], stock_fields: Dict[str, Optional[str]]) -> bool:
    target = _account_row_code()
    code = _prop_text_any(row, stock_fields["stock_code"]) if stock_fields.get("stock_code") else ""
    title = p_title(row, stock_fields["title"]) if stock_fields.get("title") else ""
    return (code or "").strip().upper() == target or (title or "").strip().upper() == target


def _resolve_cash_config_fields(cash_db: Dict[str, Any], pref_name: str) -> Dict[str, Optional[str]]:
    props = cash_db.get("properties", {})
    return {
        "cash": _find_prop_name(props, [pref_name, "可流动现金", "现金", "cash"], ["number", "rich_text", "title"]),
    }


def _num_from_prop_any(page: Dict[str, Any], key: Optional[str]) -> Optional[float]:
    if not key:
        return None
    prop = get_prop(page, key)
    if not prop:
        return None
    typ = prop.get("type")
    if typ == "number":
        num = prop.get("number")
        return float(num) if num is not None else None
    if typ in {"rich_text", "title"}:
        text = _prop_text_any(page, key).replace(",", "").strip()
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None
    return None


def _load_cash_from_config_db(client: NotionClient, cfg: Cfg) -> float:
    db_id = (cfg.cash_config_id or "").strip()
    if not db_id:
        raise RuntimeError("Missing DB_CASH_CONFIG_ID.")
    pref = (os.getenv("CASH_FIELD_NAME", "可流动现金") or "可流动现金").strip()
    db = client.get_database(db_id)
    fields = _resolve_cash_config_fields(db, pref_name=pref)
    key = fields.get("cash")
    if not key:
        raise RuntimeError(f"Cash config DB missing field: {pref}")
    rows = client.query_database_all(db_id)
    if not rows:
        raise RuntimeError("Cash config DB has no records.")
    val = _num_from_prop_any(rows[0], key)
    if val is not None and val > 0:
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


def _read_cash_config_formula_summary(client: NotionClient, cfg: Cfg) -> Dict[str, Optional[float]]:
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
        return _find_prop_name(props, candidates, ["number", "formula", "rollup", "rich_text", "title"])

    keys = {
        "total_asset": pick([pref_asset, "总资产", "total_asset"]),
        "market_value_total": pick([pref_mkt, "总持仓市值", "持仓市值合计", "market_value_total"]),
        "unrealized_pnl_total": pick([pref_unr, "总浮动盈亏", "未实现盈亏合计", "unrealized_pnl_total"]),
        "total_pnl": pick([pref_total_pnl, "总盈亏", "total_pnl"]),
        "realized_pnl_total": pick([pref_realized, "已实现盈亏", "realized_pnl_total"]),
    }

    out: Dict[str, Optional[float]] = {}
    for k, key in keys.items():
        if not key:
            out[k] = None
            continue
        prop = get_prop(row, key)
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
            val = _num_from_prop_any(row, key)
        out[k] = val
    return out


def _build_reconcile_result(code_summary: Dict[str, float], notion_summary: Dict[str, Optional[float]]) -> Dict[str, Any]:
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


def _resolve_trade_cost_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    return {
        "fee": _find_prop_name(props, ["手续费", "费用"], ["number"]),
        "tax": _find_prop_name(props, ["税费", "印花税"], ["number"]),
    }


def _replay_positions_from_trades(
    trade_rows: List[Dict[str, Any]],
    trade_fields: Dict[str, Optional[str]],
    cost_fields: Dict[str, Optional[str]],
) -> Tuple[Dict[str, float], Dict[str, float], float]:
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


def _round_lot_a(share_count: float) -> int:
    if share_count <= 0:
        return 0
    return int(math.floor(share_count / 100.0) * 100)

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


def sync_kline(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db = client.get_database(cfg.stock_master_id)
    stock_fields = _resolve_stock_fields_runtime(stock_db)
    stock_rows = client.query_database_all(cfg.stock_master_id)
    end_date = _today_or(getattr(args, "end_date", ""))
    start_date = _today_or(getattr(args, "start_date", "")) if getattr(args, "start_date", "") else _default_kline_start(end_date)
    adj = _coerce_text(getattr(args, "adj", os.getenv("KLINE_DEFAULT_ADJ", "raw"))).strip().lower() or "raw"
    if adj not in {"raw", "qfq", "hfq"}:
        raise RuntimeError("adj must be one of raw/qfq/hfq")

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    store = KlineStore(_sqlite_path())
    provider = KlineProvider(token=token, store=store)
    symbols = _split_csv(getattr(args, "symbols", ""))
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
    uniq = []
    for code in ts_codes:
        if code not in uniq:
            uniq.append(code)

    details: List[Dict[str, Any]] = []
    ok = 0
    fail = 0
    try:
        for ts_code in uniq:
            try:
                r = provider.sync_symbol(ts_code, start_date=start_date, end_date=end_date, adj=adj, force=bool(getattr(args, "force", False)))
                details.append(r)
                ok += 1
            except Exception as e:
                fail += 1
                err = str(e)
                details.append({"symbol": ts_code, "bars": 0, "status": "error", "error": err})
                store.record_sync(ts_code, start_date, end_date, 0, "error", err)
    finally:
        store.close()

    out = {
        "symbols_total": len(uniq),
        "ok": ok,
        "failed": fail,
        "start_date": start_date,
        "end_date": end_date,
        "adj": adj,
        "sqlite_path": _sqlite_path(),
        "details": details,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if fail == 0 else 2


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
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = param_cfg or {}
    trend_threshold = float(cfg.get("trend_threshold", 0.015))
    vol_cap = float(cfg.get("vol_cap", 0.10))
    band_low = float(cfg.get("band_low", 0.01))
    band_high = float(cfg.get("band_high", 0.12))
    stop_mult = float(cfg.get("stop_mult", 1.8))
    rr_min = float(cfg.get("rr_min", 0.8))
    if current_price is None or current_price <= 0:
        return {
            "action": "HOLD",
            "buy_price": None,
            "sell_price": None,
            "stop_price": None,
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "missing current price",
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
            "reason": "sample too small",
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
    if vol > vol_cap:
        return {
            "action": "HOLD",
            "buy_price": round(current_price * 0.99, 4),
            "sell_price": round(current_price * 1.01, 4),
            "stop_price": round(current_price * 0.97, 4),
            "position_delta": 0.0,
            "confidence": conf_level,
            "mode": mode,
            "reason": "volatility too high",
            "sample_count": sample_count,
        }

    ma_window = min(20, len(prices))
    moving_avg = _safe_mean(prices[-ma_window:]) if ma_window > 0 else current_price
    trend = (current_price / moving_avg - 1.0) if moving_avg > 0 else 0.0
    band = _clamp(max(vol * 1.2, band_low), band_low, band_high)

    buy_price = current_price * (1.0 - band)
    sell_price = current_price * (1.0 + band)
    stop_price = current_price * (1.0 - band * stop_mult)

    if current_cost is not None and current_cost > 0:
        buy_price = min(buy_price, current_cost * 0.995)
        sell_price = max(sell_price, current_cost * 1.005)

    action = "HOLD"
    reason = "neutral"
    if trend > trend_threshold:
        action = "BUY"
        reason = "up trend"
    elif trend < -trend_threshold:
        action = "SELL"
        reason = "down trend"

    expected_up = max(0.0, (sell_price - current_price) / current_price)
    expected_down = max(0.0, (current_price - stop_price) / current_price)
    if expected_up <= expected_down * rr_min:
        action = "HOLD"
        reason = "risk/reward not favorable"

    if _level_rank(conf_level) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"confidence {conf_level} < {min_confidence}"

    pos_base = {"HIGH": 0.15, "MEDIUM": 0.10, "LOW": 0.05}[conf_level]
    if mode == "TREND_FALLBACK":
        pos_base *= 0.5

    pos_delta = 0.0
    if action == "BUY":
        pos_delta = pos_base
    elif action == "SELL":
        pos_delta = -pos_base

    if mode == "TREND_FALLBACK" and conf_level == "LOW" and action == "BUY":
        action = "HOLD"
        pos_delta = 0.0
        reason = "fallback+low confidence"

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
def _parse_strategy_set(raw: str) -> List[str]:
    allowed = {"baseline", "chan", "atr_wave"}
    items = [x.strip().lower() for x in (raw or "").split(",") if x.strip()]
    if not items:
        items = ["baseline", "chan", "atr_wave"]
    items = [x for x in items if x in allowed]
    if not items:
        raise ValueError("strategy-set must include baseline/chan/atr_wave")
    out: List[str] = []
    for x in items:
        if x not in out:
            out.append(x)
    return out


def _recommend_chan_from_points(
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = _recommend_from_points(
        current_price=current_price,
        current_cost=current_cost,
        points=points,
        allow_small_sample=allow_small_sample,
        min_confidence=min_confidence,
        param_cfg=param_cfg,
    )
    if current_price is None or current_price <= 0:
        return base

    prices = [p.price for p in points if p.price is not None and p.price > 0]
    n = len(prices)
    if n < 7:
        base["reason"] = "CHAN sample too short; fallback baseline"
        return base

    fractals: List[Tuple[int, str]] = []
    for i in range(1, n - 1):
        if prices[i] > prices[i - 1] and prices[i] > prices[i + 1]:
            fractals.append((i, "TOP"))
        elif prices[i] < prices[i - 1] and prices[i] < prices[i + 1]:
            fractals.append((i, "BOTTOM"))
    pens: List[Tuple[int, int, str]] = []
    for i in range(1, len(fractals)):
        p0, t0 = fractals[i - 1]
        p1, t1 = fractals[i]
        if t0 == t1:
            continue
        direction = "UP" if (t0 == "BOTTOM" and t1 == "TOP") else "DOWN"
        pens.append((p0, p1, direction))

    recent = prices[-6:]
    center = _safe_mean(recent)
    spread = max(_safe_stdev(recent), 1e-6)
    upper = center + spread * 0.6
    lower = center - spread * 0.6
    trend_up = len(pens) >= 2 and pens[-1][2] == "UP" and pens[-2][2] == "UP"
    trend_down = len(pens) >= 2 and pens[-1][2] == "DOWN" and pens[-2][2] == "DOWN"

    action = "HOLD"
    reason = f"CHAN fractals={len(fractals)} pens={len(pens)} center={center:.4f}"
    if trend_up and current_price <= upper:
        action = "BUY"
        reason = f"{reason}; up-pen continuation near center"
    elif trend_down and current_price >= lower:
        action = "SELL"
        reason = f"{reason}; down-pen continuation near center"

    buy_price = min(base["buy_price"], center) if base["buy_price"] is not None else center
    sell_price = max(base["sell_price"], center) if base["sell_price"] is not None else center
    stop_price = min(base["stop_price"], lower - spread * 0.4) if base["stop_price"] is not None else lower
    conf = base["confidence"]
    if n < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"CHAN confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0

    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def _recommend_atr_wave_from_points(
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = _recommend_from_points(
        current_price=current_price,
        current_cost=current_cost,
        points=points,
        allow_small_sample=allow_small_sample,
        min_confidence=min_confidence,
        param_cfg=param_cfg,
    )
    if current_price is None or current_price <= 0:
        return base

    closes = [p.price for p in points if p.price is not None and p.price > 0]
    if len(closes) < 8:
        base["reason"] = "ATR_WAVE sample too short; fallback baseline"
        return base

    diffs = [abs(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    atr_window = min(14, len(diffs))
    atr = _safe_mean(diffs[-atr_window:]) if atr_window > 0 else 0.0
    ma_window = min(20, len(closes))
    mid = _safe_mean(closes[-ma_window:]) if ma_window > 0 else current_price
    upper = mid + atr * 1.2
    lower = mid - atr * 1.2

    action = "HOLD"
    reason = f"ATR_WAVE atr={atr:.4f} mid={mid:.4f} band=[{lower:.4f},{upper:.4f}]"
    if current_price > upper:
        action = "BUY"
        reason = f"{reason}; breakout above upper band"
    elif current_price < lower:
        action = "SELL"
        reason = f"{reason}; breakdown below lower band"
    elif abs(current_price - mid) <= max(atr * 0.25, 0.01):
        action = "HOLD"
        reason = f"{reason}; mean reversion zone"

    buy_price = min(base["buy_price"], current_price - atr * 0.4) if base["buy_price"] is not None else current_price
    sell_price = max(base["sell_price"], current_price + atr * 0.8) if base["sell_price"] is not None else current_price
    stop_price = min(base["stop_price"], current_price - atr * 1.2) if base["stop_price"] is not None else current_price

    conf = base["confidence"]
    if len(closes) < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"ATR_WAVE confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0

    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def _recommend_by_strategy_trade(
    strategy_id: str,
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    sid = strategy_id.lower()
    if sid == "baseline":
        rec = _recommend_from_points(current_price, current_cost, points, allow_small_sample, min_confidence, param_cfg=param_cfg)
    elif sid == "chan":
        rec = _recommend_chan_from_points(current_price, current_cost, points, allow_small_sample, min_confidence, param_cfg=param_cfg)
    elif sid == "atr_wave":
        rec = _recommend_atr_wave_from_points(current_price, current_cost, points, allow_small_sample, min_confidence, param_cfg=param_cfg)
    else:
        raise ValueError(f"unsupported strategy: {strategy_id}")
    rec["strategy_id"] = sid.upper()
    return rec


def _kline_close_returns(bars: List[KBar]) -> List[float]:
    closes = [b.close for b in bars if b.close > 0]
    out: List[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        curr = closes[i]
        if prev > 0:
            out.append((curr - prev) / prev)
    return out


def _recommend_from_kbars(
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = param_cfg or {}
    trend_threshold = float(cfg.get("trend_threshold", 0.015))
    vol_cap = float(cfg.get("vol_cap", 0.10))
    band_low = float(cfg.get("band_low", 0.01))
    band_high = float(cfg.get("band_high", 0.12))
    stop_mult = float(cfg.get("stop_mult", 1.8))
    rr_min = float(cfg.get("rr_min", 0.8))
    if current_price is None or current_price <= 0:
        return {
            "action": "HOLD",
            "buy_price": None,
            "sell_price": None,
            "stop_price": None,
            "position_delta": 0.0,
            "confidence": "LOW",
            "mode": "TREND_FALLBACK",
            "reason": "missing current price",
            "sample_count": len(bars),
        }

    valid = [b for b in bars if b.close > 0]
    sample_count = len(valid)
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
            "reason": "sample too small",
            "sample_count": sample_count,
        }

    returns = _kline_close_returns(valid)
    conf_score = _confidence_from_history(sample_count, returns[-30:], [])
    conf_level = _confidence_level(conf_score)
    if mode == "TREND_FALLBACK" and conf_level == "HIGH":
        conf_level = "MEDIUM"

    vol = _safe_stdev(returns[-20:]) if returns else 0.02
    if vol > vol_cap:
        return {
            "action": "HOLD",
            "buy_price": round(current_price * 0.99, 4),
            "sell_price": round(current_price * 1.01, 4),
            "stop_price": round(current_price * 0.97, 4),
            "position_delta": 0.0,
            "confidence": conf_level,
            "mode": mode,
            "reason": "volatility too high",
            "sample_count": sample_count,
        }

    closes = [b.close for b in valid]
    ma_window = min(20, len(closes))
    moving_avg = _safe_mean(closes[-ma_window:]) if ma_window > 0 else current_price
    trend = (current_price / moving_avg - 1.0) if moving_avg > 0 else 0.0
    band = _clamp(max(vol * 1.2, band_low), band_low, band_high)

    buy_price = current_price * (1.0 - band)
    sell_price = current_price * (1.0 + band)
    stop_price = current_price * (1.0 - band * stop_mult)
    if current_cost is not None and current_cost > 0:
        buy_price = min(buy_price, current_cost * 0.995)
        sell_price = max(sell_price, current_cost * 1.005)

    action = "HOLD"
    reason = "neutral"
    if trend > trend_threshold:
        action = "BUY"
        reason = "up trend"
    elif trend < -trend_threshold:
        action = "SELL"
        reason = "down trend"

    expected_up = max(0.0, (sell_price - current_price) / current_price)
    expected_down = max(0.0, (current_price - stop_price) / current_price)
    if expected_up <= expected_down * rr_min:
        action = "HOLD"
        reason = "risk/reward not favorable"
    if _level_rank(conf_level) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"confidence {conf_level} < {min_confidence}"

    pos_base = {"HIGH": 0.15, "MEDIUM": 0.10, "LOW": 0.05}[conf_level]
    if mode == "TREND_FALLBACK":
        pos_base *= 0.5
    pos_delta = pos_base if action == "BUY" else (-pos_base if action == "SELL" else 0.0)
    if mode == "TREND_FALLBACK" and conf_level == "LOW" and action == "BUY":
        action = "HOLD"
        pos_delta = 0.0
        reason = "fallback+low confidence"

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


def _recommend_chan_from_kbars(
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = _recommend_from_kbars(current_price, current_cost, bars, allow_small_sample, min_confidence, param_cfg=param_cfg)
    if current_price is None or current_price <= 0:
        return base
    if len(bars) < 7:
        base["reason"] = "CHAN sample too short; fallback baseline"
        return base

    fractals: List[Tuple[int, str]] = []
    for i in range(1, len(bars) - 1):
        if bars[i].high > bars[i - 1].high and bars[i].high > bars[i + 1].high:
            fractals.append((i, "TOP"))
        elif bars[i].low < bars[i - 1].low and bars[i].low < bars[i + 1].low:
            fractals.append((i, "BOTTOM"))

    pens: List[Tuple[int, int, str]] = []
    for i in range(1, len(fractals)):
        p0, t0 = fractals[i - 1]
        p1, t1 = fractals[i]
        if t0 == t1:
            continue
        pens.append((p0, p1, "UP" if (t0 == "BOTTOM" and t1 == "TOP") else "DOWN"))

    recent = bars[-6:]
    center = _safe_mean([b.close for b in recent])
    spread = max(_safe_stdev([b.close for b in recent]), 1e-6)
    upper = center + spread * 0.6
    lower = center - spread * 0.6
    trend_up = len(pens) >= 2 and pens[-1][2] == "UP" and pens[-2][2] == "UP"
    trend_down = len(pens) >= 2 and pens[-1][2] == "DOWN" and pens[-2][2] == "DOWN"

    action = "HOLD"
    reason = f"CHAN fractals={len(fractals)} pens={len(pens)} center={center:.4f}"
    if trend_up and current_price <= upper:
        action = "BUY"
        reason = f"{reason}; up-pen continuation near center"
    elif trend_down and current_price >= lower:
        action = "SELL"
        reason = f"{reason}; down-pen continuation near center"

    buy_price = min(base["buy_price"], center) if base["buy_price"] is not None else center
    sell_price = max(base["sell_price"], center) if base["sell_price"] is not None else center
    stop_price = min(base["stop_price"], lower - spread * 0.4) if base["stop_price"] is not None else lower
    conf = base["confidence"]
    if len(bars) < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"CHAN confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0

    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def _recommend_atr_wave_from_kbars(
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base = _recommend_from_kbars(current_price, current_cost, bars, allow_small_sample, min_confidence, param_cfg=param_cfg)
    if current_price is None or current_price <= 0:
        return base
    if len(bars) < 8:
        base["reason"] = "ATR_WAVE sample too short; fallback baseline"
        return base

    tr_values: List[float] = []
    prev_close = bars[0].close
    for b in bars[1:]:
        tr = max(b.high - b.low, abs(b.high - prev_close), abs(b.low - prev_close))
        tr_values.append(max(tr, 0.0))
        prev_close = b.close
    atr_window = min(14, len(tr_values))
    atr = _safe_mean(tr_values[-atr_window:]) if atr_window > 0 else 0.0
    closes = [b.close for b in bars]
    mid = _safe_mean(closes[-min(20, len(closes)) :]) if closes else current_price
    upper = mid + atr * 1.2
    lower = mid - atr * 1.2

    action = "HOLD"
    reason = f"ATR_WAVE atr={atr:.4f} mid={mid:.4f} band=[{lower:.4f},{upper:.4f}]"
    if current_price > upper:
        action = "BUY"
        reason = f"{reason}; breakout above upper band"
    elif current_price < lower:
        action = "SELL"
        reason = f"{reason}; breakdown below lower band"
    elif abs(current_price - mid) <= max(atr * 0.25, 0.01):
        action = "HOLD"
        reason = f"{reason}; mean reversion zone"

    buy_price = min(base["buy_price"], current_price - atr * 0.4) if base["buy_price"] is not None else current_price
    sell_price = max(base["sell_price"], current_price + atr * 0.8) if base["sell_price"] is not None else current_price
    stop_price = min(base["stop_price"], current_price - atr * 1.2) if base["stop_price"] is not None else current_price
    conf = base["confidence"]
    if len(closes) < 20 and conf == "HIGH":
        conf = "MEDIUM"
    if _level_rank(conf) < _level_rank(min_confidence):
        action = "HOLD"
        reason = f"ATR_WAVE confidence {conf} below threshold {min_confidence}"

    pos = base["position_delta"]
    if action == "BUY" and pos < 0:
        pos = abs(pos)
    if action == "SELL" and pos > 0:
        pos = -abs(pos)
    if action == "HOLD":
        pos = 0.0
    return {
        "action": action,
        "buy_price": round(float(buy_price), 4),
        "sell_price": round(float(sell_price), 4),
        "stop_price": round(float(stop_price), 4),
        "position_delta": round(float(pos), 4),
        "confidence": conf,
        "mode": base["mode"],
        "reason": reason,
        "sample_count": base["sample_count"],
    }


def _recommend_by_strategy_kline(
    strategy_id: str,
    current_price: Optional[float],
    current_cost: Optional[float],
    bars: List[KBar],
    allow_small_sample: bool,
    min_confidence: str,
    param_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    sid = strategy_id.lower()
    if sid == "baseline":
        rec = _recommend_from_kbars(current_price, current_cost, bars, allow_small_sample, min_confidence, param_cfg=param_cfg)
    elif sid == "chan":
        rec = _recommend_chan_from_kbars(current_price, current_cost, bars, allow_small_sample, min_confidence, param_cfg=param_cfg)
    elif sid == "atr_wave":
        rec = _recommend_atr_wave_from_kbars(current_price, current_cost, bars, allow_small_sample, min_confidence, param_cfg=param_cfg)
    else:
        raise ValueError(f"unsupported strategy: {strategy_id}")
    rec["strategy_id"] = sid.upper()
    return rec


def _resolve_recommend_data_source(args: argparse.Namespace) -> str:
    return (_coerce_text(getattr(args, "data_source", "kline")).strip().lower() or "kline")


def _load_trade_points_for_recommendation(client: NotionClient, cfg: Cfg) -> Dict[str, List[TradePoint]]:
    trade_db = client.get_database(cfg.std_trades_id)
    trade_fields = _resolve_trade_fields(trade_db)
    for key in ["date", "direction", "shares", "price", "stock"]:
        if not trade_fields[key]:
            raise RuntimeError(f"Missing required standard-trade field mapping: {key}")
    trade_rows = client.query_database_all(cfg.std_trades_id)
    return _build_trade_points(trade_rows, trade_fields)


def _load_kbars_for_stocks(
    stock_rows: List[Dict[str, Any]],
    stock_fields: Dict[str, Optional[str]],
    args: argparse.Namespace,
) -> Tuple[Dict[str, List[KBar]], Dict[str, str]]:
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    adj = _coerce_text(getattr(args, "adj", os.getenv("KLINE_DEFAULT_ADJ", "raw"))).strip().lower() or "raw"
    if adj not in {"raw", "qfq", "hfq"}:
        raise RuntimeError("adj must be one of raw/qfq/hfq")
    end_date = _today_or(getattr(args, "end_date", ""))
    start_date = _today_or(getattr(args, "start_date", "")) if getattr(args, "start_date", "") else _default_kline_start(end_date)
    force = bool(getattr(args, "force", False))
    store = KlineStore(_sqlite_path())
    provider = KlineProvider(token=token, store=store)
    by_stock: Dict[str, List[KBar]] = {}
    by_stock_symbol: Dict[str, str] = {}
    try:
        for row in stock_rows:
            stock_id = row.get("id", "")
            code_raw = _prop_text_any(row, stock_fields["stock_code"]) if stock_fields.get("stock_code") else ""
            ts_code = _to_tushare_ts_code(code_raw)
            if not stock_id or not ts_code:
                continue
            bars = provider.load_or_sync(ts_code, start_date=start_date, end_date=end_date, adj=adj, force=force)
            by_stock[stock_id] = bars
            by_stock_symbol[stock_id] = ts_code
    finally:
        store.close()
    return by_stock, by_stock_symbol


def _prepare_recommendation_context(
    client: NotionClient,
    cfg: Cfg,
    args: argparse.Namespace,
) -> Tuple[Dict[str, Any], Dict[str, Optional[str]], List[Dict[str, Any]], Dict[str, List[TradePoint]], Dict[str, List[KBar]], Dict[str, str]]:
    stock_db = client.get_database(cfg.stock_master_id)
    stock_fields = _resolve_stock_fields_runtime(stock_db)
    stock_rows = client.query_database_all(cfg.stock_master_id)
    if getattr(args, "refresh_prices", False):
        sync_prices(
            client,
            cfg,
            argparse.Namespace(dry_run=bool(getattr(args, "dry_run", False)), timeout=getattr(args, "timeout", 8)),
        )
        stock_rows = client.query_database_all(cfg.stock_master_id)

    data_source = _resolve_recommend_data_source(args)
    stock_points: Dict[str, List[TradePoint]] = {}
    stock_kbars: Dict[str, List[KBar]] = {}
    stock_symbols: Dict[str, str] = {}
    if data_source == "trade":
        stock_points = _load_trade_points_for_recommendation(client, cfg)
    else:
        stock_kbars, stock_symbols = _load_kbars_for_stocks(stock_rows, stock_fields, args)
    return stock_db, stock_fields, stock_rows, stock_points, stock_kbars, stock_symbols


def _collect_recommendations(
    stock_rows: List[Dict[str, Any]],
    stock_fields: Dict[str, Optional[str]],
    stock_points: Dict[str, List[TradePoint]],
    stock_kbars: Dict[str, List[KBar]],
    stock_symbols: Dict[str, str],
    args: argparse.Namespace,
) -> List[Dict[str, Any]]:
    selected_strategies = _parse_strategy_set(getattr(args, "strategy_set", "baseline,chan,atr_wave"))
    recs: List[Dict[str, Any]] = []
    market_rule = os.getenv("SNAPSHOT_MARKET_RULE", "")
    override_market = (getattr(args, "param_market", "") or "").strip().upper()
    scope = (getattr(args, "param_scope", "") or "*").strip() or "*"
    data_source = _resolve_recommend_data_source(args)
    store = ParamStore(_sqlite_path())
    param_cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    try:
        for row in stock_rows:
            if _is_account_row(row, stock_fields):
                continue
            stock_id = row.get("id", "")
            title = p_title(row, stock_fields["title"]) if stock_fields["title"] else stock_id
            code_raw = _prop_text_any(row, stock_fields["stock_code"]) if stock_fields.get("stock_code") else ""
            symbol = stock_symbols.get(stock_id, "")
            current_price = p_number(row, stock_fields["current_price"]) if stock_fields["current_price"] else None
            current_cost = p_number(row, stock_fields["current_cost"]) if stock_fields["current_cost"] else None
            kbars = stock_kbars.get(stock_id, [])
            if data_source == "kline" and current_price is None and kbars:
                current_price = kbars[-1].close
            market_code = code_raw or symbol
            market = override_market or _market_from_rule(market_code, market_rule)

            for sid in selected_strategies:
                key = (sid.upper(), market.upper(), scope)
                if key not in param_cache:
                    param_cache[key] = store.get_active_param_set(strategy_id=sid, market=market, symbol_scope=scope)
                active = param_cache[key]
                param_cfg = active.get("params", {})
                if data_source == "trade":
                    rec = _recommend_by_strategy_trade(
                        strategy_id=sid,
                        current_price=current_price,
                        current_cost=current_cost,
                        points=stock_points.get(stock_id, []),
                        allow_small_sample=bool(param_cfg.get("allow_small_sample", args.allow_small_sample)),
                        min_confidence=str(param_cfg.get("min_confidence", args.min_confidence)),
                        param_cfg=param_cfg,
                    )
                else:
                    rec = _recommend_by_strategy_kline(
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
                recs.append(rec)
    finally:
        store.close()
    return recs


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


def _build_snapshot_rows(
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
        stock_code = _coerce_text(rec.get("stock_code", ""))
        row = {
            "snapshot_date": snapshot_date,
            "strategy_id": _coerce_text(rec.get("strategy_id", "")).upper(),
            "stock_id": stock_id,
            "stock_code": stock_code,
            "stock_name": _coerce_text(rec.get("stock_name", "")),
            "market": _market_from_rule(stock_code, market_rule),
            "strategy_mode": _coerce_text(rec.get("mode", "")),
            "ret_1d": float(ret_1d),
            "hit_flag": int(_hit_flag(_coerce_text(rec.get("action", "")), ret_1d)),
            "max_drawdown": float(_max_drawdown(returns)),
            "confidence": _coerce_text(rec.get("confidence", "")),
            "sample_count": int(rec.get("sample_count", 0) or 0),
            "action": _coerce_text(rec.get("action", "")),
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


def _emit_snapshot(recs: List[Dict[str, Any]], stock_prices: Dict[str, List[float]], snapshot_date: str, dry_run: bool) -> Dict[str, Any]:
    run_id = uuid4().hex[:12]
    rows = _build_snapshot_rows(
        recs=recs,
        stock_prices=stock_prices,
        snapshot_date=snapshot_date,
        market_rule=os.getenv("SNAPSHOT_MARKET_RULE", ""),
        run_id=run_id,
    )
    sqlite_path = _sqlite_path()
    written = 0
    if not dry_run:
        store = SnapshotStore(sqlite_path)
        try:
            written = store.upsert_many(rows)
        finally:
            store.close()
    return {
        "snapshot_date": snapshot_date,
        "sqlite_path": sqlite_path,
        "run_id": run_id,
        "input_rows": len(rows),
        "upserted": written,
        "dry_run": bool(dry_run),
    }


def _build_latest_price_map(
    stock_rows: List[Dict[str, Any]],
    stock_fields: Dict[str, Optional[str]],
    stock_kbars: Dict[str, List[KBar]],
) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for row in stock_rows:
        if _is_account_row(row, stock_fields):
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


def _build_position_valuation(
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


def _calc_account_summary(
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


def _attach_position_sizing(
    recs: List[Dict[str, Any]],
    sizing_base_asset: float,
    holding_shares_by_stock: Dict[str, float],
) -> None:
    for rec in recs:
        sid = _coerce_text(rec.get("stock_id", ""))
        action = _coerce_text(rec.get("action", "")).upper()
        pos_delta = float(rec.get("position_delta", 0.0) or 0.0)
        buy_price = rec.get("buy_price")
        sell_price = rec.get("sell_price")
        holding_shares = float(holding_shares_by_stock.get(sid, 0.0) or 0.0)
        holding_lot = _round_lot_a(holding_shares)

        target_value_delta = float(sizing_base_asset) * pos_delta
        suggest_buy_shares = 0
        suggest_sell_shares = 0
        estimated_trade_value = 0.0
        sizing_note = ""

        exec_price_buy = float(buy_price) if buy_price is not None else None
        exec_price_sell = float(sell_price) if sell_price is not None else None

        if action == "BUY":
            if exec_price_buy and exec_price_buy > 0 and target_value_delta > 0:
                suggest_buy_shares = _round_lot_a(target_value_delta / exec_price_buy)
                estimated_trade_value = float(suggest_buy_shares) * exec_price_buy
            else:
                sizing_note = "BUY 无有效买入价或仓位变化<=0，建议股数=0"
        elif action == "SELL":
            if exec_price_sell and exec_price_sell > 0 and target_value_delta < 0:
                raw_shares = _round_lot_a(abs(target_value_delta) / exec_price_sell)
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


def recommend_prices(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db, stock_fields, stock_rows, stock_points, stock_kbars, stock_symbols = _prepare_recommendation_context(client, cfg, args)
    _ = stock_symbols

    db_props = stock_db.get("properties", {})
    asof_date = args.asof_date or dt.date.today().isoformat()

    cash_input = _load_cash_from_config_db(client, cfg)
    trade_db = client.get_database(cfg.std_trades_id)
    trade_rows = client.query_database_all(cfg.std_trades_id)
    trade_fields = _resolve_trade_fields(trade_db)
    cost_fields = _resolve_trade_cost_fields(trade_db)
    holding_shares_by_stock, holding_avg_cost_by_stock, realized_pnl_total = _replay_positions_from_trades(
        trade_rows=trade_rows,
        trade_fields=trade_fields,
        cost_fields=cost_fields,
    )

    need_kline_fallback = any(p_number(row, stock_fields["current_price"]) is None for row in stock_rows if stock_fields.get("current_price"))
    if need_kline_fallback and not stock_kbars and os.getenv("TUSHARE_TOKEN", "").strip():
        kbars_for_price, _ = _load_kbars_for_stocks(stock_rows, stock_fields, args)
        for sid, bars in kbars_for_price.items():
            if sid not in stock_kbars:
                stock_kbars[sid] = bars
    latest_price_by_stock = _build_latest_price_map(stock_rows, stock_fields, stock_kbars)
    position_valuation_by_stock = _build_position_valuation(
        holding_shares_by_stock=holding_shares_by_stock,
        holding_avg_cost_by_stock=holding_avg_cost_by_stock,
        latest_price_by_stock=latest_price_by_stock,
    )
    account_summary = _calc_account_summary(
        cash=cash_input,
        position_valuation_by_stock=position_valuation_by_stock,
        realized_pnl_total=realized_pnl_total,
    )
    notion_formula_summary = _read_cash_config_formula_summary(client, cfg)
    account_summary["notion_formula_summary"] = notion_formula_summary
    account_summary["reconcile"] = _build_reconcile_result(account_summary, notion_formula_summary)

    recs = _collect_recommendations(stock_rows, stock_fields, stock_points, stock_kbars, stock_symbols, args)
    _attach_position_sizing(recs, sizing_base_asset=account_summary.get("total_asset", 0.0), holding_shares_by_stock=holding_shares_by_stock)
    for rec in recs:
        sid = _coerce_text(rec.get("stock_id", ""))
        pos = position_valuation_by_stock.get(sid, {})
        rec["market_value_now"] = float(pos.get("market_value_now", 0.0) or 0.0)
        rec["unrealized_pnl_now"] = float(pos.get("unrealized_pnl_now", 0.0) or 0.0)
        rec["avg_cost_now"] = float(pos.get("avg_cost_now", 0.0) or 0.0)
        rec["last_price_now"] = float(pos.get("last_price_now", 0.0) or 0.0)
    rec_by_stock: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in recs:
        rec_by_stock[rec.get("stock_id", "")].append(rec)

    for row in stock_rows:
        if _is_account_row(row, stock_fields):
            continue
        stock_id = row.get("id", "")
        if args.dry_run:
            continue

        stock_recs = rec_by_stock.get(stock_id, [])
        rec_for_write = None
        for r in stock_recs:
            if r.get("strategy_id") == "BASELINE":
                rec_for_write = r
                break
        if rec_for_write is None and stock_recs:
            rec_for_write = stock_recs[0]
        if rec_for_write is None:
            continue

        props: Dict[str, Any] = {}
        write_map = [
            (stock_fields["out_action"], rec_for_write["action"]),
            (stock_fields["out_buy"], rec_for_write["buy_price"]),
            (stock_fields["out_sell"], rec_for_write["sell_price"]),
            (stock_fields["out_stop"], rec_for_write["stop_price"]),
            (stock_fields["out_pos"], rec_for_write["position_delta"]),
            (stock_fields.get("out_buy_shares"), rec_for_write.get("suggest_buy_shares")),
            (stock_fields.get("out_sell_shares"), rec_for_write.get("suggest_sell_shares")),
            (stock_fields.get("out_holding_shares"), rec_for_write.get("holding_shares_now")),
            (stock_fields.get("out_market_value"), rec_for_write.get("market_value_now")),
            (stock_fields.get("out_unrealized_pnl"), rec_for_write.get("unrealized_pnl_now")),
            (stock_fields["out_conf"], rec_for_write["confidence"]),
            (stock_fields["out_mode"], rec_for_write["mode"]),
            (stock_fields["out_reason"], f"[{rec_for_write['strategy_id']}] {rec_for_write['reason']}"),
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

    if getattr(args, "emit_snapshot", False):
        stock_prices: Dict[str, List[float]] = {}
        if _resolve_recommend_data_source(args) == "trade":
            for sid, points in stock_points.items():
                stock_prices[sid] = [p.price for p in points if p.price is not None and p.price > 0]
        else:
            for sid, bars in stock_kbars.items():
                stock_prices[sid] = [b.close for b in bars if b.close > 0]
        _emit_snapshot(
            recs=recs,
            stock_prices=stock_prices,
            snapshot_date=_today_or(getattr(args, "snapshot_date", "")),
            dry_run=bool(getattr(args, "dry_run", False)),
        )

    out = {"recommendations": recs, "account_summary": account_summary}
    print(json.dumps(out, ensure_ascii=False, indent=2))
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
    if _resolve_recommend_data_source(args) == "trade":
        return _backtest_recommendation_trade(client, cfg, args)

    stock_db = client.get_database(cfg.stock_master_id)
    stock_fields = _resolve_stock_fields_runtime(stock_db)
    stock_rows = client.query_database_all(cfg.stock_master_id)
    stock_kbars, stock_symbols = _load_kbars_for_stocks(stock_rows, stock_fields, args)
    selected_strategies = _parse_strategy_set(getattr(args, "strategy_set", "baseline,chan,atr_wave"))
    mode_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_mode_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_hold_counts: Dict[str, int] = defaultdict(int)
    strategy_total_counts: Dict[str, int] = defaultdict(int)
    baseline_returns: List[float] = []
    all_strategy_returns: List[float] = []
    override_market = (getattr(args, "param_market", "") or "").upper()
    market_rule = os.getenv("SNAPSHOT_MARKET_RULE", "")
    scope = (getattr(args, "param_scope", "") or "*").strip() or "*"
    store = ParamStore(_sqlite_path())
    param_cache: Dict[str, Dict[str, Any]] = {}

    row_by_id = {r.get("id", ""): r for r in stock_rows}
    try:
        for stock_id, bars in stock_kbars.items():
            valid = [b for b in bars if b.close > 0]
            if len(valid) < 8:
                continue
            row = row_by_id.get(stock_id, {})
            code_raw = _prop_text_any(row, stock_fields["stock_code"]) if row else ""
            market = override_market or _market_from_rule(code_raw or stock_symbols.get(stock_id, ""), market_rule)
            for idx in range(5, len(valid) - 1):
                hist = valid[max(0, idx - args.window) : idx]
                curr = valid[idx]
                nxt = valid[idx + 1]
                if curr.close <= 0 or nxt.close <= 0:
                    continue
                move = (nxt.close - curr.close) / curr.close
                baseline_returns.append(move)
                for sid in selected_strategies:
                    sid_upper = sid.upper()
                    cache_key = f"{sid_upper}|{market}|{scope}"
                    if cache_key not in param_cache:
                        param_cache[cache_key] = store.get_active_param_set(strategy_id=sid_upper, market=market, symbol_scope=scope)
                    active = param_cache[cache_key]
                    p_cfg = active.get("params", {})
                    rec = _recommend_by_strategy_kline(
                        strategy_id=sid,
                        current_price=curr.close,
                        current_cost=None,
                        bars=hist,
                        allow_small_sample=bool(p_cfg.get("allow_small_sample", args.allow_small_sample)),
                        min_confidence=str(p_cfg.get("min_confidence", args.min_confidence)),
                        param_cfg=p_cfg,
                    )
                    if rec["action"] == "BUY":
                        strategy_ret = move
                    elif rec["action"] == "SELL":
                        strategy_ret = -move
                    else:
                        strategy_ret = 0.0
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
            m = _returns_metrics(arr)
            total = max(strategy_total_counts.get(sid, 0), 1)
            hold_ratio = strategy_hold_counts.get(sid, 0) / total
            m["hold_ratio"] = hold_ratio
            strategy_metrics[sid] = m
    finally:
        store.close()

    out = {
        "baseline": _returns_metrics(baseline_returns),
        "strategy_all": _returns_metrics(all_strategy_returns),
        "strategy_by_mode": {k: _returns_metrics(v) for k, v in mode_returns.items()},
        "strategy_metrics": strategy_metrics,
        "strategy_mode_metrics": {k: _returns_metrics(v) for k, v in strategy_mode_returns.items()},
        "param_versions": {k: int(v.get("version", 0)) for k, v in param_cache.items()},
        "param_market": override_market or "AUTO",
        "data_source": "kline",
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def _backtest_recommendation_trade(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    trade_db = client.get_database(cfg.std_trades_id)
    trade_fields = _resolve_trade_fields(trade_db)
    for key in ["date", "direction", "shares", "price", "stock"]:
        if not trade_fields[key]:
            raise RuntimeError(f"Missing required standard-trade field mapping: {key}")

    trade_rows = client.query_database_all(cfg.std_trades_id)
    stock_points = _build_trade_points(trade_rows, trade_fields)

    selected_strategies = _parse_strategy_set(getattr(args, "strategy_set", "baseline,chan,atr_wave"))
    mode_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_mode_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_hold_counts: Dict[str, int] = defaultdict(int)
    strategy_total_counts: Dict[str, int] = defaultdict(int)
    baseline_returns: List[float] = []
    all_strategy_returns: List[float] = []
    market = (getattr(args, "param_market", "") or "SH").upper()
    scope = (getattr(args, "param_scope", "") or "*").strip() or "*"
    store = ParamStore(_sqlite_path())
    param_cache: Dict[str, Dict[str, Any]] = {}

    try:
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
                move = (nxt.price - curr.price) / curr.price

                actual_dir = curr.direction
                if actual_dir == "BUY":
                    baseline_ret = move
                elif actual_dir == "SELL":
                    baseline_ret = -move
                else:
                    baseline_ret = 0.0

                baseline_returns.append(baseline_ret)
                for sid in selected_strategies:
                    sid_upper = sid.upper()
                    if sid_upper not in param_cache:
                        param_cache[sid_upper] = store.get_active_param_set(strategy_id=sid_upper, market=market, symbol_scope=scope)
                    active = param_cache[sid_upper]
                    p_cfg = active.get("params", {})
                    rec = _recommend_by_strategy_trade(
                        strategy_id=sid,
                        current_price=curr.price,
                        current_cost=None,
                        points=hist,
                        allow_small_sample=bool(p_cfg.get("allow_small_sample", args.allow_small_sample)),
                        min_confidence=str(p_cfg.get("min_confidence", args.min_confidence)),
                        param_cfg=p_cfg,
                    )

                    if rec["action"] == "BUY":
                        strategy_ret = move
                    elif rec["action"] == "SELL":
                        strategy_ret = -move
                    else:
                        strategy_ret = 0.0

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
            m = _returns_metrics(arr)
            total = max(strategy_total_counts.get(sid, 0), 1)
            hold_ratio = strategy_hold_counts.get(sid, 0) / total
            m["hold_ratio"] = hold_ratio
            strategy_metrics[sid] = m

    finally:
        store.close()

    out = {
        "baseline": _returns_metrics(baseline_returns),
        "strategy_all": _returns_metrics(all_strategy_returns),
        "strategy_by_mode": {k: _returns_metrics(v) for k, v in mode_returns.items()},
        "strategy_metrics": strategy_metrics,
        "strategy_mode_metrics": {k: _returns_metrics(v) for k, v in strategy_mode_returns.items()},
        "param_versions": {k: int(v.get("version", 0)) for k, v in param_cache.items()},
        "param_market": market,
        "data_source": "trade",
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def snapshot_daily(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db, stock_fields, stock_rows, stock_points, stock_kbars, stock_symbols = _prepare_recommendation_context(client, cfg, args)
    _ = stock_db
    _ = stock_fields
    _ = stock_rows
    _ = stock_symbols
    recs = _collect_recommendations(stock_rows, stock_fields, stock_points, stock_kbars, stock_symbols, args)
    stock_prices: Dict[str, List[float]] = {}
    if _resolve_recommend_data_source(args) == "trade":
        for sid, points in stock_points.items():
            stock_prices[sid] = [p.price for p in points if p.price is not None and p.price > 0]
    else:
        for sid, bars in stock_kbars.items():
            stock_prices[sid] = [b.close for b in bars if b.close > 0]
    result = _emit_snapshot(
        recs=recs,
        stock_prices=stock_prices,
        snapshot_date=_today_or(getattr(args, "snapshot_date", "")),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _split_csv(raw: str) -> List[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def _load_json_arg(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    if os.path.exists(text):
        with open(text, "r", encoding="utf-8") as f:
            return json.load(f)
    return json.loads(text)


def _now_ms() -> float:
    return time.time() * 1000.0


def _log_param_event(action: str, status: str, started_ms: float, meta: Dict[str, Any], run_id: str = "", proposal_id: str = "", apply_log_id: str = "", error_code: str = "", error_msg: str = "") -> None:
    store = ParamStore(_sqlite_path())
    try:
        duration = int(max(0.0, _now_ms() - started_ms))
        store.log_event(
            module="param",
            action=action,
            status=status,
            duration_ms=duration,
            meta=meta,
            run_id=run_id,
            proposal_id=proposal_id,
            apply_log_id=apply_log_id,
            error_code=error_code,
            error_msg=error_msg,
        )
    finally:
        store.close()


def _history_payload(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    by_strategy: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    by_market: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[row["snapshot_date"]].append(row)
        by_strategy[row["strategy_id"]].append(row)
        by_market[row["market"]].append(row)

    def _agg(items: List[Dict[str, Any]]) -> Dict[str, float]:
        returns = [float(x.get("ret_1d", 0.0) or 0.0) for x in items]
        hit_vals = [float(x.get("hit_flag", 0) or 0) for x in items]
        return {
            "count": float(len(items)),
            "return_mean": _safe_mean(returns) if returns else 0.0,
            "return_sum": float(sum(returns)),
            "hit_rate": _safe_mean(hit_vals) if hit_vals else 0.0,
            "max_drawdown_mean": _safe_mean([float(x.get("max_drawdown", 0.0) or 0.0) for x in items]) if items else 0.0,
        }

    by_day_rows: List[Dict[str, Any]] = []
    for day, items in sorted(by_day.items()):
        row = {"snapshot_date": day}
        row.update(_agg(items))
        by_day_rows.append(row)
    day_returns = [float(x["return_mean"]) for x in by_day_rows]
    summary = _agg(rows)
    summary["max_drawdown_curve"] = _max_drawdown(day_returns) if day_returns else 0.0

    by_strategy_rows: List[Dict[str, Any]] = []
    for sid, items in sorted(by_strategy.items()):
        row = {"strategy_id": sid}
        row.update(_agg(items))
        by_strategy_rows.append(row)

    by_market_rows: List[Dict[str, Any]] = []
    for market, items in sorted(by_market.items()):
        row = {"market": market}
        row.update(_agg(items))
        by_market_rows.append(row)

    return {
        "summary": summary,
        "by_day": by_day_rows,
        "by_strategy": by_strategy_rows,
        "by_market": by_market_rows,
        "rows": rows,
    }


def history_query(args: argparse.Namespace) -> int:
    start_date = _today_or(getattr(args, "start_date", ""))
    end_date = _today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else start_date
    strategies = [x.upper() for x in _split_csv(getattr(args, "strategies", ""))]
    markets = [x.upper() for x in _split_csv(getattr(args, "markets", ""))]
    store = SnapshotStore(_sqlite_path())
    try:
        rows = store.query_range(start_date, end_date, strategies=strategies or None, markets=markets or None)
    finally:
        store.close()
    print(json.dumps(_history_payload(rows), ensure_ascii=False, indent=2))
    return 0


def _build_experiment_baseline(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload = _history_payload(rows)
    return {
        "summary": payload.get("summary", {}),
        "by_strategy": payload.get("by_strategy", []),
        "by_market": payload.get("by_market", []),
    }


def _gate_decision(proposal: Dict[str, Any], args: argparse.Namespace, expected_experiment_id: str = "") -> Tuple[bool, str, Dict[str, Any]]:
    validation = proposal.get("validation", {}) if isinstance(proposal.get("validation", {}), dict) else {}
    stability = float(validation.get("stability", 0.0) or 0.0)
    hit_rate = float(proposal.get("hit_rate", 0.0) or 0.0)
    dd_mean = float(proposal.get("dd_mean", 0.0) or 0.0)
    min_stability = float(getattr(args, "gate_min_stability", 0.0) or 0.0)
    min_hit_rate = float(getattr(args, "gate_min_hit_rate", 0.0) or 0.0)
    max_dd_mean = float(getattr(args, "gate_max_dd_mean", 1.0) or 1.0)
    require_experiment = bool(getattr(args, "require_experiment", False))
    proposal_exp = (proposal.get("experiment_id", "") or "").strip()
    expected_exp = (expected_experiment_id or "").strip()
    reasons: List[str] = []

    if require_experiment and not (proposal_exp or expected_exp):
        reasons.append("missing experiment id")
    if expected_exp and proposal_exp and expected_exp != proposal_exp:
        reasons.append(f"experiment mismatch: proposal={proposal_exp} expected={expected_exp}")
    if stability < min_stability:
        reasons.append(f"stability={stability:.4f} < {min_stability:.4f}")
    if hit_rate < min_hit_rate:
        reasons.append(f"hit_rate={hit_rate:.4f} < {min_hit_rate:.4f}")
    if dd_mean > max_dd_mean:
        reasons.append(f"dd_mean={dd_mean:.4f} > {max_dd_mean:.4f}")

    gate = {
        "require_experiment": require_experiment,
        "proposal_experiment_id": proposal_exp,
        "expected_experiment_id": expected_exp,
        "stability": stability,
        "hit_rate": hit_rate,
        "dd_mean": dd_mean,
        "min_stability": min_stability,
        "min_hit_rate": min_hit_rate,
        "max_dd_mean": max_dd_mean,
    }
    return len(reasons) == 0, "; ".join(reasons), gate


def _minmax_scale(values: List[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if hi - lo < 1e-12:
        return [0.5 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]


def _score_selected_rows(rows: List[Dict[str, Any]], top_n: int) -> Dict[str, Any]:
    by_stock: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_stock[str(row.get("stock_id", ""))].append(row)

    scored: List[Dict[str, Any]] = []
    for stock_id, items in by_stock.items():
        if not stock_id:
            continue
        returns = [float(x.get("ret_1d", 0.0) or 0.0) for x in items]
        hit_vals = [float(x.get("hit_flag", 0) or 0.0) for x in items]
        dd_vals = [float(x.get("max_drawdown", 0.0) or 0.0) for x in items]
        sample = len(items)
        mean_ret = _safe_mean(returns) if returns else 0.0
        momentum_5 = _safe_mean(returns[-5:]) if returns else 0.0
        hit_rate = _safe_mean(hit_vals) if hit_vals else 0.0
        dd_mean = _safe_mean(dd_vals) if dd_vals else 0.0
        vol = _safe_stdev(returns) if len(returns) > 1 else 0.0
        latest = items[-1]
        scored.append(
            {
                "stock_id": stock_id,
                "stock_code": latest.get("stock_code", ""),
                "stock_name": latest.get("stock_name", ""),
                "market": latest.get("market", ""),
                "sample_count": sample,
                "mean_ret": mean_ret,
                "momentum_5": momentum_5,
                "hit_rate": hit_rate,
                "dd_mean": dd_mean,
                "vol": vol,
            }
        )

    if not scored:
        return {"candidates": [], "selected": [], "rebalance_plan": []}

    momentum_scaled = _minmax_scale([x["momentum_5"] for x in scored])
    hit_scaled = _minmax_scale([x["hit_rate"] for x in scored])
    dd_scaled = _minmax_scale([-x["dd_mean"] for x in scored])
    vol_scaled = _minmax_scale([-x["vol"] for x in scored])

    for idx, row in enumerate(scored):
        score = momentum_scaled[idx] * 0.35 + hit_scaled[idx] * 0.30 + dd_scaled[idx] * 0.20 + vol_scaled[idx] * 0.15
        row["score"] = round(score, 6)
        row["factor_breakdown"] = {
            "momentum_5": round(momentum_scaled[idx], 6),
            "hit_rate": round(hit_scaled[idx], 6),
            "drawdown_quality": round(dd_scaled[idx], 6),
            "vol_quality": round(vol_scaled[idx], 6),
        }

    ranked = sorted(scored, key=lambda x: (x["score"], x["sample_count"]), reverse=True)
    selected = ranked[: max(1, int(top_n))]
    score_sum = sum(max(0.0, float(x["score"])) for x in selected)
    rebalance: List[Dict[str, Any]] = []
    for row in selected:
        target_weight = (max(0.0, float(row["score"])) / score_sum) if score_sum > 0 else (1.0 / float(len(selected)))
        rebalance.append(
            {
                "stock_id": row["stock_id"],
                "stock_code": row["stock_code"],
                "stock_name": row["stock_name"],
                "target_weight": round(target_weight, 6),
                "score": row["score"],
            }
        )

    return {"candidates": ranked, "selected": selected, "rebalance_plan": rebalance}


def param_recommend(args: argparse.Namespace) -> int:
    started = _now_ms()
    start_date = _today_or(getattr(args, "start_date", ""))
    end_date = _today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else start_date
    strategies = [x.upper() for x in _split_csv(getattr(args, "strategies", ""))]
    markets = [x.upper() for x in _split_csv(getattr(args, "markets", ""))]

    try:
        snap = SnapshotStore(_sqlite_path())
        try:
            rows = snap.query_range(start_date, end_date, strategies=strategies or None, markets=markets or None)
        finally:
            snap.close()

        run_id = uuid4().hex[:12]
        experiment_id = (getattr(args, "experiment_id", "") or "").strip()
        experiment_name = (getattr(args, "experiment_name", "") or "").strip()
        store = ParamStore(_sqlite_path())
        try:
            strategies_scope = ",".join(strategies) if strategies else "*"
            markets_scope = ",".join(markets) if markets else "*"
            if not experiment_id:
                exp = store.create_experiment(
                    source_start_date=start_date,
                    source_end_date=end_date,
                    strategy_scope=strategies_scope,
                    market_scope=markets_scope,
                    walk_forward_splits=int(getattr(args, "walk_forward_splits", 3)),
                    cost_bps=float(getattr(args, "cost_bps", 3.0)),
                    slippage_bps=float(getattr(args, "slippage_bps", 2.0)),
                    train_window=int(getattr(args, "train_window", 60)),
                    valid_window=int(getattr(args, "valid_window", 20)),
                    experiment_name=experiment_name,
                    baseline=_build_experiment_baseline(rows),
                )
                experiment_id = str(exp.get("experiment_id", ""))
            else:
                _ = store.get_experiment(experiment_id)

            out_rows = store.create_proposals_from_history(
                snapshot_rows=rows,
                source_start_date=start_date,
                source_end_date=end_date,
                run_id=run_id,
                dry_run=bool(getattr(args, "dry_run", False)),
                walk_forward_splits=int(getattr(args, "walk_forward_splits", 3)),
                cost_bps=float(getattr(args, "cost_bps", 3.0)),
                slippage_bps=float(getattr(args, "slippage_bps", 2.0)),
                experiment_id=experiment_id,
            )
            store.update_experiment_report(
                experiment_id=experiment_id,
                report={
                    "run_id": run_id,
                    "proposal_count": len(out_rows),
                    "source_start_date": start_date,
                    "source_end_date": end_date,
                    "dry_run": bool(getattr(args, "dry_run", False)),
                    "strategies": strategies,
                    "markets": markets,
                },
                status="READY",
            )
        finally:
            store.close()

        print(
            json.dumps(
                {
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                    "source_start_date": start_date,
                    "source_end_date": end_date,
                    "proposal_count": len(out_rows),
                    "dry_run": bool(getattr(args, "dry_run", False)),
                    "proposals": out_rows,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        _log_param_event(
            action="param_recommend",
            status="SUCCESS",
            started_ms=started,
            meta={"proposal_count": len(out_rows), "dry_run": bool(getattr(args, "dry_run", False)), "experiment_id": experiment_id},
            run_id=run_id,
        )
        return 0
    except Exception as e:
        _log_param_event(
            action="param_recommend",
            status="FAILED",
            started_ms=started,
            meta={"dry_run": bool(getattr(args, "dry_run", False))},
            error_code="PARAM_RECOMMEND_FAILED",
            error_msg=str(e),
        )
        raise


def param_diff(args: argparse.Namespace) -> int:
    started = _now_ms()
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = _load_json_arg(getattr(args, "editor_json", ""))
    try:
        store = ParamStore(_sqlite_path())
        try:
            out = store.diff(proposal_id, editor_values=editor_values or None)
        finally:
            store.close()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        _log_param_event(
            action="param_diff",
            status="SUCCESS",
            started_ms=started,
            meta={"changed_count": int(out.get("changed_count", 0))},
            proposal_id=proposal_id,
        )
        return 0
    except Exception as e:
        _log_param_event(
            action="param_diff",
            status="FAILED",
            started_ms=started,
            meta={},
            proposal_id=proposal_id,
            error_code="PARAM_DIFF_FAILED",
            error_msg=str(e),
        )
        raise


def param_apply(args: argparse.Namespace) -> int:
    started = _now_ms()
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = _load_json_arg(getattr(args, "editor_json", ""))
    operator = (getattr(args, "operator", "") or os.getenv("OPERATOR", "local_user")).strip() or "local_user"
    comment = getattr(args, "comment", "") or ""
    expected_version = int(getattr(args, "expected_version", -1))
    batch_id = (getattr(args, "batch_id", "") or "").strip()
    rollout_scope = (getattr(args, "rollout_scope", "") or getattr(args, "gray_scope", "") or "full").strip() or "full"
    experiment_id = (getattr(args, "experiment_id", "") or "").strip()

    try:
        store = ParamStore(_sqlite_path())
        try:
            proposal = store.get_proposal(proposal_id)
            gate_passed, gate_reason, gate_payload = _gate_decision(proposal, args=args, expected_experiment_id=experiment_id)
            if not gate_passed:
                raise RuntimeError(f"release gate blocked: {gate_reason}")
            effective_experiment_id = experiment_id or str(proposal.get("experiment_id", "") or "")
            out = store.apply(
                proposal_id=proposal_id,
                editor_values=editor_values or None,
                operator=operator,
                comment=comment,
                expected_version=expected_version if expected_version >= 0 else None,
                batch_id=batch_id,
                rollout_scope=rollout_scope,
                experiment_id=effective_experiment_id,
                gate_passed=gate_passed,
                gate_reason=gate_reason,
            )
        finally:
            store.close()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        _log_param_event(
            action="param_apply",
            status="SUCCESS",
            started_ms=started,
            meta={
                "changed_count": int(out.get("changed_count", 0)),
                "batch_id": batch_id,
                "rollout_scope": rollout_scope,
                "experiment_id": str(out.get("experiment_id", "")),
                "gate": gate_payload,
            },
            proposal_id=proposal_id,
            apply_log_id=str(out.get("apply_log_id", "")),
        )
        return 0
    except Exception as e:
        _log_param_event(
            action="param_apply",
            status="FAILED",
            started_ms=started,
            meta={"batch_id": batch_id, "rollout_scope": rollout_scope, "experiment_id": experiment_id},
            proposal_id=proposal_id,
            error_code="PARAM_APPLY_CONFLICT" if "version conflict" in str(e).lower() else "PARAM_APPLY_FAILED",
            error_msg=str(e),
        )
        raise


def param_rollback(args: argparse.Namespace) -> int:
    started = _now_ms()
    apply_log_id = (getattr(args, "apply_log_id", "") or "").strip()
    if not apply_log_id:
        raise RuntimeError("apply-log-id is required")
    operator = (getattr(args, "operator", "") or os.getenv("OPERATOR", "local_user")).strip() or "local_user"
    comment = getattr(args, "comment", "") or ""
    try:
        store = ParamStore(_sqlite_path())
        try:
            out = store.rollback(apply_log_id=apply_log_id, operator=operator, comment=comment)
        finally:
            store.close()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        _log_param_event(
            action="param_rollback",
            status="SUCCESS",
            started_ms=started,
            meta={"rollback_ref": apply_log_id},
            apply_log_id=str(out.get("apply_log_id", "")),
        )
        return 0
    except Exception as e:
        _log_param_event(
            action="param_rollback",
            status="FAILED",
            started_ms=started,
            meta={"rollback_ref": apply_log_id},
            error_code="PARAM_ROLLBACK_FAILED",
            error_msg=str(e),
        )
        raise


def param_draft_save(args: argparse.Namespace) -> int:
    started = _now_ms()
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = _load_json_arg(getattr(args, "editor_json", ""))
    store = ParamStore(_sqlite_path())
    try:
        out = store.save_draft(proposal_id=proposal_id, editor_values=editor_values)
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    _log_param_event(
        action="param_draft_save",
        status="SUCCESS",
        started_ms=started,
        meta={},
        proposal_id=proposal_id,
    )
    return 0


def param_monitor(args: argparse.Namespace) -> int:
    days = int(getattr(args, "days", 7) or 7)
    store = ParamStore(_sqlite_path())
    try:
        out = store.get_monitor(days=days)
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def param_migrate(args: argparse.Namespace) -> int:
    db_path = _sqlite_path()
    before = 0
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute("SELECT value FROM _meta WHERE key='schema_version'").fetchone()
            if row and row[0] is not None:
                before = int(row[0])
        except Exception:
            before = 0
        finally:
            conn.close()
    store = ParamStore(db_path)
    try:
        after = store.get_schema_version()
        required_tables = [
            "_meta",
            "strategy_param_set",
            "strategy_param_proposal",
            "strategy_param_apply_log",
            "strategy_param_draft",
            "strategy_run_event",
            "strategy_research_experiment",
        ]
        exists = {}
        for t in required_tables:
            row = store.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
            exists[t] = bool(row)
    finally:
        store.close()
    print(
        json.dumps(
            {
                "sqlite_path": db_path,
                "schema_version_before": before,
                "schema_version_after": after,
                "schema_version_target": SCHEMA_VERSION,
                "tables": exists,
                "ok": after >= SCHEMA_VERSION and all(bool(v) for v in exists.values()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def param_risk_guard(args: argparse.Namespace) -> int:
    started = _now_ms()
    days = max(1, int(getattr(args, "days", 7) or 7))
    apply_lookback_days = max(days, int(getattr(args, "apply_lookback_days", 30) or 30))
    min_hit_rate = float(getattr(args, "min_hit_rate", 0.45) or 0.45)
    max_drawdown_curve = float(getattr(args, "max_drawdown_curve", 0.2) or 0.2)
    dry_run = bool(getattr(args, "dry_run", False))

    end_date = dt.date.today().isoformat()
    start_date = (dt.date.today() - dt.timedelta(days=days - 1)).isoformat()
    snap = SnapshotStore(_sqlite_path())
    try:
        rows = snap.query_range(start_date=start_date, end_date=end_date)
    finally:
        snap.close()

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = f"{str(row.get('strategy_id', '')).upper()}|{str(row.get('market', '')).upper()}"
        grouped[key].append(row)

    stat: Dict[str, Dict[str, float]] = {}
    for key, items in grouped.items():
        hit_vals = [float(x.get("hit_flag", 0) or 0.0) for x in items]
        by_day: Dict[str, List[float]] = defaultdict(list)
        for x in items:
            by_day[str(x.get("snapshot_date", ""))].append(float(x.get("ret_1d", 0.0) or 0.0))
        day_mean = [(_safe_mean(arr) if arr else 0.0) for _, arr in sorted(by_day.items())]
        stat[key] = {
            "hit_rate": _safe_mean(hit_vals) if hit_vals else 0.0,
            "drawdown_curve": _max_drawdown(day_mean) if day_mean else 0.0,
            "sample_count": float(len(items)),
        }

    store = ParamStore(_sqlite_path())
    actions: List[Dict[str, Any]] = []
    try:
        candidates = store.list_recent_applies(days=apply_lookback_days)
        for row in candidates:
            sid = str(row.get("strategy_id", "")).upper()
            market = str(row.get("market", "")).upper()
            key = f"{sid}|{market}"
            s = stat.get(key, {"hit_rate": 0.0, "drawdown_curve": 0.0, "sample_count": 0.0})
            breached = []
            if float(s.get("hit_rate", 0.0)) < min_hit_rate:
                breached.append(f"hit_rate={float(s.get('hit_rate', 0.0)):.4f} < {min_hit_rate:.4f}")
            if float(s.get("drawdown_curve", 0.0)) > max_drawdown_curve:
                breached.append(f"drawdown_curve={float(s.get('drawdown_curve', 0.0)):.4f} > {max_drawdown_curve:.4f}")
            if not breached:
                continue
            apply_log_id = str(row.get("apply_log_id", ""))
            action = {
                "apply_log_id": apply_log_id,
                "strategy_id": sid,
                "market": market,
                "breach": breached,
                "dry_run": dry_run,
                "rolled_back": False,
            }
            if not dry_run and apply_log_id:
                rb = store.rollback(apply_log_id=apply_log_id, operator="risk_guard", comment="auto rollback by param-risk-guard")
                action["rolled_back"] = True
                action["rollback_apply_log_id"] = rb.get("apply_log_id")
            actions.append(action)
    finally:
        store.close()

    out = {
        "window_days": days,
        "apply_lookback_days": apply_lookback_days,
        "min_hit_rate": min_hit_rate,
        "max_drawdown_curve": max_drawdown_curve,
        "dry_run": dry_run,
        "candidate_count": len(actions),
        "actions": actions,
    }
    _log_param_event(
        action="param_risk_guard",
        status="SUCCESS",
        started_ms=started,
        meta={"candidate_count": len(actions), "dry_run": dry_run, "days": days},
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def select_stock(args: argparse.Namespace) -> int:
    start_date = _today_or(getattr(args, "start_date", "")) if getattr(args, "start_date", "") else (dt.date.today() - dt.timedelta(days=60)).isoformat()
    end_date = _today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else dt.date.today().isoformat()
    strategies = [x.upper() for x in _split_csv(getattr(args, "strategies", ""))]
    markets = [x.upper() for x in _split_csv(getattr(args, "markets", ""))]
    top_n = max(1, int(getattr(args, "top_n", 10) or 10))
    min_samples = max(1, int(getattr(args, "min_samples", 5) or 5))

    snap = SnapshotStore(_sqlite_path())
    try:
        rows = snap.query_range(start_date=start_date, end_date=end_date, strategies=strategies or None, markets=markets or None)
    finally:
        snap.close()
    rows = [r for r in rows if int(r.get("sample_count", 0) or 0) >= min_samples]
    score_out = _score_selected_rows(rows, top_n=top_n)
    print(
        json.dumps(
            {
                "start_date": start_date,
                "end_date": end_date,
                "strategy_filter": strategies,
                "market_filter": markets,
                "min_samples": min_samples,
                "top_n": top_n,
                "candidate_count": len(score_out.get("candidates", [])),
                "selected_count": len(score_out.get("selected", [])),
                "selected": score_out.get("selected", []),
                "rebalance_plan": score_out.get("rebalance_plan", []),
                "candidates": score_out.get("candidates", []),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _resolve_snapshot_notion_fields(snapshot_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = snapshot_db.get("properties", {})
    return {
        "title": find_title_property_name(snapshot_db),
        "date": _find_prop_name(props, ["日期", "快照日期", "snapshot_date"], ["date", "rich_text"]),
        "strategy": _find_prop_name(props, ["策略", "strategy_id"], ["select", "status", "rich_text", "title"]),
        "stock_code": _find_prop_name(props, ["股票代码", "代码", "stock_code"], ["rich_text", "title"]),
        "stock_name": _find_prop_name(props, ["股票", "股票名称", "stock_name"], ["rich_text", "title"]),
        "market": _find_prop_name(props, ["市场", "market"], ["select", "status", "rich_text", "title"]),
        "ret_1d": _find_prop_name(props, ["收益", "ret_1d", "1D收益"], ["number"]),
        "max_drawdown": _find_prop_name(props, ["回撤", "max_drawdown"], ["number"]),
        "hit_rate": _find_prop_name(props, ["命中率", "hit_rate", "hit_flag"], ["number"]),
        "sample_count": _find_prop_name(props, ["样本数", "sample_count"], ["number"]),
        "action": _find_prop_name(props, ["动作", "action"], ["select", "status", "rich_text", "title"]),
        "confidence": _find_prop_name(props, ["置信度", "confidence"], ["select", "status", "rich_text", "title"]),
    }


def _snapshot_notion_key_from_page(page: Dict[str, Any], fields: Dict[str, Optional[str]]) -> str:
    date_v = _prop_text_any(page, fields["date"]) if fields.get("date") else ""
    strategy_v = _prop_text_any(page, fields["strategy"]) if fields.get("strategy") else ""
    code_v = _prop_text_any(page, fields["stock_code"]) if fields.get("stock_code") else ""
    return f"{date_v}|{strategy_v.upper()}|{code_v.upper()}"


def sync_snapshot_notion(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    if not cfg.strategy_snapshot_id:
        raise RuntimeError("Missing DB_STRATEGY_SNAPSHOT_ID for snapshot notion sync.")
    snapshot_date = _today_or(getattr(args, "snapshot_date", ""))
    store = SnapshotStore(_sqlite_path())
    try:
        rows = store.query_range(snapshot_date, snapshot_date)
    finally:
        store.close()

    snapshot_db = client.get_database(cfg.strategy_snapshot_id)
    db_props = snapshot_db.get("properties", {})
    fields = _resolve_snapshot_notion_fields(snapshot_db)
    if not fields.get("date") or not fields.get("strategy") or not fields.get("stock_code"):
        raise RuntimeError("Notion snapshot DB must contain 日期/策略/股票代码 fields.")

    existing_rows: List[Dict[str, Any]]
    date_field = fields["date"]
    date_field_type = db_props.get(date_field, {}).get("type") if date_field else ""
    if date_field and date_field_type == "date":
        existing_rows = client.query_database_all(
            cfg.strategy_snapshot_id,
            filter_obj={"property": date_field, "date": {"equals": snapshot_date}},
        )
    else:
        existing_rows = client.query_database_all(cfg.strategy_snapshot_id)
    existing_index: Dict[str, str] = {}
    for row in existing_rows:
        key = _snapshot_notion_key_from_page(row, fields)
        if key and key not in existing_index:
            existing_index[key] = row.get("id", "")

    created = 0
    updated = 0
    for row in rows:
        key = f"{row['snapshot_date']}|{row['strategy_id'].upper()}|{row['stock_code'].upper()}"
        payload: Dict[str, Any] = {}
        write_map = [
            (fields["date"], row["snapshot_date"]),
            (fields["strategy"], row["strategy_id"]),
            (fields["stock_code"], row["stock_code"]),
            (fields["stock_name"], row["stock_name"]),
            (fields["market"], row["market"]),
            (fields["ret_1d"], row["ret_1d"]),
            (fields["max_drawdown"], row["max_drawdown"]),
            (fields["hit_rate"], row["hit_flag"]),
            (fields["sample_count"], row["sample_count"]),
            (fields["action"], row["action"]),
            (fields["confidence"], row["confidence"]),
        ]
        for prop_name, value in write_map:
            if not prop_name:
                continue
            mapped = _write_prop_value(db_props, prop_name, value)
            if mapped is not None:
                payload[prop_name] = mapped
        title_name = fields.get("title")
        if title_name and title_name not in payload:
            title_text = f"{row['snapshot_date']} {row['strategy_id']} {row['stock_code']}"
            mapped = _write_prop_value(db_props, title_name, title_text)
            if mapped is not None:
                payload[title_name] = mapped

        existing_id = existing_index.get(key, "")
        if existing_id:
            if not getattr(args, "dry_run", False):
                client.update_page(existing_id, payload)
            updated += 1
        else:
            if not getattr(args, "dry_run", False):
                created_page = client.create_page(cfg.strategy_snapshot_id, payload)
                new_id = created_page.get("id", "")
                if new_id:
                    existing_index[key] = new_id
            created += 1

    out = {
        "snapshot_date": snapshot_date,
        "local_rows": len(rows),
        "updated": updated,
        "created": created,
        "dry_run": bool(getattr(args, "dry_run", False)),
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

    s_ma = sub.add_parser("migrate-apply", help="执行历史数据迁移到交易流水")
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

    sub.add_parser("validate-manual", help="校验手工记录必填字段")

    s_ys = sub.add_parser("sync-annual", help="按交易+分红重算年度收益")
    s_ys.add_argument("--dry-run", action="store_true")

    s_rec = sub.add_parser("recommend-prices", help="计算并回写每只股票的下一次交易建议价位")
    s_rec.add_argument("--dry-run", action="store_true", help="仅输出建议，不回写Notion")
    s_rec.add_argument("--asof-date", default="", help="建议更新时间，默认今天 YYYY-MM-DD")
    s_rec.add_argument("--allow-small-sample", action="store_true", default=True)
    s_rec.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    s_rec.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    s_rec.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")
    s_rec.add_argument("--data-source", choices=["kline", "trade"], default="kline", help="策略输入数据源，默认 kline")
    s_rec.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K线复权口径")
    s_rec.add_argument("--start-date", default="", help="K线开始日期，默认自动回看")
    s_rec.add_argument("--end-date", default="", help="K线结束日期，默认今天 YYYY-MM-DD")
    s_rec.add_argument("--refresh-prices", action="store_true", help="先同步实时价格再计算建议")
    s_rec.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")
    s_rec.add_argument("--emit-snapshot", action="store_true", help="recommend后写入每日快照")
    s_rec.add_argument("--snapshot-date", default="", help="快照日期，默认今天 YYYY-MM-DD")
    s_rec.add_argument("--param-market", default="", help="参数市场，默认按股票代码推断")
    s_rec.add_argument("--param-scope", default="*", help="参数作用域，默认 *")

    s_bt = sub.add_parser("backtest-recommendation", help="回测建议模型")
    s_bt.add_argument("--window", type=int, default=60, help="历史窗口长度（默认按日线K线）")
    s_bt.add_argument("--allow-small-sample", action="store_true", default=True)
    s_bt.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    s_bt.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    s_bt.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")
    s_bt.add_argument("--data-source", choices=["kline", "trade"], default="kline", help="回测输入数据源，默认 kline")
    s_bt.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K线复权口径")
    s_bt.add_argument("--start-date", default="", help="K线开始日期，默认自动回看")
    s_bt.add_argument("--end-date", default="", help="K线结束日期，默认今天 YYYY-MM-DD")
    s_bt.add_argument("--force", action="store_true", help="强制重新同步K线")
    s_bt.add_argument("--param-market", default="", help="参数市场，默认按股票代码推断")
    s_bt.add_argument("--param-scope", default="*", help="参数作用域，默认 *")

    s_sp = sub.add_parser("sync-prices", help="自动拉取实时行情并回写当前市价")
    s_sp.add_argument("--dry-run", action="store_true", help="仅拉取并输出统计，不写入Notion")
    s_sp.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")

    s_sk = sub.add_parser("sync-kline", help="同步日线K线到本地SQLite缓存")
    s_sk.add_argument("--start-date", default="", help="开始日期，默认自动回看")
    s_sk.add_argument("--end-date", default="", help="结束日期，默认今天 YYYY-MM-DD")
    s_sk.add_argument("--symbols", default="", help="逗号分隔股票代码，如 600519,000001")
    s_sk.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K线复权口径")
    s_sk.add_argument("--force", action="store_true", help="强制覆盖缓存")

    s_sd = sub.add_parser("snapshot-daily", help="生成并落库每日策略快照")
    s_sd.add_argument("--dry-run", action="store_true", help="仅预览，不写入SQLite")
    s_sd.add_argument("--snapshot-date", default="", help="快照日期，默认今天 YYYY-MM-DD")
    s_sd.add_argument("--allow-small-sample", action="store_true", default=True)
    s_sd.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    s_sd.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    s_sd.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")
    s_sd.add_argument("--data-source", choices=["kline", "trade"], default="kline", help="快照输入数据源，默认 kline")
    s_sd.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K线复权口径")
    s_sd.add_argument("--start-date", default="", help="K线开始日期，默认自动回看")
    s_sd.add_argument("--end-date", default="", help="K线结束日期，默认今天 YYYY-MM-DD")
    s_sd.add_argument("--force", action="store_true", help="强制重新同步K线")
    s_sd.add_argument("--refresh-prices", action="store_true", help="先同步实时价格再快照")
    s_sd.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")
    s_sd.add_argument("--param-market", default="", help="参数市场，默认按股票代码推断")
    s_sd.add_argument("--param-scope", default="*", help="参数作用域，默认 *")

    s_sn = sub.add_parser("sync-snapshot-notion", help="同步指定日期快照到Notion策略快照库")
    s_sn.add_argument("--snapshot-date", default="", help="同步日期，默认今天 YYYY-MM-DD")
    s_sn.add_argument("--dry-run", action="store_true", help="仅预览，不写入Notion")

    s_hq = sub.add_parser("history-query", help="查询历史快照并输出JSON")
    s_hq.add_argument("--start-date", default="", help="开始日期，默认今天 YYYY-MM-DD")
    s_hq.add_argument("--end-date", default="", help="结束日期，默认开始日期 YYYY-MM-DD")
    s_hq.add_argument("--strategies", default="", help="策略过滤，如 BASELINE,CHAN")
    s_hq.add_argument("--markets", default="", help="市场过滤，如 SH,SZ,HK")

    s_pr = sub.add_parser("param-recommend", help="基于历史快照生成参数推荐")
    s_pr.add_argument("--start-date", default="", help="开始日期，默认今天 YYYY-MM-DD")
    s_pr.add_argument("--end-date", default="", help="结束日期，默认开始日期 YYYY-MM-DD")
    s_pr.add_argument("--strategies", default="", help="策略过滤，如 BASELINE,CHAN")
    s_pr.add_argument("--markets", default="", help="市场过滤，如 SH,SZ,HK")
    s_pr.add_argument("--dry-run", action="store_true", help="仅计算推荐，不写入proposal表")
    s_pr.add_argument("--walk-forward-splits", type=int, default=3, help="walk-forward 分窗数量")
    s_pr.add_argument("--cost-bps", type=float, default=3.0, help="交易成本bps")
    s_pr.add_argument("--slippage-bps", type=float, default=2.0, help="滑点bps")
    s_pr.add_argument("--experiment-id", default="", help="绑定已有实验ID；留空则自动创建")
    s_pr.add_argument("--experiment-name", default="", help="实验名称（自动创建时生效）")
    s_pr.add_argument("--train-window", type=int, default=60, help="实验训练窗口（交易日）")
    s_pr.add_argument("--valid-window", type=int, default=20, help="实验验证窗口（交易日）")

    s_pd = sub.add_parser("param-diff", help="比较当前值、推荐值、人工编辑值")
    s_pd.add_argument("--proposal-id", required=True, help="参数推荐ID")
    s_pd.add_argument("--editor-json", default="", help="人工编辑JSON文本或json文件路径")

    s_pa = sub.add_parser("param-apply", help="应用参数推荐（支持人工编辑）")
    s_pa.add_argument("--proposal-id", required=True, help="参数推荐ID")
    s_pa.add_argument("--editor-json", default="", help="人工编辑JSON文本或json文件路径")
    s_pa.add_argument("--expected-version", type=int, default=-1, help="并发保护版本号，-1表示不校验")
    s_pa.add_argument("--operator", default="", help="操作者，默认取 OPERATOR 或 local_user")
    s_pa.add_argument("--comment", default="", help="应用备注")
    s_pa.add_argument("--batch-id", default="", help="发布批次ID")
    s_pa.add_argument("--rollout-scope", default="full", help="灰度范围，如 full/market:SH/strategy:BASELINE")
    s_pa.add_argument("--gray-scope", default="", help="兼容旧参数名，等价于 --rollout-scope")
    s_pa.add_argument("--experiment-id", default="", help="发布闸门绑定实验ID")
    s_pa.add_argument("--require-experiment", action="store_true", help="发布必须绑定实验ID")
    s_pa.add_argument("--gate-min-stability", type=float, default=0.0, help="发布闸门：最小稳定性")
    s_pa.add_argument("--gate-min-hit-rate", type=float, default=0.0, help="发布闸门：最小命中率")
    s_pa.add_argument("--gate-max-dd-mean", type=float, default=1.0, help="发布闸门：最大平均回撤")

    s_proll = sub.add_parser("param-rollback", help="按 apply_log_id 回滚参数")
    s_proll.add_argument("--apply-log-id", required=True, help="apply_log_id")
    s_proll.add_argument("--operator", default="", help="操作者，默认取 OPERATOR 或 local_user")
    s_proll.add_argument("--comment", default="", help="回滚备注")

    s_pdraft = sub.add_parser("param-draft-save", help="保存参数编辑草稿")
    s_pdraft.add_argument("--proposal-id", required=True, help="参数推荐ID")
    s_pdraft.add_argument("--editor-json", default="", help="人工编辑JSON文本或json文件路径")

    s_pmon = sub.add_parser("param-monitor", help="输出参数系统健康与最近异常")
    s_pmon.add_argument("--days", type=int, default=7, help="统计窗口天数")

    sub.add_parser("param-migrate", help="执行参数系统Schema迁移并输出校验结果")

    s_prg = sub.add_parser("param-risk-guard", help="风险守护：命中率/回撤劣化自动回滚")
    s_prg.add_argument("--days", type=int, default=7, help="观察窗口天数")
    s_prg.add_argument("--apply-lookback-days", type=int, default=30, help="回看最近应用窗口天数")
    s_prg.add_argument("--min-hit-rate", type=float, default=0.45, help="最小命中率阈值")
    s_prg.add_argument("--max-drawdown-curve", type=float, default=0.2, help="最大回撤阈值")
    s_prg.add_argument("--dry-run", action="store_true", help="仅预览，不执行回滚")

    s_ss = sub.add_parser("select-stock", help="规则打分选股并输出调仓建议")
    s_ss.add_argument("--start-date", default="", help="开始日期，默认今天往前60天")
    s_ss.add_argument("--end-date", default="", help="结束日期，默认今天")
    s_ss.add_argument("--strategies", default="", help="策略过滤，如 BASELINE,CHAN")
    s_ss.add_argument("--markets", default="", help="市场过滤，如 SH,SZ,HK")
    s_ss.add_argument("--top-n", type=int, default=10, help="输出Top N")
    s_ss.add_argument("--min-samples", type=int, default=5, help="最小样本数过滤")

    return p
def main() -> int:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(script_dir)
    load_dotenv(os.path.join(root, ".env"))

    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "history-query":
        return history_query(args)
    if args.cmd == "param-recommend":
        return param_recommend(args)
    if args.cmd == "param-diff":
        return param_diff(args)
    if args.cmd == "param-apply":
        return param_apply(args)
    if args.cmd == "param-rollback":
        return param_rollback(args)
    if args.cmd == "param-draft-save":
        return param_draft_save(args)
    if args.cmd == "param-monitor":
        return param_monitor(args)
    if args.cmd == "param-migrate":
        return param_migrate(args)
    if args.cmd == "param-risk-guard":
        return param_risk_guard(args)
    if args.cmd == "select-stock":
        return select_stock(args)

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
    if args.cmd == "sync-kline":
        return sync_kline(client, cfg, args)
    if args.cmd == "snapshot-daily":
        return snapshot_daily(client, cfg, args)
    if args.cmd == "sync-snapshot-notion":
        return sync_snapshot_notion(client, cfg, args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())




