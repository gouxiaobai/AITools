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
from param_store import ParamStore


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


def load_cfg() -> Cfg:
    return Cfg(
        stock_master_id=os.getenv("DB_STOCK_MASTER_ID", "9ff0bf7d-9ae4-41c8-9440-729daaa2a95d"),
        std_trades_id=os.getenv("DB_STD_TRADES_ID", "33c225a4-e273-810f-ae9f-d44f9d44d528"),
        std_dividend_id=os.getenv("DB_STD_DIVIDEND_ID", "33c225a4-e273-8112-9444-f798532e60cf"),
        annual_id=os.getenv("DB_ANNUAL_ID", "33c225a4-e273-8162-8804-dfde58582535"),
        buy_wide_id=os.getenv("DB_BUY_WIDE_ID", "0d485b47-e903-4fd3-901e-1bb4d09200f1"),
        t_record_id=os.getenv("DB_T_RECORD_ID", "93dde4b0-5d6f-4c49-a825-e49ae95be420"),
        strategy_snapshot_id=os.getenv("DB_STRATEGY_SNAPSHOT_ID", ""),
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
    rows = client.query_database_all(cfg.stock_master_id)
    by_name: Dict[str, str] = {}
    by_code: Dict[str, str] = {}
    for r in rows:
        pid = r.get("id")
        name = p_title(r, "鑲＄エ")
        code = p_rich(r, "鑲＄エ浠ｇ爜")
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
        raise ValueError(f"stock '{args.stock}' not found in 鑲＄エ涓绘。 title or 鑲＄エ浠ｇ爜")

    title = f"{args.date} {args.direction} {args.stock} {args.shares}@{args.price}"
    props: Dict[str, Any] = {
        "璁板綍": title_prop(title),
        "鏃ユ湡": {"date": {"start": args.date}},
        "鏂瑰悜": {"select": {"name": args.direction}},
        "鑲℃暟": {"number": float(args.shares)},
        "浠锋牸": {"number": float(args.price)},
        "手续费": {"number": float(args.fee)},
        "绋庤垂": {"number": float(args.tax)},
        "鑲＄エ": {"relation": [{"id": stock_id}]},
        "source_table": {"select": {"name": "manual"}},
        "import_status": {"select": {"name": "ready"}},
    }
    if args.strategy:
        props["绛栫暐"] = {"select": {"name": args.strategy}}
    if args.note:
        props["澶囨敞"] = {"rich_text": [{"type": "text", "text": {"content": args.note[:2000]}}]}

    page = client.create_page(cfg.std_trades_id, props)
    print(f"鏂板浜ゆ槗鎴愬姛: id={page.get('id')}")
    return 0


def validate_manual_entries(client: NotionClient, cfg: Cfg) -> int:
    rows = client.query_database_all(cfg.std_trades_id)
    required = ["鏃ユ湡", "鏂瑰悜", "鑲℃暟", "浠锋牸", "鑲＄エ", "璁板綍"]
    failures: List[Tuple[str, List[str]]] = []
    checked = 0

    for r in rows:
        source = p_select(r, "source_table")
        if source and source != "manual":
            continue
        checked += 1
        missing: List[str] = []
        if not p_date(r, "鏃ユ湡"):
            missing.append("鏃ユ湡")
        if p_select(r, "鏂瑰悜") not in {"BUY", "SELL"}:
            missing.append("鏂瑰悜")
        if p_number(r, "鑲℃暟") is None:
            missing.append("鑲℃暟")
        if p_number(r, "浠锋牸") is None:
            missing.append("浠锋牸")
        if len(p_relation_ids(r, "鑲＄エ")) == 0:
            missing.append("鑲＄エ")
        if not p_title(r, "璁板綍"):
            missing.append("璁板綍")
        if missing:
            failures.append((r.get("id", ""), missing))

    print(f"妫€鏌ヨ褰曟暟(manual/鏈爣娉?: {checked}")
    print(f"涓嶅悎瑙勮褰曟暟: {len(failures)}")
    for rid, missing in failures[:20]:
        print(f"- {rid}: 缂哄け {','.join(missing)}")
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
        "out_conf": _find_prop_name(props, ["建议置信度"], ["select", "status", "rich_text", "title"]),
        "out_mode": _find_prop_name(props, ["建议模式"], ["select", "status", "rich_text", "title"]),
        "out_reason": _find_prop_name(props, ["建议原因", "触发原因"], ["rich_text", "title"]),
        "out_time": _find_prop_name(props, ["建议更新时间"], ["date", "rich_text"]),
    }


def _resolve_trade_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    return {
        "date": _find_prop_name(props, ["鏃ユ湡"], ["date"]),
        "direction": _find_prop_name(props, ["鏂瑰悜"], ["select", "status"]),
        "shares": _find_prop_name(props, ["鑲℃暟"], ["number"]),
        "price": _find_prop_name(props, ["浠锋牸"], ["number"]),
        "stock": _find_prop_name(props, ["鑲＄エ"], ["relation"]),
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
        fields["out_action"] = _find_prop_by_keywords(props, ["建议动作"], ["select", "status", "rich_text", "title"])
    if not fields.get("out_buy"):
        fields["out_buy"] = _find_prop_by_keywords(props, ["建议买入价"], ["number"])
    if not fields.get("out_sell"):
        fields["out_sell"] = _find_prop_by_keywords(props, ["建议卖出价"], ["number"])
    if not fields.get("out_stop"):
        fields["out_stop"] = _find_prop_by_keywords(props, ["建议止损价"], ["number"])
    if not fields.get("out_pos"):
        fields["out_pos"] = _find_prop_by_keywords(props, ["建议仓位变化"], ["number"])
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
    if vol > 0.10:
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
    band = _clamp(max(vol * 1.2, 0.01), 0.01, 0.12)

    buy_price = current_price * (1.0 - band)
    sell_price = current_price * (1.0 + band)
    stop_price = current_price * (1.0 - band * 1.8)

    if current_cost is not None and current_cost > 0:
        buy_price = min(buy_price, current_cost * 0.995)
        sell_price = max(sell_price, current_cost * 1.005)

    action = "HOLD"
    reason = "neutral"
    if trend > 0.015:
        action = "BUY"
        reason = "up trend"
    elif trend < -0.015:
        action = "SELL"
        reason = "down trend"

    expected_up = max(0.0, (sell_price - current_price) / current_price)
    expected_down = max(0.0, (current_price - stop_price) / current_price)
    if expected_up <= expected_down * 0.8:
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
) -> Dict[str, Any]:
    base = _recommend_from_points(
        current_price=current_price,
        current_cost=current_cost,
        points=points,
        allow_small_sample=allow_small_sample,
        min_confidence=min_confidence,
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
) -> Dict[str, Any]:
    base = _recommend_from_points(
        current_price=current_price,
        current_cost=current_cost,
        points=points,
        allow_small_sample=allow_small_sample,
        min_confidence=min_confidence,
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


def _recommend_by_strategy(
    strategy_id: str,
    current_price: Optional[float],
    current_cost: Optional[float],
    points: List[TradePoint],
    allow_small_sample: bool,
    min_confidence: str,
) -> Dict[str, Any]:
    sid = strategy_id.lower()
    if sid == "baseline":
        rec = _recommend_from_points(current_price, current_cost, points, allow_small_sample, min_confidence)
    elif sid == "chan":
        rec = _recommend_chan_from_points(current_price, current_cost, points, allow_small_sample, min_confidence)
    elif sid == "atr_wave":
        rec = _recommend_atr_wave_from_points(current_price, current_cost, points, allow_small_sample, min_confidence)
    else:
        raise ValueError(f"unsupported strategy: {strategy_id}")
    rec["strategy_id"] = sid.upper()
    return rec


def _prepare_recommendation_context(
    client: NotionClient,
    cfg: Cfg,
    args: argparse.Namespace,
) -> Tuple[Dict[str, Any], Dict[str, Optional[str]], List[Dict[str, Any]], Dict[str, List[TradePoint]]]:
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
    return stock_db, stock_fields, stock_rows, stock_points


def _collect_recommendations(
    stock_rows: List[Dict[str, Any]],
    stock_fields: Dict[str, Optional[str]],
    stock_points: Dict[str, List[TradePoint]],
    args: argparse.Namespace,
) -> List[Dict[str, Any]]:
    selected_strategies = _parse_strategy_set(getattr(args, "strategy_set", "baseline,chan,atr_wave"))
    recs: List[Dict[str, Any]] = []
    for row in stock_rows:
        stock_id = row.get("id", "")
        title = p_title(row, stock_fields["title"]) if stock_fields["title"] else stock_id
        code_raw = _prop_text_any(row, stock_fields["stock_code"]) if stock_fields.get("stock_code") else ""
        current_price = p_number(row, stock_fields["current_price"]) if stock_fields["current_price"] else None
        current_cost = p_number(row, stock_fields["current_cost"]) if stock_fields["current_cost"] else None
        for sid in selected_strategies:
            rec = _recommend_by_strategy(
                strategy_id=sid,
                current_price=current_price,
                current_cost=current_cost,
                points=stock_points.get(stock_id, []),
                allow_small_sample=args.allow_small_sample,
                min_confidence=args.min_confidence,
            )
            rec["stock_id"] = stock_id
            rec["stock_name"] = title
            rec["stock_code"] = code_raw
            recs.append(rec)
    return recs


def _price_returns(points: List[TradePoint]) -> List[float]:
    valid_prices = [p.price for p in points if p.price is not None and p.price > 0]
    out: List[float] = []
    for i in range(1, len(valid_prices)):
        prev = valid_prices[i - 1]
        curr = valid_prices[i]
        if prev > 0:
            out.append((curr - prev) / prev)
    return out


def _ret_1d(points: List[TradePoint]) -> float:
    valid_prices = [p.price for p in points if p.price is not None and p.price > 0]
    if len(valid_prices) < 2:
        return 0.0
    prev = valid_prices[-2]
    curr = valid_prices[-1]
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
    stock_points: Dict[str, List[TradePoint]],
    snapshot_date: str,
    market_rule: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    now = dt.datetime.now().isoformat(timespec="seconds")
    rows: List[Dict[str, Any]] = []
    for rec in recs:
        stock_id = rec.get("stock_id", "")
        points = stock_points.get(stock_id, [])
        returns = _price_returns(points)
        ret_1d = _ret_1d(points)
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


def _emit_snapshot(recs: List[Dict[str, Any]], stock_points: Dict[str, List[TradePoint]], snapshot_date: str, dry_run: bool) -> Dict[str, Any]:
    run_id = uuid4().hex[:12]
    rows = _build_snapshot_rows(
        recs=recs,
        stock_points=stock_points,
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


def recommend_prices(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db, stock_fields, stock_rows, stock_points = _prepare_recommendation_context(client, cfg, args)

    db_props = stock_db.get("properties", {})
    asof_date = args.asof_date or dt.date.today().isoformat()

    recs = _collect_recommendations(stock_rows, stock_fields, stock_points, args)
    rec_by_stock: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in recs:
        rec_by_stock[rec.get("stock_id", "")].append(rec)

    for row in stock_rows:
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
        _emit_snapshot(
            recs=recs,
            stock_points=stock_points,
            snapshot_date=_today_or(getattr(args, "snapshot_date", "")),
            dry_run=bool(getattr(args, "dry_run", False)),
        )

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

    selected_strategies = _parse_strategy_set(getattr(args, "strategy_set", "baseline,chan,atr_wave"))
    mode_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_mode_returns: Dict[str, List[float]] = defaultdict(list)
    strategy_hold_counts: Dict[str, int] = defaultdict(int)
    strategy_total_counts: Dict[str, int] = defaultdict(int)
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
                rec = _recommend_by_strategy(
                    strategy_id=sid,
                    current_price=curr.price,
                    current_cost=None,
                    points=hist,
                    allow_small_sample=args.allow_small_sample,
                    min_confidence=args.min_confidence,
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

    out = {
        "baseline": _returns_metrics(baseline_returns),
        "strategy_all": _returns_metrics(all_strategy_returns),
        "strategy_by_mode": {k: _returns_metrics(v) for k, v in mode_returns.items()},
        "strategy_metrics": strategy_metrics,
        "strategy_mode_metrics": {k: _returns_metrics(v) for k, v in strategy_mode_returns.items()},
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def snapshot_daily(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db, stock_fields, stock_rows, stock_points = _prepare_recommendation_context(client, cfg, args)
    _ = stock_db
    _ = stock_fields
    _ = stock_rows
    recs = _collect_recommendations(stock_rows, stock_fields, stock_points, args)
    result = _emit_snapshot(
        recs=recs,
        stock_points=stock_points,
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


def param_recommend(args: argparse.Namespace) -> int:
    start_date = _today_or(getattr(args, "start_date", ""))
    end_date = _today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else start_date
    strategies = [x.upper() for x in _split_csv(getattr(args, "strategies", ""))]
    markets = [x.upper() for x in _split_csv(getattr(args, "markets", ""))]

    snap = SnapshotStore(_sqlite_path())
    try:
        rows = snap.query_range(start_date, end_date, strategies=strategies or None, markets=markets or None)
    finally:
        snap.close()

    run_id = uuid4().hex[:12]
    store = ParamStore(_sqlite_path())
    try:
        out_rows = store.create_proposals_from_history(
            snapshot_rows=rows,
            source_start_date=start_date,
            source_end_date=end_date,
            run_id=run_id,
            dry_run=bool(getattr(args, "dry_run", False)),
        )
    finally:
        store.close()

    print(
        json.dumps(
            {
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
    return 0


def param_diff(args: argparse.Namespace) -> int:
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = _load_json_arg(getattr(args, "editor_json", ""))
    store = ParamStore(_sqlite_path())
    try:
        out = store.diff(proposal_id, editor_values=editor_values or None)
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def param_apply(args: argparse.Namespace) -> int:
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = _load_json_arg(getattr(args, "editor_json", ""))
    operator = (getattr(args, "operator", "") or os.getenv("OPERATOR", "local_user")).strip() or "local_user"
    comment = getattr(args, "comment", "") or ""
    expected_version = int(getattr(args, "expected_version", -1))

    store = ParamStore(_sqlite_path())
    try:
        out = store.apply(
            proposal_id=proposal_id,
            editor_values=editor_values or None,
            operator=operator,
            comment=comment,
            expected_version=expected_version if expected_version >= 0 else None,
        )
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def param_rollback(args: argparse.Namespace) -> int:
    apply_log_id = (getattr(args, "apply_log_id", "") or "").strip()
    if not apply_log_id:
        raise RuntimeError("apply-log-id is required")
    operator = (getattr(args, "operator", "") or os.getenv("OPERATOR", "local_user")).strip() or "local_user"
    comment = getattr(args, "comment", "") or ""
    store = ParamStore(_sqlite_path())
    try:
        out = store.rollback(apply_log_id=apply_log_id, operator=operator, comment=comment)
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def param_draft_save(args: argparse.Namespace) -> int:
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
        raise RuntimeError("Notion snapshot DB must contain 鏃ユ湡/绛栫暐/鑲＄エ浠ｇ爜 fields.")

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
    s_rec.add_argument("--refresh-prices", action="store_true", help="先同步实时价格再计算建议")
    s_rec.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")
    s_rec.add_argument("--emit-snapshot", action="store_true", help="recommend后写入每日快照")
    s_rec.add_argument("--snapshot-date", default="", help="快照日期，默认今天 YYYY-MM-DD")

    s_bt = sub.add_parser("backtest-recommendation", help="回测建议模型")
    s_bt.add_argument("--window", type=int, default=60, help="历史窗口长度（按交易事件）")
    s_bt.add_argument("--allow-small-sample", action="store_true", default=True)
    s_bt.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    s_bt.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    s_bt.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")

    s_sp = sub.add_parser("sync-prices", help="自动拉取实时行情并回写当前市价")
    s_sp.add_argument("--dry-run", action="store_true", help="仅拉取并输出统计，不写入Notion")
    s_sp.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")

    s_sd = sub.add_parser("snapshot-daily", help="生成并落库每日策略快照")
    s_sd.add_argument("--dry-run", action="store_true", help="仅预览，不写入SQLite")
    s_sd.add_argument("--snapshot-date", default="", help="快照日期，默认今天 YYYY-MM-DD")
    s_sd.add_argument("--allow-small-sample", action="store_true", default=True)
    s_sd.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    s_sd.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    s_sd.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")
    s_sd.add_argument("--refresh-prices", action="store_true", help="先同步实时价格再快照")
    s_sd.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")

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

    s_pd = sub.add_parser("param-diff", help="比较当前值、推荐值、人工编辑值")
    s_pd.add_argument("--proposal-id", required=True, help="参数推荐ID")
    s_pd.add_argument("--editor-json", default="", help="人工编辑JSON文本或json文件路径")

    s_pa = sub.add_parser("param-apply", help="应用参数推荐（支持人工编辑）")
    s_pa.add_argument("--proposal-id", required=True, help="参数推荐ID")
    s_pa.add_argument("--editor-json", default="", help="人工编辑JSON文本或json文件路径")
    s_pa.add_argument("--expected-version", type=int, default=-1, help="并发保护版本号，-1表示不校验")
    s_pa.add_argument("--operator", default="", help="操作者，默认取 OPERATOR 或 local_user")
    s_pa.add_argument("--comment", default="", help="应用备注")

    s_proll = sub.add_parser("param-rollback", help="按 apply_log_id 回滚参数")
    s_proll.add_argument("--apply-log-id", required=True, help="apply_log_id")
    s_proll.add_argument("--operator", default="", help="操作者，默认取 OPERATOR 或 local_user")
    s_proll.add_argument("--comment", default="", help="回滚备注")

    s_pdraft = sub.add_parser("param-draft-save", help="保存参数编辑草稿")
    s_pdraft.add_argument("--proposal-id", required=True, help="参数推荐ID")
    s_pdraft.add_argument("--editor-json", default="", help="人工编辑JSON文本或json文件路径")

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
    if args.cmd == "snapshot-daily":
        return snapshot_daily(client, cfg, args)
    if args.cmd == "sync-snapshot-notion":
        return sync_snapshot_notion(client, cfg, args)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())




