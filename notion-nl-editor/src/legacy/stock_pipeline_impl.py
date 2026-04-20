import argparse
import datetime as dt
import json
import math
import os
import sqlite3
import statistics
import sys
import time
import warnings
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import uuid4

from core.config import Cfg, load_cfg
from core.env_utils import load_dotenv
from core.notion_client import NotionClient
from core.notion_props import (
    find_title_property_name,
    get_prop,
    p_date,
    p_formula_number,
    p_number,
    p_relation_ids,
    p_rich,
    p_select,
    p_title,
    rt_plain,
    text_prop,
    title_prop,
)
from param_store import ParamStore, SCHEMA_VERSION
from services.risk.gates import PARAM_APPLY_GATE_BLOCKED, GateThreshold, evaluate_release_gate
from services.recommendation.engine import ExecutionContext
from services.selection.selector import SnapshotSlice, score_snapshot_slice
from stores.snapshot_store import SnapshotStore as LayerSnapshotStore

_LEGACY_WARNED_KEYS: set[str] = set()


def _warn_legacy_forward(name: str) -> None:
    if name in _LEGACY_WARNED_KEYS:
        return
    _LEGACY_WARNED_KEYS.add(name)
    warnings.warn(
        f"legacy.stock_pipeline_impl.{name} is deprecated; use commands/* + services/* entrypoints.",
        DeprecationWarning,
        stacklevel=2,
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


# Prefer layered store implementation while keeping legacy symbol name.
SnapshotStore = LayerSnapshotStore


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
        "cash": _find_prop_name(props, [pref_name, "可流动现金", "现金", "cash"], ["number", "formula", "rollup", "rich_text", "title"]),
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
    _warn_legacy_forward("sync_prices")
    from services.recommendation.market_sync import sync_prices as _impl

    return _impl(client, cfg, args)


def sync_kline(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    _warn_legacy_forward("sync_kline")
    from services.recommendation.market_sync import sync_kline as _impl

    return _impl(client, cfg, args)


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
    ctx = ExecutionContext(param_cache={})
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
                cache_key = f"{key[0]}|{key[1]}|{key[2]}"
                active = ctx.get_cached_param(cache_key)
                if active is None:
                    active = store.get_active_param_set(strategy_id=sid, market=market, symbol_scope=scope)
                    ctx.put_cached_param(cache_key, active)
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
    _warn_legacy_forward("recommend_prices")
    from services.recommendation.signal_eval import recommend_prices as _impl

    return _impl(client, cfg, args)


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
    _warn_legacy_forward("backtest_recommendation")
    from services.recommendation.signal_eval import backtest_recommendation as _impl

    return _impl(client, cfg, args)


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
    ctx = ExecutionContext(param_cache={})

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
                    active = ctx.get_cached_param(sid_upper)
                    if active is None:
                        active = store.get_active_param_set(strategy_id=sid_upper, market=market, symbol_scope=scope)
                        ctx.put_cached_param(sid_upper, active)
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
        "param_versions": {k: int(v.get("version", 0)) for k, v in ctx.param_cache.items()},
        "param_market": market,
        "data_source": "trade",
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def snapshot_daily(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    _warn_legacy_forward("snapshot_daily")
    from services.recommendation.snapshot_sync import snapshot_daily as _impl

    return _impl(client, cfg, args)


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
    from commands.research import history_query as _impl

    return _impl(args)


def _build_experiment_baseline(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload = _history_payload(rows)
    return {
        "summary": payload.get("summary", {}),
        "by_strategy": payload.get("by_strategy", []),
        "by_market": payload.get("by_market", []),
    }


def _gate_decision(proposal: Dict[str, Any], args: argparse.Namespace, expected_experiment_id: str = "") -> Tuple[bool, str, Dict[str, Any]]:
    threshold = GateThreshold(
        min_stability=float(getattr(args, "gate_min_stability", 0.0) or 0.0),
        min_hit_rate=float(getattr(args, "gate_min_hit_rate", 0.0) or 0.0),
        max_dd_mean=float(getattr(args, "gate_max_dd_mean", 1.0) or 1.0),
        require_experiment=bool(getattr(args, "require_experiment", False)),
    )
    return evaluate_release_gate(proposal=proposal, expected_experiment_id=expected_experiment_id, threshold=threshold)


def _minmax_scale(values: List[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if hi - lo < 1e-12:
        return [0.5 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]


def _score_selected_rows(rows: List[Dict[str, Any]], top_n: int) -> Dict[str, Any]:
    return score_snapshot_slice(
        SnapshotSlice(rows=rows, strategy_filter=[], market_filter=[], start_date="", end_date=""),
        top_n=top_n,
    )


def param_recommend(args: argparse.Namespace) -> int:
    from commands.param import param_recommend as _impl

    return _impl(args)


def param_diff(args: argparse.Namespace) -> int:
    from commands.param import param_diff as _impl

    return _impl(args)


def param_apply(args: argparse.Namespace) -> int:
    from commands.param import param_apply as _impl

    return _impl(args)


def param_rollback(args: argparse.Namespace) -> int:
    from commands.param import param_rollback as _impl

    return _impl(args)


def param_draft_save(args: argparse.Namespace) -> int:
    from commands.param import param_draft_save as _impl

    return _impl(args)


def param_monitor(args: argparse.Namespace) -> int:
    from commands.param import param_monitor as _impl

    return _impl(args)


def param_migrate(args: argparse.Namespace) -> int:
    from commands.param import param_migrate as _impl

    return _impl(args)


def param_risk_guard(args: argparse.Namespace) -> int:
    from commands.param import param_risk_guard as _impl

    return _impl(args)


def select_stock(args: argparse.Namespace) -> int:
    from commands.research import select_stock as _impl

    return _impl(args)


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
    _warn_legacy_forward("sync_snapshot_notion")
    from services.recommendation.snapshot_sync import sync_snapshot_notion as _impl

    return _impl(client, cfg, args)


def build_parser() -> argparse.ArgumentParser:
    from app.cli import build_parser as _impl

    return _impl()
def main() -> int:
    from app.cli import main as _impl

    return _impl()


if __name__ == "__main__":
    raise SystemExit(main())




