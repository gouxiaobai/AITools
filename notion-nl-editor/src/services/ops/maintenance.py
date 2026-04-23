import argparse
import datetime as dt
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from core.config import Cfg
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
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if typ == "status":
        sel = prop.get("status")
        return sel.get("name", "") if sel else ""
    if typ == "date":
        date_obj = prop.get("date")
        return date_obj.get("start", "") if date_obj else ""
    if typ == "number":
        num = prop.get("number")
        return "" if num is None else str(num)
    return ""


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


def _resolve_stock_fields(stock_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = stock_db.get("properties", {})
    return {
        "title": find_title_property_name(stock_db),
        "stock_code": _find_prop_name(props, ["股票代码", "代码"], ["rich_text", "title"]),
        "current_price": _find_prop_name(props, ["当前市价", "最新价", "现价", "市价"], ["number"]),
    }


def _resolve_stock_fields_runtime(stock_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    fields = _resolve_stock_fields(stock_db)
    props = stock_db.get("properties", {})
    if not fields.get("stock_code"):
        fields["stock_code"] = _find_prop_by_keywords(props, ["股票代码", "代码"], ["rich_text", "title"])
    if not fields.get("current_price"):
        fields["current_price"] = _find_prop_by_keywords(props, ["当前市价", "最新价", "现价", "市价"], ["number"])
    return fields


def _resolve_trade_write_fields(trade_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = trade_db.get("properties", {})
    title_name = find_title_property_name(trade_db)
    return {
        "title": title_name,
        "date": _find_prop_name(props, ["日期"], ["date"]),
        "direction": _find_prop_name(props, ["方向"], ["select", "status"]),
        "shares": _find_prop_name(props, ["股数"], ["number"]),
        "price": _find_prop_name(props, ["价格"], ["number"]),
        "fee": _find_prop_name(props, ["手续费", "费用"], ["number"]),
        "tax": _find_prop_name(props, ["税费", "印花税"], ["number"]),
        "stock": _find_prop_name(props, ["股票"], ["relation"]),
        "account": _find_prop_name(props, ["账户", "account"], ["relation"]),
        "strategy": _find_prop_name(props, ["策略"], ["select", "status", "rich_text", "title"]),
        "note": _find_prop_name(props, ["备注"], ["rich_text", "title"]),
        "source_table": _find_prop_name(props, ["source_table"], ["select", "status"]),
        "import_status": _find_prop_name(props, ["import_status"], ["select", "status"]),
    }


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


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


def _normalize_lookup_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    for ch in [" ", "_", "-", "\t", "\r", "\n"]:
        text = text.replace(ch, "")
    return text


def _match_keywords(name: str, keywords: List[str]) -> bool:
    haystack = str(name or "").casefold()
    return any(str(keyword or "").casefold() in haystack for keyword in keywords if keyword)


def _resolve_cash_trade_relation_field(cash_db: Dict[str, Any], std_trades_id: str) -> str:
    props = cash_db.get("properties", {})
    override = (os.getenv("CASH_TRADE_RELATION_FIELD_NAME", "") or "").strip()
    if override:
        info = props.get(override)
        if not info:
            raise RuntimeError(f"Cash config relation field not found: {override}")
        if info.get("type") != "relation":
            raise RuntimeError(f"Cash config field is not relation: {override}")
        return override

    relation_props = [(name, info) for name, info in props.items() if info.get("type") == "relation"]
    if not relation_props:
        raise RuntimeError("Cash config DB has no relation field for trades.")

    exact_db_matches = [
        name for name, info in relation_props if (info.get("relation", {}) or {}).get("database_id") == std_trades_id
    ]
    if len(exact_db_matches) == 1:
        return exact_db_matches[0]
    if len(exact_db_matches) > 1:
        keyword_matches = [
            name
            for name in exact_db_matches
            if _match_keywords(name, ["交易流水", "流水", "关联交易", "trade", "trades", "transaction", "txn"])
        ]
        if len(keyword_matches) == 1:
            return keyword_matches[0]
        raise RuntimeError("Cash config DB has multiple trade relation fields.")

    keyword_matches = [
        name
        for name, _ in relation_props
        if _match_keywords(name, ["交易流水", "流水", "关联交易", "trade", "trades", "transaction", "txn"])
    ]
    if len(keyword_matches) == 1:
        return keyword_matches[0]
    if len(relation_props) == 1:
        return relation_props[0][0]
    raise RuntimeError("Unable to resolve cash config trade relation field.")


def _resolve_trade_account_relation_field(trade_db: Dict[str, Any], cash_db_id: str) -> Optional[str]:
    props = trade_db.get("properties", {})
    override = (os.getenv("TRADE_ACCOUNT_RELATION_FIELD_NAME", "") or "").strip()
    if override:
        info = props.get(override)
        if not info:
            raise RuntimeError(f"Trade account relation field not found: {override}")
        if info.get("type") != "relation":
            raise RuntimeError(f"Trade account field is not relation: {override}")
        return override

    relation_props = [(name, info) for name, info in props.items() if info.get("type") == "relation"]
    exact_db_matches = [
        name for name, info in relation_props if (info.get("relation", {}) or {}).get("database_id") == cash_db_id
    ]
    if len(exact_db_matches) == 1:
        return exact_db_matches[0]
    if len(exact_db_matches) > 1:
        keyword_matches = [
            name
            for name in exact_db_matches
            if _match_keywords(name, ["账户", "总账户", "account", "cash config", "cash account"])
        ]
        if len(keyword_matches) == 1:
            return keyword_matches[0]
        raise RuntimeError("Trade DB has multiple cash-account relation fields.")

    keyword_matches = [
        name
        for name, _ in relation_props
        if _match_keywords(name, ["账户", "总账户", "account", "cash config", "cash account"])
    ]
    if len(keyword_matches) == 1:
        return keyword_matches[0]
    return None


def _is_cash_account_row(row: Dict[str, Any], title_name: Optional[str]) -> bool:
    targets = {
        _normalize_lookup_text("ACCOUNT"),
        _normalize_lookup_text("总账户"),
        _normalize_lookup_text(os.getenv("ACCOUNT_ROW_CODE", "ACCOUNT")),
    }
    texts: List[str] = []
    if title_name:
        texts.append(_prop_text_any(row, title_name))
    for prop_name, prop in row.get("properties", {}).items():
        if prop.get("type") in {"title", "rich_text", "select", "status"}:
            texts.append(_prop_text_any(row, prop_name))
    return any(_normalize_lookup_text(text) in targets for text in texts if text)


def _resolve_cash_account_relation_target(client: NotionClient, cfg: Cfg) -> Dict[str, Any]:
    db_id = (cfg.cash_config_id or "").strip()
    if not db_id:
        raise RuntimeError("Missing DB_CASH_CONFIG_ID.")

    cash_db = client.get_database(db_id)
    relation_prop = _resolve_cash_trade_relation_field(cash_db, cfg.std_trades_id)
    title_name = find_title_property_name(cash_db)
    rows = client.query_database_all(db_id)
    if not rows:
        raise RuntimeError("Cash config DB has no records.")

    matches = [row for row in rows if _is_cash_account_row(row, title_name)]
    if len(matches) == 1:
        row = matches[0]
    elif len(matches) > 1:
        raise RuntimeError("Multiple cash config rows matched ACCOUNT/总账户.")
    elif len(rows) == 1:
        row = rows[0]
    else:
        raise RuntimeError("Cash config account row not found; expected ACCOUNT/总账户.")

    related_ids = p_relation_ids(row, relation_prop)
    return {
        "page_id": row.get("id", ""),
        "relation_prop": relation_prop,
        "related_ids": list(related_ids),
    }


def _resolve_cash_account_page(client: NotionClient, cfg: Cfg) -> Dict[str, Any]:
    db_id = (cfg.cash_config_id or "").strip()
    if not db_id:
        raise RuntimeError("Missing DB_CASH_CONFIG_ID.")

    cash_db = client.get_database(db_id)
    title_name = find_title_property_name(cash_db)
    rows = client.query_database_all(db_id)
    if not rows:
        raise RuntimeError("Cash config DB has no records.")

    matches = [row for row in rows if _is_cash_account_row(row, title_name)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise RuntimeError("Multiple cash config rows matched ACCOUNT/总账户.")
    if len(rows) == 1:
        return rows[0]
    raise RuntimeError("Cash config account row not found; expected ACCOUNT/总账户.")


def _merge_relation_ids(existing_ids: List[str], new_ids: List[str]) -> Tuple[List[str], int]:
    merged: List[str] = []
    seen = set()
    for rid in list(existing_ids) + list(new_ids):
        if not rid or rid in seen:
            continue
        seen.add(rid)
        merged.append(rid)
    return merged, max(len(merged) - len(list(dict.fromkeys(existing_ids))), 0)


def _sync_cash_account_relations(
    client: NotionClient,
    target: Dict[str, Any],
    trade_page_ids: List[str],
) -> int:
    clean_ids = [rid for rid in trade_page_ids if rid]
    if not clean_ids:
        return 0
    merged_ids, added = _merge_relation_ids(target.get("related_ids", []), clean_ids)
    if added <= 0:
        return 0
    client.update_page(
        target["page_id"],
        {
            target["relation_prop"]: {
                "relation": [{"id": rid} for rid in merged_ids],
            }
        },
    )
    target["related_ids"] = merged_ids
    return added


def _resolve_trade_account_target(client: NotionClient, cfg: Cfg, trade_db: Dict[str, Any]) -> Dict[str, str]:
    cash_page = _resolve_cash_account_page(client, cfg)
    account_field = _resolve_trade_account_relation_field(trade_db, (cfg.cash_config_id or "").strip())
    return {
        "account_field": account_field or "",
        "cash_page_id": str(cash_page.get("id", "") or ""),
    }


def _attach_trade_account_relation(
    props: Dict[str, Any],
    trade_target: Dict[str, str],
) -> bool:
    account_field = trade_target.get("account_field", "")
    cash_page_id = trade_target.get("cash_page_id", "")
    if not account_field or not cash_page_id:
        return False
    props[account_field] = {"relation": [{"id": cash_page_id}]}
    return True


def _backfill_trade_account_relations(
    client: NotionClient,
    cfg: Cfg,
    trade_target: Dict[str, str],
) -> Dict[str, int]:
    account_field = trade_target.get("account_field", "")
    cash_page_id = trade_target.get("cash_page_id", "")
    if not account_field or not cash_page_id:
        return {"scanned": 0, "already_linked": 0, "missing_before": 0, "linked": 0}

    trade_rows = client.query_database_all(cfg.std_trades_id)
    scanned = 0
    already_linked = 0
    missing_before = 0
    linked = 0
    for row in trade_rows:
        row_id = str(row.get("id", "") or "")
        if not row_id:
            continue
        scanned += 1
        current_ids = p_relation_ids(row, account_field)
        if cash_page_id in current_ids:
            already_linked += 1
            continue
        missing_before += 1
        merged_ids, added = _merge_relation_ids(current_ids, [cash_page_id])
        if added <= 0:
            continue
        client.update_page(
            row_id,
            {
                account_field: {
                    "relation": [{"id": rid} for rid in merged_ids],
                }
            },
        )
        linked += 1
    return {
        "scanned": scanned,
        "already_linked": already_linked,
        "missing_before": missing_before,
        "linked": linked,
    }


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
    for row in rows:
        pid = row.get("id")
        name = p_title(row, fields["title"]) if fields.get("title") else ""
        code = _prop_text_any(row, fields["stock_code"]) if fields.get("stock_code") else ""
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

    trade_db = client.get_database(cfg.std_trades_id)
    trade_target = _resolve_trade_account_target(client, cfg, trade_db)
    cash_target = None if trade_target.get("account_field") else _resolve_cash_account_relation_target(client, cfg)
    inserted = 0
    created_trade_ids: List[str] = []
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
        _attach_trade_account_relation(props, trade_target)
        page = client.create_page(cfg.std_trades_id, props)
        created_trade_ids.append(page.get("id", ""))
        inserted += 1
        if inserted % 20 == 0:
            print(f"progress {inserted}/{len(todo)}")
        time.sleep(0.12)

    if cash_target is not None:
        linked = _sync_cash_account_relations(client, cash_target, created_trade_ids)
        print(f"cash relation linked: {linked}")
    else:
        print(f"trade account linked: {len(created_trade_ids)}")
    print(f"导入完成: {inserted} 条")
    return 0


def add_trade(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    if args.direction not in {"BUY", "SELL"}:
        raise ValueError("direction must be BUY or SELL")
    try:
        dt.datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("--date must be YYYY-MM-DD") from exc

    by_name, by_code = stock_index(client, cfg)
    stock_id = by_code.get(args.stock) or by_name.get(args.stock)
    if not stock_id:
        raise ValueError(f"stock '{args.stock}' not found in stock master by title/code")

    title = f"{args.date} {args.direction} {args.stock} {args.shares}@{args.price}"
    trade_db = client.get_database(cfg.std_trades_id)
    trade_target = _resolve_trade_account_target(client, cfg, trade_db)
    cash_target = None if trade_target.get("account_field") else _resolve_cash_account_relation_target(client, cfg)
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
    _attach_trade_account_relation(props, trade_target)

    page = client.create_page(cfg.std_trades_id, props)
    if cash_target is not None:
        linked = _sync_cash_account_relations(client, cash_target, [page.get("id", "")])
        print(f"cash relation linked: {linked}")
    elif trade_target.get("account_field"):
        print("trade account linked: 1")
    print(f"新增交易成功: id={page.get('id')}")
    return 0


def backfill_cash_relations(client: NotionClient, cfg: Cfg) -> int:
    trade_db = client.get_database(cfg.std_trades_id)
    trade_target = _resolve_trade_account_target(client, cfg, trade_db)
    if trade_target.get("account_field"):
        stats = _backfill_trade_account_relations(client, cfg, trade_target)
        print(f"trade rows scanned: {stats['scanned']}")
        print(f"already linked: {stats['already_linked']}")
        print(f"missing before backfill: {stats['missing_before']}")
        print(f"linked now: {stats['linked']}")
        return 0

    cash_target = _resolve_cash_account_relation_target(client, cfg)
    trade_rows = client.query_database_all(cfg.std_trades_id)
    trade_ids = [str(row.get("id", "") or "") for row in trade_rows if row.get("id")]
    existing_ids = set(cash_target.get("related_ids", []))
    missing_ids = [rid for rid in trade_ids if rid not in existing_ids]
    linked = _sync_cash_account_relations(client, cash_target, missing_ids)

    print(f"trade rows scanned: {len(trade_ids)}")
    print(f"already linked: {len(existing_ids)}")
    print(f"missing before backfill: {len(missing_ids)}")
    print(f"linked now: {linked}")
    return 0


def validate_manual_entries(client: NotionClient, cfg: Cfg) -> int:
    trade_db = client.get_database(cfg.std_trades_id)
    rows = client.query_database_all(cfg.std_trades_id)
    fields = _resolve_trade_write_fields(trade_db)
    failures: List[Tuple[str, List[str]]] = []
    checked = 0

    for row in rows:
        source_field = fields.get("source_table")
        source = p_select(row, source_field) if source_field else ""
        if source and source != "manual":
            continue
        checked += 1
        missing: List[str] = []
        if not fields.get("date") or not p_date(row, fields["date"]):
            missing.append("日期")
        if not fields.get("direction") or p_select(row, fields["direction"]) not in {"BUY", "SELL"}:
            missing.append("方向")
        if not fields.get("shares") or p_number(row, fields["shares"]) is None:
            missing.append("股数")
        if not fields.get("price") or p_number(row, fields["price"]) is None:
            missing.append("价格")
        if not fields.get("stock") or len(p_relation_ids(row, fields["stock"])) == 0:
            missing.append("股票")
        if not fields.get("title"):
            missing.append("标题")
        elif get_prop(row, fields["title"]).get("type") == "title":
            if not p_title(row, fields["title"]):
                missing.append("标题")
        elif not _prop_text_any(row, fields["title"]):
            missing.append("标题")
        if missing:
            failures.append((row.get("id", ""), missing))

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

    def init_year(year: str) -> None:
        if year not in totals:
            totals[year] = {"已实现收益": 0.0, "T收益": 0.0, "分红收益": 0.0}

    for row in tx_rows:
        year = parse_year(p_date(row, "日期"))
        if not year:
            continue
        init_year(year)
        realized = p_formula_number(row, "单笔已实现收益")
        t_profit = p_formula_number(row, "单笔T收益")
        if realized is not None:
            totals[year]["已实现收益"] += float(realized)
        if t_profit is not None:
            totals[year]["T收益"] += float(t_profit)

    for row in div_rows:
        year = parse_year(p_date(row, "日期"))
        if not year:
            continue
        init_year(year)
        amount = p_number(row, "金额")
        if amount is not None:
            totals[year]["分红收益"] += float(amount)

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

    for year in years:
        vals = totals[year]
        props = {
            "已实现收益": {"number": round(vals["已实现收益"], 6)},
            "T收益": {"number": round(vals["T收益"], 6)},
            "分红收益": {"number": round(vals["分红收益"], 6)},
        }
        if year in annual_index and annual_index[year]:
            client.update_page(annual_index[year], props)
        else:
            create_props = {
                "年份": title_prop(year),
                "已实现收益": {"number": round(vals["已实现收益"], 6)},
                "T收益": {"number": round(vals["T收益"], 6)},
                "分红收益": {"number": round(vals["分红收益"], 6)},
            }
            client.create_page(cfg.annual_id, create_props)

    print(f"年度汇总同步完成: {len(years)} 年")
    return 0


__all__ = [
    "add_trade",
    "annual_sync",
    "audit",
    "backfill_cash_relations",
    "migrate_apply",
    "migrate_preview",
    "validate_manual_entries",
]
