import argparse
import json
from typing import Any, Dict, List, Optional

from core.config import Cfg
from core.notion_client import NotionClient
from core.notion_props import find_title_property_name
from core.runtime import sqlite_path, today_or
from services.recommendation.common import find_prop_name, prop_text_any, resolve_recommend_data_source, write_prop_value
from services.recommendation.collector import collect_recommendations
from services.recommendation.context import prepare_recommendation_context
from services.recommendation.snapshot_writer import build_stock_prices, emit_snapshot
from stores.snapshot_store import SnapshotStore


def snapshot_daily(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    stock_db, stock_fields, stock_rows, stock_points, stock_kbars, stock_symbols = prepare_recommendation_context(client, cfg, args)
    _ = stock_db
    _ = stock_fields
    _ = stock_rows
    _ = stock_symbols
    recs = collect_recommendations(stock_rows, stock_fields, stock_points, stock_kbars, stock_symbols, args)
    stock_prices = build_stock_prices(
        data_source=resolve_recommend_data_source(args),
        stock_points=stock_points,
        stock_kbars=stock_kbars,
    )
    result = emit_snapshot(
        recs=recs,
        stock_prices=stock_prices,
        snapshot_date=today_or(getattr(args, "snapshot_date", "")),
        dry_run=bool(getattr(args, "dry_run", False)),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _resolve_snapshot_notion_fields(snapshot_db: Dict[str, Any]) -> Dict[str, Optional[str]]:
    props = snapshot_db.get("properties", {})
    return {
        "title": find_title_property_name(snapshot_db),
        "date": find_prop_name(props, ["日期", "快照日期", "snapshot_date"], ["date", "rich_text"]),
        "strategy": find_prop_name(props, ["策略", "strategy_id"], ["select", "status", "rich_text", "title"]),
        "stock_code": find_prop_name(props, ["股票代码", "代码", "stock_code"], ["rich_text", "title"]),
        "stock_name": find_prop_name(props, ["股票", "股票名称", "stock_name"], ["rich_text", "title"]),
        "market": find_prop_name(props, ["市场", "market"], ["select", "status", "rich_text", "title"]),
        "ret_1d": find_prop_name(props, ["收益", "ret_1d", "1D收益"], ["number"]),
        "max_drawdown": find_prop_name(props, ["回撤", "max_drawdown"], ["number"]),
        "hit_rate": find_prop_name(props, ["命中率", "hit_rate", "hit_flag"], ["number"]),
        "sample_count": find_prop_name(props, ["样本数", "sample_count"], ["number"]),
        "action": find_prop_name(props, ["动作", "action"], ["select", "status", "rich_text", "title"]),
        "confidence": find_prop_name(props, ["置信度", "confidence"], ["select", "status", "rich_text", "title"]),
    }


def _snapshot_notion_key_from_page(page: Dict[str, Any], fields: Dict[str, Optional[str]]) -> str:
    date_v = prop_text_any(page, fields["date"]) if fields.get("date") else ""
    strategy_v = prop_text_any(page, fields["strategy"]) if fields.get("strategy") else ""
    code_v = prop_text_any(page, fields["stock_code"]) if fields.get("stock_code") else ""
    return f"{date_v}|{strategy_v.upper()}|{code_v.upper()}"


def sync_snapshot_notion(client: NotionClient, cfg: Cfg, args: argparse.Namespace) -> int:
    if not cfg.strategy_snapshot_id:
        raise RuntimeError("Missing DB_STRATEGY_SNAPSHOT_ID for snapshot notion sync.")
    snapshot_date = today_or(getattr(args, "snapshot_date", ""))
    store = SnapshotStore(sqlite_path())
    try:
        rows = store.query_range(snapshot_date, snapshot_date)
    finally:
        store.close()

    snapshot_db = client.get_database(cfg.strategy_snapshot_id)
    db_props = snapshot_db.get("properties", {})
    fields = _resolve_snapshot_notion_fields(snapshot_db)
    if not fields.get("date") or not fields.get("strategy") or not fields.get("stock_code"):
        raise RuntimeError("Notion snapshot DB must contain 日期/策略/股票代码 fields.")

    date_field = fields["date"]
    date_type = db_props.get(date_field, {}).get("type") if date_field else ""
    if date_field and date_type == "date":
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
            mapped = write_prop_value(db_props, prop_name, value)
            if mapped is not None:
                payload[prop_name] = mapped
        title_name = fields.get("title")
        if title_name and title_name not in payload:
            title_text = f"{row['snapshot_date']} {row['strategy_id']} {row['stock_code']}"
            mapped = write_prop_value(db_props, title_name, title_text)
            if mapped is not None:
                payload[title_name] = mapped

        existing_id = existing_index.get(key, "")
        if existing_id:
            if not bool(getattr(args, "dry_run", False)):
                client.update_page(existing_id, payload)
            updated += 1
        else:
            if not bool(getattr(args, "dry_run", False)):
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
