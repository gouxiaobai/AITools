"""Compatibility facade.

This module preserves the legacy import surface while new logic lives in
`services/*` and `commands/*`. New recommendation features should not be
added here.
"""

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
from app.cli import build_parser, main
from commands.audit import audit, validate_manual_entries
from commands.migrate import add_trade, annual_sync, migrate_apply, migrate_preview
from commands.param import param_apply, param_diff, param_draft_save, param_migrate, param_monitor, param_recommend, param_risk_guard, param_rollback
from commands.research import history_query, manual_filter_list, manual_filter_set, select_stock, snapshot_market_daily, sync_market_universe
from commands.signal import backtest_recommendation, recommend_prices, snapshot_daily, sync_kline, sync_prices, sync_snapshot_notion
from stores.snapshot_store import SnapshotStore

__all__ = [
    "Cfg",
    "NotionClient",
    "SnapshotStore",
    "add_trade",
    "annual_sync",
    "audit",
    "backtest_recommendation",
    "build_parser",
    "find_title_property_name",
    "get_prop",
    "history_query",
    "manual_filter_list",
    "manual_filter_set",
    "load_cfg",
    "load_dotenv",
    "main",
    "migrate_apply",
    "migrate_preview",
    "p_date",
    "p_formula_number",
    "p_number",
    "p_relation_ids",
    "p_rich",
    "p_select",
    "p_title",
    "param_apply",
    "param_diff",
    "param_draft_save",
    "param_migrate",
    "param_monitor",
    "param_recommend",
    "param_risk_guard",
    "param_rollback",
    "recommend_prices",
    "rt_plain",
    "select_stock",
    "snapshot_market_daily",
    "sync_market_universe",
    "snapshot_daily",
    "sync_kline",
    "sync_prices",
    "sync_snapshot_notion",
    "text_prop",
    "title_prop",
    "validate_manual_entries",
]


if __name__ == "__main__":
    raise SystemExit(main())
