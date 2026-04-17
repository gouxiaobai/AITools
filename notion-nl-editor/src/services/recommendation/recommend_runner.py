import argparse
import datetime as dt
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional

from core.config import Cfg
from core.notion_client import NotionClient
from core.notion_props import p_number
from core.runtime import today_or
from services.recommendation.collector import collect_recommendations_from_context
from services.recommendation.common import coerce_text, resolve_recommend_data_source, write_prop_value
from services.recommendation.context import load_kbars_for_stocks, prepare_recommendation_context_obj
from services.recommendation.data_prep import replay_positions_from_trades, resolve_trade_cost_fields, resolve_trade_fields
from services.recommendation.portfolio import (
    attach_position_sizing,
    build_latest_price_map,
    build_position_valuation,
    build_reconcile_result,
    calc_account_summary,
    is_account_row,
    load_cash_from_config_db,
    read_cash_config_formula_summary,
)
from services.recommendation.snapshot_writer import build_stock_prices, emit_snapshot
from services.recommendation.types import RecommendationRequest


class RecommendRunner:
    def __init__(self, client: NotionClient, cfg: Cfg) -> None:
        self.client = client
        self.cfg = cfg

    def recommend_prices(self, req: RecommendationRequest, args: argparse.Namespace) -> int:
        ctx = prepare_recommendation_context_obj(self.client, self.cfg, args)
        db_props = ctx.stock_db.get("properties", {})
        asof_date = req.asof_date or dt.date.today().isoformat()

        cash_input = load_cash_from_config_db(self.client, self.cfg)
        trade_db = self.client.get_database(self.cfg.std_trades_id)
        trade_rows = self.client.query_database_all(self.cfg.std_trades_id)
        trade_fields = resolve_trade_fields(trade_db)
        cost_fields = resolve_trade_cost_fields(trade_db)
        holding_shares_by_stock, holding_avg_cost_by_stock, realized_pnl_total = replay_positions_from_trades(
            trade_rows=trade_rows,
            trade_fields=trade_fields,
            cost_fields=cost_fields,
        )

        need_kline_fallback = any(p_number(row, ctx.stock_fields["current_price"]) is None for row in ctx.stock_rows if ctx.stock_fields.get("current_price"))
        if need_kline_fallback and not ctx.stock_kbars and os.getenv("TUSHARE_TOKEN", "").strip():
            kbars_for_price, _ = load_kbars_for_stocks(ctx.stock_rows, ctx.stock_fields, args)
            for sid, bars in kbars_for_price.items():
                if sid not in ctx.stock_kbars:
                    ctx.stock_kbars[sid] = bars

        latest_price_by_stock = build_latest_price_map(ctx.stock_rows, ctx.stock_fields, ctx.stock_kbars)
        position_valuation_by_stock = build_position_valuation(
            holding_shares_by_stock=holding_shares_by_stock,
            holding_avg_cost_by_stock=holding_avg_cost_by_stock,
            latest_price_by_stock=latest_price_by_stock,
        )
        account_summary = calc_account_summary(
            cash=cash_input,
            position_valuation_by_stock=position_valuation_by_stock,
            realized_pnl_total=realized_pnl_total,
        )
        notion_formula_summary = read_cash_config_formula_summary(self.client, self.cfg)
        account_summary["notion_formula_summary"] = notion_formula_summary
        account_summary["reconcile"] = build_reconcile_result(account_summary, notion_formula_summary)

        recs = collect_recommendations_from_context(ctx, args)
        attach_position_sizing(
            recs,
            sizing_base_asset=account_summary.get("total_asset", 0.0),
            holding_shares_by_stock=holding_shares_by_stock,
        )
        for rec in recs:
            sid = coerce_text(rec.get("stock_id", ""))
            pos = position_valuation_by_stock.get(sid, {})
            rec["market_value_now"] = float(pos.get("market_value_now", 0.0) or 0.0)
            rec["unrealized_pnl_now"] = float(pos.get("unrealized_pnl_now", 0.0) or 0.0)
            rec["avg_cost_now"] = float(pos.get("avg_cost_now", 0.0) or 0.0)
            rec["last_price_now"] = float(pos.get("last_price_now", 0.0) or 0.0)

        self._write_back_recommendations(
            stock_rows=ctx.stock_rows,
            stock_fields=ctx.stock_fields,
            db_props=db_props,
            recs=recs,
            asof_date=asof_date,
            dry_run=req.dry_run,
        )

        if req.emit_snapshot:
            stock_prices = build_stock_prices(
                data_source=resolve_recommend_data_source(args),
                stock_points=ctx.stock_points,
                stock_kbars=ctx.stock_kbars,
            )
            emit_snapshot(
                recs=recs,
                stock_prices=stock_prices,
                snapshot_date=today_or(req.snapshot_date),
                dry_run=req.dry_run,
            )

        out = {"recommendations": recs, "account_summary": account_summary}
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    def _write_back_recommendations(
        self,
        stock_rows: List[Dict[str, Any]],
        stock_fields: Dict[str, Optional[str]],
        db_props: Dict[str, Any],
        recs: List[Dict[str, Any]],
        asof_date: str,
        dry_run: bool,
    ) -> None:
        rec_by_stock: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for rec in recs:
            rec_by_stock[rec.get("stock_id", "")].append(rec)

        for row in stock_rows:
            if is_account_row(row, stock_fields):
                continue
            stock_id = row.get("id", "")
            if dry_run:
                continue
            stock_recs = rec_by_stock.get(stock_id, [])
            rec_for_write = None
            for item in stock_recs:
                if item.get("strategy_id") == "BASELINE":
                    rec_for_write = item
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
                payload = write_prop_value(db_props, prop_name, value)
                if payload is not None:
                    props[prop_name] = payload
            if props:
                self.client.update_page(stock_id, props)
