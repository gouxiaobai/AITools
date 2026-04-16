import argparse
import io
import json
import os
import sys
from contextlib import redirect_stdout
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from stock_pipeline import (  # noqa: E402
    NotionClient,
    backtest_recommendation,
    history_query,
    load_cfg,
    load_dotenv,
    param_apply,
    param_diff,
    param_draft_save,
    param_monitor,
    param_recommend,
    param_rollback,
    recommend_prices,
    select_stock,
    snapshot_daily,
    sync_prices,
    sync_snapshot_notion,
)


def _run_and_capture(fn, *args, **kwargs) -> Tuple[int, str, Optional[Any], Optional[str]]:
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            code = fn(*args, **kwargs)
        raw = buffer.getvalue().strip()
        parsed = json.loads(raw) if raw else None
        return int(code), raw, parsed, None
    except Exception as exc:
        return 1, buffer.getvalue().strip(), None, str(exc)


def _init_client(token_override: str, notion_version: str) -> Tuple[Optional[NotionClient], Optional[str]]:
    load_dotenv(os.path.join(ROOT_DIR, ".env"))
    token = token_override.strip() or os.getenv("NOTION_TOKEN", "").strip()
    if not token:
        return None, "未检测到 NOTION_TOKEN，请先在 .env 配置或在左侧输入 Token。"
    return NotionClient(token=token, version=notion_version), None


def _mark_run(step: str, ok: bool, detail: str = "") -> None:
    st.session_state["last_step"] = step
    st.session_state["last_ok"] = ok
    st.session_state["last_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state["last_detail"] = detail


def _show_run_status() -> None:
    step = st.session_state.get("last_step", "未执行")
    ok = st.session_state.get("last_ok", None)
    ts = st.session_state.get("last_time", "-")
    detail = st.session_state.get("last_detail", "")
    status = "成功" if ok is True else ("失败" if ok is False else "未执行")
    c1, c2, c3 = st.columns([2, 1, 2])
    c1.metric("上次步骤", step)
    c2.metric("状态", status)
    c3.metric("执行时间", ts)
    if detail:
        st.caption(detail)


def _as_df(items: Any) -> pd.DataFrame:
    if isinstance(items, list):
        return pd.DataFrame(items)
    if isinstance(items, dict):
        return pd.DataFrame([items])
    return pd.DataFrame()


def _show_json_debug(title: str, raw: str) -> None:
    with st.expander(f"{title}（JSON）", expanded=False):
        st.code(raw or "(empty)", language="json")


def _render_sync_result(parsed: Optional[Any], raw: str) -> None:
    if isinstance(parsed, dict):
        a, b, c, d = st.columns(4)
        a.metric("股票总数", parsed.get("total_symbols", 0))
        b.metric("拉取成功", parsed.get("price_fetched", 0))
        c.metric("已更新", parsed.get("updated", 0))
        d.metric("跳过", parsed.get("skipped", 0))
        st.dataframe(_as_df(parsed), use_container_width=True, hide_index=True)
    _show_json_debug("价格同步结果", raw)


def _render_recommend_result(parsed: Optional[Any], raw: str) -> None:
    rec_list: List[Dict[str, Any]] = []
    account_summary: Dict[str, Any] = {}
    if isinstance(parsed, list):
        rec_list = parsed
    elif isinstance(parsed, dict):
        if isinstance(parsed.get("recommendations"), list):
            rec_list = parsed.get("recommendations", [])
        if isinstance(parsed.get("account_summary"), dict):
            account_summary = parsed.get("account_summary", {})

    if account_summary:
        st.subheader("账户总览")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("总资产", f"{float(account_summary.get('total_asset', 0.0)):.2f}")
        c2.metric("总盈亏", f"{float(account_summary.get('total_pnl', 0.0)):.2f}")
        c3.metric("已实现盈亏", f"{float(account_summary.get('realized_pnl_total', 0.0)):.2f}")
        c4.metric("未实现盈亏", f"{float(account_summary.get('unrealized_pnl_total', 0.0)):.2f}")
        c5, c6, c7 = st.columns(3)
        c5.metric("可流动现金", f"{float(account_summary.get('cash', 0.0)):.2f}")
        c6.metric("持仓成本", f"{float(account_summary.get('invested_cost_total', 0.0)):.2f}")
        c7.metric("持仓市值", f"{float(account_summary.get('market_value_total', 0.0)):.2f}")
        c8, c9 = st.columns(2)
        c8.metric("已定价持仓数", int(float(account_summary.get("priced_positions", 0.0))))
        c9.metric("未定价持仓数", int(float(account_summary.get("unpriced_positions", 0.0))))
        st.caption("建议股数换算基准：总资产（可流动现金 + 持仓市值）；现金来自独立现金库 DB_CASH_CONFIG_ID。")
        reconcile = account_summary.get("reconcile", {})
        if isinstance(reconcile, dict) and reconcile.get("has_reference", False):
            if bool(reconcile.get("ok", True)):
                st.success(
                    f"Notion公式对账通过（最大偏差 {float(reconcile.get('max_delta', 0.0)):.2f}，阈值 {float(reconcile.get('threshold', 0.0)):.2f}）"
                )
            else:
                st.warning(
                    f"Notion公式对账偏差超阈值（最大偏差 {float(reconcile.get('max_delta', 0.0)):.2f}，阈值 {float(reconcile.get('threshold', 0.0)):.2f}）"
                )
            checks = reconcile.get("checks", [])
            if isinstance(checks, list) and checks:
                ck_df = pd.DataFrame(checks)
                if not ck_df.empty:
                    ck_df = ck_df.rename(columns={"key": "指标", "code": "代码值", "notion": "Notion值", "delta": "偏差"})
                    st.dataframe(
                        ck_df.style.format({"代码值": "{:.2f}", "Notion值": "{:.2f}", "偏差": "{:.2f}"}),
                        use_container_width=True,
                        hide_index=True,
                    )

    if rec_list:
        df = pd.DataFrame(rec_list)
        for col in ["action", "confidence", "strategy_id", "stock_name", "stock_code", "reason", "mode"]:
            if col not in df.columns:
                df[col] = ""
        for col in [
            "buy_price",
            "sell_price",
            "stop_price",
            "sample_count",
            "suggest_buy_shares",
            "suggest_sell_shares",
            "estimated_trade_value",
            "holding_shares_now",
            "market_value_now",
            "unrealized_pnl_now",
        ]:
            if col not in df.columns:
                df[col] = None

        df["action"] = df["action"].astype(str).str.upper()
        df["confidence"] = df["confidence"].astype(str).str.upper()
        df["strategy_id"] = df["strategy_id"].astype(str).str.upper()
        df["stock_name"] = df["stock_name"].astype(str)
        df["stock_code"] = df["stock_code"].astype(str)
        df["stock_key"] = (df["stock_name"].str.strip() + " (" + df["stock_code"].str.strip() + ")").str.strip()
        df["is_executable"] = (
            df["action"].isin(["BUY", "SELL"])
            & df["buy_price"].notna()
            & df["sell_price"].notna()
            & df["stop_price"].notna()
        )

        action_rank = {"BUY": 0, "SELL": 1, "HOLD": 2}
        conf_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        df["action_rank"] = df["action"].map(action_rank).fillna(9)
        df["conf_rank"] = df["confidence"].map(conf_rank).fillna(9)
        df["sample_sort"] = pd.to_numeric(df["sample_count"], errors="coerce").fillna(0)
        df = df.sort_values(by=["action_rank", "conf_rank", "sample_sort"], ascending=[True, True, False])

        st.subheader("建议筛选")
        f1, f2, f3, f4 = st.columns([1.2, 1.2, 1.8, 1.2])
        action_filter = f1.multiselect("动作", options=["BUY", "SELL", "HOLD"], default=["BUY", "SELL", "HOLD"], key="rec_action_filter")
        conf_filter = f2.multiselect("置信度", options=["HIGH", "MEDIUM", "LOW"], default=["HIGH", "MEDIUM", "LOW"], key="rec_conf_filter")
        keyword = f3.text_input("关键词（股票名/代码）", value="", key="rec_keyword").strip().lower()
        only_exec = f4.checkbox("仅看可执行建议", value=False, key="rec_only_exec")
        show_all_strategies = st.checkbox("展开全部策略（默认仅显示 BASELINE）", value=False, key="rec_show_all_strategy")

        fdf = df[df["action"].isin(action_filter) & df["confidence"].isin(conf_filter)].copy()
        if keyword:
            fdf = fdf[
                fdf["stock_name"].str.lower().str.contains(keyword, na=False)
                | fdf["stock_code"].str.lower().str.contains(keyword, na=False)
            ]
        if only_exec:
            fdf = fdf[fdf["is_executable"]]

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("建议总数", int(len(fdf)))
        c2.metric("BUY", int((fdf["action"] == "BUY").sum()))
        c3.metric("SELL", int((fdf["action"] == "SELL").sum()))
        c4.metric("HOLD", int((fdf["action"] == "HOLD").sum()))
        c5.metric("高优先机会", int((fdf["action"].isin(["BUY", "SELL"]) & fdf["confidence"].isin(["HIGH", "MEDIUM"])).sum()))

        st.caption("操作提示：BUY 可考虑分批建仓；SELL 可考虑减仓/止盈；HOLD 等待触发区间。")

        if fdf.empty:
            st.info("当前筛选条件下没有建议结果。")
            _show_json_debug("交易建议结果", raw)
            return

        st.subheader("决策卡片墙")
        grouped = list(fdf.groupby("stock_key", sort=False))
        for idx, (stock_key, g) in enumerate(grouped):
            g = g.copy()
            g = g.sort_values(by=["strategy_id"], ascending=[True])
            baseline = g[g["strategy_id"] == "BASELINE"]
            primary = baseline.iloc[0] if not baseline.empty else g.iloc[0]
            show_rows = g if show_all_strategies else pd.DataFrame([primary])

            with st.container(border=True):
                a = str(primary.get("action", "")).upper()
                if a == "BUY":
                    action_text = f":green[{a}]"
                elif a == "SELL":
                    action_text = f":red[{a}]"
                else:
                    action_text = f":gray[{a}]"
                st.markdown(f"**{stock_key}**  ·  动作 {action_text}  ·  置信度 `{str(primary.get('confidence', ''))}`")

                for _, row in show_rows.iterrows():
                    strategy = str(row.get("strategy_id", ""))
                    buy = row.get("buy_price")
                    sell = row.get("sell_price")
                    stop = row.get("stop_price")
                    buy_shares = int(float(row.get("suggest_buy_shares", 0) or 0))
                    sell_shares = int(float(row.get("suggest_sell_shares", 0) or 0))
                    trade_value = float(row.get("estimated_trade_value", 0.0) or 0.0)
                    holding_now = int(float(row.get("holding_shares_now", 0.0) or 0.0))
                    unrealized_now = float(row.get("unrealized_pnl_now", 0.0) or 0.0)
                    market_now = float(row.get("market_value_now", 0.0) or 0.0)
                    c_buy, c_sell, c_stop, c_meta = st.columns([1, 1, 1, 1.5])
                    c_buy.metric(f"{strategy} 买入价", f"{float(buy):.4f}" if pd.notna(buy) else "N/A")
                    c_sell.metric(f"{strategy} 卖出价", f"{float(sell):.4f}" if pd.notna(sell) else "N/A")
                    c_stop.metric(f"{strategy} 止损价", f"{float(stop):.4f}" if pd.notna(stop) else "N/A")
                    c_meta.metric(f"{strategy} 样本数", int(float(row.get("sample_count", 0) or 0)))
                    m1, m2, m3 = st.columns(3)
                    m1.metric(f"{strategy} 建议买入股数", buy_shares)
                    m2.metric(f"{strategy} 建议卖出股数", sell_shares)
                    m3.metric(f"{strategy} 当前持仓股数", holding_now)
                    m4, m5 = st.columns(2)
                    m4.metric(f"{strategy} 当前持仓市值", f"{market_now:.2f}")
                    m5.metric(f"{strategy} 当前浮动盈亏", f"{unrealized_now:.2f}")
                    st.caption(f"{strategy} 预计成交金额: {trade_value:.2f}")
                    reason = str(row.get("reason", "")).strip()
                    mode = str(row.get("mode", "")).strip()
                    size_note = str(row.get("sizing_note", "")).strip()
                    note = f"模式: {mode or '-'}；理由: {reason or '-'}"
                    st.caption(note)
                    if size_note:
                        st.caption(f"股数换算: {size_note}")
                    if not (pd.notna(buy) and pd.notna(sell) and pd.notna(stop)):
                        st.warning(f"{strategy} 缺少价格字段（买入/卖出/止损）中的至少一项，谨慎执行。")

                if not show_all_strategies and len(g) > 1:
                    with st.expander(f"查看 {stock_key} 全部策略（{len(g)} 条）", expanded=False):
                        view = g[
                            [
                                "strategy_id",
                                "action",
                                "buy_price",
                                "sell_price",
                                "stop_price",
                                "suggest_buy_shares",
                                "suggest_sell_shares",
                                "holding_shares_now",
                                "estimated_trade_value",
                                "market_value_now",
                                "unrealized_pnl_now",
                                "confidence",
                                "mode",
                                "sample_count",
                                "reason",
                            ]
                        ]
                        st.dataframe(view, use_container_width=True, hide_index=True)

            if idx < len(grouped) - 1:
                st.markdown("")

        st.subheader("建议明细表")
        detail = fdf[
            [
                "stock_name",
                "stock_code",
                "strategy_id",
                "action",
                "buy_price",
                "sell_price",
                "stop_price",
                "suggest_buy_shares",
                "suggest_sell_shares",
                "holding_shares_now",
                "estimated_trade_value",
                "market_value_now",
                "unrealized_pnl_now",
                "confidence",
                "sample_count",
                "mode",
                "reason",
                "is_executable",
            ]
        ].copy()
        detail = detail.rename(
            columns={
                "stock_name": "股票",
                "stock_code": "代码",
                "strategy_id": "策略",
                "action": "动作",
                "buy_price": "买入价",
                "sell_price": "卖出价",
                "stop_price": "止损价",
                "suggest_buy_shares": "建议买入股数",
                "suggest_sell_shares": "建议卖出股数",
                "holding_shares_now": "当前持仓股数",
                "estimated_trade_value": "预计成交金额",
                "market_value_now": "当前持仓市值",
                "unrealized_pnl_now": "当前浮动盈亏",
                "confidence": "置信度",
                "sample_count": "样本数",
                "mode": "模式",
                "reason": "理由",
                "is_executable": "可执行",
            }
        )
        st.dataframe(
            detail.style.format(
                {
                    "买入价": "{:.4f}",
                    "卖出价": "{:.4f}",
                    "止损价": "{:.4f}",
                    "预计成交金额": "{:.2f}",
                    "当前持仓市值": "{:.2f}",
                    "当前浮动盈亏": "{:.2f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
    elif isinstance(parsed, list) or isinstance(parsed, dict):
        st.info("建议结果为空，请先确认股票池与数据源是否可用。")
    _show_json_debug("交易建议结果", raw)


def _render_backtest_result(parsed: Optional[Any], raw: str) -> None:
    if isinstance(parsed, dict):
        baseline = parsed.get("baseline", {}) if isinstance(parsed.get("baseline"), dict) else {}
        strategy_all = parsed.get("strategy_all", {}) if isinstance(parsed.get("strategy_all"), dict) else {}
        strategy_metrics = parsed.get("strategy_metrics", {}) if isinstance(parsed.get("strategy_metrics"), dict) else {}
        strategy_by_mode = parsed.get("strategy_by_mode", {}) if isinstance(parsed.get("strategy_by_mode"), dict) else {}
        source = str(parsed.get("data_source", "unknown")).upper()

        st.caption(f"数据源: `{source}`")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("策略 Sharpe-like", f"{float(strategy_all.get('sharpe_like', 0.0)):.4f}", delta=f"{float(strategy_all.get('sharpe_like', 0.0)) - float(baseline.get('sharpe_like', 0.0)):+.4f}")
        c2.metric("策略均值收益", f"{float(strategy_all.get('mean', 0.0)):.4%}", delta=f"{float(strategy_all.get('mean', 0.0)) - float(baseline.get('mean', 0.0)):+.4%}")
        c3.metric("策略最大回撤", f"{float(strategy_all.get('max_drawdown', 0.0)):.4%}", delta=f"{float(baseline.get('max_drawdown', 0.0)) - float(strategy_all.get('max_drawdown', 0.0)):+.4%}")
        c4.metric("样本数", int(float(strategy_all.get("count", 0.0))))

        if strategy_metrics:
            rows: List[Dict[str, Any]] = []
            for sid, m in strategy_metrics.items():
                rows.append(
                    {
                        "策略": sid,
                        "样本数": int(float(m.get("count", 0.0))),
                        "平均收益": float(m.get("mean", 0.0)),
                        "波动": float(m.get("vol", 0.0)),
                        "Sharpe-like": float(m.get("sharpe_like", 0.0)),
                        "最大回撤": float(m.get("max_drawdown", 0.0)),
                        "空仓占比": float(m.get("hold_ratio", 0.0)),
                    }
                )
            sdf = pd.DataFrame(rows).sort_values(by="Sharpe-like", ascending=False)
            top_row = sdf.iloc[0]
            st.success(f"当前最佳策略: {top_row['策略']}  |  Sharpe-like={top_row['Sharpe-like']:.4f}  |  回撤={top_row['最大回撤']:.4%}")

            st.subheader("策略对比表")
            st.dataframe(
                sdf.style.format(
                    {
                        "平均收益": "{:.4%}",
                        "波动": "{:.4%}",
                        "Sharpe-like": "{:.4f}",
                        "最大回撤": "{:.4%}",
                        "空仓占比": "{:.2%}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

            c5, c6 = st.columns(2)
            with c5:
                st.subheader("Sharpe-like 排行")
                sharpe_df = sdf[["策略", "Sharpe-like"]].set_index("策略")
                st.bar_chart(sharpe_df)
            with c6:
                st.subheader("收益 vs 回撤")
                rr_df = sdf[["策略", "平均收益", "最大回撤"]].set_index("策略")
                st.bar_chart(rr_df)

            st.subheader("空仓占比")
            hold_df = sdf[["策略", "空仓占比"]].set_index("策略")
            st.bar_chart(hold_df)

        if strategy_by_mode:
            mode_rows: List[Dict[str, Any]] = []
            for mode, m in strategy_by_mode.items():
                if not isinstance(m, dict):
                    continue
                mode_rows.append(
                    {
                        "模式": mode,
                        "样本数": int(float(m.get("count", 0.0))),
                        "平均收益": float(m.get("mean", 0.0)),
                        "Sharpe-like": float(m.get("sharpe_like", 0.0)),
                        "最大回撤": float(m.get("max_drawdown", 0.0)),
                    }
                )
            if mode_rows:
                mdf = pd.DataFrame(mode_rows).sort_values(by="Sharpe-like", ascending=False)
                st.subheader("策略模式表现")
                st.dataframe(
                    mdf.style.format(
                        {"平均收益": "{:.4%}", "Sharpe-like": "{:.4f}", "最大回撤": "{:.4%}"}
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
                st.bar_chart(mdf.set_index("模式")[["Sharpe-like", "平均收益"]])
    _show_json_debug("回测结果", raw)


def _render_history_result(parsed: Optional[Any], raw: str) -> None:
    if not isinstance(parsed, dict):
        _show_json_debug("历史查询结果", raw)
        return
    summary = parsed.get("summary", {}) if isinstance(parsed.get("summary"), dict) else {}
    by_day = parsed.get("by_day", []) if isinstance(parsed.get("by_day"), list) else []
    by_strategy = parsed.get("by_strategy", []) if isinstance(parsed.get("by_strategy"), list) else []
    by_market = parsed.get("by_market", []) if isinstance(parsed.get("by_market"), list) else []
    rows = parsed.get("rows", []) if isinstance(parsed.get("rows"), list) else []

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("累计收益", f"{summary.get('return_sum', 0):.4f}")
    c2.metric("回撤曲线", f"{summary.get('max_drawdown_curve', 0):.4f}")
    c3.metric("命中率", f"{summary.get('hit_rate', 0):.2%}")
    c4.metric("样本数", int(summary.get("count", 0)))

    if by_day:
        day_df = pd.DataFrame(by_day)
        st.subheader("按天趋势")
        if {"snapshot_date", "return_mean"}.issubset(day_df.columns):
            st.line_chart(day_df[["snapshot_date", "return_mean"]].set_index("snapshot_date"))
        st.dataframe(day_df, use_container_width=True, hide_index=True)

    c5, c6 = st.columns(2)
    if by_strategy:
        c5.subheader("按策略对比")
        c5.dataframe(pd.DataFrame(by_strategy), use_container_width=True, hide_index=True)
    if by_market:
        c6.subheader("按市场对比")
        c6.dataframe(pd.DataFrame(by_market), use_container_width=True, hide_index=True)

    if rows:
        st.subheader("策略-股票明细")
        rows_df = pd.DataFrame(rows)
        st.dataframe(rows_df, use_container_width=True, hide_index=True)
        st.download_button(
            "导出明细 CSV",
            data=rows_df.to_csv(index=False).encode("utf-8"),
            file_name="strategy_snapshot_rows.csv",
            mime="text/csv",
        )
    _show_json_debug("历史查询结果", raw)


def main() -> None:
    st.set_page_config(page_title="股票策略控制台", layout="wide")
    st.title("股票策略控制台")
    st.caption("一键执行（同步+建议） -> 回测分析 -> 历史追踪")

    with st.sidebar:
        st.header("全局配置")
        token_input = st.text_input("NOTION_TOKEN（可留空走 .env）", type="password", key="sidebar_notion_token")
        notion_version = st.text_input(
            "Notion-Version",
            value=os.getenv("NOTION_VERSION", "2022-06-28"),
            key="sidebar_notion_version",
        )
        min_conf = st.selectbox("最小置信度", options=["LOW", "MEDIUM", "HIGH"], index=1, key="sidebar_min_confidence")
        strategy_set = st.multiselect(
            "并行策略",
            options=["baseline", "chan", "atr_wave"],
            default=["baseline", "chan", "atr_wave"],
            key="sidebar_strategy_set",
        )
        timeout = st.number_input("行情请求超时（秒）", min_value=3, max_value=30, value=8, step=1, key="sidebar_timeout_sec")

    client, err = _init_client(token_input, notion_version)
    cfg = load_cfg()
    if not strategy_set:
        st.error("请至少选择一个策略。")
        st.stop()
    if err:
        st.error(err)
        st.stop()

    _show_run_status()
    tab_signal, tab_backtest, tab_history, tab_select, tab_param, tab_health = st.tabs(
        ["交易建议", "回测分析", "历史追踪", "研究选股", "参数调优", "系统健康"]
    )

    with tab_signal:
        st.subheader("交易建议")
        allow_small_sample = st.toggle("允许小样本建议（<20）", value=True, key="signal_allow_small_sample")
        dry_sync_then_rec = st.checkbox("一键流程中价格同步 dry-run", value=False, key="signal_sync_dry_run")
        dry_rec = st.checkbox("仅预览，不写入 Notion（dry-run）", value=True, key="signal_recommend_dry_run")
        if st.button("一键执行：先同步价格，再生成建议", type="primary", use_container_width=True, key="signal_run_pipeline"):
            sync_args = argparse.Namespace(dry_run=dry_sync_then_rec, timeout=int(timeout))
            sync_code, sync_raw, sync_parsed, sync_err = _run_and_capture(sync_prices, client, cfg, sync_args)
            if sync_err or sync_code != 0:
                msg = sync_err or sync_raw or "价格同步失败"
                st.error(f"步骤1失败（价格同步）：{msg}")
                _mark_run("价格同步", False, msg)
            else:
                st.success("步骤1完成：价格同步成功")
                _mark_run("价格同步", True, "实时行情已同步")
                _render_sync_result(sync_parsed, sync_raw)

                rec_args = argparse.Namespace(
                    dry_run=dry_rec,
                    asof_date="",
                    allow_small_sample=allow_small_sample,
                    min_confidence=min_conf,
                    strategy_set=",".join(strategy_set),
                    refresh_prices=False,
                    timeout=int(timeout),
                    emit_snapshot=False,
                    snapshot_date="",
                )
                code, raw, parsed, run_err = _run_and_capture(recommend_prices, client, cfg, rec_args)
                if run_err or code != 0:
                    msg = run_err or raw or "建议生成失败"
                    st.error(f"步骤2失败（建议生成）：{msg}")
                    _mark_run("建议生成", False, msg)
                else:
                    st.success("步骤2完成：建议生成成功")
                    _mark_run("建议生成", True, "建议结果已更新")
                    _render_recommend_result(parsed, raw)

    with tab_backtest:
        st.subheader("回测分析")
        window = st.number_input("回测窗口（交易事件）", min_value=10, max_value=240, value=60, step=5, key="backtest_window")
        allow_small_sample_bt = st.toggle("允许小样本", value=True, key="backtest_allow_small_sample")
        if st.button("执行回测", type="primary", use_container_width=True, key="backtest_run"):
            bt_args = argparse.Namespace(
                window=int(window),
                allow_small_sample=allow_small_sample_bt,
                min_confidence=min_conf,
                strategy_set=",".join(strategy_set),
            )
            code, raw, parsed, run_err = _run_and_capture(backtest_recommendation, client, cfg, bt_args)
            if run_err or code != 0:
                msg = run_err or raw or "回测失败"
                st.error(msg)
                _mark_run("回测", False, msg)
            else:
                st.success("回测完成")
                _mark_run("回测", True, "回测结果已更新")
                _render_backtest_result(parsed, raw)

    with tab_history:
        st.subheader("每日快照与历史趋势")
        today_str = datetime.now().strftime("%Y-%m-%d")
        c1, c2 = st.columns(2)
        snapshot_date = c1.text_input("快照日期", value=today_str, key="history_snapshot_date")
        sync_dry = c2.checkbox("Notion 同步 dry-run", value=True, key="history_snapshot_sync_dry")

        b1, b2 = st.columns(2)
        if b1.button("手动落库快照", use_container_width=True, key="history_snapshot_save"):
            snap_args = argparse.Namespace(
                dry_run=False,
                snapshot_date=snapshot_date,
                allow_small_sample=True,
                min_confidence=min_conf,
                strategy_set=",".join(strategy_set),
                refresh_prices=False,
                timeout=int(timeout),
            )
            code, raw, parsed, run_err = _run_and_capture(snapshot_daily, client, cfg, snap_args)
            if run_err or code != 0:
                msg = run_err or raw or "快照落库失败"
                st.error(msg)
                _mark_run("快照落库", False, msg)
            else:
                st.success("快照落库完成")
                _mark_run("快照落库", True, "SQLite 快照已更新")
                st.dataframe(_as_df(parsed), use_container_width=True, hide_index=True)
                _show_json_debug("快照落库结果", raw)

        if b2.button("同步当日快照到 Notion", use_container_width=True, key="history_snapshot_sync_notion"):
            sync_args = argparse.Namespace(snapshot_date=snapshot_date, dry_run=sync_dry)
            code, raw, parsed, run_err = _run_and_capture(sync_snapshot_notion, client, cfg, sync_args)
            if run_err or code != 0:
                msg = run_err or raw or "Notion 同步失败"
                st.error(msg)
                _mark_run("同步快照Notion", False, msg)
            else:
                st.success("Notion 同步完成")
                _mark_run("同步快照Notion", True, "Notion 策略快照库已更新")
                st.dataframe(_as_df(parsed), use_container_width=True, hide_index=True)
                _show_json_debug("Notion 同步结果", raw)

        st.markdown("---")
        st.subheader("历史查询")
        c3, c4 = st.columns(2)
        start_date = c3.text_input("开始日期", value=today_str, key="history_query_start")
        end_date = c4.text_input("结束日期", value=today_str, key="history_query_end")
        c5, c6 = st.columns(2)
        strategy_filter = c5.multiselect(
            "策略过滤",
            options=["BASELINE", "CHAN", "ATR_WAVE"],
            default=[],
            key="history_query_strategy_filter",
        )
        market_filter = c6.multiselect(
            "市场过滤",
            options=["SH", "SZ", "HK", "US", "OTHER"],
            default=[],
            key="history_query_market_filter",
        )
        if st.button("查询历史趋势", type="primary", use_container_width=True, key="history_query_run"):
            h_args = argparse.Namespace(
                start_date=start_date,
                end_date=end_date,
                strategies=",".join(strategy_filter),
                markets=",".join(market_filter),
            )
            code, raw, parsed, run_err = _run_and_capture(history_query, h_args)
            if run_err or code != 0:
                msg = run_err or raw or "历史查询失败"
                st.error(msg)
                _mark_run("历史查询", False, msg)
            else:
                st.success("历史查询完成")
                _mark_run("历史查询", True, "历史趋势已刷新")
                _render_history_result(parsed, raw)

    with tab_param:
        st.subheader("参数应用工作台")
        st.caption("1) 生成推荐  2) 人工调整  3) 预检并应用")
        today_str = datetime.now().strftime("%Y-%m-%d")
        p1, p2 = st.columns(2)
        pr_start = p1.text_input("推荐开始日期", value=today_str, key="param_start")
        pr_end = p2.text_input("推荐结束日期", value=today_str, key="param_end")
        p3, p4 = st.columns(2)
        pr_strategies = p3.multiselect(
            "策略",
            options=["BASELINE", "CHAN", "ATR_WAVE"],
            default=["BASELINE", "CHAN", "ATR_WAVE"],
            key="param_rec_strategies",
        )
        pr_markets = p4.multiselect(
            "市场",
            options=["SH", "SZ", "HK", "US", "OTHER"],
            default=[],
            key="param_rec_markets",
        )
        dry_param_rec = st.checkbox("仅预览推荐，不落库", value=False, key="param_rec_dry")

        if st.button("生成推荐参数", type="primary", use_container_width=True, key="param_generate"):
            args = argparse.Namespace(
                start_date=pr_start,
                end_date=pr_end,
                strategies=",".join(pr_strategies),
                markets=",".join(pr_markets),
                dry_run=dry_param_rec,
            )
            code, raw, parsed, run_err = _run_and_capture(param_recommend, args)
            if run_err or code != 0:
                msg = run_err or raw or "参数推荐生成失败"
                st.error(msg)
                _mark_run("参数推荐", False, msg)
            else:
                st.success("参数推荐生成完成")
                _mark_run("参数推荐", True, "参数推荐已生成")
                st.session_state["param_last_recommend"] = parsed
                _show_json_debug("参数推荐结果", raw)

        rec_obj = st.session_state.get("param_last_recommend", {})
        proposals = rec_obj.get("proposals", []) if isinstance(rec_obj, dict) else []
        if proposals:
            pro_df = pd.DataFrame(
                [
                    {
                        "proposal_id": x.get("proposal_id", ""),
                        "strategy_id": x.get("strategy_id", ""),
                        "market": x.get("market", ""),
                        "score": x.get("score", 0.0),
                        "sample_count": x.get("sample_count", 0),
                        "created_at": x.get("created_at", ""),
                    }
                    for x in proposals
                ]
            )
            st.dataframe(pro_df, use_container_width=True, hide_index=True)
            selected_proposal = st.selectbox("选择推荐ID", options=pro_df["proposal_id"].tolist(), key="param_selected_proposal")

            draft_key = f"param_editor_values_{selected_proposal}"
            if draft_key not in st.session_state:
                st.session_state[draft_key] = {}
            if st.button("加载并预检参数差异", use_container_width=True, key="param_load_diff"):
                diff_args = argparse.Namespace(proposal_id=selected_proposal, editor_json=json.dumps(st.session_state[draft_key], ensure_ascii=False))
                code, raw, parsed, run_err = _run_and_capture(param_diff, diff_args)
                if run_err or code != 0:
                    msg = run_err or raw or "参数差异预检失败"
                    st.error(msg)
                    _mark_run("参数预检", False, msg)
                else:
                    st.session_state["param_last_diff"] = parsed
                    _mark_run("参数预检", True, "参数差异已刷新")
                    _show_json_debug("参数差异结果", raw)

            diff_obj = st.session_state.get("param_last_diff", {})
            if isinstance(diff_obj, dict) and diff_obj.get("proposal_id") == selected_proposal:
                rows = diff_obj.get("rows", [])
                if isinstance(rows, list) and rows:
                    df_rows = pd.DataFrame(
                        [
                            {
                                "参数名": r.get("param_name"),
                                "当前值": r.get("current_value"),
                                "推荐值": r.get("recommended_value"),
                                "你的值": r.get("your_value"),
                                "变更幅度": float(r.get("delta_pct", 0.0)),
                                "风险提示": r.get("risk"),
                                "校验": "通过" if r.get("valid") else f"失败: {r.get('error', '')}",
                                "changed": bool(r.get("changed", False)),
                            }
                            for r in rows
                        ]
                    )
                    only_changed = st.checkbox("仅显示有变化项", value=True, key="param_only_changed")
                    view_df = df_rows[df_rows["changed"]] if only_changed else df_rows
                    edit_df = st.data_editor(
                        view_df[["参数名", "当前值", "推荐值", "你的值", "变更幅度", "风险提示", "校验"]],
                        hide_index=True,
                        use_container_width=True,
                        disabled=["参数名", "当前值", "推荐值", "变更幅度", "风险提示", "校验"],
                        key="param_editor_grid",
                    )
                    editor_map: Dict[str, Any] = {}
                    for _, row in edit_df.iterrows():
                        editor_map[str(row["参数名"])] = row["你的值"]
                    st.session_state[draft_key] = editor_map
                    _run_and_capture(param_draft_save, argparse.Namespace(proposal_id=selected_proposal, editor_json=json.dumps(editor_map, ensure_ascii=False)))

                    recheck_args = argparse.Namespace(proposal_id=selected_proposal, editor_json=json.dumps(editor_map, ensure_ascii=False))
                    r_code, r_raw, r_parsed, r_err = _run_and_capture(param_diff, recheck_args)
                    if not r_err and r_code == 0 and isinstance(r_parsed, dict):
                        c1, c2, c3 = st.columns(3)
                        c1.metric("将修改项", int(r_parsed.get("changed_count", 0)))
                        c2.metric("影响策略", 1)
                        c3.metric("高风险项", int(r_parsed.get("high_risk_count", 0)))
                        v = r_parsed.get("validation", {}) if isinstance(r_parsed.get("validation", {}), dict) else {}
                        c4, c5, c6 = st.columns(3)
                        c4.metric("稳定性", f"{float(v.get('stability', 0.0)):.3f}")
                        c5.metric("成本(bps)", f"{float(v.get('cost_bps', 0.0)):.1f}")
                        c6.metric("滑点(bps)", f"{float(v.get('slippage_bps', 0.0)):.1f}")

                        rel1, rel2 = st.columns(2)
                        rollout_scope = rel1.text_input("灰度范围", value="full", key="param_rollout_scope")
                        batch_id = rel2.text_input("发布批次ID", value=f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}", key="param_batch_id")
                        gx1, gx2 = st.columns(2)
                        experiment_id = gx1.text_input("实验ID（发布闸门）", value=str(rec_obj.get("experiment_id", "")), key="param_experiment_id")
                        require_experiment = gx2.checkbox("发布必须绑定实验", value=True, key="param_require_experiment")
                        g1, g2, g3 = st.columns(3)
                        gate_min_stability = g1.number_input("最小稳定性", min_value=0.0, max_value=1.0, value=0.30, step=0.05, key="param_gate_stability")
                        gate_min_hit_rate = g2.number_input("最小命中率", min_value=0.0, max_value=1.0, value=0.45, step=0.05, key="param_gate_hit")
                        gate_max_dd = g3.number_input("最大均值回撤", min_value=0.0, max_value=1.0, value=0.20, step=0.05, key="param_gate_dd")
                        confirm_apply = st.checkbox("我确认应用以上参数变更", value=False, key="param_confirm_apply")
                        ap1, ap2 = st.columns(2)
                        if ap1.button("应用参数", type="primary", use_container_width=True, disabled=not confirm_apply, key="param_apply_button"):
                            apply_args = argparse.Namespace(
                                proposal_id=selected_proposal,
                                editor_json=json.dumps(editor_map, ensure_ascii=False),
                                expected_version=int(r_parsed.get("current_version", -1)),
                                operator=os.getenv("OPERATOR", "local_user"),
                                comment="dashboard_apply",
                                batch_id=batch_id,
                                rollout_scope=rollout_scope,
                                experiment_id=experiment_id,
                                require_experiment=require_experiment,
                                gate_min_stability=float(gate_min_stability),
                                gate_min_hit_rate=float(gate_min_hit_rate),
                                gate_max_dd_mean=float(gate_max_dd),
                            )
                            a_code, a_raw, a_parsed, a_err = _run_and_capture(param_apply, apply_args)
                            if a_err or a_code != 0:
                                msg = a_err or a_raw or "参数应用失败"
                                st.error(msg)
                                _mark_run("参数应用", False, msg)
                            else:
                                st.success("参数应用成功")
                                _mark_run("参数应用", True, "参数已生效")
                                st.session_state["param_last_apply"] = a_parsed
                                _show_json_debug("参数应用结果", a_raw)
                        last_apply = st.session_state.get("param_last_apply", {})
                        rollback_id = last_apply.get("apply_log_id", "") if isinstance(last_apply, dict) else ""
                        if ap2.button("回滚上次应用", use_container_width=True, disabled=not bool(rollback_id), key="param_rollback_button"):
                            roll_args = argparse.Namespace(
                                apply_log_id=rollback_id,
                                operator=os.getenv("OPERATOR", "local_user"),
                                comment="dashboard_rollback",
                            )
                            rb_code, rb_raw, rb_parsed, rb_err = _run_and_capture(param_rollback, roll_args)
                            if rb_err or rb_code != 0:
                                msg = rb_err or rb_raw or "回滚失败"
                                st.error(msg)
                                _mark_run("参数回滚", False, msg)
                            else:
                                st.success("回滚成功")
                                _mark_run("参数回滚", True, "参数已回滚")
                                st.session_state["param_last_apply"] = rb_parsed
                                _show_json_debug("参数回滚结果", rb_raw)

    with tab_select:
        st.subheader("研究选股（规则+打分）")
        today_str = datetime.now().strftime("%Y-%m-%d")
        s1, s2 = st.columns(2)
        ss_start = s1.text_input("回看开始日期", value=today_str, key="select_start_date")
        ss_end = s2.text_input("回看结束日期", value=today_str, key="select_end_date")
        s3, s4, s5 = st.columns(3)
        ss_strategy = s3.multiselect(
            "策略过滤",
            options=["BASELINE", "CHAN", "ATR_WAVE"],
            default=["BASELINE", "CHAN", "ATR_WAVE"],
            key="select_filter_strategy",
        )
        ss_market = s4.multiselect(
            "市场过滤",
            options=["SH", "SZ", "HK", "US", "OTHER"],
            default=[],
            key="select_filter_market",
        )
        ss_topn = s5.number_input("Top N", min_value=1, max_value=50, value=10, step=1, key="select_top_n")
        ss_min_samples = st.number_input("最小样本数", min_value=1, max_value=60, value=5, step=1, key="select_min_samples")
        if st.button("执行选股打分", type="primary", use_container_width=True, key="select_run"):
            sel_args = argparse.Namespace(
                start_date=ss_start,
                end_date=ss_end,
                strategies=",".join(ss_strategy),
                markets=",".join(ss_market),
                top_n=int(ss_topn),
                min_samples=int(ss_min_samples),
            )
            code, raw, parsed, run_err = _run_and_capture(select_stock, sel_args)
            if run_err or code != 0:
                msg = run_err or raw or "选股打分失败"
                st.error(msg)
                _mark_run("研究选股", False, msg)
            else:
                st.success("选股打分完成")
                _mark_run("研究选股", True, "候选与调仓建议已生成")
                if isinstance(parsed, dict):
                    sel = parsed.get("selected", [])
                    reb = parsed.get("rebalance_plan", [])
                    if isinstance(sel, list) and sel:
                        st.subheader("候选股票")
                        st.dataframe(pd.DataFrame(sel), use_container_width=True, hide_index=True)
                    if isinstance(reb, list) and reb:
                        st.subheader("调仓建议")
                        st.dataframe(pd.DataFrame(reb), use_container_width=True, hide_index=True)
                _show_json_debug("选股打分结果", raw)

    with tab_health:
        st.subheader("系统健康")
        days = st.number_input("统计窗口（天）", min_value=1, max_value=30, value=7, step=1, key="health_days")
        if st.button("刷新健康指标", type="primary", use_container_width=True, key="health_refresh"):
            mon_args = argparse.Namespace(days=int(days))
            code, raw, parsed, run_err = _run_and_capture(param_monitor, mon_args)
            if run_err or code != 0:
                msg = run_err or raw or "健康指标刷新失败"
                st.error(msg)
                _mark_run("系统健康", False, msg)
            else:
                st.success("健康指标刷新完成")
                _mark_run("系统健康", True, "健康指标已更新")
                if isinstance(parsed, dict):
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("事件总数", int(parsed.get("event_total", 0)))
                    c2.metric("成功率", f"{float(parsed.get('success_rate', 0.0)):.2%}")
                    c3.metric("失败数", int(parsed.get("event_failed", 0)))
                    c4.metric("平均耗时(ms)", f"{float(parsed.get('avg_duration_ms', 0.0)):.1f}")
                    t1, t2, t3, t4 = st.columns(4)
                    t1.metric("发布次数", int(parsed.get("apply_count", 0)))
                    t2.metric("回滚率", f"{float(parsed.get('rollback_rate', 0.0)):.2%}")
                    t3.metric("冲突率", f"{float(parsed.get('conflict_rate', 0.0)):.2%}")
                    t4.metric("回滚次数", int(parsed.get("rollback_count", 0)))
                    apply_stat = parsed.get("apply_stat", {})
                    if isinstance(apply_stat, dict) and apply_stat:
                        st.subheader("参数发布状态")
                        stat_df = pd.DataFrame([{"status": k, "count": v} for k, v in apply_stat.items()])
                        st.dataframe(stat_df, use_container_width=True, hide_index=True)
                    failures = parsed.get("recent_failures", [])
                    if isinstance(failures, list) and failures:
                        st.subheader("最近失败任务")
                        st.dataframe(pd.DataFrame(failures), use_container_width=True, hide_index=True)
                    fail_dist = parsed.get("failure_distribution", [])
                    if isinstance(fail_dist, list) and fail_dist:
                        st.subheader("失败分布（按错误码）")
                        st.dataframe(pd.DataFrame(fail_dist), use_container_width=True, hide_index=True)
                    slow_tasks = parsed.get("slow_tasks", [])
                    if isinstance(slow_tasks, list) and slow_tasks:
                        st.subheader("慢任务Top")
                        st.dataframe(pd.DataFrame(slow_tasks), use_container_width=True, hide_index=True)
                _show_json_debug("系统健康结果", raw)

    st.markdown("---")
    st.caption("建议：使用一键执行（先同步再生成建议），最后做快照与历史查询。")


if __name__ == "__main__":
    main()
