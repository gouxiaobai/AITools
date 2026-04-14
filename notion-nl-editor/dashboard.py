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
    load_cfg,
    load_dotenv,
    recommend_prices,
    sync_prices,
)


def _run_and_capture(fn, *args, **kwargs) -> Tuple[int, str, Optional[Any], Optional[str]]:
    buffer = io.StringIO()
    try:
        with redirect_stdout(buffer):
            code = fn(*args, **kwargs)
        raw = buffer.getvalue().strip()
        parsed = None
        if raw:
            parsed = json.loads(raw)
        return int(code), raw, parsed, None
    except Exception as exc:
        return 1, buffer.getvalue().strip(), None, str(exc)


def _init_client(token_override: str, notion_version: str) -> Tuple[Optional[NotionClient], Optional[str]]:
    load_dotenv(os.path.join(ROOT_DIR, ".env"))
    token = token_override.strip() or os.getenv("NOTION_TOKEN", "").strip()
    if not token:
        return None, "未检测到 NOTION_TOKEN。请先在 .env 配置，或在左侧输入 Token。"
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
    with st.expander(f"{title}（调试 JSON）", expanded=False):
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
    if isinstance(parsed, list):
        df = _as_df(parsed)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("建议总数", len(df))
        c2.metric("BUY", int((df.get("action", pd.Series()) == "BUY").sum()) if "action" in df else 0)
        c3.metric("SELL", int((df.get("action", pd.Series()) == "SELL").sum()) if "action" in df else 0)
        c4.metric("HOLD", int((df.get("action", pd.Series()) == "HOLD").sum()) if "action" in df else 0)

        f1, f2 = st.columns([1, 1])
        conf_opts = sorted(df["confidence"].dropna().unique().tolist()) if "confidence" in df.columns else []
        action_opts = sorted(df["action"].dropna().unique().tolist()) if "action" in df.columns else []
        conf_filter = f1.multiselect("筛选置信度", conf_opts, default=conf_opts, key="conf_filter")
        action_filter = f2.multiselect("筛选动作", action_opts, default=action_opts, key="action_filter")

        if "confidence" in df.columns:
            df = df[df["confidence"].isin(conf_filter)]
        if "action" in df.columns:
            df = df[df["action"].isin(action_filter)]
        sort_cols: List[str] = [c for c in ["confidence", "action", "sample_count"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=[True] * len(sort_cols))
        st.dataframe(df, use_container_width=True, hide_index=True)
        if {"stock_name", "strategy_id", "action"}.issubset(df.columns):
            show_cols = [c for c in ["action", "buy_price", "sell_price", "stop_price", "confidence", "sample_count"] if c in df.columns]
            if show_cols:
                compare = (
                    df[["stock_name", "strategy_id"] + show_cols]
                    .pivot_table(index="stock_name", columns="strategy_id", values=show_cols, aggfunc="first")
                )
                compare.columns = [f"{s}_{k}" for k, s in compare.columns]
                compare = compare.reset_index()
                st.subheader("策略对比视图（按股票并排）")
                st.dataframe(compare, use_container_width=True, hide_index=True)
    _show_json_debug("交易建议结果", raw)


def _render_backtest_result(parsed: Optional[Any], raw: str) -> None:
    if isinstance(parsed, dict):
        baseline = parsed.get("baseline", {}) if isinstance(parsed.get("baseline"), dict) else {}
        strategy = parsed.get("strategy_all", {}) if isinstance(parsed.get("strategy_all"), dict) else {}
        by_mode = parsed.get("strategy_by_mode", {}) if isinstance(parsed.get("strategy_by_mode"), dict) else {}
        by_strategy = parsed.get("strategy_metrics", {}) if isinstance(parsed.get("strategy_metrics"), dict) else {}
        by_strategy_mode = parsed.get("strategy_mode_metrics", {}) if isinstance(parsed.get("strategy_mode_metrics"), dict) else {}

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("基线 Sharpe-like", f"{baseline.get('sharpe_like', 0):.4f}")
        c2.metric("策略 Sharpe-like", f"{strategy.get('sharpe_like', 0):.4f}")
        c3.metric("基线回撤", f"{baseline.get('max_drawdown', 0):.4f}")
        c4.metric("策略回撤", f"{strategy.get('max_drawdown', 0):.4f}")

        mode_rows = []
        for mode, metric in by_mode.items():
            row = {"mode": mode}
            if isinstance(metric, dict):
                row.update(metric)
            mode_rows.append(row)
        if mode_rows:
            st.subheader("按模式对比")
            st.dataframe(pd.DataFrame(mode_rows), use_container_width=True, hide_index=True)

        strategy_rows = []
        for sid, metric in by_strategy.items():
            row = {"strategy_id": sid}
            if isinstance(metric, dict):
                row.update(metric)
            strategy_rows.append(row)
        if strategy_rows:
            st.subheader("按策略对比")
            st.dataframe(pd.DataFrame(strategy_rows), use_container_width=True, hide_index=True)

        strategy_mode_rows = []
        for key, metric in by_strategy_mode.items():
            row = {"strategy_mode": key}
            if isinstance(metric, dict):
                row.update(metric)
            strategy_mode_rows.append(row)
        if strategy_mode_rows:
            st.subheader("按策略+模式对比")
            st.dataframe(pd.DataFrame(strategy_mode_rows), use_container_width=True, hide_index=True)

        st.subheader("总体对比")
        compare_rows = [
            {"bucket": "baseline", **baseline},
            {"bucket": "strategy_all", **strategy},
        ]
        st.dataframe(pd.DataFrame(compare_rows), use_container_width=True, hide_index=True)
    _show_json_debug("回测结果", raw)


def main() -> None:
    st.set_page_config(page_title="股票策略控制台", layout="wide")
    st.title("股票策略平台控制台")
    st.caption("手动执行日频流程：实时行情同步 -> 建议生成 -> 回测分析")

    with st.sidebar:
        st.header("全局配置")
        token_input = st.text_input("NOTION_TOKEN（可留空走 .env）", type="password")
        notion_version = st.text_input("Notion-Version", value=os.getenv("NOTION_VERSION", "2022-06-28"))
        min_conf = st.selectbox("最小置信度", options=["LOW", "MEDIUM", "HIGH"], index=1)
        strategy_set = st.multiselect(
            "并行策略",
            options=["baseline", "chan", "atr_wave"],
            default=["baseline", "chan", "atr_wave"],
        )
        timeout = st.number_input("行情请求超时（秒）", min_value=3, max_value=30, value=8, step=1)

    client, err = _init_client(token_input, notion_version)
    cfg = load_cfg()
    if not strategy_set:
        st.error("请至少选择一个策略。")
        st.stop()
    if err:
        st.error(err)
        st.stop()

    _show_run_status()
    tab_price, tab_signal, tab_backtest = st.tabs(["实时行情", "交易建议", "回测分析"])

    with tab_price:
        st.subheader("实时行情同步")
        dry_sync = st.checkbox("仅预览，不写入 Notion（dry-run）", value=False, key="tab_sync_dry")
        if st.button("执行价格同步", type="primary", use_container_width=True):
            args = argparse.Namespace(dry_run=dry_sync, timeout=int(timeout))
            code, raw, parsed, run_err = _run_and_capture(sync_prices, client, cfg, args)
            if run_err or code != 0:
                msg = run_err or raw or "价格同步失败"
                st.error(msg)
                _mark_run("价格同步", False, msg)
            else:
                st.success("价格同步完成")
                _mark_run("价格同步", True, "实时行情已同步")
                _render_sync_result(parsed, raw)

    with tab_signal:
        st.subheader("交易建议（串行流程）")
        st.caption("步骤 1 -> 步骤 2 -> 步骤 3，任一步失败将阻断后续步骤。")
        allow_small_sample = st.toggle("允许小样本建议（<20笔）", value=True, key="tab_allow_small")
        dry_rec = st.checkbox("仅预览，不写入 Notion（dry-run）", value=True, key="tab_rec_dry")
        auto_sync = st.checkbox("步骤1自动执行价格同步", value=True, key="tab_auto_sync")

        step1_btn, step2_btn, step3_btn = st.columns(3)
        run_sync = step1_btn.button("步骤1：同步价格", use_container_width=True)
        run_recommend = step2_btn.button("步骤2：生成建议", use_container_width=True)
        run_pipeline = step3_btn.button("步骤3：一键串行执行", use_container_width=True)

        if run_sync:
            args = argparse.Namespace(dry_run=dry_rec, timeout=int(timeout))
            code, raw, parsed, run_err = _run_and_capture(sync_prices, client, cfg, args)
            if run_err or code != 0:
                msg = run_err or raw or "步骤1失败：价格同步失败"
                st.error(msg)
                _mark_run("步骤1 同步价格", False, msg)
            else:
                st.success("步骤1完成")
                _mark_run("步骤1 同步价格", True, "价格同步成功")
                _render_sync_result(parsed, raw)

        if run_recommend:
            rec_args = argparse.Namespace(
                dry_run=dry_rec,
                asof_date="",
                allow_small_sample=allow_small_sample,
                min_confidence=min_conf,
                strategy_set=",".join(strategy_set),
                refresh_prices=False,
                timeout=int(timeout),
            )
            code, raw, parsed, run_err = _run_and_capture(recommend_prices, client, cfg, rec_args)
            if run_err or code != 0:
                msg = run_err or raw or "步骤2失败：建议生成失败"
                st.error(msg)
                _mark_run("步骤2 生成建议", False, msg)
            else:
                st.success("步骤2完成")
                _mark_run("步骤2 生成建议", True, "建议生成成功")
                _render_recommend_result(parsed, raw)

        if run_pipeline:
            ok = True
            if auto_sync:
                sync_args = argparse.Namespace(dry_run=dry_rec, timeout=int(timeout))
                s_code, s_raw, s_parsed, s_err = _run_and_capture(sync_prices, client, cfg, sync_args)
                if s_err or s_code != 0:
                    msg = s_err or s_raw or "步骤1失败：价格同步失败"
                    st.error(msg)
                    _mark_run("串行流程", False, msg)
                    ok = False
                else:
                    st.success("步骤1完成")
                    _render_sync_result(s_parsed, s_raw)

            if ok:
                rec_args = argparse.Namespace(
                    dry_run=dry_rec,
                    asof_date="",
                    allow_small_sample=allow_small_sample,
                    min_confidence=min_conf,
                    strategy_set=",".join(strategy_set),
                    refresh_prices=False,
                    timeout=int(timeout),
                )
                r_code, r_raw, r_parsed, r_err = _run_and_capture(recommend_prices, client, cfg, rec_args)
                if r_err or r_code != 0:
                    msg = r_err or r_raw or "步骤2失败：建议生成失败"
                    st.error(msg)
                    _mark_run("串行流程", False, msg)
                    ok = False
                else:
                    st.success("步骤2完成")
                    _render_recommend_result(r_parsed, r_raw)

            if ok:
                bt_args = argparse.Namespace(
                    window=60,
                    allow_small_sample=allow_small_sample,
                    min_confidence=min_conf,
                    strategy_set=",".join(strategy_set),
                )
                b_code, b_raw, b_parsed, b_err = _run_and_capture(backtest_recommendation, client, cfg, bt_args)
                if b_err or b_code != 0:
                    msg = b_err or b_raw or "步骤3失败：回测失败"
                    st.error(msg)
                    _mark_run("串行流程", False, msg)
                else:
                    st.success("步骤3完成")
                    _mark_run("串行流程", True, "步骤1-3执行成功")
                    _render_backtest_result(b_parsed, b_raw)

    with tab_backtest:
        st.subheader("回测分析")
        window = st.number_input("回测窗口（交易事件）", min_value=10, max_value=240, value=60, step=5)
        allow_small_sample_bt = st.toggle("回测允许小样本建议", value=True, key="bt_allow_small")
        if st.button("执行回测", type="primary", use_container_width=True):
            bt_args = argparse.Namespace(
                window=int(window),
                allow_small_sample=allow_small_sample_bt,
                min_confidence=min_conf,
                strategy_set=",".join(strategy_set),
            )
            code, raw, parsed, run_err = _run_and_capture(backtest_recommendation, client, cfg, bt_args)
            if run_err or code != 0:
                msg = run_err or raw or "回测执行失败"
                st.error(msg)
                _mark_run("回测分析", False, msg)
            else:
                st.success("回测完成")
                _mark_run("回测分析", True, "回测执行成功")
                _render_backtest_result(parsed, raw)

    st.markdown("---")
    st.caption("建议：先在“实时行情”执行同步，再到“交易建议”执行串行流程，最后在“回测分析”查看稳定性。")


if __name__ == "__main__":
    main()
