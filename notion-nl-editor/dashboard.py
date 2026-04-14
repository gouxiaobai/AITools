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
    if isinstance(parsed, list):
        df = _as_df(parsed)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("建议总数", len(df))
        c2.metric("BUY", int((df.get("action", pd.Series()) == "BUY").sum()) if "action" in df else 0)
        c3.metric("SELL", int((df.get("action", pd.Series()) == "SELL").sum()) if "action" in df else 0)
        c4.metric("HOLD", int((df.get("action", pd.Series()) == "HOLD").sum()) if "action" in df else 0)
        st.dataframe(df, use_container_width=True, hide_index=True)
    _show_json_debug("交易建议结果", raw)


def _render_backtest_result(parsed: Optional[Any], raw: str) -> None:
    if isinstance(parsed, dict):
        baseline = parsed.get("baseline", {}) if isinstance(parsed.get("baseline"), dict) else {}
        strategy = parsed.get("strategy_all", {}) if isinstance(parsed.get("strategy_all"), dict) else {}
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("基线 Sharpe-like", f"{baseline.get('sharpe_like', 0):.4f}")
        c2.metric("策略 Sharpe-like", f"{strategy.get('sharpe_like', 0):.4f}")
        c3.metric("基线回撤", f"{baseline.get('max_drawdown', 0):.4f}")
        c4.metric("策略回撤", f"{strategy.get('max_drawdown', 0):.4f}")
        st.dataframe(_as_df(parsed.get("strategy_metrics", {})), use_container_width=True)
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
    st.caption("实时行情 -> 建议生成 -> 回测分析 -> 历史追踪")

    with st.sidebar:
        st.header("全局配置")
        token_input = st.text_input("NOTION_TOKEN（可留空走 .env）", type="password")
        notion_version = st.text_input("Notion-Version", value=os.getenv("NOTION_VERSION", "2022-06-28"))
        min_conf = st.selectbox("最小置信度", options=["LOW", "MEDIUM", "HIGH"], index=1)
        strategy_set = st.multiselect("并行策略", options=["baseline", "chan", "atr_wave"], default=["baseline", "chan", "atr_wave"])
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
    tab_price, tab_signal, tab_backtest, tab_history, tab_param, tab_health = st.tabs(
        ["实时行情", "交易建议", "回测分析", "历史追踪", "参数调优", "系统健康"]
    )

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
        st.subheader("交易建议")
        allow_small_sample = st.toggle("允许小样本建议（<20）", value=True, key="tab_allow_small")
        dry_rec = st.checkbox("仅预览，不写入 Notion（dry-run）", value=True, key="tab_rec_dry")
        if st.button("执行建议生成", type="primary", use_container_width=True):
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
                st.error(msg)
                _mark_run("建议生成", False, msg)
            else:
                st.success("建议生成完成")
                _mark_run("建议生成", True, "建议结果已更新")
                _render_recommend_result(parsed, raw)

    with tab_backtest:
        st.subheader("回测分析")
        window = st.number_input("回测窗口（交易事件）", min_value=10, max_value=240, value=60, step=5)
        allow_small_sample_bt = st.toggle("允许小样本", value=True, key="bt_allow_small")
        if st.button("执行回测", type="primary", use_container_width=True):
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
        snapshot_date = c1.text_input("快照日期", value=today_str, key="snapshot_date")
        sync_dry = c2.checkbox("Notion 同步 dry-run", value=True, key="snapshot_notion_dry")

        b1, b2 = st.columns(2)
        if b1.button("手动落库快照", use_container_width=True):
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

        if b2.button("同步当日快照到 Notion", use_container_width=True):
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
        start_date = c3.text_input("开始日期", value=today_str, key="hist_start")
        end_date = c4.text_input("结束日期", value=today_str, key="hist_end")
        c5, c6 = st.columns(2)
        strategy_filter = c5.multiselect("策略过滤", options=["BASELINE", "CHAN", "ATR_WAVE"], default=[])
        market_filter = c6.multiselect("市场过滤", options=["SH", "SZ", "HK", "US", "OTHER"], default=[])
        if st.button("查询历史趋势", type="primary", use_container_width=True):
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
        pr_strategies = p3.multiselect("策略", options=["BASELINE", "CHAN", "ATR_WAVE"], default=["BASELINE", "CHAN", "ATR_WAVE"])
        pr_markets = p4.multiselect("市场", options=["SH", "SZ", "HK", "US", "OTHER"], default=[])
        dry_param_rec = st.checkbox("仅预览推荐，不落库", value=False, key="param_rec_dry")

        if st.button("生成推荐参数", type="primary", use_container_width=True):
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
            if st.button("加载并预检参数差异", use_container_width=True):
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
                        confirm_apply = st.checkbox("我确认应用以上参数变更", value=False, key="param_confirm_apply")
                        ap1, ap2 = st.columns(2)
                        if ap1.button("应用参数", type="primary", use_container_width=True, disabled=not confirm_apply):
                            apply_args = argparse.Namespace(
                                proposal_id=selected_proposal,
                                editor_json=json.dumps(editor_map, ensure_ascii=False),
                                expected_version=int(r_parsed.get("current_version", -1)),
                                operator=os.getenv("OPERATOR", "local_user"),
                                comment="dashboard_apply",
                                batch_id=batch_id,
                                rollout_scope=rollout_scope,
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
                        if ap2.button("回滚上次应用", use_container_width=True, disabled=not bool(rollback_id)):
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

    with tab_health:
        st.subheader("系统健康")
        days = st.number_input("统计窗口（天）", min_value=1, max_value=30, value=7, step=1)
        if st.button("刷新健康指标", type="primary", use_container_width=True):
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
                    apply_stat = parsed.get("apply_stat", {})
                    if isinstance(apply_stat, dict) and apply_stat:
                        st.subheader("参数发布状态")
                        stat_df = pd.DataFrame([{"status": k, "count": v} for k, v in apply_stat.items()])
                        st.dataframe(stat_df, use_container_width=True, hide_index=True)
                    failures = parsed.get("recent_failures", [])
                    if isinstance(failures, list) and failures:
                        st.subheader("最近失败任务")
                        st.dataframe(pd.DataFrame(failures), use_container_width=True, hide_index=True)
                _show_json_debug("系统健康结果", raw)

    st.markdown("---")
    st.caption("建议：先同步实时行情，再生成建议，最后做快照与历史查询。")


if __name__ == "__main__":
    main()
