import argparse
import io
import json
import os
import sys
from contextlib import redirect_stdout
from typing import Any, Dict, Optional, Tuple

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
        text = buffer.getvalue().strip()
        parsed = None
        if text:
            parsed = json.loads(text)
        return int(code), text, parsed, None
    except Exception as e:
        return 1, buffer.getvalue().strip(), None, str(e)


def _init_client(token_override: str, notion_version: str) -> Tuple[Optional[NotionClient], Optional[str]]:
    load_dotenv(os.path.join(ROOT_DIR, ".env"))
    token = token_override.strip() or os.getenv("NOTION_TOKEN", "").strip()
    if not token:
        return None, "未检测到 NOTION_TOKEN。请先在 .env 配置，或在左侧输入 Token。"
    return NotionClient(token=token, version=notion_version), None


st.set_page_config(page_title="Stock 建议面板", layout="wide")
st.title("Stock 交易建议面板")
st.caption("可视化执行：同步实时市价、生成交易建议、查看回测结果")

with st.sidebar:
    st.header("连接设置")
    token_input = st.text_input("NOTION_TOKEN（可留空走 .env）", type="password")
    notion_version = st.text_input("Notion-Version", value=os.getenv("NOTION_VERSION", "2022-06-28"))
    min_conf = st.selectbox("最小置信度", options=["LOW", "MEDIUM", "HIGH"], index=1)
    allow_small_sample = st.toggle("允许小样本建议（<20笔）", value=True)
    timeout = st.number_input("行情请求超时（秒）", min_value=3, max_value=30, value=8, step=1)
    window = st.number_input("回测窗口（交易事件）", min_value=10, max_value=240, value=60, step=5)

client, client_err = _init_client(token_input, notion_version)
cfg = load_cfg()

if client_err:
    st.error(client_err)
    st.stop()

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("1) 同步实时市价")
    dry_sync = st.checkbox("仅预览不同步（dry-run）", value=False, key="sync_dry")
    if st.button("执行 sync-prices", use_container_width=True):
        args = argparse.Namespace(dry_run=dry_sync, timeout=int(timeout))
        code, raw, parsed, err = _run_and_capture(sync_prices, client, cfg, args)
        if err:
            st.error(err)
        elif code != 0:
            st.error(raw or "sync-prices 执行失败")
        else:
            st.success("sync-prices 执行成功")
            if isinstance(parsed, dict):
                a, b, c, d = st.columns(4)
                a.metric("股票总数", parsed.get("total_symbols", 0))
                b.metric("拉取成功", parsed.get("price_fetched", 0))
                c.metric("已更新", parsed.get("updated", 0))
                d.metric("跳过", parsed.get("skipped", 0))
            st.code(raw or "{}", language="json")

with col2:
    st.subheader("2) 生成交易建议")
    refresh_prices = st.checkbox("先同步实时市价", value=True, key="rec_refresh")
    dry_rec = st.checkbox("仅预览不回写（dry-run）", value=True, key="rec_dry")
    if st.button("执行 recommend-prices", use_container_width=True):
        if refresh_prices:
            sync_args = argparse.Namespace(dry_run=dry_rec, timeout=int(timeout))
            sync_code, _, _, sync_err = _run_and_capture(sync_prices, client, cfg, sync_args)
            if sync_err or sync_code != 0:
                st.error(sync_err or "实时市价同步失败，已中止建议计算")
                st.stop()

        rec_args = argparse.Namespace(
            dry_run=dry_rec,
            asof_date="",
            allow_small_sample=allow_small_sample,
            min_confidence=min_conf,
            refresh_prices=False,
            timeout=int(timeout),
        )
        code, raw, parsed, err = _run_and_capture(recommend_prices, client, cfg, rec_args)
        if err:
            st.error(err)
        elif code != 0:
            st.error(raw or "recommend-prices 执行失败")
        else:
            st.success("recommend-prices 执行成功")
            if isinstance(parsed, list):
                st.dataframe(parsed, use_container_width=True, hide_index=True)
            else:
                st.code(raw or "[]", language="json")

with col3:
    st.subheader("3) 回测结果")
    if st.button("执行 backtest-recommendation", use_container_width=True):
        bt_args = argparse.Namespace(
            window=int(window),
            allow_small_sample=allow_small_sample,
            min_confidence=min_conf,
        )
        code, raw, parsed, err = _run_and_capture(backtest_recommendation, client, cfg, bt_args)
        if err:
            st.error(err)
        elif code != 0:
            st.error(raw or "backtest-recommendation 执行失败")
        else:
            st.success("backtest-recommendation 执行成功")
            if isinstance(parsed, dict):
                st.json(parsed)
            else:
                st.code(raw or "{}", language="json")

st.markdown("---")
st.caption("建议：先执行“同步实时市价”，再执行“生成交易建议（dry-run）”确认结果，最后关闭 dry-run 写回 Notion。")
