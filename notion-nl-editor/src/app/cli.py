import argparse
import os
import sys

from commands.audit import audit, validate_manual_entries
from commands.migrate import add_trade, annual_sync, backfill_cash_relations, migrate_apply, migrate_preview
from commands.param import (
    param_apply,
    param_diff,
    param_draft_save,
    param_migrate,
    param_monitor,
    param_recommend,
    param_risk_guard,
    param_rollback,
)
from commands.research import history_query, manual_filter_list, manual_filter_set, select_stock, snapshot_market_daily, sync_market_universe
from commands.signal import (
    backtest_recommendation,
    recommend_prices,
    snapshot_daily,
    sync_kline,
    sync_prices,
    sync_snapshot_notion,
)
from core.config import load_cfg
from core.env_utils import load_dotenv
from core.notion_client import NotionClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stock page pipeline for audit, migration and automation.")
    parser.add_argument("--token", default=os.getenv("NOTION_TOKEN"))
    parser.add_argument("--notion-version", default=os.getenv("NOTION_VERSION", "2022-06-28"))

    sub = parser.add_subparsers(dest="cmd", required=True)

    cmd_audit = sub.add_parser("audit", help="输出结构缺口审计清单")
    cmd_audit.add_argument("--json", action="store_true")

    cmd_migrate_preview = sub.add_parser("migrate-preview", help="预览历史数据迁移结果")
    cmd_migrate_preview.add_argument("--sample", type=int, default=20)

    cmd_migrate_apply = sub.add_parser("migrate-apply", help="执行历史数据迁移到交易流水")
    cmd_migrate_apply.add_argument("--limit", type=int, default=0, help="0=全部；否则仅导入前 N 条")

    cmd_add_trade = sub.add_parser("add-trade", help="新增一笔标准交易")
    cmd_add_trade.add_argument("--date", required=True, help="YYYY-MM-DD")
    cmd_add_trade.add_argument("--direction", required=True, choices=["BUY", "SELL"])
    cmd_add_trade.add_argument("--stock", required=True, help="股票名称或股票代码")
    cmd_add_trade.add_argument("--shares", required=True, type=float)
    cmd_add_trade.add_argument("--price", required=True, type=float)
    cmd_add_trade.add_argument("--fee", type=float, default=0.0)
    cmd_add_trade.add_argument("--tax", type=float, default=0.0)
    cmd_add_trade.add_argument("--strategy", default="")
    cmd_add_trade.add_argument("--note", default="")
    sub.add_parser("backfill-cash-relations", help="backfill trade relations into cash config account row")

    sub.add_parser("validate-manual", help="校验手工记录必填字段")

    cmd_sync_annual = sub.add_parser("sync-annual", help="按交易+分红重算年度收益")
    cmd_sync_annual.add_argument("--dry-run", action="store_true")

    cmd_recommend = sub.add_parser("recommend-prices", help="计算并回填每只股票的下一次交易建议价位")
    cmd_recommend.add_argument("--dry-run", action="store_true", help="仅输出建议，不回填 Notion")
    cmd_recommend.add_argument("--asof-date", default="", help="建议更新时间，默认今天 YYYY-MM-DD")
    cmd_recommend.add_argument("--allow-small-sample", action="store_true", default=True)
    cmd_recommend.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    cmd_recommend.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    cmd_recommend.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")
    cmd_recommend.add_argument("--data-source", choices=["kline", "trade"], default="kline", help="策略输入数据源")
    cmd_recommend.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K 线复权口径")
    cmd_recommend.add_argument("--start-date", default="", help="K 线开始日期，默认自动回看")
    cmd_recommend.add_argument("--end-date", default="", help="K 线结束日期，默认今天 YYYY-MM-DD")
    cmd_recommend.add_argument("--refresh-prices", action="store_true", help="先同步实时价格再计算建议")
    cmd_recommend.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")
    cmd_recommend.add_argument("--emit-snapshot", action="store_true", help="recommend 后写入每日快照")
    cmd_recommend.add_argument("--snapshot-date", default="", help="快照日期，默认今天 YYYY-MM-DD")
    cmd_recommend.add_argument("--param-market", default="", help="参数市场，默认按股票代码推断")
    cmd_recommend.add_argument("--param-scope", default="*", help="参数作用域，默认 *")

    cmd_backtest = sub.add_parser("backtest-recommendation", help="回测建议模型")
    cmd_backtest.add_argument("--window", type=int, default=60, help="历史窗口长度（按日线 K 线）")
    cmd_backtest.add_argument("--allow-small-sample", action="store_true", default=True)
    cmd_backtest.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    cmd_backtest.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    cmd_backtest.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")
    cmd_backtest.add_argument("--data-source", choices=["kline", "trade"], default="kline", help="回测输入数据源")
    cmd_backtest.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K 线复权口径")
    cmd_backtest.add_argument("--start-date", default="", help="K 线开始日期，默认自动回看")
    cmd_backtest.add_argument("--end-date", default="", help="K 线结束日期，默认今天 YYYY-MM-DD")
    cmd_backtest.add_argument("--force", action="store_true", help="强制重新同步 K 线")
    cmd_backtest.add_argument("--param-market", default="", help="参数市场，默认按股票代码推断")
    cmd_backtest.add_argument("--param-scope", default="*", help="参数作用域，默认 *")

    cmd_sync_prices = sub.add_parser("sync-prices", help="自动拉取实时行情并回填当前市价")
    cmd_sync_prices.add_argument("--dry-run", action="store_true", help="仅拉取并输出统计，不写入 Notion")
    cmd_sync_prices.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")

    cmd_sync_kline = sub.add_parser("sync-kline", help="同步日线 K 线到本地 SQLite 缓存")
    cmd_sync_kline.add_argument("--start-date", default="", help="开始日期，默认自动回看")
    cmd_sync_kline.add_argument("--end-date", default="", help="结束日期，默认今天 YYYY-MM-DD")
    cmd_sync_kline.add_argument("--symbols", default="", help="逗号分隔股票代码，如 600519,000001")
    cmd_sync_kline.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K 线复权口径")
    cmd_sync_kline.add_argument("--force", action="store_true", help="强制覆盖缓存")

    cmd_snapshot_daily = sub.add_parser("snapshot-daily", help="生成并落库每日策略快照")
    cmd_snapshot_daily.add_argument("--dry-run", action="store_true", help="仅预览，不写入 SQLite")
    cmd_snapshot_daily.add_argument("--snapshot-date", default="", help="快照日期，默认今天 YYYY-MM-DD")
    cmd_snapshot_daily.add_argument("--allow-small-sample", action="store_true", default=True)
    cmd_snapshot_daily.add_argument("--disallow-small-sample", action="store_false", dest="allow_small_sample")
    cmd_snapshot_daily.add_argument("--min-confidence", choices=["LOW", "MEDIUM", "HIGH"], default="MEDIUM")
    cmd_snapshot_daily.add_argument("--strategy-set", default="baseline,chan,atr_wave", help="comma list: baseline,chan,atr_wave")
    cmd_snapshot_daily.add_argument("--data-source", choices=["kline", "trade"], default="kline", help="快照输入数据源")
    cmd_snapshot_daily.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="K 线复权口径")
    cmd_snapshot_daily.add_argument("--start-date", default="", help="K 线开始日期，默认自动回看")
    cmd_snapshot_daily.add_argument("--end-date", default="", help="K 线结束日期，默认今天 YYYY-MM-DD")
    cmd_snapshot_daily.add_argument("--force", action="store_true", help="强制重新同步 K 线")
    cmd_snapshot_daily.add_argument("--refresh-prices", action="store_true", help="先同步实时价格再快照")
    cmd_snapshot_daily.add_argument("--timeout", type=int, default=8, help="实时行情请求超时秒数")
    cmd_snapshot_daily.add_argument("--param-market", default="", help="参数市场，默认按股票代码推断")
    cmd_snapshot_daily.add_argument("--param-scope", default="*", help="参数作用域，默认 *")

    cmd_sync_snapshot = sub.add_parser("sync-snapshot-notion", help="同步指定日期快照到 Notion 策略快照库")
    cmd_sync_snapshot.add_argument("--snapshot-date", default="", help="同步日期，默认今天 YYYY-MM-DD")
    cmd_sync_snapshot.add_argument("--dry-run", action="store_true", help="仅预览，不写入 Notion")

    cmd_history = sub.add_parser("history-query", help="查询历史快照并输出 JSON")
    cmd_history.add_argument("--start-date", default="", help="开始日期，默认今天 YYYY-MM-DD")
    cmd_history.add_argument("--end-date", default="", help="结束日期，默认开始日期 YYYY-MM-DD")
    cmd_history.add_argument("--strategies", default="", help="策略过滤，如 BASELINE,CHAN")
    cmd_history.add_argument("--markets", default="", help="市场过滤，如 SH,SZ,HK")

    cmd_param_recommend = sub.add_parser("param-recommend", help="基于历史快照生成参数推荐")
    cmd_param_recommend.add_argument("--start-date", default="", help="开始日期，默认今天 YYYY-MM-DD")
    cmd_param_recommend.add_argument("--end-date", default="", help="结束日期，默认开始日期 YYYY-MM-DD")
    cmd_param_recommend.add_argument("--strategies", default="", help="策略过滤，如 BASELINE,CHAN")
    cmd_param_recommend.add_argument("--markets", default="", help="市场过滤，如 SH,SZ,HK")
    cmd_param_recommend.add_argument("--dry-run", action="store_true", help="仅计算推荐，不写入 proposal 表")
    cmd_param_recommend.add_argument("--walk-forward-splits", type=int, default=3, help="walk-forward 分窗数量")
    cmd_param_recommend.add_argument("--cost-bps", type=float, default=3.0, help="交易成本 bps")
    cmd_param_recommend.add_argument("--slippage-bps", type=float, default=2.0, help="滑点 bps")
    cmd_param_recommend.add_argument("--experiment-id", default="", help="绑定已有实验 ID；留空则自动创建")
    cmd_param_recommend.add_argument("--experiment-name", default="", help="实验名称（自动创建时生效）")
    cmd_param_recommend.add_argument("--train-window", type=int, default=60, help="训练窗口（交易日）")
    cmd_param_recommend.add_argument("--valid-window", type=int, default=20, help="验证窗口（交易日）")

    cmd_param_diff = sub.add_parser("param-diff", help="比较当前值、推荐值、人工编辑值")
    cmd_param_diff.add_argument("--proposal-id", required=True, help="参数推荐 ID")
    cmd_param_diff.add_argument("--editor-json", default="", help="人工编辑 JSON 文本或 json 文件路径")

    cmd_param_apply = sub.add_parser("param-apply", help="应用参数推荐（支持人工编辑）")
    cmd_param_apply.add_argument("--proposal-id", required=True, help="参数推荐 ID")
    cmd_param_apply.add_argument("--editor-json", default="", help="人工编辑 JSON 文本或 json 文件路径")
    cmd_param_apply.add_argument("--expected-version", type=int, default=-1, help="并发保护版本号，-1 表示不校验")
    cmd_param_apply.add_argument("--operator", default="", help="操作者，默认取 OPERATOR 或 local_user")
    cmd_param_apply.add_argument("--comment", default="", help="应用备注")
    cmd_param_apply.add_argument("--batch-id", default="", help="发布批次 ID")
    cmd_param_apply.add_argument("--rollout-scope", default="full", help="灰度范围，如 full/market:SH/strategy:BASELINE")
    cmd_param_apply.add_argument("--gray-scope", default="", help="兼容旧参数名，等价于 --rollout-scope")
    cmd_param_apply.add_argument("--experiment-id", default="", help="发布闸门绑定实验 ID")
    cmd_param_apply.add_argument("--require-experiment", action="store_true", help="发布必须绑定实验 ID")
    cmd_param_apply.add_argument("--gate-min-stability", type=float, default=0.0, help="发布闸门：最小稳定性")
    cmd_param_apply.add_argument("--gate-min-hit-rate", type=float, default=0.0, help="发布闸门：最小命中率")
    cmd_param_apply.add_argument("--gate-max-dd-mean", type=float, default=1.0, help="发布闸门：最大平均回撤")

    cmd_param_rollback = sub.add_parser("param-rollback", help="按 apply_log_id 回滚参数")
    cmd_param_rollback.add_argument("--apply-log-id", required=True, help="apply_log_id")
    cmd_param_rollback.add_argument("--operator", default="", help="操作者，默认取 OPERATOR 或 local_user")
    cmd_param_rollback.add_argument("--comment", default="", help="回滚备注")

    cmd_param_draft_save = sub.add_parser("param-draft-save", help="保存参数编辑草稿")
    cmd_param_draft_save.add_argument("--proposal-id", required=True, help="参数推荐 ID")
    cmd_param_draft_save.add_argument("--editor-json", default="", help="人工编辑 JSON 文本或 json 文件路径")

    cmd_param_monitor = sub.add_parser("param-monitor", help="输出参数系统健康与最近异常")
    cmd_param_monitor.add_argument("--days", type=int, default=7, help="统计窗口天数")

    sub.add_parser("param-migrate", help="执行参数系统 Schema 迁移并输出校验结果")

    cmd_param_risk_guard = sub.add_parser("param-risk-guard", help="风险守护：命中率/回撤劣化自动回滚")
    cmd_param_risk_guard.add_argument("--days", type=int, default=7, help="观察窗口天数")
    cmd_param_risk_guard.add_argument("--apply-lookback-days", type=int, default=30, help="回看最近应用窗口天数")
    cmd_param_risk_guard.add_argument("--min-hit-rate", type=float, default=0.45, help="最小命中率阈值")
    cmd_param_risk_guard.add_argument("--max-drawdown-curve", type=float, default=0.2, help="最大回撤曲线阈值")
    cmd_param_risk_guard.add_argument("--dry-run", action="store_true", help="仅预览，不执行回滚")

    cmd_select_stock = sub.add_parser("select-stock", help="规则打分选股并输出调仓建议")
    cmd_select_stock.add_argument("--start-date", default="", help="开始日期，默认今天往前 60 天")
    cmd_select_stock.add_argument("--end-date", default="", help="结束日期，默认今天")
    cmd_select_stock.add_argument("--strategies", default="", help="策略过滤，如 BASELINE,CHAN")
    cmd_select_stock.add_argument("--markets", default="", help="市场过滤，如 SH,SZ,HK")
    cmd_select_stock.add_argument("--top-n", type=int, default=10, help="输出 Top N")
    cmd_select_stock.add_argument("--min-samples", type=int, default=5, help="最小样本数过滤")
    cmd_select_stock.add_argument("--manual-filter-mode", choices=["off", "strict", "overlay"], default="strict", help="manual filter mode: off/strict/overlay")

    cmd_sync_market_universe = sub.add_parser("sync-market-universe", help="sync full-market universe to sqlite")
    cmd_sync_market_universe.add_argument("--list-status", default="L", help="Tushare list_status: L/S/P")
    cmd_sync_market_universe.add_argument("--markets", default="SH,SZ,BJ", help="market filter, e.g. SH,SZ,BJ")

    cmd_snapshot_market = sub.add_parser("snapshot-market-daily", help="emit market-wide daily snapshot")
    cmd_snapshot_market.add_argument("--snapshot-date", default="", help="snapshot date, default today YYYY-MM-DD")
    cmd_snapshot_market.add_argument("--start-date", default="", help="kline start date")
    cmd_snapshot_market.add_argument("--end-date", default="", help="kline end date")
    cmd_snapshot_market.add_argument("--markets", default="SH,SZ,BJ", help="market filter, e.g. SH,SZ,BJ")
    cmd_snapshot_market.add_argument("--strategy-id", default="MARKET_SCAN", help="snapshot strategy id")
    cmd_snapshot_market.add_argument("--min-bars", type=int, default=60, help="minimum bars")
    cmd_snapshot_market.add_argument("--adj", choices=["raw", "qfq", "hfq"], default=os.getenv("KLINE_DEFAULT_ADJ", "raw"), help="kline adj")
    cmd_snapshot_market.add_argument("--include-inactive", action="store_true", help="include inactive symbols")
    cmd_snapshot_market.add_argument("--sync-missing", action="store_true", default=True, help="sync missing kline automatically")
    cmd_snapshot_market.add_argument("--no-sync-missing", action="store_false", dest="sync_missing", help="do not sync missing kline")
    cmd_snapshot_market.add_argument("--dry-run", action="store_true", help="preview only")

    cmd_manual_filter_set = sub.add_parser("manual-filter-set", help="upsert one manual stock filter")
    cmd_manual_filter_set.add_argument("--stock-code", required=True, help="6-digit stock code")
    cmd_manual_filter_set.add_argument("--decision", required=True, choices=["include", "exclude", "watch", "clear"], help="include/exclude/watch/clear")
    cmd_manual_filter_set.add_argument("--reason", default="", help="manual decision reason")
    cmd_manual_filter_set.add_argument("--operator", default="", help="operator name")

    sub.add_parser("manual-filter-list", help="list manual stock filters")

    return parser


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
    if args.cmd == "param-monitor":
        return param_monitor(args)
    if args.cmd == "param-migrate":
        return param_migrate(args)
    if args.cmd == "param-risk-guard":
        return param_risk_guard(args)
    if args.cmd == "select-stock":
        return select_stock(args)
    if args.cmd == "sync-market-universe":
        return sync_market_universe(args)
    if args.cmd == "snapshot-market-daily":
        return snapshot_market_daily(args)
    if args.cmd == "manual-filter-set":
        return manual_filter_set(args)
    if args.cmd == "manual-filter-list":
        return manual_filter_list(args)

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
    if args.cmd == "backfill-cash-relations":
        return backfill_cash_relations(client, cfg)
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
    if args.cmd == "sync-kline":
        return sync_kline(client, cfg, args)
    if args.cmd == "snapshot-daily":
        return snapshot_daily(client, cfg, args)
    if args.cmd == "sync-snapshot-notion":
        return sync_snapshot_notion(client, cfg, args)
    return 1


__all__ = ["build_parser", "main"]
