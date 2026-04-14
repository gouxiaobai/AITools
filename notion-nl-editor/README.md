# Notion NL Editor

A minimal Python project to edit a Notion database with either:
- structured JSON rules, or
- simple Chinese natural language templates.

## 1) Setup

```powershell
cd D:\Testwork\AITools\notion-nl-editor
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env`:

```env
NOTION_TOKEN=secret_xxx
NOTION_DATABASE_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
NOTION_VERSION=2022-06-28
```

Important:
- Share your Notion database with your integration first.
- Use `preview` before `apply`.

## Stock pipeline (audit + migration + auto annual sync)

This project now includes `src/stock_pipeline.py` for your Stock page workflow:

- audit structural gaps
- preview/apply historical migration
- add standardized trades
- validate required fields
- sync annual summary from standard trade/dividend tables

### Commands

```powershell
# 1) Audit current gaps
python src\stock_pipeline.py audit

# 2) Preview historical migration
python src\stock_pipeline.py migrate-preview --sample 20

# 3) Apply historical migration (all)
python src\stock_pipeline.py migrate-apply

# 4) Add one standardized trade
python src\stock_pipeline.py add-trade --date 2026-04-09 --direction BUY --stock 600519 --shares 100 --price 1500 --fee 5 --tax 0 --strategy trend --note "manual entry"

# 5) Validate manual records required fields
python src\stock_pipeline.py validate-manual

# 6) Recalculate annual summary from standard tables
python src\stock_pipeline.py sync-annual --dry-run
python src\stock_pipeline.py sync-annual

# 7) Generate next-trade recommendations per stock (dry-run)
python src\stock_pipeline.py recommend-prices --dry-run --min-confidence MEDIUM

# 8) Pull realtime prices and write to current price field
python src\stock_pipeline.py sync-prices

# 9) Pull realtime prices then generate recommendations in one command
python src\stock_pipeline.py recommend-prices --refresh-prices --min-confidence MEDIUM

# 10) Backtest recommendation model
python src\stock_pipeline.py backtest-recommendation --window 60 --min-confidence MEDIUM

# 11) Run parallel strategies (BASELINE + CHAN + ATR_WAVE)
python src\stock_pipeline.py recommend-prices --dry-run --strategy-set baseline,chan,atr_wave
python src\stock_pipeline.py backtest-recommendation --window 60 --strategy-set baseline,chan,atr_wave

# 12) Emit daily snapshot to local SQLite
python src\stock_pipeline.py snapshot-daily --snapshot-date 2026-04-14

# 13) Emit snapshot directly after recommendation
python src\stock_pipeline.py recommend-prices --emit-snapshot --snapshot-date 2026-04-14

# 14) Sync one-day snapshot to Notion strategy snapshot DB
python src\stock_pipeline.py sync-snapshot-notion --snapshot-date 2026-04-14

# 15) Query history from SQLite
python src\stock_pipeline.py history-query --start-date 2026-04-01 --end-date 2026-04-14 --strategies BASELINE,CHAN --markets SH,SZ
```

## Visual Dashboard

Run a local visual UI to execute price sync / recommendation / backtest:

```powershell
cd D:\Testwork\AITools\notion-nl-editor
pip install -r requirements.txt
streamlit run .\dashboard.py
```

Features:
- one-click `sync-prices`
- one-click `recommend-prices` (optional pre-sync)
- one-click `backtest-recommendation`
- table/json output in browser

By default, these commands use the Stock database IDs in `.env.example`.
Override by setting env vars in `.env`:

- `DB_STOCK_MASTER_ID`
- `DB_STD_TRADES_ID`
- `DB_STD_DIVIDEND_ID`
- `DB_ANNUAL_ID`
- `DB_BUY_WIDE_ID`
- `DB_T_RECORD_ID`
- `DB_STRATEGY_SNAPSHOT_ID`
- `SQLITE_PATH` (default: `./data/strategy_snapshots.db`)
- `SNAPSHOT_MARKET_RULE` (optional, e.g. `60:SH,00:SZ,30:SZ`)

## 2) Commands

### Show database properties

```powershell
python src\notion_nl_editor.py properties
```

### Preview by JSON rule

```powershell
python src\notion_nl_editor.py preview --rule-file examples\rules.todo_to_high.json
```

### Apply by JSON rule

```powershell
python src\notion_nl_editor.py apply --rule-file examples\rules.todo_to_high.json --yes
```

### Preview by NL template

```powershell
python src\notion_nl_editor.py preview --nl "把\"状态\"为\"待处理\"的记录，将\"优先级\"改为\"高\""
```

### Apply by NL template

```powershell
python src\notion_nl_editor.py apply --nl "把\"状态\"为\"待处理\"的记录，将\"优先级\"改为\"高\"" --yes
```

## 3) Supported simple NL templates

1. `把"属性A"为"值A"的记录，将"属性B"改为"值B"`
2. `将"日期属性"在今天之前的记录，"状态属性"改为"值"`

If you need broader NL understanding, connect an LLM parser later and output the same rule JSON schema.

## 4) Rule JSON schema

```json
{
  "description": "optional",
  "filters": [
    {"property": "状态", "type": "select", "op": "equals", "value": "待处理"}
  ],
  "updates": {
    "优先级": {"type": "select", "value": "高"}
  }
}
```
