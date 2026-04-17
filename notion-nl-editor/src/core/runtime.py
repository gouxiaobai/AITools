import datetime as dt
import json
import os
import time
from typing import Any, Dict, List


def sqlite_path() -> str:
    raw = os.getenv("SQLITE_PATH", "./data/strategy_snapshots.db")
    return os.path.abspath(raw)


def today_or(raw: str) -> str:
    date_text = (raw or "").strip()
    if not date_text:
        return dt.date.today().isoformat()
    dt.datetime.strptime(date_text, "%Y-%m-%d")
    return date_text


def split_csv(raw: str) -> List[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def load_json_arg(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    if os.path.exists(text):
        with open(text, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return json.loads(text)


def now_ms() -> float:
    return time.time() * 1000.0
