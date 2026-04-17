import argparse
import statistics
from typing import Any, Dict, List, Optional

from core.notion_props import get_prop, rt_plain, text_prop, title_prop


def safe_mean(values: List[float]) -> float:
    return float(statistics.mean(values)) if values else 0.0


def safe_stdev(values: List[float]) -> float:
    return float(statistics.pstdev(values)) if len(values) > 1 else 0.0


def coerce_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def parse_strategy_set(raw: str) -> List[str]:
    from services.recommendation.signals import get_registered_strategy_ids

    allowed = set(get_registered_strategy_ids())
    if not allowed:
        allowed = {"baseline", "chan", "atr_wave"}
    items = [x.strip().lower() for x in (raw or "").split(",") if x.strip()]
    if not items:
        items = ["baseline", "chan", "atr_wave"]
    items = [x for x in items if x in allowed]
    if not items:
        raise ValueError("strategy-set must include baseline/chan/atr_wave")
    out: List[str] = []
    for item in items:
        if item not in out:
            out.append(item)
    return out


def resolve_recommend_data_source(args: argparse.Namespace) -> str:
    return coerce_text(getattr(args, "data_source", "kline")).strip().lower() or "kline"


def find_prop_name(
    db_props: Dict[str, Dict[str, Any]],
    candidates: List[str],
    expected_types: Optional[List[str]] = None,
) -> Optional[str]:
    for name in candidates:
        info = db_props.get(name)
        if not info:
            continue
        if expected_types and info.get("type") not in expected_types:
            continue
        return name
    return None


def prop_text_any(page: Dict[str, Any], key: Optional[str]) -> str:
    if not key:
        return ""
    prop = get_prop(page, key)
    typ = prop.get("type")
    if typ == "title":
        return rt_plain(prop.get("title", []))
    if typ == "rich_text":
        return rt_plain(prop.get("rich_text", []))
    if typ == "select":
        sel = prop.get("select")
        return sel.get("name", "") if sel else ""
    if typ == "status":
        sel = prop.get("status")
        return sel.get("name", "") if sel else ""
    if typ == "date":
        date_obj = prop.get("date")
        return date_obj.get("start", "") if date_obj else ""
    if typ == "number":
        num = prop.get("number")
        return "" if num is None else str(num)
    return ""


def write_prop_value(db_props: Dict[str, Dict[str, Any]], prop_name: str, value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    prop_info = db_props.get(prop_name)
    if not prop_info:
        return None
    typ = prop_info.get("type")
    if typ == "number":
        return {"number": float(value)}
    if typ == "select":
        return {"select": {"name": coerce_text(value)}}
    if typ == "status":
        return {"status": {"name": coerce_text(value)}}
    if typ == "rich_text":
        return text_prop(coerce_text(value))
    if typ == "date":
        return {"date": {"start": coerce_text(value)}}
    if typ == "title":
        return title_prop(coerce_text(value))
    return None
