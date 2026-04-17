from collections import defaultdict
from typing import Any, Dict, List

from services.research.stats import max_drawdown, safe_mean


def find_breach_actions(rows: List[Dict[str, Any]], applied_rows: List[Dict[str, Any]], min_hit_rate: float, max_drawdown_curve: float) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = f"{str(row.get('strategy_id', '')).upper()}|{str(row.get('market', '')).upper()}"
        grouped[key].append(row)
    stat: Dict[str, Dict[str, float]] = {}
    for key, items in grouped.items():
        hit_vals = [float(x.get("hit_flag", 0) or 0.0) for x in items]
        by_day: Dict[str, List[float]] = defaultdict(list)
        for x in items:
            by_day[str(x.get("snapshot_date", ""))].append(float(x.get("ret_1d", 0.0) or 0.0))
        day_mean = [safe_mean(arr) if arr else 0.0 for _, arr in sorted(by_day.items())]
        stat[key] = {
            "hit_rate": safe_mean(hit_vals) if hit_vals else 0.0,
            "drawdown_curve": max_drawdown(day_mean) if day_mean else 0.0,
        }
    actions: List[Dict[str, Any]] = []
    for row in applied_rows:
        sid = str(row.get("strategy_id", "")).upper()
        market = str(row.get("market", "")).upper()
        key = f"{sid}|{market}"
        s = stat.get(key, {"hit_rate": 0.0, "drawdown_curve": 0.0})
        breaches = []
        if float(s.get("hit_rate", 0.0)) < float(min_hit_rate):
            breaches.append(f"hit_rate={float(s.get('hit_rate', 0.0)):.4f} < {float(min_hit_rate):.4f}")
        if float(s.get("drawdown_curve", 0.0)) > float(max_drawdown_curve):
            breaches.append(f"drawdown_curve={float(s.get('drawdown_curve', 0.0)):.4f} > {float(max_drawdown_curve):.4f}")
        if breaches:
            actions.append(
                {
                    "apply_log_id": str(row.get("apply_log_id", "")),
                    "strategy_id": sid,
                    "market": market,
                    "breach": breaches,
                }
            )
    return actions
