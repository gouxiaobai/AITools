from collections import defaultdict
from typing import Any, Dict, List

from services.research.stats import max_drawdown, safe_mean


def build_history_payload(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    by_strategy: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    by_market: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[str(row.get("snapshot_date", ""))].append(row)
        by_strategy[str(row.get("strategy_id", ""))].append(row)
        by_market[str(row.get("market", ""))].append(row)

    def _agg(items: List[Dict[str, Any]]) -> Dict[str, float]:
        returns = [float(x.get("ret_1d", 0.0) or 0.0) for x in items]
        hit_vals = [float(x.get("hit_flag", 0.0) or 0.0) for x in items]
        return {
            "count": float(len(items)),
            "return_mean": safe_mean(returns) if returns else 0.0,
            "return_sum": float(sum(returns)),
            "hit_rate": safe_mean(hit_vals) if hit_vals else 0.0,
            "max_drawdown_mean": safe_mean([float(x.get("max_drawdown", 0.0) or 0.0) for x in items]) if items else 0.0,
        }

    by_day_rows: List[Dict[str, Any]] = []
    for day, items in sorted(by_day.items()):
        row = {"snapshot_date": day}
        row.update(_agg(items))
        by_day_rows.append(row)

    day_returns = [float(x["return_mean"]) for x in by_day_rows]
    summary = _agg(rows)
    summary["max_drawdown_curve"] = max_drawdown(day_returns) if day_returns else 0.0

    by_strategy_rows: List[Dict[str, Any]] = []
    for strategy_id, items in sorted(by_strategy.items()):
        row = {"strategy_id": strategy_id}
        row.update(_agg(items))
        by_strategy_rows.append(row)

    by_market_rows: List[Dict[str, Any]] = []
    for market, items in sorted(by_market.items()):
        row = {"market": market}
        row.update(_agg(items))
        by_market_rows.append(row)

    return {
        "summary": summary,
        "by_day": by_day_rows,
        "by_strategy": by_strategy_rows,
        "by_market": by_market_rows,
        "rows": rows,
    }


def build_experiment_baseline(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    payload = build_history_payload(rows)
    return {
        "summary": payload.get("summary", {}),
        "by_strategy": payload.get("by_strategy", []),
        "by_market": payload.get("by_market", []),
    }


def build_experiment_protocol(train_window: int, valid_window: int, test_window: int) -> Dict[str, Any]:
    return {
        "train_window": max(1, int(train_window)),
        "valid_window": max(1, int(valid_window)),
        "test_window": max(1, int(test_window)),
        "template": "train/valid/test fixed-window",
    }


def build_research_report(
    rows: List[Dict[str, Any]],
    proposal_rows: List[Dict[str, Any]],
    protocol: Dict[str, Any],
) -> Dict[str, Any]:
    history = build_history_payload(rows)
    summary = history.get("summary", {})
    sample_count = int(sum(int(x.get("sample_count", 0) or 0) for x in proposal_rows))
    mean_stability = safe_mean([float((x.get("validation", {}) or {}).get("stability", 0.0) or 0.0) for x in proposal_rows])
    regime_consistency = safe_mean([float((x.get("validation", {}) or {}).get("regime_consistency", 0.0) or 0.0) for x in proposal_rows])
    feasible_ratio = safe_mean([float((x.get("validation", {}) or {}).get("execution_feasibility_ratio", 0.0) or 0.0) for x in proposal_rows])
    proposal_hit_rate = safe_mean([float(x.get("hit_rate", 0.0) or 0.0) for x in proposal_rows])
    proposal_dd_mean = safe_mean([float(x.get("dd_mean", 0.0) or 0.0) for x in proposal_rows])

    return {
        "protocol": protocol,
        "performance": {
            "return_mean": float(summary.get("return_mean", 0.0) or 0.0),
            "return_sum": float(summary.get("return_sum", 0.0) or 0.0),
            "hit_rate": float(summary.get("hit_rate", 0.0) or 0.0),
            "proposal_hit_rate": proposal_hit_rate,
        },
        "risk": {
            "max_drawdown_curve": float(summary.get("max_drawdown_curve", 0.0) or 0.0),
            "max_drawdown_mean": float(summary.get("max_drawdown_mean", 0.0) or 0.0),
            "proposal_dd_mean": proposal_dd_mean,
        },
        "stability": {
            "stability_mean": mean_stability,
            "regime_consistency": regime_consistency,
            "sample_count": sample_count,
        },
        "execution": {
            "execution_feasibility_ratio": feasible_ratio,
            "rows": len(rows),
            "proposal_count": len(proposal_rows),
        },
        "failure_analysis_template": {
            "weekly_false_positive": [],
            "weekly_false_negative": [],
            "extreme_regime_degradation": [],
            "notes": "fill weekly from live observation",
        },
    }
