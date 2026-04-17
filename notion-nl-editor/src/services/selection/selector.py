from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List

from services.research.stats import minmax_scale, safe_mean, safe_stdev


@dataclass
class SnapshotSlice:
    rows: List[Dict[str, Any]]
    strategy_filter: List[str]
    market_filter: List[str]
    start_date: str
    end_date: str


def score_snapshot_slice(snapshot: SnapshotSlice, top_n: int) -> Dict[str, Any]:
    by_stock: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in snapshot.rows:
        by_stock[str(row.get("stock_id", ""))].append(row)

    scored: List[Dict[str, Any]] = []
    for stock_id, items in by_stock.items():
        if not stock_id:
            continue
        returns = [float(x.get("ret_1d", 0.0) or 0.0) for x in items]
        hit_vals = [float(x.get("hit_flag", 0) or 0.0) for x in items]
        dd_vals = [float(x.get("max_drawdown", 0.0) or 0.0) for x in items]
        sample = len(items)
        mean_ret = safe_mean(returns) if returns else 0.0
        momentum_5 = safe_mean(returns[-5:]) if returns else 0.0
        hit_rate = safe_mean(hit_vals) if hit_vals else 0.0
        dd_mean = safe_mean(dd_vals) if dd_vals else 0.0
        vol = safe_stdev(returns) if len(returns) > 1 else 0.0
        reliability_vals = [float(x.get("reliability_score", 0.0) or 0.0) for x in items]
        feasibility_vals = [1.0 if str(x.get("execution_feasibility", "")).upper() in {"TRADEABLE", "CAUTION"} else 0.0 for x in items]
        latest = items[-1]
        scored.append(
            {
                "stock_id": stock_id,
                "stock_code": latest.get("stock_code", ""),
                "stock_name": latest.get("stock_name", ""),
                "market": latest.get("market", ""),
                "sample_count": sample,
                "mean_ret": mean_ret,
                "momentum_5": momentum_5,
                "hit_rate": hit_rate,
                "dd_mean": dd_mean,
                "vol": vol,
                "reliability": safe_mean(reliability_vals) if reliability_vals else 0.0,
                "execution_feasibility": safe_mean(feasibility_vals) if feasibility_vals else 0.0,
            }
        )

    if not scored:
        return {"candidates": [], "selected": [], "rebalance_plan": []}

    momentum_scaled = minmax_scale([x["momentum_5"] for x in scored])
    hit_scaled = minmax_scale([x["hit_rate"] for x in scored])
    dd_scaled = minmax_scale([-x["dd_mean"] for x in scored])
    vol_scaled = minmax_scale([-x["vol"] for x in scored])
    reliability_scaled = minmax_scale([x["reliability"] for x in scored])
    feasibility_scaled = minmax_scale([x["execution_feasibility"] for x in scored])
    for idx, row in enumerate(scored):
        score = (
            momentum_scaled[idx] * 0.30
            + hit_scaled[idx] * 0.25
            + dd_scaled[idx] * 0.15
            + vol_scaled[idx] * 0.10
            + reliability_scaled[idx] * 0.15
            + feasibility_scaled[idx] * 0.05
        )
        row["score"] = round(score, 6)
        row["factor_breakdown"] = {
            "momentum_5": round(momentum_scaled[idx], 6),
            "hit_rate": round(hit_scaled[idx], 6),
            "drawdown_quality": round(dd_scaled[idx], 6),
            "vol_quality": round(vol_scaled[idx], 6),
            "reliability": round(reliability_scaled[idx], 6),
            "execution_feasibility": round(feasibility_scaled[idx], 6),
        }
    ranked = sorted(scored, key=lambda x: (x["score"], x["sample_count"]), reverse=True)
    selected = ranked[: max(1, int(top_n))]
    score_sum = sum(max(0.0, float(x["score"])) for x in selected)
    rebalance: List[Dict[str, Any]] = []
    for row in selected:
        target_weight = (max(0.0, float(row["score"])) / score_sum) if score_sum > 0 else (1.0 / float(len(selected)))
        rebalance.append(
            {
                "stock_id": row["stock_id"],
                "stock_code": row["stock_code"],
                "stock_name": row["stock_name"],
                "target_weight": round(target_weight, 6),
                "score": row["score"],
            }
        )
    return {"candidates": ranked, "selected": selected, "rebalance_plan": rebalance}
