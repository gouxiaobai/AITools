import math
import statistics
from typing import Dict, List


def safe_mean(values: List[float]) -> float:
    return statistics.mean(values) if values else 0.0


def safe_stdev(values: List[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def max_drawdown(returns: List[float]) -> float:
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in returns:
        equity *= 1.0 + r
        peak = max(peak, equity)
        dd = (peak - equity) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd


def returns_metrics(returns: List[float]) -> Dict[str, float]:
    if not returns:
        return {"count": 0.0, "mean": 0.0, "vol": 0.0, "sharpe_like": 0.0, "max_drawdown": 0.0}
    mean_r = safe_mean(returns)
    vol = safe_stdev(returns)
    sharpe_like = (mean_r / vol * math.sqrt(len(returns))) if vol > 0 else 0.0
    return {
        "count": float(len(returns)),
        "mean": mean_r,
        "vol": vol,
        "sharpe_like": sharpe_like,
        "max_drawdown": max_drawdown(returns),
    }


def minmax_scale(values: List[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if hi - lo < 1e-12:
        return [0.5 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]
