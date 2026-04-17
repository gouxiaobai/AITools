from dataclasses import dataclass
from typing import Any, Dict, Tuple


PARAM_APPLY_GATE_BLOCKED = "PARAM_APPLY_GATE_BLOCKED"


@dataclass
class GateThreshold:
    min_stability: float
    min_hit_rate: float
    max_dd_mean: float
    require_experiment: bool
    min_regime_consistency: float = 0.0
    min_execution_feasibility: float = 0.0
    min_benchmark_delta: float = -1.0


def evaluate_release_gate(proposal: Dict[str, Any], expected_experiment_id: str, threshold: GateThreshold) -> Tuple[bool, str, Dict[str, Any]]:
    validation = proposal.get("validation", {}) if isinstance(proposal.get("validation", {}), dict) else {}
    stability = float(validation.get("stability", 0.0) or 0.0)
    regime_consistency = float(validation.get("regime_consistency", 0.0) or 0.0)
    execution_feasibility = float(validation.get("execution_feasibility_ratio", 0.0) or 0.0)
    benchmark_delta = float(validation.get("benchmark_delta", 0.0) or 0.0)
    hit_rate = float(proposal.get("hit_rate", 0.0) or 0.0)
    dd_mean = float(proposal.get("dd_mean", 0.0) or 0.0)
    proposal_exp = (proposal.get("experiment_id", "") or "").strip()
    expected_exp = (expected_experiment_id or "").strip()
    reasons = []
    if threshold.require_experiment and not (proposal_exp or expected_exp):
        reasons.append("missing experiment id")
    if expected_exp and proposal_exp and expected_exp != proposal_exp:
        reasons.append(f"experiment mismatch: proposal={proposal_exp} expected={expected_exp}")
    if stability < float(threshold.min_stability):
        reasons.append(f"stability={stability:.4f} < {threshold.min_stability:.4f}")
    if hit_rate < float(threshold.min_hit_rate):
        reasons.append(f"hit_rate={hit_rate:.4f} < {threshold.min_hit_rate:.4f}")
    if dd_mean > float(threshold.max_dd_mean):
        reasons.append(f"dd_mean={dd_mean:.4f} > {threshold.max_dd_mean:.4f}")
    if regime_consistency < float(threshold.min_regime_consistency):
        reasons.append(f"regime_consistency={regime_consistency:.4f} < {threshold.min_regime_consistency:.4f}")
    if execution_feasibility < float(threshold.min_execution_feasibility):
        reasons.append(f"execution_feasibility={execution_feasibility:.4f} < {threshold.min_execution_feasibility:.4f}")
    if benchmark_delta < float(threshold.min_benchmark_delta):
        reasons.append(f"benchmark_delta={benchmark_delta:.4f} < {threshold.min_benchmark_delta:.4f}")
    payload = {
        "proposal_experiment_id": proposal_exp,
        "expected_experiment_id": expected_exp,
        "stability": stability,
        "hit_rate": hit_rate,
        "dd_mean": dd_mean,
        "regime_consistency": regime_consistency,
        "execution_feasibility": execution_feasibility,
        "benchmark_delta": benchmark_delta,
        "threshold": {
            "require_experiment": threshold.require_experiment,
            "min_stability": threshold.min_stability,
            "min_hit_rate": threshold.min_hit_rate,
            "max_dd_mean": threshold.max_dd_mean,
            "min_regime_consistency": threshold.min_regime_consistency,
            "min_execution_feasibility": threshold.min_execution_feasibility,
            "min_benchmark_delta": threshold.min_benchmark_delta,
        },
    }
    return len(reasons) == 0, "; ".join(reasons), payload
