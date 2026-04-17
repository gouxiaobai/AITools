from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Literal, Optional

from services.recommendation.data_prep import TradePoint
from stores.kline_store import KBar

DataSource = Literal["trade", "kline"]
StrategyEvaluator = Callable[["StrategyEvalRequest"], Dict[str, Any]]


@dataclass
class StrategyEvalRequest:
    strategy_id: str
    source: DataSource
    current_price: Optional[float]
    current_cost: Optional[float]
    allow_small_sample: bool
    min_confidence: str
    param_cfg: Optional[Dict[str, Any]] = None
    points: List[TradePoint] = field(default_factory=list)
    bars: List[KBar] = field(default_factory=list)


class StrategyRegistry:
    def __init__(self) -> None:
        self._evaluators: Dict[str, StrategyEvaluator] = {}

    def register(self, strategy_id: str, evaluator: StrategyEvaluator) -> None:
        key = (strategy_id or "").strip().lower()
        if not key:
            raise ValueError("strategy_id cannot be empty")
        self._evaluators[key] = evaluator

    def get(self, strategy_id: str) -> StrategyEvaluator:
        key = (strategy_id or "").strip().lower()
        if key not in self._evaluators:
            raise ValueError(f"unsupported strategy: {strategy_id}")
        return self._evaluators[key]

    def strategy_ids(self) -> List[str]:
        return sorted(self._evaluators.keys())

    def evaluate(self, req: StrategyEvalRequest) -> Dict[str, Any]:
        evaluator = self.get(req.strategy_id)
        rec = evaluator(req)
        rec["strategy_id"] = (req.strategy_id or "").strip().upper()
        return rec


_REGISTRY = StrategyRegistry()


def get_strategy_registry() -> StrategyRegistry:
    return _REGISTRY


def register_strategy(strategy_id: str, evaluator: StrategyEvaluator) -> None:
    _REGISTRY.register(strategy_id, evaluator)


def get_strategy_ids() -> List[str]:
    return _REGISTRY.strategy_ids()
