from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from services.recommendation.data_prep import TradePoint
from stores.kline_store import KBar


@dataclass
class RecommendationContext:
    stock_db: Dict[str, Any]
    stock_fields: Dict[str, Optional[str]]
    stock_rows: List[Dict[str, Any]]
    stock_points: Dict[str, List[TradePoint]]
    stock_kbars: Dict[str, List[KBar]]
    stock_symbols: Dict[str, str]


@dataclass
class RecommendationRequest:
    dry_run: bool = False
    asof_date: str = ""
    snapshot_date: str = ""
    emit_snapshot: bool = False
    strategy_set: str = "baseline,chan,atr_wave"
    allow_small_sample: bool = True
    min_confidence: str = "MEDIUM"
    data_source: str = "kline"
    adj: str = "raw"
    start_date: str = ""
    end_date: str = ""
    force: bool = False
    refresh_prices: bool = False
    timeout: int = 8
    param_market: str = ""
    param_scope: str = "*"
    window: int = 60
    execution_delay_days: int = 1
    cost_bps: float = 3.0
    slippage_bps: float = 2.0
    min_trade_lot: int = 100
    limit_move_pct: float = 0.098
    halt_move_pct: float = 0.18
