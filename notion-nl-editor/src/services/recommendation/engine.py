from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ExecutionContext:
    stock_rows: Optional[list] = None
    stock_kbars: Optional[dict] = None
    stock_symbols: Optional[dict] = None
    param_cache: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    snapshot_slice: Optional[dict] = None

    def get_cached_param(self, key: str) -> Optional[Dict[str, Any]]:
        return self.param_cache.get(key)

    def put_cached_param(self, key: str, value: Dict[str, Any]) -> None:
        self.param_cache[key] = value
