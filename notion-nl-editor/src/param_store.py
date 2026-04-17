
import datetime as dt
import hashlib
import json
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

SCHEMA_VERSION = 3

PARAM_SCHEMA: Dict[str, Dict[str, Any]] = {
    "band_low": {"type": "number", "min": 0.005, "max": 0.2, "step": 0.001},
    "band_high": {"type": "number", "min": 0.01, "max": 0.3, "step": 0.001},
    "stop_mult": {"type": "number", "min": 1.0, "max": 3.0, "step": 0.1},
    "trend_threshold": {"type": "number", "min": 0.005, "max": 0.08, "step": 0.001},
    "vol_cap": {"type": "number", "min": 0.02, "max": 0.3, "step": 0.01},
    "rr_min": {"type": "number", "min": 0.3, "max": 2.0, "step": 0.05},
    "min_confidence": {"type": "enum", "choices": ["LOW", "MEDIUM", "HIGH"]},
    "allow_small_sample": {"type": "bool"},
}

DEFAULT_PARAM_VALUES: Dict[str, Dict[str, Any]] = {
    "BASELINE": {"band_low": 0.01, "band_high": 0.03, "stop_mult": 1.8, "trend_threshold": 0.015, "vol_cap": 0.10, "rr_min": 0.8, "min_confidence": "MEDIUM", "allow_small_sample": True},
    "CHAN": {"band_low": 0.008, "band_high": 0.025, "stop_mult": 1.6, "trend_threshold": 0.012, "vol_cap": 0.12, "rr_min": 0.75, "min_confidence": "MEDIUM", "allow_small_sample": True},
    "ATR_WAVE": {"band_low": 0.012, "band_high": 0.035, "stop_mult": 2.2, "trend_threshold": 0.018, "vol_cap": 0.14, "rr_min": 0.9, "min_confidence": "MEDIUM", "allow_small_sample": True},
}


def _now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _norm_strategy(strategy_id: str) -> str:
    return (strategy_id or "").strip().upper() or "BASELINE"


def _payload_hash(proposal_id: str, payload: Dict[str, Any]) -> str:
    s = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(f"{proposal_id}|{s}".encode("utf-8")).hexdigest()


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _validate_param_value(name: str, value: Any) -> Tuple[bool, str, Any]:
    spec = PARAM_SCHEMA.get(name)
    if not spec:
        return False, f"Unknown param: {name}", value
    typ = spec["type"]
    if typ == "number":
        try:
            num = float(value)
        except Exception:
            return False, f"{name} must be number", value
        if num < float(spec["min"]) or num > float(spec["max"]):
            return False, f"{name} out of range [{spec['min']}, {spec['max']}]", num
        step = float(spec["step"])
        base = float(spec["min"])
        steps = round((num - base) / step)
        if abs((base + steps * step) - num) > 1e-8:
            return False, f"{name} step must be {step}", num
        return True, "", round(num, 6)
    if typ == "enum":
        s = str(value).strip().upper()
        if s not in spec["choices"]:
            return False, f"{name} must be in {spec['choices']}", s
        return True, "", s
    if typ == "bool":
        return True, "", _coerce_bool(value)
    return False, f"Unsupported type for {name}", value


def validate_param_payload(payload: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any]]:
    errors: List[str] = []
    normalized: Dict[str, Any] = {}
    for k in PARAM_SCHEMA.keys():
        if k not in payload:
            errors.append(f"Missing param: {k}")
            continue
        ok, err, v = _validate_param_value(k, payload[k])
        if not ok:
            errors.append(err)
        else:
            normalized[k] = v
    return len(errors) == 0, errors, normalized

class ParamStore:
    def __init__(self, sqlite_path: str) -> None:
        self.sqlite_path = os.path.abspath(sqlite_path)
        parent = os.path.dirname(self.sqlite_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(self.sqlite_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _table_exists(self, table: str) -> bool:
        row = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return bool(row)

    def _column_exists(self, table: str, col: str) -> bool:
        if not self._table_exists(table):
            return False
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r["name"] == col for r in rows)

    def _ensure_column(self, table: str, col: str, ddl: str) -> None:
        if not self._column_exists(table, col):
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")

    def _init_schema(self) -> None:
        self.conn.execute("CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        row = self.conn.execute("SELECT value FROM _meta WHERE key='schema_version'").fetchone()
        current = int(row["value"]) if row else 0

        if current < 1:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_param_set (
                    strategy_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    symbol_scope TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (strategy_id, market, symbol_scope)
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_param_proposal (
                    proposal_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    symbol_scope TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    current_params_json TEXT NOT NULL,
                    proposed_params_json TEXT NOT NULL,
                    score REAL NOT NULL,
                    source_start_date TEXT NOT NULL,
                    source_end_date TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_param_apply_log (
                    apply_log_id TEXT PRIMARY KEY,
                    proposal_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    symbol_scope TEXT NOT NULL,
                    old_params_json TEXT NOT NULL,
                    new_params_json TEXT NOT NULL,
                    operator TEXT NOT NULL,
                    comment TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    changed_count INTEGER NOT NULL,
                    skipped_count INTEGER NOT NULL,
                    warnings_json TEXT NOT NULL,
                    rollback_ref TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_param_draft (
                    draft_id TEXT PRIMARY KEY,
                    proposal_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    market TEXT NOT NULL,
                    symbol_scope TEXT NOT NULL,
                    editor_values_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            current = 1

        if current < 2:
            self._ensure_column("strategy_param_proposal", "validation_json", "TEXT NOT NULL DEFAULT '{}' ")
            self._ensure_column("strategy_param_apply_log", "batch_id", "TEXT")
            self._ensure_column("strategy_param_apply_log", "rollout_scope", "TEXT")
            self._ensure_column("strategy_param_apply_log", "status", "TEXT NOT NULL DEFAULT 'APPLIED'")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_run_event (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    module TEXT NOT NULL,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    proposal_id TEXT,
                    apply_log_id TEXT,
                    error_code TEXT,
                    error_msg TEXT,
                    meta_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            current = 2

        if current < 3:
            self._ensure_column("strategy_param_proposal", "experiment_id", "TEXT")
            self._ensure_column("strategy_param_proposal", "sample_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("strategy_param_proposal", "hit_rate", "REAL NOT NULL DEFAULT 0")
            self._ensure_column("strategy_param_proposal", "mean_ret", "REAL NOT NULL DEFAULT 0")
            self._ensure_column("strategy_param_proposal", "dd_mean", "REAL NOT NULL DEFAULT 0")
            self._ensure_column("strategy_param_apply_log", "experiment_id", "TEXT")
            self._ensure_column("strategy_param_apply_log", "gate_passed", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column("strategy_param_apply_log", "gate_reason", "TEXT NOT NULL DEFAULT ''")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_research_experiment (
                    experiment_id TEXT PRIMARY KEY,
                    experiment_name TEXT NOT NULL,
                    strategy_scope TEXT NOT NULL,
                    market_scope TEXT NOT NULL,
                    source_start_date TEXT NOT NULL,
                    source_end_date TEXT NOT NULL,
                    train_window INTEGER NOT NULL,
                    valid_window INTEGER NOT NULL,
                    walk_forward_splits INTEGER NOT NULL,
                    cost_bps REAL NOT NULL,
                    slippage_bps REAL NOT NULL,
                    baseline_json TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            current = 3

        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_apply_proposal ON strategy_param_apply_log (proposal_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_apply_hash ON strategy_param_apply_log (proposal_id, payload_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_proposal_created ON strategy_param_proposal (created_at DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_proposal_experiment ON strategy_param_proposal (experiment_id, created_at DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_apply_status_created ON strategy_param_apply_log (status, created_at DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_apply_experiment ON strategy_param_apply_log (experiment_id, created_at DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_run_event_created ON strategy_run_event (created_at DESC)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_experiment_created ON strategy_research_experiment (created_at DESC)")
        self.conn.execute("INSERT INTO _meta(key, value) VALUES('schema_version', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(current),))
        self.conn.commit()

    def get_schema_version(self) -> int:
        row = self.conn.execute("SELECT value FROM _meta WHERE key='schema_version'").fetchone()
        return int(row["value"]) if row else 0

    def log_event(self, module: str, action: str, status: str, duration_ms: int, meta: Optional[Dict[str, Any]] = None, run_id: str = "", proposal_id: str = "", apply_log_id: str = "", error_code: str = "", error_msg: str = "") -> str:
        event_id = uuid4().hex[:12]
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO strategy_run_event(event_id, run_id, module, action, status, duration_ms, proposal_id, apply_log_id, error_code, error_msg, meta_json, created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_id,
                    run_id or None,
                    module,
                    action,
                    status,
                    int(duration_ms),
                    proposal_id or None,
                    apply_log_id or None,
                    error_code or None,
                    error_msg or None,
                    json.dumps(meta or {}, ensure_ascii=False, sort_keys=True),
                    _now(),
                ),
            )
        return event_id

    def _load_param_set(self, strategy_id: str, market: str, symbol_scope: str) -> Tuple[Dict[str, Any], int]:
        cur = self.conn.execute(
            "SELECT params_json, version FROM strategy_param_set WHERE strategy_id=? AND market=? AND symbol_scope=?",
            (_norm_strategy(strategy_id), market.upper(), symbol_scope),
        ).fetchone()
        if not cur:
            defaults = DEFAULT_PARAM_VALUES.get(_norm_strategy(strategy_id), DEFAULT_PARAM_VALUES["BASELINE"]).copy()
            ok, _, normalized = validate_param_payload(defaults)
            return (normalized if ok else defaults), 0
        return json.loads(cur["params_json"]), int(cur["version"])

    def _save_param_set(self, strategy_id: str, market: str, symbol_scope: str, payload: Dict[str, Any], version: int) -> None:
        self.conn.execute(
            """
            INSERT INTO strategy_param_set(strategy_id, market, symbol_scope, params_json, version, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(strategy_id, market, symbol_scope) DO UPDATE SET
                params_json=excluded.params_json,
                version=excluded.version,
                updated_at=excluded.updated_at
            """,
            (_norm_strategy(strategy_id), market.upper(), symbol_scope, json.dumps(payload, ensure_ascii=False, sort_keys=True), version, _now()),
        )

    def get_active_param_set(self, strategy_id: str, market: str, symbol_scope: str = "*") -> Dict[str, Any]:
        params, version = self._load_param_set(strategy_id, market, symbol_scope)
        return {"strategy_id": _norm_strategy(strategy_id), "market": market.upper(), "symbol_scope": symbol_scope, "params": params, "version": version}

    # Backward-compatible aliases for legacy call sites.
    def get_param_set(self, strategy_id: str, market: str, symbol_scope: str = "*") -> Dict[str, Any]:
        return self.get_active_param_set(strategy_id=strategy_id, market=market, symbol_scope=symbol_scope)

    def get_params(self, strategy_id: str, market: str, symbol_scope: str = "*") -> Dict[str, Any]:
        return self.get_active_param_set(strategy_id=strategy_id, market=market, symbol_scope=symbol_scope).get("params", {})

    def create_proposals_from_history(self, snapshot_rows: List[Dict[str, Any]], source_start_date: str, source_end_date: str, run_id: str, symbol_scope: str = "*", dry_run: bool = False, walk_forward_splits: int = 3, cost_bps: float = 3.0, slippage_bps: float = 2.0, experiment_id: str = "") -> List[Dict[str, Any]]:
        buckets: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for row in snapshot_rows:
            key = (_norm_strategy(str(row.get("strategy_id", ""))), str(row.get("market", "OTHER")).upper())
            buckets.setdefault(key, []).append(row)

        out: List[Dict[str, Any]] = []
        cost_ret = (float(cost_bps) + float(slippage_bps)) / 10000.0
        for (strategy_id, market), items in sorted(buckets.items()):
            returns = [float(x.get("ret_1d", 0.0) or 0.0) - cost_ret for x in items]
            n = len(returns)
            if n == 0:
                continue
            hit_rate = sum(float(x.get("hit_flag", 0) or 0) for x in items) / n
            mean_ret = sum(returns) / n
            dd_mean = sum(float(x.get("max_drawdown", 0.0) or 0.0) for x in items) / n
            split_n = max(1, n // 3)
            train_returns = returns[:split_n]
            valid_returns = returns[split_n : split_n * 2]
            test_returns = returns[split_n * 2 :]
            train_mean = sum(train_returns) / len(train_returns) if train_returns else 0.0
            valid_mean = sum(valid_returns) / len(valid_returns) if valid_returns else 0.0
            test_mean = sum(test_returns) / len(test_returns) if test_returns else 0.0

            chunk = max(1, n // max(1, int(walk_forward_splits)))
            wf_scores: List[float] = []
            for i in range(0, n, chunk):
                w = returns[i : i + chunk]
                if w:
                    wf_scores.append(sum(w) / len(w))
            stability = 1.0 - min(1.0, (max(wf_scores) - min(wf_scores)) / (abs(mean_ret) + 1e-6)) if wf_scores else 0.0
            score = mean_ret * 100.0 + hit_rate * 10.0 - dd_mean * 50.0 + stability * 2.0
            momentum_benchmark = []
            for i in range(1, len(returns)):
                momentum_benchmark.append(returns[i] if returns[i - 1] >= 0 else -returns[i])
            benchmark_random = [r if (i % 2 == 0) else -r for i, r in enumerate(returns)]
            benchmark_proxy = returns
            benchmark_mean = {
                "index_proxy": (sum(benchmark_proxy) / len(benchmark_proxy)) if benchmark_proxy else 0.0,
                "momentum": (sum(momentum_benchmark) / len(momentum_benchmark)) if momentum_benchmark else 0.0,
                "random": (sum(benchmark_random) / len(benchmark_random)) if benchmark_random else 0.0,
            }
            benchmark_delta = mean_ret - max(benchmark_mean.values()) if benchmark_mean else mean_ret
            regimes = [str(x.get("risk_regime", "") or "") for x in items if str(x.get("risk_regime", "") or "")]
            major_regime = max(set(regimes), key=regimes.count) if regimes else ""
            regime_consistency = (regimes.count(major_regime) / len(regimes)) if regimes else 0.0
            execution_flags = [str(x.get("execution_feasibility", "") or "") for x in items]
            execution_feasibility_ratio = (sum(1 for x in execution_flags if x in {"TRADEABLE", "CAUTION"}) / len(execution_flags)) if execution_flags else 0.0

            current_params, base_version = self._load_param_set(strategy_id, market, symbol_scope)
            proposed = dict(current_params)
            proposed["band_low"] = round(max(0.005, min(0.2, float(proposed.get("band_low", 0.01)) + (0.002 if hit_rate < 0.48 else -0.001))), 3)
            proposed["band_high"] = round(max(0.01, min(0.3, float(proposed.get("band_high", 0.03)) + (0.002 if mean_ret > 0 else -0.002))), 3)
            proposed["stop_mult"] = round(max(1.0, min(3.0, float(proposed.get("stop_mult", 1.8)) + (0.2 if dd_mean > 0.08 else -0.1))), 1)
            proposed["trend_threshold"] = round(max(0.005, min(0.08, float(proposed.get("trend_threshold", 0.015)) + (0.002 if stability < 0.6 else -0.001))), 3)
            proposed["vol_cap"] = round(max(0.02, min(0.3, float(proposed.get("vol_cap", 0.10)) + (0.01 if dd_mean > 0.08 else -0.01))), 2)
            proposed["rr_min"] = round(max(0.3, min(2.0, float(proposed.get("rr_min", 0.8)) + (0.05 if mean_ret < 0 else -0.05))), 2)
            proposed["min_confidence"] = "HIGH" if hit_rate < 0.45 else ("MEDIUM" if hit_rate < 0.60 else "LOW")
            proposed["allow_small_sample"] = True if mean_ret > 0.0 else False
            _, _, proposed = validate_param_payload(proposed)

            proposal_id = uuid4().hex[:12]
            validation = {
                "walk_forward_splits": int(walk_forward_splits),
                "cost_bps": float(cost_bps),
                "slippage_bps": float(slippage_bps),
                "stability": round(stability, 6),
                "train_mean": round(train_mean, 6),
                "valid_mean": round(valid_mean, 6),
                "test_mean": round(test_mean, 6),
                "wf_scores": [round(x, 6) for x in wf_scores],
                "benchmark_mean": {k: round(v, 6) for k, v in benchmark_mean.items()},
                "benchmark_delta": round(benchmark_delta, 6),
                "regime_consistency": round(regime_consistency, 6),
                "execution_feasibility_ratio": round(execution_feasibility_ratio, 6),
            }
            row = {
                "proposal_id": proposal_id,
                "strategy_id": strategy_id,
                "market": market,
                "symbol_scope": symbol_scope,
                "base_version": base_version,
                "current_params": current_params,
                "proposed_params": proposed,
                "score": round(score, 6),
                "source_start_date": source_start_date,
                "source_end_date": source_end_date,
                "run_id": run_id,
                "experiment_id": experiment_id or "",
                "created_at": _now(),
                "sample_count": len(items),
                "hit_rate": round(hit_rate, 6),
                "mean_ret": round(mean_ret, 6),
                "dd_mean": round(dd_mean, 6),
                "validation": validation,
            }
            out.append(row)

        if not dry_run and out:
            with self.conn:
                for row in out:
                    self.conn.execute(
                        """
                        INSERT INTO strategy_param_proposal(
                            proposal_id, strategy_id, market, symbol_scope, base_version,
                            current_params_json, proposed_params_json, score,
                            source_start_date, source_end_date, run_id, created_at, validation_json,
                            experiment_id, sample_count, hit_rate, mean_ret, dd_mean
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            row["proposal_id"], row["strategy_id"], row["market"], row["symbol_scope"], row["base_version"],
                            json.dumps(row["current_params"], ensure_ascii=False, sort_keys=True),
                            json.dumps(row["proposed_params"], ensure_ascii=False, sort_keys=True),
                            row["score"], row["source_start_date"], row["source_end_date"], row["run_id"], row["created_at"],
                            json.dumps(row["validation"], ensure_ascii=False, sort_keys=True),
                            row.get("experiment_id", "") or None,
                            int(row.get("sample_count", 0) or 0),
                            float(row.get("hit_rate", 0.0) or 0.0),
                            float(row.get("mean_ret", 0.0) or 0.0),
                            float(row.get("dd_mean", 0.0) or 0.0),
                        ),
                    )
        return out

    def create_proposals(self, snapshot_rows: List[Dict[str, Any]], source_start_date: str, source_end_date: str, run_id: str, symbol_scope: str = "*", dry_run: bool = False, walk_forward_splits: int = 3, cost_bps: float = 3.0, slippage_bps: float = 2.0, experiment_id: str = "") -> List[Dict[str, Any]]:
        return self.create_proposals_from_history(
            snapshot_rows=snapshot_rows,
            source_start_date=source_start_date,
            source_end_date=source_end_date,
            run_id=run_id,
            symbol_scope=symbol_scope,
            dry_run=dry_run,
            walk_forward_splits=walk_forward_splits,
            cost_bps=cost_bps,
            slippage_bps=slippage_bps,
            experiment_id=experiment_id,
        )

    def get_proposal(self, proposal_id: str) -> Dict[str, Any]:
        row = self.conn.execute("SELECT * FROM strategy_param_proposal WHERE proposal_id=?", (proposal_id,)).fetchone()
        if not row:
            raise RuntimeError(f"proposal not found: {proposal_id}")
        return {
            "proposal_id": row["proposal_id"],
            "strategy_id": row["strategy_id"],
            "market": row["market"],
            "symbol_scope": row["symbol_scope"],
            "base_version": int(row["base_version"]),
            "current_params": json.loads(row["current_params_json"]),
            "proposed_params": json.loads(row["proposed_params_json"]),
            "score": float(row["score"]),
            "source_start_date": row["source_start_date"],
            "source_end_date": row["source_end_date"],
            "run_id": row["run_id"],
            "experiment_id": row["experiment_id"] if "experiment_id" in row.keys() else None,
            "created_at": row["created_at"],
            "sample_count": int(row["sample_count"]) if "sample_count" in row.keys() else 0,
            "hit_rate": float(row["hit_rate"]) if "hit_rate" in row.keys() else 0.0,
            "mean_ret": float(row["mean_ret"]) if "mean_ret" in row.keys() else 0.0,
            "dd_mean": float(row["dd_mean"]) if "dd_mean" in row.keys() else 0.0,
            "validation": json.loads(row["validation_json"] or "{}"),
        }

    def save_draft(self, proposal_id: str, editor_values: Dict[str, Any]) -> Dict[str, Any]:
        proposal = self.get_proposal(proposal_id)
        draft_id = f"{proposal_id}:{proposal['strategy_id']}:{proposal['market']}:{proposal['symbol_scope']}"
        now = _now()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO strategy_param_draft(draft_id, proposal_id, strategy_id, market, symbol_scope, editor_values_json, updated_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(draft_id) DO UPDATE SET editor_values_json=excluded.editor_values_json, updated_at=excluded.updated_at
                """,
                (draft_id, proposal_id, proposal["strategy_id"], proposal["market"], proposal["symbol_scope"], json.dumps(editor_values, ensure_ascii=False, sort_keys=True), now),
            )
        return {"draft_id": draft_id, "updated_at": now}

    def load_draft(self, proposal_id: str) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT editor_values_json, updated_at FROM strategy_param_draft WHERE proposal_id=? ORDER BY updated_at DESC LIMIT 1",
            (proposal_id,),
        ).fetchone()
        if not row:
            return {"editor_values": {}, "updated_at": ""}
        return {"editor_values": json.loads(row["editor_values_json"]), "updated_at": row["updated_at"]}

    def diff(self, proposal_id: str, editor_values: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        proposal = self.get_proposal(proposal_id)
        current_params, current_version = self._load_param_set(proposal["strategy_id"], proposal["market"], proposal["symbol_scope"])
        proposed = proposal["proposed_params"]
        editor = dict(proposed)
        if editor_values:
            editor.update(editor_values)

        rows: List[Dict[str, Any]] = []
        warnings: List[str] = []
        changed_count = 0
        high_risk_count = 0
        for name in PARAM_SCHEMA.keys():
            curr = current_params.get(name)
            rec = proposed.get(name)
            yours = editor.get(name)
            valid, err, norm = _validate_param_value(name, yours)
            if valid:
                yours = norm
            else:
                warnings.append(err)
            delta_pct = 0.0
            if isinstance(curr, (int, float)) and isinstance(yours, (int, float)) and float(curr) != 0.0:
                delta_pct = (float(yours) - float(curr)) / float(curr)
            risk = "LOW"
            if abs(delta_pct) >= 0.2:
                risk = "HIGH"
                high_risk_count += 1
            elif abs(delta_pct) >= 0.1:
                risk = "MEDIUM"
            changed = curr != yours
            if changed:
                changed_count += 1
            rows.append({"param_name": name, "current_value": curr, "recommended_value": rec, "your_value": yours, "delta_pct": round(delta_pct, 6), "risk": risk, "valid": bool(valid), "error": err, "changed": changed, "spec": PARAM_SCHEMA[name]})

        return {
            "proposal_id": proposal_id,
            "strategy_id": proposal["strategy_id"],
            "market": proposal["market"],
            "symbol_scope": proposal["symbol_scope"],
            "base_version": proposal["base_version"],
            "current_version": current_version,
            "rows": rows,
            "changed_count": changed_count,
            "skipped_count": max(0, len(rows) - changed_count),
            "high_risk_count": high_risk_count,
            "warnings": sorted(list(set(warnings))),
            "validation": proposal.get("validation", {}),
        }

    def apply(self, proposal_id: str, editor_values: Optional[Dict[str, Any]] = None, operator: str = "local_user", comment: str = "", expected_version: Optional[int] = None, batch_id: str = "", rollout_scope: str = "full", experiment_id: str = "", gate_passed: bool = True, gate_reason: str = "") -> Dict[str, Any]:
        diff = self.diff(proposal_id, editor_values=editor_values or {})
        if diff["warnings"]:
            raise RuntimeError(f"validation failed: {'; '.join(diff['warnings'])}")

        proposal = self.get_proposal(proposal_id)
        current_params, current_version = self._load_param_set(proposal["strategy_id"], proposal["market"], proposal["symbol_scope"])
        if expected_version is not None and expected_version >= 0 and current_version != expected_version:
            raise RuntimeError(f"version conflict: current={current_version}, expected={expected_version}")

        final_payload = {x["param_name"]: x["your_value"] for x in diff["rows"]}
        ok, errs, normalized = validate_param_payload(final_payload)
        if not ok:
            raise RuntimeError(f"validation failed: {'; '.join(errs)}")
        final_payload = normalized

        ph = _payload_hash(proposal_id, final_payload)
        existing = self.conn.execute(
            "SELECT apply_log_id FROM strategy_param_apply_log WHERE proposal_id=? AND payload_hash=? ORDER BY created_at DESC LIMIT 1",
            (proposal_id, ph),
        ).fetchone()
        if existing:
            return {"apply_log_id": existing["apply_log_id"], "changed_count": 0, "skipped_count": len(PARAM_SCHEMA), "warnings": ["idempotent replay"], "idempotent": True, "version": current_version, "batch_id": batch_id, "rollout_scope": rollout_scope, "experiment_id": experiment_id or proposal.get("experiment_id"), "gate_passed": bool(gate_passed), "gate_reason": gate_reason}

        changed_count = sum(1 for k in PARAM_SCHEMA.keys() if current_params.get(k) != final_payload.get(k))
        skipped_count = len(PARAM_SCHEMA) - changed_count
        apply_log_id = uuid4().hex[:12]
        new_version = current_version + 1
        with self.conn:
            self._save_param_set(proposal["strategy_id"], proposal["market"], proposal["symbol_scope"], final_payload, new_version)
            self.conn.execute(
                """
                INSERT INTO strategy_param_apply_log(
                    apply_log_id, proposal_id, strategy_id, market, symbol_scope,
                    old_params_json, new_params_json, operator, comment, payload_hash,
                    changed_count, skipped_count, warnings_json, rollback_ref, created_at,
                    batch_id, rollout_scope, status, experiment_id, gate_passed, gate_reason
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    apply_log_id,
                    proposal_id,
                    proposal["strategy_id"],
                    proposal["market"],
                    proposal["symbol_scope"],
                    json.dumps(current_params, ensure_ascii=False, sort_keys=True),
                    json.dumps(final_payload, ensure_ascii=False, sort_keys=True),
                    operator or "local_user",
                    comment or "",
                    ph,
                    changed_count,
                    skipped_count,
                    json.dumps([], ensure_ascii=False),
                    None,
                    _now(),
                    batch_id or None,
                    rollout_scope,
                    "APPLIED",
                    experiment_id or proposal.get("experiment_id") or None,
                    1 if gate_passed else 0,
                    gate_reason or "",
                ),
            )
        return {"apply_log_id": apply_log_id, "changed_count": changed_count, "skipped_count": skipped_count, "warnings": [], "idempotent": False, "version": new_version, "batch_id": batch_id, "rollout_scope": rollout_scope, "experiment_id": experiment_id or proposal.get("experiment_id"), "gate_passed": bool(gate_passed), "gate_reason": gate_reason}

    def apply_proposal(self, proposal_id: str, editor_values: Optional[Dict[str, Any]] = None, operator: str = "local_user", comment: str = "", expected_version: Optional[int] = None, batch_id: str = "", rollout_scope: str = "", gray_scope: str = "", experiment_id: str = "", gate_passed: bool = True, gate_reason: str = "") -> Dict[str, Any]:
        effective_rollout_scope = (rollout_scope or gray_scope or "full").strip() or "full"
        return self.apply(
            proposal_id=proposal_id,
            editor_values=editor_values,
            operator=operator,
            comment=comment,
            expected_version=expected_version,
            batch_id=batch_id,
            rollout_scope=effective_rollout_scope,
            experiment_id=experiment_id,
            gate_passed=gate_passed,
            gate_reason=gate_reason,
        )

    def rollback(self, apply_log_id: str, operator: str = "local_user", comment: str = "") -> Dict[str, Any]:
        row = self.conn.execute("SELECT * FROM strategy_param_apply_log WHERE apply_log_id=?", (apply_log_id,)).fetchone()
        if not row:
            raise RuntimeError(f"apply log not found: {apply_log_id}")
        strategy_id = row["strategy_id"]
        market = row["market"]
        symbol_scope = row["symbol_scope"]
        old_payload = json.loads(row["old_params_json"])
        current_payload, current_version = self._load_param_set(strategy_id, market, symbol_scope)
        changed_count = sum(1 for k in PARAM_SCHEMA.keys() if current_payload.get(k) != old_payload.get(k))

        rollback_apply_id = uuid4().hex[:12]
        new_version = current_version + 1
        with self.conn:
            self._save_param_set(strategy_id, market, symbol_scope, old_payload, new_version)
            self.conn.execute(
                """
                INSERT INTO strategy_param_apply_log(
                    apply_log_id, proposal_id, strategy_id, market, symbol_scope,
                    old_params_json, new_params_json, operator, comment, payload_hash,
                    changed_count, skipped_count, warnings_json, rollback_ref, created_at,
                    batch_id, rollout_scope, status, experiment_id, gate_passed, gate_reason
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    rollback_apply_id,
                    row["proposal_id"],
                    strategy_id,
                    market,
                    symbol_scope,
                    json.dumps(current_payload, ensure_ascii=False, sort_keys=True),
                    json.dumps(old_payload, ensure_ascii=False, sort_keys=True),
                    operator or "local_user",
                    comment or "",
                    _payload_hash(row["proposal_id"], old_payload),
                    changed_count,
                    len(PARAM_SCHEMA) - changed_count,
                    json.dumps([], ensure_ascii=False),
                    apply_log_id,
                    _now(),
                    row["batch_id"],
                    row["rollout_scope"],
                    "ROLLED_BACK",
                    row["experiment_id"] if "experiment_id" in row.keys() else None,
                    1,
                    f"rollback from {apply_log_id}",
                ),
            )
        return {"apply_log_id": rollback_apply_id, "rollback_ref": apply_log_id, "changed_count": changed_count, "skipped_count": len(PARAM_SCHEMA) - changed_count, "warnings": [], "version": new_version}

    def rollback_apply(self, apply_log_id: str, operator: str = "local_user", comment: str = "") -> Dict[str, Any]:
        return self.rollback(apply_log_id=apply_log_id, operator=operator, comment=comment)

    def create_experiment(self, source_start_date: str, source_end_date: str, strategy_scope: str, market_scope: str, walk_forward_splits: int, cost_bps: float, slippage_bps: float, train_window: int, valid_window: int, experiment_name: str = "", baseline: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        experiment_id = uuid4().hex[:12]
        now = _now()
        row = {
            "experiment_id": experiment_id,
            "experiment_name": (experiment_name or f"exp_{source_start_date}_{source_end_date}")[:120],
            "strategy_scope": strategy_scope or "*",
            "market_scope": market_scope or "*",
            "source_start_date": source_start_date,
            "source_end_date": source_end_date,
            "train_window": max(1, int(train_window)),
            "valid_window": max(1, int(valid_window)),
            "walk_forward_splits": max(1, int(walk_forward_splits)),
            "cost_bps": float(cost_bps),
            "slippage_bps": float(slippage_bps),
            "baseline": baseline or {},
            "report": {},
            "status": "CREATED",
            "created_at": now,
            "updated_at": now,
        }
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO strategy_research_experiment(
                    experiment_id, experiment_name, strategy_scope, market_scope,
                    source_start_date, source_end_date, train_window, valid_window,
                    walk_forward_splits, cost_bps, slippage_bps, baseline_json, report_json,
                    status, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["experiment_id"],
                    row["experiment_name"],
                    row["strategy_scope"],
                    row["market_scope"],
                    row["source_start_date"],
                    row["source_end_date"],
                    row["train_window"],
                    row["valid_window"],
                    row["walk_forward_splits"],
                    row["cost_bps"],
                    row["slippage_bps"],
                    json.dumps(row["baseline"], ensure_ascii=False, sort_keys=True),
                    json.dumps(row["report"], ensure_ascii=False, sort_keys=True),
                    row["status"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        return row

    def update_experiment_report(self, experiment_id: str, report: Dict[str, Any], status: str = "READY") -> Dict[str, Any]:
        now = _now()
        with self.conn:
            self.conn.execute(
                "UPDATE strategy_research_experiment SET report_json=?, status=?, updated_at=? WHERE experiment_id=?",
                (json.dumps(report or {}, ensure_ascii=False, sort_keys=True), status, now, experiment_id),
            )
        return self.get_experiment(experiment_id)

    def get_experiment(self, experiment_id: str) -> Dict[str, Any]:
        row = self.conn.execute("SELECT * FROM strategy_research_experiment WHERE experiment_id=?", (experiment_id,)).fetchone()
        if not row:
            raise RuntimeError(f"experiment not found: {experiment_id}")
        return {
            "experiment_id": row["experiment_id"],
            "experiment_name": row["experiment_name"],
            "strategy_scope": row["strategy_scope"],
            "market_scope": row["market_scope"],
            "source_start_date": row["source_start_date"],
            "source_end_date": row["source_end_date"],
            "train_window": int(row["train_window"]),
            "valid_window": int(row["valid_window"]),
            "walk_forward_splits": int(row["walk_forward_splits"]),
            "cost_bps": float(row["cost_bps"]),
            "slippage_bps": float(row["slippage_bps"]),
            "baseline": json.loads(row["baseline_json"] or "{}"),
            "report": json.loads(row["report_json"] or "{}"),
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_experiments(self, limit: int = 50) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            "SELECT experiment_id, experiment_name, strategy_scope, market_scope, source_start_date, source_end_date, status, updated_at FROM strategy_research_experiment ORDER BY updated_at DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(x) for x in cur]

    def list_recent_applies(self, days: int = 7) -> List[Dict[str, Any]]:
        cutoff = (dt.datetime.now() - dt.timedelta(days=max(1, int(days)))).isoformat(timespec="seconds")
        rows = self.conn.execute(
            """
            SELECT * FROM strategy_param_apply_log
            WHERE created_at >= ? AND status='APPLIED'
              AND apply_log_id NOT IN (
                  SELECT rollback_ref FROM strategy_param_apply_log
                  WHERE rollback_ref IS NOT NULL AND rollback_ref <> ''
              )
            ORDER BY created_at DESC
            """,
            (cutoff,),
        ).fetchall()
        return [dict(x) for x in rows]

    def get_monitor(self, days: int = 7) -> Dict[str, Any]:
        days = max(1, int(days))
        cutoff = (dt.datetime.now() - dt.timedelta(days=days)).isoformat(timespec="seconds")
        ev = self.conn.execute(
            "SELECT status, COUNT(*) AS cnt, AVG(duration_ms) AS avg_ms FROM strategy_run_event WHERE created_at >= ? GROUP BY status",
            (cutoff,),
        ).fetchall()
        total = sum(int(r["cnt"]) for r in ev)
        success = sum(int(r["cnt"]) for r in ev if r["status"] == "SUCCESS")
        failed = sum(int(r["cnt"]) for r in ev if r["status"] == "FAILED")
        avg_ms = 0.0
        if total > 0:
            avg_ms = sum((float(r["avg_ms"] or 0.0) * int(r["cnt"])) for r in ev) / total

        recent_failures = self.conn.execute(
            "SELECT module, action, error_code, error_msg, created_at FROM strategy_run_event WHERE created_at >= ? AND status='FAILED' ORDER BY created_at DESC LIMIT 20",
            (cutoff,),
        ).fetchall()
        apply_rows = self.conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM strategy_param_apply_log WHERE created_at >= ? GROUP BY status",
            (cutoff,),
        ).fetchall()
        apply_stat = {r["status"]: int(r["cnt"]) for r in apply_rows}
        apply_total = sum(apply_stat.values())
        rollback_count = int(apply_stat.get("ROLLED_BACK", 0))
        apply_count = int(apply_stat.get("APPLIED", 0))
        rollback_rate = (float(rollback_count) / float(apply_total)) if apply_total else 0.0
        conflict_row = self.conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM strategy_run_event
            WHERE created_at >= ? AND module='param' AND action='param_apply'
              AND status='FAILED' AND (error_msg LIKE '%version conflict%' OR error_code='PARAM_APPLY_CONFLICT')
            """,
            (cutoff,),
        ).fetchone()
        conflict_count = int(conflict_row["cnt"]) if conflict_row else 0
        conflict_rate = (float(conflict_count) / float(apply_total)) if apply_total else 0.0
        fail_dist_rows = self.conn.execute(
            """
            SELECT COALESCE(error_code, 'UNKNOWN') AS error_code, COUNT(*) AS cnt
            FROM strategy_run_event
            WHERE created_at >= ? AND status='FAILED'
            GROUP BY COALESCE(error_code, 'UNKNOWN')
            ORDER BY cnt DESC
            """,
            (cutoff,),
        ).fetchall()
        slow_rows = self.conn.execute(
            """
            SELECT module, action, status, duration_ms, created_at
            FROM strategy_run_event
            WHERE created_at >= ?
            ORDER BY duration_ms DESC, created_at DESC
            LIMIT 20
            """,
            (cutoff,),
        ).fetchall()
        return {
            "days": days,
            "schema_version": self.get_schema_version(),
            "event_total": total,
            "event_success": success,
            "event_failed": failed,
            "success_rate": (float(success) / float(total)) if total > 0 else 0.0,
            "avg_duration_ms": round(avg_ms, 2),
            "apply_stat": apply_stat,
            "apply_count": apply_count,
            "rollback_count": rollback_count,
            "rollback_rate": rollback_rate,
            "conflict_count": conflict_count,
            "conflict_rate": conflict_rate,
            "failure_distribution": [dict(x) for x in fail_dist_rows],
            "slow_tasks": [dict(x) for x in slow_rows],
            "recent_experiments": self.list_experiments(limit=10),
            "recent_failures": [dict(x) for x in recent_failures],
        }

    def monitor(self, days: int = 7) -> Dict[str, Any]:
        return self.get_monitor(days=days)

    def log_run_event(self, module: str, action: str, status: str, duration_ms: int, meta: Optional[Dict[str, Any]] = None, run_id: str = "", proposal_id: str = "", apply_log_id: str = "", error_code: str = "", error_msg: str = "") -> str:
        return self.log_event(
            module=module,
            action=action,
            status=status,
            duration_ms=duration_ms,
            meta=meta,
            run_id=run_id,
            proposal_id=proposal_id,
            apply_log_id=apply_log_id,
            error_code=error_code,
            error_msg=error_msg,
        )
