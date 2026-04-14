import datetime as dt
import hashlib
import json
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4


PARAM_SCHEMA: Dict[str, Dict[str, Any]] = {
    "band_low": {"type": "number", "min": 0.005, "max": 0.2, "step": 0.001},
    "band_high": {"type": "number", "min": 0.01, "max": 0.3, "step": 0.001},
    "stop_mult": {"type": "number", "min": 1.0, "max": 3.0, "step": 0.1},
    "min_confidence": {"type": "enum", "choices": ["LOW", "MEDIUM", "HIGH"]},
    "allow_small_sample": {"type": "bool"},
}


DEFAULT_PARAM_VALUES: Dict[str, Dict[str, Any]] = {
    "BASELINE": {"band_low": 0.01, "band_high": 0.03, "stop_mult": 1.8, "min_confidence": "MEDIUM", "allow_small_sample": True},
    "CHAN": {"band_low": 0.008, "band_high": 0.025, "stop_mult": 1.6, "min_confidence": "MEDIUM", "allow_small_sample": True},
    "ATR_WAVE": {"band_low": 0.012, "band_high": 0.035, "stop_mult": 2.2, "min_confidence": "MEDIUM", "allow_small_sample": True},
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
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


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

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_param_set (
                strategy_id TEXT NOT NULL,
                market TEXT NOT NULL,
                symbol_scope TEXT NOT NULL,
                params_json TEXT NOT NULL,
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (strategy_id, market, symbol_scope)
            )
            """
        )
        self.conn.execute(
            """
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
            """
        )
        self.conn.execute(
            """
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
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_param_draft (
                draft_id TEXT PRIMARY KEY,
                proposal_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                market TEXT NOT NULL,
                symbol_scope TEXT NOT NULL,
                editor_values_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_apply_proposal ON strategy_param_apply_log (proposal_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_apply_hash ON strategy_param_apply_log (proposal_id, payload_hash)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_param_proposal_created ON strategy_param_proposal (created_at DESC)")
        self.conn.commit()

    def _load_param_set(self, strategy_id: str, market: str, symbol_scope: str) -> Tuple[Dict[str, Any], int]:
        cur = self.conn.execute(
            """
            SELECT params_json, version FROM strategy_param_set
            WHERE strategy_id=? AND market=? AND symbol_scope=?
            """,
            (_norm_strategy(strategy_id), market.upper(), symbol_scope),
        ).fetchone()
        if not cur:
            defaults = DEFAULT_PARAM_VALUES.get(_norm_strategy(strategy_id), DEFAULT_PARAM_VALUES["BASELINE"]).copy()
            return defaults, 0
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

    def create_proposals_from_history(
        self,
        snapshot_rows: List[Dict[str, Any]],
        source_start_date: str,
        source_end_date: str,
        run_id: str,
        symbol_scope: str = "*",
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        buckets: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for row in snapshot_rows:
            key = (_norm_strategy(str(row.get("strategy_id", ""))), str(row.get("market", "OTHER")).upper())
            buckets.setdefault(key, []).append(row)

        out: List[Dict[str, Any]] = []
        for (strategy_id, market), items in sorted(buckets.items()):
            returns = [float(x.get("ret_1d", 0.0) or 0.0) for x in items]
            hit_rate = (sum(float(x.get("hit_flag", 0) or 0) for x in items) / len(items)) if items else 0.0
            mean_ret = (sum(returns) / len(returns)) if returns else 0.0
            dd_mean = (sum(float(x.get("max_drawdown", 0.0) or 0.0) for x in items) / len(items)) if items else 0.0
            score = mean_ret * 100.0 + hit_rate * 10.0 - dd_mean * 50.0

            current_params, base_version = self._load_param_set(strategy_id, market, symbol_scope)
            proposed = dict(current_params)
            proposed["band_low"] = round(max(0.005, min(0.2, float(proposed.get("band_low", 0.01)) + (0.002 if hit_rate < 0.48 else -0.001))), 3)
            proposed["band_high"] = round(max(0.01, min(0.3, float(proposed.get("band_high", 0.03)) + (0.002 if mean_ret > 0 else -0.002))), 3)
            proposed["stop_mult"] = round(max(1.0, min(3.0, float(proposed.get("stop_mult", 1.8)) + (0.2 if dd_mean > 0.08 else -0.1))), 1)
            proposed["min_confidence"] = "HIGH" if hit_rate < 0.45 else ("MEDIUM" if hit_rate < 0.60 else "LOW")
            proposed["allow_small_sample"] = True if mean_ret > 0.0 else False
            _, _, proposed = validate_param_payload(proposed)

            proposal_id = uuid4().hex[:12]
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
                "created_at": _now(),
                "sample_count": len(items),
                "hit_rate": round(hit_rate, 6),
                "mean_ret": round(mean_ret, 6),
                "dd_mean": round(dd_mean, 6),
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
                            source_start_date, source_end_date, run_id, created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            row["proposal_id"],
                            row["strategy_id"],
                            row["market"],
                            row["symbol_scope"],
                            row["base_version"],
                            json.dumps(row["current_params"], ensure_ascii=False, sort_keys=True),
                            json.dumps(row["proposed_params"], ensure_ascii=False, sort_keys=True),
                            row["score"],
                            row["source_start_date"],
                            row["source_end_date"],
                            row["run_id"],
                            row["created_at"],
                        ),
                    )
        return out

    def get_proposal(self, proposal_id: str) -> Dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT * FROM strategy_param_proposal WHERE proposal_id=?
            """,
            (proposal_id,),
        ).fetchone()
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
            "created_at": row["created_at"],
        }

    def save_draft(self, proposal_id: str, editor_values: Dict[str, Any]) -> Dict[str, Any]:
        proposal = self.get_proposal(proposal_id)
        draft_id = f"{proposal_id}:{proposal['strategy_id']}:{proposal['market']}:{proposal['symbol_scope']}"
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO strategy_param_draft(draft_id, proposal_id, strategy_id, market, symbol_scope, editor_values_json, updated_at)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(draft_id) DO UPDATE SET
                    editor_values_json=excluded.editor_values_json,
                    updated_at=excluded.updated_at
                """,
                (
                    draft_id,
                    proposal_id,
                    proposal["strategy_id"],
                    proposal["market"],
                    proposal["symbol_scope"],
                    json.dumps(editor_values, ensure_ascii=False, sort_keys=True),
                    _now(),
                ),
            )
        return {"draft_id": draft_id, "updated_at": _now()}

    def load_draft(self, proposal_id: str) -> Dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT editor_values_json, updated_at FROM strategy_param_draft
            WHERE proposal_id=?
            ORDER BY updated_at DESC LIMIT 1
            """,
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
            rows.append(
                {
                    "param_name": name,
                    "current_value": curr,
                    "recommended_value": rec,
                    "your_value": yours,
                    "delta_pct": round(delta_pct, 6),
                    "risk": risk,
                    "valid": bool(valid),
                    "error": err,
                    "changed": changed,
                    "spec": PARAM_SCHEMA[name],
                }
            )

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
        }

    def apply(
        self,
        proposal_id: str,
        editor_values: Optional[Dict[str, Any]] = None,
        operator: str = "local_user",
        comment: str = "",
        expected_version: Optional[int] = None,
    ) -> Dict[str, Any]:
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
            return {
                "apply_log_id": existing["apply_log_id"],
                "changed_count": 0,
                "skipped_count": len(PARAM_SCHEMA),
                "warnings": ["idempotent replay"],
                "idempotent": True,
                "version": current_version,
            }

        changed_count = sum(1 for k in PARAM_SCHEMA.keys() if current_params.get(k) != final_payload.get(k))
        skipped_count = len(PARAM_SCHEMA) - changed_count
        warnings: List[str] = []

        apply_log_id = uuid4().hex[:12]
        new_version = current_version + 1
        with self.conn:
            self._save_param_set(proposal["strategy_id"], proposal["market"], proposal["symbol_scope"], final_payload, new_version)
            self.conn.execute(
                """
                INSERT INTO strategy_param_apply_log(
                    apply_log_id, proposal_id, strategy_id, market, symbol_scope,
                    old_params_json, new_params_json, operator, comment, payload_hash,
                    changed_count, skipped_count, warnings_json, rollback_ref, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                    json.dumps(warnings, ensure_ascii=False),
                    None,
                    _now(),
                ),
            )

        return {
            "apply_log_id": apply_log_id,
            "changed_count": changed_count,
            "skipped_count": skipped_count,
            "warnings": warnings,
            "idempotent": False,
            "version": new_version,
        }

    def rollback(self, apply_log_id: str, operator: str = "local_user", comment: str = "") -> Dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT * FROM strategy_param_apply_log WHERE apply_log_id=?
            """,
            (apply_log_id,),
        ).fetchone()
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
                    changed_count, skipped_count, warnings_json, rollback_ref, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                ),
            )

        return {
            "apply_log_id": rollback_apply_id,
            "rollback_ref": apply_log_id,
            "changed_count": changed_count,
            "skipped_count": len(PARAM_SCHEMA) - changed_count,
            "warnings": [],
            "version": new_version,
        }
