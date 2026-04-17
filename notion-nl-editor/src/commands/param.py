import argparse
import datetime as dt
import json
import os
import sqlite3
from typing import Any, Dict
from uuid import uuid4

from core.runtime import load_json_arg, now_ms, split_csv, sqlite_path, today_or
from param_store import ParamStore, SCHEMA_VERSION
from services.research.experiments import build_experiment_baseline, build_experiment_protocol, build_research_report
from services.risk.gates import PARAM_APPLY_GATE_BLOCKED, GateThreshold, evaluate_release_gate
from services.risk.guard import find_breach_actions
from stores.snapshot_store import SnapshotStore


def _log_param_event(
    action: str,
    status: str,
    started_ms: float,
    meta: Dict[str, Any],
    run_id: str = "",
    proposal_id: str = "",
    apply_log_id: str = "",
    error_code: str = "",
    error_msg: str = "",
) -> None:
    store = ParamStore(sqlite_path())
    try:
        duration = int(max(0.0, now_ms() - started_ms))
        store.log_event(
            module="param",
            action=action,
            status=status,
            duration_ms=duration,
            meta=meta,
            run_id=run_id,
            proposal_id=proposal_id,
            apply_log_id=apply_log_id,
            error_code=error_code,
            error_msg=error_msg,
        )
    finally:
        store.close()


def _gate_decision(proposal: Dict[str, Any], args: argparse.Namespace, expected_experiment_id: str = "") -> tuple[bool, str, Dict[str, Any]]:
    threshold = GateThreshold(
        min_stability=float(getattr(args, "gate_min_stability", 0.0) or 0.0),
        min_hit_rate=float(getattr(args, "gate_min_hit_rate", 0.0) or 0.0),
        max_dd_mean=float(getattr(args, "gate_max_dd_mean", 1.0) or 1.0),
        require_experiment=bool(getattr(args, "require_experiment", False)),
        min_regime_consistency=float(getattr(args, "gate_min_regime_consistency", os.getenv("GATE_MIN_REGIME_CONSISTENCY", 0.4)) or 0.4),
        min_execution_feasibility=float(getattr(args, "gate_min_execution_feasibility", os.getenv("GATE_MIN_EXEC_FEASIBILITY", 0.5)) or 0.5),
        min_benchmark_delta=float(getattr(args, "gate_min_benchmark_delta", os.getenv("GATE_MIN_BENCHMARK_DELTA", -0.005)) or -0.005),
    )
    return evaluate_release_gate(proposal=proposal, expected_experiment_id=expected_experiment_id, threshold=threshold)


def param_recommend(args: argparse.Namespace) -> int:
    started = now_ms()
    start_date = today_or(getattr(args, "start_date", ""))
    end_date = today_or(getattr(args, "end_date", "")) if getattr(args, "end_date", "") else start_date
    strategies = [x.upper() for x in split_csv(getattr(args, "strategies", ""))]
    markets = [x.upper() for x in split_csv(getattr(args, "markets", ""))]

    try:
        snap = SnapshotStore(sqlite_path())
        try:
            rows = snap.query_range(start_date, end_date, strategies=strategies or None, markets=markets or None)
        finally:
            snap.close()

        run_id = uuid4().hex[:12]
        experiment_id = (getattr(args, "experiment_id", "") or "").strip()
        experiment_name = (getattr(args, "experiment_name", "") or "").strip()
        store = ParamStore(sqlite_path())
        try:
            strategies_scope = ",".join(strategies) if strategies else "*"
            markets_scope = ",".join(markets) if markets else "*"
            if not experiment_id:
                exp = store.create_experiment(
                    source_start_date=start_date,
                    source_end_date=end_date,
                    strategy_scope=strategies_scope,
                    market_scope=markets_scope,
                    walk_forward_splits=int(getattr(args, "walk_forward_splits", 3)),
                    cost_bps=float(getattr(args, "cost_bps", 3.0)),
                    slippage_bps=float(getattr(args, "slippage_bps", 2.0)),
                    train_window=int(getattr(args, "train_window", 60)),
                    valid_window=int(getattr(args, "valid_window", 20)),
                    experiment_name=experiment_name,
                    baseline=build_experiment_baseline(rows),
                )
                experiment_id = str(exp.get("experiment_id", ""))
            else:
                _ = store.get_experiment(experiment_id)

            out_rows = store.create_proposals_from_history(
                snapshot_rows=rows,
                source_start_date=start_date,
                source_end_date=end_date,
                run_id=run_id,
                dry_run=bool(getattr(args, "dry_run", False)),
                walk_forward_splits=int(getattr(args, "walk_forward_splits", 3)),
                cost_bps=float(getattr(args, "cost_bps", 3.0)),
                slippage_bps=float(getattr(args, "slippage_bps", 2.0)),
                experiment_id=experiment_id,
            )
            protocol = build_experiment_protocol(
                train_window=int(getattr(args, "train_window", 60)),
                valid_window=int(getattr(args, "valid_window", 20)),
                test_window=int(getattr(args, "valid_window", 20)),
            )
            research_report = build_research_report(rows=rows, proposal_rows=out_rows, protocol=protocol)
            store.update_experiment_report(
                experiment_id=experiment_id,
                report={
                    "run_id": run_id,
                    "proposal_count": len(out_rows),
                    "source_start_date": start_date,
                    "source_end_date": end_date,
                    "dry_run": bool(getattr(args, "dry_run", False)),
                    "strategies": strategies,
                    "markets": markets,
                    "research": research_report,
                },
                status="READY",
            )
        finally:
            store.close()

        print(
            json.dumps(
                {
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                    "source_start_date": start_date,
                    "source_end_date": end_date,
                    "proposal_count": len(out_rows),
                    "dry_run": bool(getattr(args, "dry_run", False)),
                    "proposals": out_rows,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        _log_param_event(
            action="param_recommend",
            status="SUCCESS",
            started_ms=started,
            meta={"proposal_count": len(out_rows), "dry_run": bool(getattr(args, "dry_run", False)), "experiment_id": experiment_id},
            run_id=run_id,
        )
        return 0
    except Exception as exc:
        _log_param_event(
            action="param_recommend",
            status="FAILED",
            started_ms=started,
            meta={"dry_run": bool(getattr(args, "dry_run", False))},
            error_code="PARAM_RECOMMEND_FAILED",
            error_msg=str(exc),
        )
        raise


def param_diff(args: argparse.Namespace) -> int:
    started = now_ms()
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = load_json_arg(getattr(args, "editor_json", ""))
    try:
        store = ParamStore(sqlite_path())
        try:
            out = store.diff(proposal_id, editor_values=editor_values or None)
        finally:
            store.close()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        _log_param_event(
            action="param_diff",
            status="SUCCESS",
            started_ms=started,
            meta={"changed_count": int(out.get("changed_count", 0))},
            proposal_id=proposal_id,
        )
        return 0
    except Exception as exc:
        _log_param_event(
            action="param_diff",
            status="FAILED",
            started_ms=started,
            meta={},
            proposal_id=proposal_id,
            error_code="PARAM_DIFF_FAILED",
            error_msg=str(exc),
        )
        raise


def param_apply(args: argparse.Namespace) -> int:
    started = now_ms()
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = load_json_arg(getattr(args, "editor_json", ""))
    operator = (getattr(args, "operator", "") or os.getenv("OPERATOR", "local_user")).strip() or "local_user"
    comment = getattr(args, "comment", "") or ""
    expected_version = int(getattr(args, "expected_version", -1))
    batch_id = (getattr(args, "batch_id", "") or "").strip()
    rollout_scope = (getattr(args, "rollout_scope", "") or getattr(args, "gray_scope", "") or "full").strip() or "full"
    experiment_id = (getattr(args, "experiment_id", "") or "").strip()

    try:
        store = ParamStore(sqlite_path())
        try:
            proposal = store.get_proposal(proposal_id)
            gate_passed, gate_reason, gate_payload = _gate_decision(proposal, args=args, expected_experiment_id=experiment_id)
            if not gate_passed:
                raise RuntimeError(f"release gate blocked: {gate_reason}")
            effective_experiment_id = experiment_id or str(proposal.get("experiment_id", "") or "")
            out = store.apply(
                proposal_id=proposal_id,
                editor_values=editor_values or None,
                operator=operator,
                comment=comment,
                expected_version=expected_version if expected_version >= 0 else None,
                batch_id=batch_id,
                rollout_scope=rollout_scope,
                experiment_id=effective_experiment_id,
                gate_passed=gate_passed,
                gate_reason=gate_reason,
            )
        finally:
            store.close()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        _log_param_event(
            action="param_apply",
            status="SUCCESS",
            started_ms=started,
            meta={
                "changed_count": int(out.get("changed_count", 0)),
                "batch_id": batch_id,
                "rollout_scope": rollout_scope,
                "experiment_id": str(out.get("experiment_id", "")),
                "gate": gate_payload,
            },
            proposal_id=proposal_id,
            apply_log_id=str(out.get("apply_log_id", "")),
        )
        return 0
    except Exception as exc:
        err_text = str(exc)
        err_lower = err_text.lower()
        if "release gate blocked" in err_lower:
            err_code = PARAM_APPLY_GATE_BLOCKED
        elif "version conflict" in err_lower:
            err_code = "PARAM_APPLY_CONFLICT"
        else:
            err_code = "PARAM_APPLY_FAILED"
        _log_param_event(
            action="param_apply",
            status="FAILED",
            started_ms=started,
            meta={"batch_id": batch_id, "rollout_scope": rollout_scope, "experiment_id": experiment_id},
            proposal_id=proposal_id,
            error_code=err_code,
            error_msg=err_text,
        )
        raise


def param_rollback(args: argparse.Namespace) -> int:
    started = now_ms()
    apply_log_id = (getattr(args, "apply_log_id", "") or "").strip()
    if not apply_log_id:
        raise RuntimeError("apply-log-id is required")
    operator = (getattr(args, "operator", "") or os.getenv("OPERATOR", "local_user")).strip() or "local_user"
    comment = getattr(args, "comment", "") or ""
    try:
        store = ParamStore(sqlite_path())
        try:
            out = store.rollback(apply_log_id=apply_log_id, operator=operator, comment=comment)
        finally:
            store.close()
        print(json.dumps(out, ensure_ascii=False, indent=2))
        _log_param_event(
            action="param_rollback",
            status="SUCCESS",
            started_ms=started,
            meta={"rollback_ref": apply_log_id},
            apply_log_id=str(out.get("apply_log_id", "")),
        )
        return 0
    except Exception as exc:
        _log_param_event(
            action="param_rollback",
            status="FAILED",
            started_ms=started,
            meta={"rollback_ref": apply_log_id},
            error_code="PARAM_ROLLBACK_FAILED",
            error_msg=str(exc),
        )
        raise


def param_draft_save(args: argparse.Namespace) -> int:
    started = now_ms()
    proposal_id = (getattr(args, "proposal_id", "") or "").strip()
    if not proposal_id:
        raise RuntimeError("proposal-id is required")
    editor_values = load_json_arg(getattr(args, "editor_json", ""))
    store = ParamStore(sqlite_path())
    try:
        out = store.save_draft(proposal_id=proposal_id, editor_values=editor_values)
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    _log_param_event(
        action="param_draft_save",
        status="SUCCESS",
        started_ms=started,
        meta={},
        proposal_id=proposal_id,
    )
    return 0


def param_monitor(args: argparse.Namespace) -> int:
    days = int(getattr(args, "days", 7) or 7)
    store = ParamStore(sqlite_path())
    try:
        out = store.get_monitor(days=days)
    finally:
        store.close()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def param_migrate(args: argparse.Namespace) -> int:
    db_path = sqlite_path()
    before = 0
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute("SELECT value FROM _meta WHERE key='schema_version'").fetchone()
            if row and row[0] is not None:
                before = int(row[0])
        except Exception:
            before = 0
        finally:
            conn.close()
    store = ParamStore(db_path)
    try:
        after = store.get_schema_version()
        required_tables = [
            "_meta",
            "strategy_param_set",
            "strategy_param_proposal",
            "strategy_param_apply_log",
            "strategy_param_draft",
            "strategy_run_event",
            "strategy_research_experiment",
        ]
        exists: Dict[str, bool] = {}
        for table_name in required_tables:
            row = store.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()
            exists[table_name] = bool(row)
    finally:
        store.close()
    print(
        json.dumps(
            {
                "sqlite_path": db_path,
                "schema_version_before": before,
                "schema_version_after": after,
                "schema_version_target": SCHEMA_VERSION,
                "tables": exists,
                "ok": after >= SCHEMA_VERSION and all(bool(v) for v in exists.values()),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def param_risk_guard(args: argparse.Namespace) -> int:
    started = now_ms()
    days = max(1, int(getattr(args, "days", 7) or 7))
    apply_lookback_days = max(days, int(getattr(args, "apply_lookback_days", 30) or 30))
    min_hit_rate = float(getattr(args, "min_hit_rate", 0.45) or 0.45)
    max_drawdown_curve = float(getattr(args, "max_drawdown_curve", 0.2) or 0.2)
    dry_run = bool(getattr(args, "dry_run", False))

    end_date = dt.date.today().isoformat()
    start_date = (dt.date.today() - dt.timedelta(days=days - 1)).isoformat()
    snap = SnapshotStore(sqlite_path())
    try:
        rows = snap.query_range(start_date=start_date, end_date=end_date)
    finally:
        snap.close()

    store = ParamStore(sqlite_path())
    actions: list[Dict[str, Any]] = []
    try:
        candidates = store.list_recent_applies(days=apply_lookback_days)
        breach_rows = find_breach_actions(
            rows=rows,
            applied_rows=candidates,
            min_hit_rate=min_hit_rate,
            max_drawdown_curve=max_drawdown_curve,
        )
        for breach in breach_rows:
            apply_log_id = str(breach.get("apply_log_id", ""))
            action = dict(breach)
            action["dry_run"] = dry_run
            action["rolled_back"] = False
            if not dry_run and apply_log_id:
                rollback_out = store.rollback(
                    apply_log_id=apply_log_id,
                    operator="risk_guard",
                    comment="auto rollback by param-risk-guard",
                )
                action["rolled_back"] = True
                action["rollback_apply_log_id"] = rollback_out.get("apply_log_id")
            actions.append(action)
    finally:
        store.close()

    out = {
        "window_days": days,
        "apply_lookback_days": apply_lookback_days,
        "min_hit_rate": min_hit_rate,
        "max_drawdown_curve": max_drawdown_curve,
        "dry_run": dry_run,
        "candidate_count": len(actions),
        "actions": actions,
    }
    _log_param_event(
        action="param_risk_guard",
        status="SUCCESS",
        started_ms=started,
        meta={"candidate_count": len(actions), "dry_run": dry_run, "days": days},
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


__all__ = [
    "param_apply",
    "param_diff",
    "param_draft_save",
    "param_migrate",
    "param_monitor",
    "param_recommend",
    "param_risk_guard",
    "param_rollback",
]
