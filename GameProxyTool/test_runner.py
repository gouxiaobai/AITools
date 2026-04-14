"""
Suite runner for scripted API replay tests.
"""
import json
import os
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any, Callable, Optional

from store import Packet, PacketStore


def _now() -> float:
    return time.time()


def _get_path(data: Any, path: str) -> tuple[bool, Any]:
    if path == "" or path is None:
        return True, data
    cur = data
    for part in path.split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return False, None
            cur = cur[part]
            continue
        if isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return False, None
            if idx < 0 or idx >= len(cur):
                return False, None
            cur = cur[idx]
            continue
        return False, None
    return True, cur


def _contains(actual: Any, expected: Any) -> bool:
    if isinstance(actual, str):
        return str(expected) in actual
    if isinstance(actual, list):
        return expected in actual
    if isinstance(actual, dict) and isinstance(expected, dict):
        for k, v in expected.items():
            if k not in actual or actual[k] != v:
                return False
        return True
    return False


def _evaluate_expect(decoded: dict, expect_list: list[dict]) -> list[str]:
    errors: list[str] = []
    for i, exp in enumerate(expect_list):
        kind = exp.get("type") or exp.get("op")
        path = exp.get("path", "")
        found, actual = _get_path(decoded, path)
        if kind == "exists":
            should_exist = bool(exp.get("value", True))
            ok = found if should_exist else (not found)
            if not ok:
                errors.append(f"expect[{i}] exists failed: path={path} should_exist={should_exist}")
        elif kind == "equals":
            if not found:
                errors.append(f"expect[{i}] equals failed: path not found: {path}")
            elif actual != exp.get("value"):
                errors.append(
                    f"expect[{i}] equals failed: path={path} actual={actual!r} expected={exp.get('value')!r}"
                )
        elif kind == "contains":
            if not found:
                errors.append(f"expect[{i}] contains failed: path not found: {path}")
            elif not _contains(actual, exp.get("value")):
                errors.append(
                    f"expect[{i}] contains failed: path={path} actual={actual!r} expected contains {exp.get('value')!r}"
                )
        else:
            errors.append(f"expect[{i}] unsupported type: {kind!r}")
    return errors


@dataclass
class StepWaiter:
    store: PacketStore
    start_ts: float
    expected_action: Optional[int]

    def wait(self, timeout_ms: int, stop_event: Optional[threading.Event] = None) -> Optional[Packet]:
        q: Queue = Queue(maxsize=200)

        def _listener(pkt: Packet):
            if pkt.direction != "S2C":
                return
            if pkt.timestamp < self.start_ts:
                return
            if self.expected_action is not None and pkt.action != self.expected_action:
                return
            try:
                q.put_nowait(pkt)
            except Exception:
                pass

        self.store.add_listener(_listener)
        deadline = _now() + max(0.001, timeout_ms / 1000.0)
        try:
            while _now() < deadline:
                if stop_event and stop_event.is_set():
                    return None
                remain = min(0.1, max(0.0, deadline - _now()))
                try:
                    return q.get(timeout=remain)
                except Empty:
                    continue
            return None
        finally:
            self.store.remove_listener(_listener)


class TestRunner:
    def __init__(self, store: PacketStore, proxy, reports_dir: str = "reports"):
        self.store = store
        self.proxy = proxy
        self.reports_dir = reports_dir

    def load_suite(self, suite_path: str) -> dict:
        with open(suite_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def run_suite_from_path(
        self,
        suite_path: str,
        conn_index: int = 0,
        stop_event: Optional[threading.Event] = None,
        progress_cb: Optional[Callable[[str, dict], None]] = None,
    ) -> dict:
        suite = self.load_suite(suite_path)
        result = self.run_suite(suite, conn_index=conn_index, stop_event=stop_event, progress_cb=progress_cb)
        result["suite_path"] = suite_path
        return result

    def run_suite(
        self,
        suite: dict,
        conn_index: int = 0,
        stop_event: Optional[threading.Event] = None,
        progress_cb: Optional[Callable[[str, dict], None]] = None,
    ) -> dict:
        start_ts = _now()
        suite_name = suite.get("name", "unnamed_suite")
        cases = suite.get("cases")
        if not cases:
            # Backward-compatible shortcut: a single case directly with steps.
            cases = [{"name": suite_name, "steps": suite.get("steps", [])}]

        total_steps = sum(len(case.get("steps", [])) for case in cases)
        result = {
            "suite_name": suite_name,
            "suite_status": "running",
            "started_at": start_ts,
            "ended_at": None,
            "duration_ms": None,
            "total_cases": len(cases),
            "passed_cases": 0,
            "failed_cases": 0,
            "stopped_cases": 0,
            "total_steps": total_steps,
            "passed_steps": 0,
            "failed_steps": 0,
            "stopped_steps": 0,
            "cases": [],
            "conn_index": conn_index,
        }
        if progress_cb:
            progress_cb("suite_start", {"suite_name": suite_name, "total_cases": len(cases), "total_steps": total_steps})

        for ci, case in enumerate(cases):
            if stop_event and stop_event.is_set():
                break
            case_result = self.run_case(case, conn_index=conn_index, stop_event=stop_event, progress_cb=progress_cb)
            case_result["index"] = ci
            result["cases"].append(case_result)

            if case_result["case_status"] == "passed":
                result["passed_cases"] += 1
            elif case_result["case_status"] == "stopped":
                result["stopped_cases"] += 1
            else:
                result["failed_cases"] += 1

            result["passed_steps"] += case_result["passed_steps"]
            result["failed_steps"] += case_result["failed_steps"]
            result["stopped_steps"] += case_result["stopped_steps"]

        end_ts = _now()
        result["ended_at"] = end_ts
        result["duration_ms"] = int((end_ts - start_ts) * 1000)
        if stop_event and stop_event.is_set():
            result["suite_status"] = "stopped"
        elif result["failed_cases"] > 0:
            result["suite_status"] = "failed"
        else:
            result["suite_status"] = "passed"

        report_path = self._write_report(result)
        result["report_path"] = report_path
        if progress_cb:
            progress_cb("suite_end", {"suite_status": result["suite_status"], "report_path": report_path})
        return result

    def run_case(
        self,
        case: dict,
        conn_index: int = 0,
        stop_event: Optional[threading.Event] = None,
        progress_cb: Optional[Callable[[str, dict], None]] = None,
    ) -> dict:
        case_name = case.get("name", "unnamed_case")
        steps = case.get("steps", [])
        case_start = _now()
        out = {
            "case_name": case_name,
            "case_status": "running",
            "started_at": case_start,
            "ended_at": None,
            "duration_ms": None,
            "passed_steps": 0,
            "failed_steps": 0,
            "stopped_steps": 0,
            "steps": [],
        }
        if progress_cb:
            progress_cb("case_start", {"case_name": case_name, "total_steps": len(steps)})

        for si, step in enumerate(steps):
            if stop_event and stop_event.is_set():
                break
            step_result = self._run_step(step, conn_index=conn_index, stop_event=stop_event)
            step_result["index"] = si
            out["steps"].append(step_result)

            if step_result["step_status"] == "passed":
                out["passed_steps"] += 1
            elif step_result["step_status"] == "stopped":
                out["stopped_steps"] += 1
            else:
                out["failed_steps"] += 1

            if progress_cb:
                progress_cb(
                    "step_end",
                    {
                        "case_name": case_name,
                        "step_index": si,
                        "step_status": step_result["step_status"],
                    },
                )

            if step_result["step_status"] != "passed":
                break

        out["ended_at"] = _now()
        out["duration_ms"] = int((out["ended_at"] - out["started_at"]) * 1000)
        if stop_event and stop_event.is_set():
            out["case_status"] = "stopped"
        elif out["failed_steps"] > 0:
            out["case_status"] = "failed"
        elif out["stopped_steps"] > 0:
            out["case_status"] = "stopped"
        else:
            out["case_status"] = "passed"
        if progress_cb:
            progress_cb("case_end", {"case_name": case_name, "case_status": out["case_status"]})
        return out

    def _run_step(self, step: dict, conn_index: int, stop_event: Optional[threading.Event] = None) -> dict:
        step_start = _now()
        action = int(step["action"])
        params = dict(step.get("params", {}))
        timeout_ms = int(step.get("timeout_ms", 5000))
        expected_action = step.get("response_action", action)
        if expected_action is not None:
            expected_action = int(expected_action)
        expect_list = list(step.get("expect", []))

        result = {
            "step_status": "running",
            "action": action,
            "response_action": expected_action,
            "timeout_ms": timeout_ms,
            "started_at": step_start,
            "ended_at": None,
            "latency_ms": None,
            "matched_packet_id": None,
            "msg_id": None,
            "error": None,
            "assert_errors": [],
            "response_excerpt": None,
        }

        waiter = StepWaiter(self.store, step_start, expected_action)
        msg = dict(params)
        msg["action"] = action
        try:
            msg_id = self.proxy.inject(msg, conn_index=conn_index)
            result["msg_id"] = msg_id
        except Exception as e:
            result["step_status"] = "failed"
            result["error"] = f"inject failed: {e}"
            result["ended_at"] = _now()
            return result

        pkt = waiter.wait(timeout_ms=timeout_ms, stop_event=stop_event)
        if stop_event and stop_event.is_set():
            result["step_status"] = "stopped"
            result["error"] = "stopped"
            result["ended_at"] = _now()
            return result
        if pkt is None:
            result["step_status"] = "failed"
            result["error"] = f"timeout waiting S2C action={expected_action}"
            result["ended_at"] = _now()
            return result

        end = _now()
        result["ended_at"] = end
        result["latency_ms"] = int((pkt.timestamp - step_start) * 1000)
        result["matched_packet_id"] = pkt.id
        result["response_excerpt"] = pkt.decoded
        assert_errors = _evaluate_expect(pkt.decoded, expect_list)
        result["assert_errors"] = assert_errors
        result["step_status"] = "passed" if not assert_errors else "failed"
        if assert_errors:
            result["error"] = assert_errors[0]
        return result

    def _write_report(self, result: dict) -> str:
        os.makedirs(self.reports_dir, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        name = f"{ts}_{result.get('suite_name', 'suite')}.json"
        safe = "".join(c if c.isalnum() or c in ("_", "-", ".") else "_" for c in name)
        path = os.path.join(self.reports_dir, safe)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        return path


class TestRunService:
    def __init__(self, runner: TestRunner, keep_last: int = 20):
        self.runner = runner
        self._runs: dict[str, dict] = {}
        self._order: deque[str] = deque(maxlen=max(1, keep_last))
        self._lock = threading.Lock()

    def start_run(self, suite: Optional[dict] = None, suite_path: Optional[str] = None, conn_index: int = 0) -> dict:
        if not suite and not suite_path:
            raise ValueError("suite or suite_path is required")

        run_id = uuid.uuid4().hex[:12]
        record = {
            "run_id": run_id,
            "status": "running",
            "created_at": _now(),
            "started_at": None,
            "ended_at": None,
            "suite_name": None,
            "suite_path": suite_path,
            "conn_index": conn_index,
            "progress": {
                "total_cases": 0,
                "completed_cases": 0,
                "total_steps": 0,
                "completed_steps": 0,
                "current_case": None,
            },
            "result": None,
            "report_path": None,
            "error": None,
            "_stop_event": threading.Event(),
        }
        with self._lock:
            self._runs[run_id] = record
            self._order.append(run_id)
            self._trim_unsafe()

        t = threading.Thread(
            target=self._run_thread, args=(run_id, suite, suite_path, conn_index), daemon=True
        )
        t.start()
        return self._public_record(record)

    def get_run(self, run_id: str) -> Optional[dict]:
        with self._lock:
            rec = self._runs.get(run_id)
            return self._public_record(rec) if rec else None

    def list_runs(self) -> list[dict]:
        with self._lock:
            ids = list(self._order)
            ids.reverse()
            return [self._public_record(self._runs[rid]) for rid in ids if rid in self._runs]

    def stop_run(self, run_id: str) -> Optional[dict]:
        with self._lock:
            rec = self._runs.get(run_id)
            if not rec:
                return None
            rec["_stop_event"].set()
            return self._public_record(rec)

    def _run_thread(self, run_id: str, suite: Optional[dict], suite_path: Optional[str], conn_index: int):
        def progress_cb(evt: str, data: dict):
            with self._lock:
                rec = self._runs.get(run_id)
                if not rec:
                    return
                if evt == "suite_start":
                    rec["started_at"] = _now()
                    rec["suite_name"] = data.get("suite_name")
                    rec["progress"]["total_cases"] = data.get("total_cases", 0)
                    rec["progress"]["total_steps"] = data.get("total_steps", 0)
                elif evt == "case_start":
                    rec["progress"]["current_case"] = data.get("case_name")
                elif evt == "case_end":
                    rec["progress"]["completed_cases"] += 1
                elif evt == "step_end":
                    rec["progress"]["completed_steps"] += 1

        with self._lock:
            rec = self._runs.get(run_id)
        if not rec:
            return
        stop_event = rec["_stop_event"]
        try:
            if suite_path:
                result = self.runner.run_suite_from_path(
                    suite_path, conn_index=conn_index, stop_event=stop_event, progress_cb=progress_cb
                )
            else:
                result = self.runner.run_suite(
                    suite, conn_index=conn_index, stop_event=stop_event, progress_cb=progress_cb
                )
            with self._lock:
                rec = self._runs.get(run_id)
                if not rec:
                    return
                rec["result"] = result
                rec["report_path"] = result.get("report_path")
                rec["ended_at"] = _now()
                rec["status"] = result.get("suite_status", "failed")
        except Exception as e:
            with self._lock:
                rec = self._runs.get(run_id)
                if not rec:
                    return
                rec["ended_at"] = _now()
                rec["status"] = "failed"
                rec["error"] = str(e)

    def _trim_unsafe(self):
        keep = set(self._order)
        stale = [rid for rid in self._runs.keys() if rid not in keep]
        for rid in stale:
            self._runs.pop(rid, None)

    @staticmethod
    def _public_record(rec: Optional[dict]) -> Optional[dict]:
        if rec is None:
            return None
        return {k: v for k, v in rec.items() if not k.startswith("_")}
