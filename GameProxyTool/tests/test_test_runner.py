import threading
import time
import unittest
import os
import shutil
import uuid

from store import PacketStore
from test_runner import TestRunner


class _FakeProxy:
    def __init__(self, store: PacketStore):
        self.store = store
        self._next = 1

    def active_count(self):
        return 1

    def inject(self, msg, conn_index=0):
        sid = self._next
        self._next += 1

        action = int(msg["action"])
        def _respond():
            time.sleep(0.02)
            self.store.add("S2C", b"\x02", {"commResp": {"action": action, "code": 0}})

        threading.Thread(target=_respond, daemon=True).start()
        return sid


class _NoResponseProxy:
    def __init__(self):
        self._next = 1

    def inject(self, msg, conn_index=0):
        sid = self._next
        self._next += 1
        return sid


class TestRunnerTests(unittest.TestCase):
    def _make_reports_dir(self):
        path = os.path.join("tests_tmp", f"reports_{uuid.uuid4().hex[:8]}")
        os.makedirs(path, exist_ok=True)
        return path

    def test_run_suite_success(self):
        store = PacketStore()
        proxy = _FakeProxy(store)
        td = self._make_reports_dir()
        try:
            runner = TestRunner(store, proxy, reports_dir=td)
            suite = {
                "name": "ok_suite",
                "cases": [
                    {
                        "name": "case1",
                        "steps": [
                            {
                                "action": 50006,
                                "timeout_ms": 500,
                                "expect": [{"type": "equals", "path": "commResp.code", "value": 0}],
                            }
                        ],
                    }
                ],
            }
            result = runner.run_suite(suite)
            self.assertEqual(result["suite_status"], "passed")
            self.assertEqual(result["passed_cases"], 1)
            self.assertTrue(result["report_path"])
        finally:
            shutil.rmtree(td, ignore_errors=True)

    def test_run_step_timeout(self):
        store = PacketStore()
        proxy = _NoResponseProxy()
        td = self._make_reports_dir()
        try:
            runner = TestRunner(store, proxy, reports_dir=td)
            suite = {
                "name": "timeout_suite",
                "steps": [{"action": 1, "timeout_ms": 50}],
            }
            result = runner.run_suite(suite)
            self.assertEqual(result["suite_status"], "failed")
            self.assertEqual(result["failed_steps"], 1)
        finally:
            shutil.rmtree(td, ignore_errors=True)

    def test_assertion_failure(self):
        store = PacketStore()
        proxy = _FakeProxy(store)
        td = self._make_reports_dir()
        try:
            runner = TestRunner(store, proxy, reports_dir=td)
            suite = {
                "name": "assert_suite",
                "steps": [
                    {
                        "action": 2,
                        "timeout_ms": 500,
                        "expect": [{"type": "equals", "path": "commResp.code", "value": 123}],
                    }
                ],
            }
            result = runner.run_suite(suite)
            self.assertEqual(result["suite_status"], "failed")
            self.assertEqual(result["failed_steps"], 1)
        finally:
            shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
