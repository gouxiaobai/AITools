import os
import tempfile
import unittest
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from param_store import ParamStore, SCHEMA_VERSION, validate_param_payload


class ParamStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(tempfile.gettempdir(), f"param_store_test_{os.getpid()}.db")
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass
        self.store = ParamStore(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        try:
            os.remove(self.db_path)
        except FileNotFoundError:
            pass

    def _create_one_proposal(self) -> str:
        rows = [
            {"strategy_id": "BASELINE", "market": "SH", "ret_1d": 0.01, "hit_flag": 1, "max_drawdown": 0.02},
            {"strategy_id": "BASELINE", "market": "SH", "ret_1d": -0.005, "hit_flag": 0, "max_drawdown": 0.03},
            {"strategy_id": "BASELINE", "market": "SH", "ret_1d": 0.008, "hit_flag": 1, "max_drawdown": 0.01},
        ]
        out = self.store.create_proposals_from_history(
            snapshot_rows=rows,
            source_start_date="2026-04-01",
            source_end_date="2026-04-14",
            run_id="run_ut_1",
            dry_run=False,
            walk_forward_splits=2,
        )
        self.assertTrue(out)
        return out[0]["proposal_id"]

    def test_schema_upgraded(self) -> None:
        self.assertGreaterEqual(self.store.get_schema_version(), SCHEMA_VERSION)
        row = self.store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_research_experiment'"
        ).fetchone()
        self.assertIsNotNone(row)

    def test_validate_payload(self) -> None:
        ok, errs, normalized = validate_param_payload(
            {
                "band_low": 0.01,
                "band_high": 0.03,
                "stop_mult": 1.8,
                "trend_threshold": 0.015,
                "vol_cap": 0.1,
                "rr_min": 0.8,
                "min_confidence": "medium",
                "allow_small_sample": "true",
            }
        )
        self.assertTrue(ok)
        self.assertFalse(errs)
        self.assertEqual(normalized["min_confidence"], "MEDIUM")
        self.assertTrue(normalized["allow_small_sample"])

    def test_apply_idempotent_and_version_conflict(self) -> None:
        proposal_id = self._create_one_proposal()
        diff = self.store.diff(proposal_id)
        current_version = int(diff["current_version"])
        first = self.store.apply(proposal_id=proposal_id, expected_version=current_version, batch_id="b1", rollout_scope="full")
        self.assertFalse(first["idempotent"])
        self.assertEqual(first["version"], current_version + 1)

        second = self.store.apply(proposal_id=proposal_id, expected_version=first["version"], batch_id="b1", rollout_scope="full")
        self.assertTrue(second["idempotent"])

        with self.assertRaises(RuntimeError):
            self.store.apply(proposal_id=proposal_id, expected_version=current_version, batch_id="b1", rollout_scope="full")

    def test_rollback(self) -> None:
        proposal_id = self._create_one_proposal()
        diff = self.store.diff(proposal_id)
        current_version = int(diff["current_version"])
        applied = self.store.apply(proposal_id=proposal_id, expected_version=current_version, batch_id="b2", rollout_scope="full")
        rolled = self.store.rollback(apply_log_id=applied["apply_log_id"], comment="ut_rollback")
        self.assertTrue(rolled["rollback_ref"])
        self.assertGreaterEqual(int(rolled["version"]), int(applied["version"]) + 1)

    def test_experiment_create_and_link(self) -> None:
        exp = self.store.create_experiment(
            source_start_date="2026-04-01",
            source_end_date="2026-04-14",
            strategy_scope="BASELINE",
            market_scope="SH",
            walk_forward_splits=3,
            cost_bps=3.0,
            slippage_bps=2.0,
            train_window=60,
            valid_window=20,
            experiment_name="ut_exp",
        )
        got = self.store.get_experiment(exp["experiment_id"])
        self.assertEqual(got["experiment_name"], "ut_exp")
        proposal_id = self._create_one_proposal()
        out = self.store.apply_proposal(
            proposal_id=proposal_id,
            expected_version=self.store.diff(proposal_id)["current_version"],
            experiment_id=exp["experiment_id"],
            gate_passed=True,
            gate_reason="ok",
        )
        self.assertEqual(out["experiment_id"], exp["experiment_id"])

    def test_monitor_metrics(self) -> None:
        self.store.log_event(module="param", action="x", status="SUCCESS", duration_ms=10)
        self.store.log_event(module="param", action="x", status="FAILED", duration_ms=20, error_code="E1", error_msg="oops")
        mon = self.store.get_monitor(days=7)
        self.assertIn("failure_distribution", mon)
        self.assertIn("slow_tasks", mon)
        self.assertGreaterEqual(mon.get("event_total", 0), 2)


if __name__ == "__main__":
    unittest.main()
