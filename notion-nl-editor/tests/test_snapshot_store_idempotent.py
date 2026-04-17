import os
import sys
import tempfile
import unittest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from stores.snapshot_store import SnapshotStore


class SnapshotStoreIdempotentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.db_path = os.path.join(tempfile.gettempdir(), f"snapshot_store_test_{os.getpid()}.db")
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_upsert_idempotent_and_update_on_conflict(self) -> None:
        row = {
            "snapshot_date": "2026-04-17",
            "strategy_id": "BASELINE",
            "stock_id": "S1",
            "stock_code": "600001",
            "stock_name": "A",
            "market": "SH",
            "strategy_mode": "FULL_MODEL",
            "ret_1d": 0.01,
            "hit_flag": 1,
            "max_drawdown": 0.02,
            "confidence": "HIGH",
            "sample_count": 30,
            "action": "BUY",
            "buy_price": 10.1,
            "sell_price": 11.1,
            "stop_price": 9.6,
            "position_delta": 0.1,
            "run_id": "r1",
            "created_at": "2026-04-17T10:00:00",
            "updated_at": "2026-04-17T10:00:00",
        }
        store = SnapshotStore(self.db_path)
        try:
            self.assertEqual(store.upsert_many([row]), 1)
            row2 = dict(row)
            row2["run_id"] = "r2"
            row2["ret_1d"] = 0.03
            row2["updated_at"] = "2026-04-17T11:00:00"
            self.assertEqual(store.upsert_many([row2]), 1)
            rows = store.query_range("2026-04-17", "2026-04-17")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["run_id"], "r2")
            self.assertAlmostEqual(rows[0]["ret_1d"], 0.03, places=6)
        finally:
            store.close()

    def test_market_universe_upsert_and_query(self) -> None:
        store = SnapshotStore(self.db_path)
        try:
            rows = [
                {
                    "ts_code": "600001.SH",
                    "stock_code": "600001",
                    "stock_name": "A",
                    "market": "SH",
                    "list_status": "L",
                    "list_date": "20010101",
                    "delist_date": "",
                    "is_active": 1,
                    "updated_at": "2026-04-17T10:00:00",
                },
                {
                    "ts_code": "300001.SZ",
                    "stock_code": "300001",
                    "stock_name": "B",
                    "market": "SZ",
                    "list_status": "P",
                    "list_date": "20020101",
                    "delist_date": "",
                    "is_active": 0,
                    "updated_at": "2026-04-17T10:00:00",
                },
            ]
            self.assertEqual(store.upsert_market_universe(rows), 2)
            active_rows = store.query_market_universe(markets=["SH", "SZ"], active_only=True)
            self.assertEqual(len(active_rows), 1)
            self.assertEqual(active_rows[0]["ts_code"], "600001.SH")
        finally:
            store.close()

    def test_manual_filter_upsert_and_delete(self) -> None:
        store = SnapshotStore(self.db_path)
        try:
            self.assertEqual(
                store.upsert_manual_filters(
                    [
                        {
                            "stock_code": "600519",
                            "decision": "include",
                            "reason": "core",
                            "operator": "tester",
                            "updated_at": "2026-04-17T10:00:00",
                        }
                    ]
                ),
                1,
            )
            rows = store.query_manual_filters()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["decision"], "include")
            self.assertEqual(store.delete_manual_filter("600519"), 1)
            self.assertEqual(store.delete_manual_filter("600519"), 0)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
