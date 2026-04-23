import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from core.config import Cfg
from services.ops import maintenance


def title_value(text: str) -> dict:
    return {"type": "title", "title": [{"type": "text", "text": {"content": text}, "plain_text": text}]}


def relation_value(ids: list[str]) -> dict:
    return {"type": "relation", "relation": [{"id": rid} for rid in ids]}


class FakeNotionClient:
    def __init__(self, dbs: dict, rows_by_db: dict) -> None:
        self.dbs = dbs
        self.rows_by_db = rows_by_db
        self.created_pages = []
        self.updated_pages = []

    def get_database(self, database_id: str) -> dict:
        return self.dbs[database_id]

    def query_database_all(self, database_id: str, filter_obj=None) -> list:
        return self.rows_by_db.get(database_id, [])

    def create_page(self, database_id: str, properties: dict) -> dict:
        page_id = f"created-{len(self.created_pages) + 1}"
        page = {"id": page_id, "properties": self._typed_properties(database_id, properties)}
        self.rows_by_db.setdefault(database_id, []).append(page)
        self.created_pages.append((database_id, page))
        return page

    def update_page(self, page_id: str, properties: dict) -> dict:
        self.updated_pages.append((page_id, properties))
        row = self._find_page(page_id)
        if row is None:
            raise AssertionError(f"page not found: {page_id}")
        for name, payload in properties.items():
            row["properties"][name] = self._typed_payload(payload)
        return row

    def _find_page(self, page_id: str) -> dict | None:
        for rows in self.rows_by_db.values():
            for row in rows:
                if row.get("id") == page_id:
                    return row
        return None

    def _typed_properties(self, database_id: str, properties: dict) -> dict:
        typed = {}
        for name, payload in properties.items():
            typed[name] = self._typed_payload(payload)
        for name, info in self.dbs[database_id]["properties"].items():
            typed.setdefault(name, {"type": info.get("type")})
        return typed

    @staticmethod
    def _typed_payload(payload: dict) -> dict:
        if "relation" in payload:
            return {"type": "relation", "relation": payload["relation"]}
        if "title" in payload:
            return {"type": "title", "title": payload["title"]}
        if "rich_text" in payload:
            return {"type": "rich_text", "rich_text": payload["rich_text"]}
        if "select" in payload:
            return {"type": "select", "select": payload["select"]}
        if "status" in payload:
            return {"type": "status", "status": payload["status"]}
        if "number" in payload:
            return {"type": "number", "number": payload["number"]}
        if "date" in payload:
            return {"type": "date", "date": payload["date"]}
        return payload


class CashRelationBackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = Cfg(
            stock_master_id="stock-db",
            std_trades_id="trade-db",
            std_dividend_id="div-db",
            annual_id="annual-db",
            buy_wide_id="buy-db",
            t_record_id="legacy-db",
            strategy_snapshot_id="",
            cash_config_id="cash-db",
        )
        self.cash_db = {
            "properties": {
                "名称": {"type": "title"},
                "交易流水": {"type": "relation", "relation": {"database_id": "trade-db"}},
                "可流动现金": {"type": "number"},
            }
        }
        self.trade_db = {
            "properties": {
                "记录": {"type": "title"},
                "日期": {"type": "date"},
                "方向": {"type": "select"},
                "股数": {"type": "number"},
                "价格": {"type": "number"},
                "手续费": {"type": "number"},
                "税费": {"type": "number"},
                "股票": {"type": "relation"},
                "账户": {"type": "relation", "relation": {"database_id": "cash-db"}},
                "策略": {"type": "rich_text"},
                "备注": {"type": "rich_text"},
                "source_table": {"type": "select"},
                "import_status": {"type": "select"},
            }
        }

    def make_client(
        self,
        *,
        trade_rows: list[dict] | None = None,
        cash_relation_ids: list[str] | None = None,
        include_trade_account_field: bool = True,
    ) -> FakeNotionClient:
        cash_rows = [
            {
                "id": "cash-1",
                "properties": {
                    "名称": title_value("总账户"),
                    "交易流水": relation_value(cash_relation_ids or []),
                    "可流动现金": {"type": "number", "number": 100000.0},
                },
            }
        ]
        trade_db = {"properties": dict(self.trade_db["properties"])}
        if not include_trade_account_field:
            trade_db["properties"].pop("账户", None)
        dbs = {
            "cash-db": self.cash_db,
            "trade-db": trade_db,
            "stock-db": {"properties": {"名称": {"type": "title"}}},
        }
        rows_by_db = {"cash-db": cash_rows, "trade-db": trade_rows or [], "stock-db": []}
        return FakeNotionClient(dbs, rows_by_db)

    def test_resolve_trade_account_target_prefers_trade_relation_field(self) -> None:
        client = self.make_client()

        target = maintenance._resolve_trade_account_target(client, self.cfg, client.get_database("trade-db"))

        self.assertEqual(target["account_field"], "账户")
        self.assertEqual(target["cash_page_id"], "cash-1")

    def test_add_trade_sets_trade_account_relation(self) -> None:
        client = self.make_client(cash_relation_ids=["trade-old"])
        args = SimpleNamespace(
            date="2026-04-21",
            direction="BUY",
            stock="600519",
            shares=100.0,
            price=10.5,
            fee=1.2,
            tax=0.0,
            strategy="trend",
            note="manual entry",
        )

        with (
            patch.object(maintenance, "stock_index", return_value=({}, {"600519": "stock-1"})),
            patch.object(
                maintenance,
                "_resolve_trade_write_fields",
                return_value={
                    "title": "记录",
                    "date": "日期",
                    "direction": "方向",
                    "shares": "股数",
                    "price": "价格",
                    "fee": "手续费",
                    "tax": "税费",
                    "stock": "股票",
                    "account": "账户",
                    "strategy": "策略",
                    "note": "备注",
                    "source_table": "source_table",
                    "import_status": "import_status",
                },
            ),
        ):
            rc = maintenance.add_trade(client, self.cfg, args)

        self.assertEqual(rc, 0)
        self.assertEqual(len(client.created_pages), 1)
        trade_props = client.created_pages[0][1]["properties"]
        self.assertEqual([item["id"] for item in trade_props["账户"]["relation"]], ["cash-1"])
        self.assertEqual(len(client.updated_pages), 0)

    def test_backfill_cash_relations_updates_trade_account_field(self) -> None:
        client = self.make_client(
            trade_rows=[
                {"id": "trade-1", "properties": {"账户": relation_value(["cash-1"])}},
                {"id": "trade-2", "properties": {"账户": relation_value([])}},
            ]
        )

        rc_first = maintenance.backfill_cash_relations(client, self.cfg)
        rc_second = maintenance.backfill_cash_relations(client, self.cfg)

        self.assertEqual(rc_first, 0)
        self.assertEqual(rc_second, 0)
        trade_1 = client.rows_by_db["trade-db"][0]["properties"]["账户"]["relation"]
        trade_2 = client.rows_by_db["trade-db"][1]["properties"]["账户"]["relation"]
        self.assertEqual([item["id"] for item in trade_1], ["cash-1"])
        self.assertEqual([item["id"] for item in trade_2], ["cash-1"])
        self.assertEqual(len(client.updated_pages), 1)

    def test_migrate_apply_sets_trade_account_on_created_rows(self) -> None:
        client = self.make_client(cash_relation_ids=["trade-old"])
        record_key = next(
            const[0]
            for const in maintenance.migrate_apply.__code__.co_consts
            if isinstance(const, tuple) and const and "source_table" in const
        )
        candidates = [
            {
                record_key: "old row A",
                "source_table": "old_buy_record",
                "source_row_id": "legacy-1",
                "source_title": "src-1",
                "source_stock_col": "600519",
                "source_value": 10.0,
                "import_status": "pending_shares",
                "stock_id": "stock-1",
            },
            {
                record_key: "old row B",
                "source_table": "old_buy_record",
                "source_row_id": "legacy-2",
                "source_title": "src-2",
                "source_stock_col": "000001",
                "source_value": 20.0,
                "import_status": "pending_shares",
                "stock_id": "",
            },
        ]

        with (
            patch.object(maintenance, "extract_candidates", return_value=candidates),
            patch.object(maintenance, "existing_source_keys", return_value=set()),
            patch.object(maintenance.time, "sleep", return_value=None),
        ):
            rc = maintenance.migrate_apply(client, self.cfg, limit=0)

        self.assertEqual(rc, 0)
        self.assertEqual(len(client.created_pages), 2)
        for _, page in client.created_pages:
            self.assertEqual([item["id"] for item in page["properties"]["账户"]["relation"]], ["cash-1"])
        self.assertEqual(len(client.updated_pages), 0)

    def test_fallback_mode_still_updates_cash_reverse_relation(self) -> None:
        client = self.make_client(
            trade_rows=[
                {"id": "trade-1", "properties": {}},
                {"id": "trade-2", "properties": {}},
            ],
            cash_relation_ids=["trade-1"],
            include_trade_account_field=False,
        )

        rc = maintenance.backfill_cash_relations(client, self.cfg)

        self.assertEqual(rc, 0)
        cash_relations = client.rows_by_db["cash-db"][0]["properties"]["交易流水"]["relation"]
        self.assertEqual([item["id"] for item in cash_relations], ["trade-1", "trade-2"])
        self.assertEqual(len(client.updated_pages), 1)


if __name__ == "__main__":
    unittest.main()
