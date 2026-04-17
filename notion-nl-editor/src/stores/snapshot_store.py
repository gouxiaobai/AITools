import os
import sqlite3
from typing import Any, Dict, List, Optional


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


class SnapshotStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        ensure_parent_dir(db_path)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_daily_snapshot (
                snapshot_date TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                stock_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                market TEXT NOT NULL,
                strategy_mode TEXT NOT NULL,
                ret_1d REAL NOT NULL,
                hit_flag INTEGER NOT NULL,
                max_drawdown REAL NOT NULL,
                confidence TEXT NOT NULL,
                sample_count INTEGER NOT NULL,
                action TEXT NOT NULL,
                buy_price REAL,
                sell_price REAL,
                stop_price REAL,
                position_delta REAL NOT NULL,
                run_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (snapshot_date, strategy_id, stock_id)
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshot_date_strategy ON strategy_daily_snapshot (snapshot_date, strategy_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshot_date_market ON strategy_daily_snapshot (snapshot_date, market)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshot_stock_strategy ON strategy_daily_snapshot (stock_id, strategy_id)"
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS market_stock_universe (
                ts_code TEXT NOT NULL PRIMARY KEY,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                market TEXT NOT NULL,
                list_status TEXT NOT NULL,
                list_date TEXT NOT NULL,
                delist_date TEXT NOT NULL,
                is_active INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_market_universe_market_active ON market_stock_universe (market, is_active)"
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_stock_filter (
                stock_code TEXT NOT NULL PRIMARY KEY,
                decision TEXT NOT NULL,
                reason TEXT NOT NULL,
                operator TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manual_filter_decision ON manual_stock_filter (decision)"
        )
        self.conn.commit()

    def upsert_many(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO strategy_daily_snapshot (
                snapshot_date, strategy_id, stock_id, stock_code, stock_name, market, strategy_mode,
                ret_1d, hit_flag, max_drawdown, confidence, sample_count, action,
                buy_price, sell_price, stop_price, position_delta, run_id, created_at, updated_at
            ) VALUES (
                :snapshot_date, :strategy_id, :stock_id, :stock_code, :stock_name, :market, :strategy_mode,
                :ret_1d, :hit_flag, :max_drawdown, :confidence, :sample_count, :action,
                :buy_price, :sell_price, :stop_price, :position_delta, :run_id, :created_at, :updated_at
            )
            ON CONFLICT(snapshot_date, strategy_id, stock_id) DO UPDATE SET
                stock_code=excluded.stock_code,
                stock_name=excluded.stock_name,
                market=excluded.market,
                strategy_mode=excluded.strategy_mode,
                ret_1d=excluded.ret_1d,
                hit_flag=excluded.hit_flag,
                max_drawdown=excluded.max_drawdown,
                confidence=excluded.confidence,
                sample_count=excluded.sample_count,
                action=excluded.action,
                buy_price=excluded.buy_price,
                sell_price=excluded.sell_price,
                stop_price=excluded.stop_price,
                position_delta=excluded.position_delta,
                run_id=excluded.run_id,
                updated_at=excluded.updated_at
        """
        with self.conn:
            self.conn.executemany(sql, rows)
        return len(rows)

    def query_range(
        self,
        start_date: str,
        end_date: str,
        strategies: Optional[List[str]] = None,
        markets: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT * FROM strategy_daily_snapshot
            WHERE snapshot_date >= ? AND snapshot_date <= ?
        """
        args: List[Any] = [start_date, end_date]
        if strategies:
            marks = ",".join(["?"] * len(strategies))
            sql += f" AND strategy_id IN ({marks})"
            args.extend([x.upper() for x in strategies])
        if markets:
            marks = ",".join(["?"] * len(markets))
            sql += f" AND market IN ({marks})"
            args.extend([x.upper() for x in markets])
        sql += " ORDER BY snapshot_date ASC, strategy_id ASC, stock_code ASC"
        cur = self.conn.execute(sql, args)
        return [dict(row) for row in cur.fetchall()]

    def upsert_market_universe(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO market_stock_universe (
                ts_code, stock_code, stock_name, market, list_status,
                list_date, delist_date, is_active, updated_at
            ) VALUES (
                :ts_code, :stock_code, :stock_name, :market, :list_status,
                :list_date, :delist_date, :is_active, :updated_at
            )
            ON CONFLICT(ts_code) DO UPDATE SET
                stock_code=excluded.stock_code,
                stock_name=excluded.stock_name,
                market=excluded.market,
                list_status=excluded.list_status,
                list_date=excluded.list_date,
                delist_date=excluded.delist_date,
                is_active=excluded.is_active,
                updated_at=excluded.updated_at
        """
        with self.conn:
            self.conn.executemany(sql, rows)
        return len(rows)

    def query_market_universe(
        self,
        markets: Optional[List[str]] = None,
        active_only: bool = True,
        limit: int = 0,
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM market_stock_universe WHERE 1=1"
        args: List[Any] = []
        if active_only:
            sql += " AND is_active=1"
        if markets:
            marks = ",".join(["?"] * len(markets))
            sql += f" AND market IN ({marks})"
            args.extend([x.upper() for x in markets if x])
        sql += " ORDER BY market ASC, stock_code ASC"
        if limit and int(limit) > 0:
            sql += " LIMIT ?"
            args.append(int(limit))
        cur = self.conn.execute(sql, args)
        return [dict(row) for row in cur.fetchall()]

    def upsert_manual_filters(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
            INSERT INTO manual_stock_filter (
                stock_code, decision, reason, operator, updated_at
            ) VALUES (
                :stock_code, :decision, :reason, :operator, :updated_at
            )
            ON CONFLICT(stock_code) DO UPDATE SET
                decision=excluded.decision,
                reason=excluded.reason,
                operator=excluded.operator,
                updated_at=excluded.updated_at
        """
        with self.conn:
            self.conn.executemany(sql, rows)
        return len(rows)

    def delete_manual_filter(self, stock_code: str) -> int:
        with self.conn:
            cur = self.conn.execute("DELETE FROM manual_stock_filter WHERE stock_code=?", ((stock_code or "").upper(),))
        return int(cur.rowcount or 0)

    def query_manual_filters(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute(
            """
            SELECT stock_code, decision, reason, operator, updated_at
            FROM manual_stock_filter
            ORDER BY decision ASC, stock_code ASC
            """
        )
        return [dict(row) for row in cur.fetchall()]
