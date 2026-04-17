import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List

from stores.snapshot_store import ensure_parent_dir


@dataclass
class KBar:
    symbol: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    vol: float
    amount: float
    adj: str


class KlineStore:
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
            CREATE TABLE IF NOT EXISTS kline_daily (
                symbol TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                vol REAL NOT NULL,
                amount REAL NOT NULL,
                adj TEXT NOT NULL,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, trade_date, adj)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kline_sync_log (
                symbol TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                bars INTEGER NOT NULL,
                status TEXT NOT NULL,
                error TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(symbol, start_date, end_date)
            )
            """
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_kline_symbol_date ON kline_daily(symbol, trade_date)")
        self.conn.commit()

    def upsert_bars(self, bars: List[KBar], source: str) -> int:
        if not bars:
            return 0
        now = dt.datetime.now().isoformat(timespec="seconds")
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO kline_daily(symbol, trade_date, open, high, low, close, vol, amount, adj, source, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, trade_date, adj) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    vol=excluded.vol,
                    amount=excluded.amount,
                    source=excluded.source,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        bar.symbol,
                        bar.trade_date,
                        float(bar.open),
                        float(bar.high),
                        float(bar.low),
                        float(bar.close),
                        float(bar.vol),
                        float(bar.amount),
                        bar.adj,
                        source,
                        now,
                    )
                    for bar in bars
                ],
            )
        return len(bars)

    def query_bars(self, symbol: str, start_date: str, end_date: str, adj: str) -> List[KBar]:
        cur = self.conn.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, vol, amount, adj
            FROM kline_daily
            WHERE symbol=? AND adj=? AND trade_date>=? AND trade_date<=?
            ORDER BY trade_date ASC
            """,
            (symbol, adj, start_date, end_date),
        )
        out: List[KBar] = []
        for row in cur.fetchall():
            out.append(
                KBar(
                    symbol=str(row["symbol"]),
                    trade_date=str(row["trade_date"]),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    vol=float(row["vol"]),
                    amount=float(row["amount"]),
                    adj=str(row["adj"]),
                )
            )
        return out

    def record_sync(self, symbol: str, start_date: str, end_date: str, bars: int, status: str, error: str = "") -> None:
        now = dt.datetime.now().isoformat(timespec="seconds")
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO kline_sync_log(symbol, start_date, end_date, bars, status, error, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, start_date, end_date) DO UPDATE SET
                    bars=excluded.bars,
                    status=excluded.status,
                    error=excluded.error,
                    updated_at=excluded.updated_at
                """,
                (symbol, start_date, end_date, int(bars), status, error[:500], now),
            )

    def query_latest(self, symbol: str, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT trade_date, open, high, low, close, vol AS volume, amount
            FROM kline_daily
            WHERE symbol=?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            ((symbol or "").upper(), max(1, int(limit))),
        ).fetchall()
        out = [dict(x) for x in rows]
        out.reverse()
        return out
