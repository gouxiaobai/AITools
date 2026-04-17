import os
from dataclasses import dataclass


@dataclass
class Cfg:
    stock_master_id: str
    std_trades_id: str
    std_dividend_id: str
    annual_id: str
    buy_wide_id: str
    t_record_id: str
    strategy_snapshot_id: str
    cash_config_id: str


def load_cfg() -> Cfg:
    return Cfg(
        stock_master_id=os.getenv("DB_STOCK_MASTER_ID", "9ff0bf7d-9ae4-41c8-9440-729daaa2a95d"),
        std_trades_id=os.getenv("DB_STD_TRADES_ID", "33c225a4-e273-810f-ae9f-d44f9d44d528"),
        std_dividend_id=os.getenv("DB_STD_DIVIDEND_ID", "33c225a4-e273-8112-9444-f798532e60cf"),
        annual_id=os.getenv("DB_ANNUAL_ID", "33c225a4-e273-8162-8804-dfde58582535"),
        buy_wide_id=os.getenv("DB_BUY_WIDE_ID", "0d485b47-e903-4fd3-901e-1bb4d09200f1"),
        t_record_id=os.getenv("DB_T_RECORD_ID", "93dde4b0-5d6f-4c49-a825-e49ae95be420"),
        strategy_snapshot_id=os.getenv("DB_STRATEGY_SNAPSHOT_ID", ""),
        cash_config_id=os.getenv("DB_CASH_CONFIG_ID", ""),
    )
