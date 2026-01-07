"""Type definitions and enums for binance-data library."""

from typing import Literal
from enum import Enum


class DataType(str, Enum):
    """Binance data types."""

    KLINE = "klines"
    AGG_TRADES = "aggTrades"
    TRADES = "trades"
    BOOK_DEPTH = "bookDepth"
    BOOK_TICKER = "bookTicker"


class AssetType(str, Enum):
    """Binance asset types."""

    SPOT = "spot"
    FUTURES_UM = "um"  # USDT-Margined Futures
    FUTURES_CM = "cm"  # COIN-Margined Futures
    OPTIONS = "option"


class TimePeriod(str, Enum):
    """Time period for data files."""

    DAILY = "daily"
    MONTHLY = "monthly"


class OutputFormat(str, Enum):
    """Output format for processed data."""

    PARQUET = "parquet"
    CSV = "csv"


# Valid Binance data intervals
BinanceInterval = Literal[
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]
