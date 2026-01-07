"""Type definitions and enums for binance-data library."""

from typing import Literal, TypedDict, Union
from enum import Enum
from pathlib import Path


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


# TypedDict definitions for download and processing results


class DownloadResultSuccess(TypedDict):
    """Successful download result."""

    status: Literal["success"]
    key: str
    zip_path: Union[str, Path]
    size: int


class DownloadResultSkipped(TypedDict):
    """Skipped download result."""

    status: Literal["skipped"]
    key: str
    zip_path: str
    reason: str


class DownloadResultError(TypedDict):
    """Failed download result."""

    status: Literal["error"]
    key: str
    error: str


DownloadResult = Union[
    DownloadResultSuccess, DownloadResultSkipped, DownloadResultError
]


class ProcessResultSuccess(TypedDict):
    """Successful processing result."""

    status: Literal["success"]
    file_path: Union[str, Path]
    output_path: str
    rows: int
    timestamp_unit: str


class ProcessResultSkipped(TypedDict):
    """Skipped processing result."""

    status: Literal["skipped"]
    file_path: Union[str, Path]
    output_path: str
    reason: str


class ProcessResultError(TypedDict):
    """Failed processing result."""

    status: Literal["error"]
    file_path: Union[str, Path]
    error: str


ProcessResult = Union[ProcessResultSuccess, ProcessResultSkipped, ProcessResultError]
