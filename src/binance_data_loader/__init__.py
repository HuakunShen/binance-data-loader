"""
binance-data - A Python library for downloading and processing Binance Vision data.
"""

from binance_data_loader.downloader import BinanceDataDownloader
from binance_data_loader.processor import DataProcessor
from binance_data_loader.metadata import BinanceDataMetadata
from binance_data_loader.loader import (
    BinanceDataLoader,
    load_kline_data,
    get_date_range,
)
from binance_data_loader.types import (
    DownloadResult,
    DownloadResultSuccess,
    DownloadResultSkipped,
    DownloadResultError,
    ProcessResult,
    ProcessResultSuccess,
    ProcessResultSkipped,
    ProcessResultError,
)

__all__ = [
    "BinanceDataDownloader",
    "DataProcessor",
    "BinanceDataMetadata",
    "BinanceDataLoader",
    "load_kline_data",
    "get_date_range",
    "DownloadResult",
    "DownloadResultSuccess",
    "DownloadResultSkipped",
    "DownloadResultError",
    "ProcessResult",
    "ProcessResultSuccess",
    "ProcessResultSkipped",
    "ProcessResultError",
]
