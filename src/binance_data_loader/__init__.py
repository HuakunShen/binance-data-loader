"""
binance-data-loader — download and process Binance Vision historical data.

Processor-first architecture:
    - BinanceDataDownloader  pure download infrastructure (S3 list + HTTP + progress)
    - KlineProcessor         all kline-specific logic (path, parsing, validation)
    - AggTradeProcessor      all aggTrades-specific logic (path, parsing, resample)
    - BaseProcessor          ABC to implement custom data types

Quick start::

    from binance_data_loader import BinanceDataDownloader, KlineProcessor, AssetType
    from pathlib import Path

    summary = BinanceDataDownloader(
        symbol="BTCUSDT",
        processor=KlineProcessor(interval="1m"),
        asset_type=AssetType.FUTURES_UM,
        destination_dir=Path("/data/binance"),
    ).download()
    print(summary)
"""

from binance_data_loader.downloader import BinanceDataDownloader, DownloadSummary
from binance_data_loader.loader import (
    BinanceDataLoader,
    get_date_range,
    load_kline_data,
)
from binance_data_loader.metadata import BinanceDataMetadata
from binance_data_loader.processors import (
    AggTradeProcessor,
    BaseProcessor,
    KlineProcessor,
)
from binance_data_loader.types import AssetType, DataType

__all__ = [
    # Downloader
    "BinanceDataDownloader",
    "DownloadSummary",
    # Processors
    "BaseProcessor",
    "KlineProcessor",
    "AggTradeProcessor",
    # Loader
    "BinanceDataLoader",
    "load_kline_data",
    "get_date_range",
    # Metadata
    "BinanceDataMetadata",
    # Enums
    "AssetType",
    "DataType",
]
