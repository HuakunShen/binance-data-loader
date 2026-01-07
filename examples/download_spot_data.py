"""
Example: Download spot kline data.

This script demonstrates how to download Binance spot data for
different symbols, intervals, and date ranges.
"""

from datetime import datetime, timedelta, UTC
from binance_data_loader import BinanceDataDownloader, BinanceDataLoader
from pathlib import Path


def download_eth_1s_data():
    """Download ETHUSDT 1s spot data for first week of January 2024."""
    print("=== Downloading ETHUSDT 1s Spot Data (Jan 1-7, 2024) ===")

    downloader = BinanceDataDownloader(
        prefix="data/spot/daily/klines/ETHUSDT/1s/",
        destination_dir=Path("./data"),
        output_format="parquet",
        keep_zip=True,
        max_workers=10,
        max_processors=4,
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        end_date=datetime(2024, 1, 7, tzinfo=UTC),
    )

    download_results, process_results = downloader.download()

    print(f"\nDownloaded {len(download_results)} files")
    print(f"Processed {len(process_results[0])} files successfully")

    # Load and display the downloaded data
    print("\n=== Loading and displaying data ===")
    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="csv",
    )

    df = loader.load(
        symbol="ETHUSDT",
        interval="1s",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 7, tzinfo=UTC),
    )

    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print("\nFirst 5 rows:")
    print(df.head(5))
    print("\nLast 5 rows:")
    print(df.tail(5))
    print("\nDataFrame schema:")
    print(df.schema)


def download_btc_1m_recent_month():
    """Download BTCUSDT 1m spot data for last month."""
    print("=== Downloading BTCUSDT 1m Spot Data (Last Month) ===")

    one_month_ago = datetime.now(tz=UTC) - timedelta(days=30)
    today = datetime.now(tz=UTC)

    downloader = BinanceDataDownloader(
        prefix="data/spot/daily/klines/BTCUSDT/1m/",
        destination_dir=Path("./data"),
        output_format="parquet",
        keep_zip=True,
        max_workers=10,
        max_processors=4,
        start_date=one_month_ago,
        end_date=today,
    )

    download_results, process_results = downloader.download()

    print(f"\nDownloaded {len(download_results)} files")
    print(f"Processed {len(process_results[0])} files successfully")

    # Load and display the downloaded data
    print("\n=== Loading and displaying data ===")
    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="parquet",
    )

    df = loader.load(
        symbol="BTCUSDT",
        interval="1m",
        start_time=one_month_ago,
        end_time=today,
    )

    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print("\nFirst 5 rows:")
    print(df.head(5))
    print("\nLast 5 rows:")
    print(df.tail(5))
    print("\nDataFrame schema:")
    print(df.schema)


def download_csv_format_example():
    """Download data in CSV format instead of Parquet."""
    print("=== Downloading ETHUSDT 5m Spot Data (CSV Format) ===")

    downloader = BinanceDataDownloader(
        prefix="data/spot/daily/klines/ETHUSDT/5m/",
        destination_dir=Path("./data"),
        output_format="csv",  # CSV format
        keep_zip=False,
        max_workers=10,
        max_processors=4,
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        end_date=datetime(2024, 1, 31, tzinfo=UTC),
    )

    download_results, process_results = downloader.download()

    print(f"\nDownloaded {len(download_results)} files")
    print(f"Converted {len(process_results[0])} files to CSV")

    # Load and display the downloaded data
    print("\n=== Loading and displaying data ===")
    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="csv",
    )

    df = loader.load(
        symbol="ETHUSDT",
        interval="5m",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 31, tzinfo=UTC),
    )

    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns}")
    print("\nFirst 5 rows:")
    print(df.head(5))
    print("\nLast 5 rows:")
    print(df.tail(5))
    print("\nDataFrame schema:")
    print(df.schema)


if __name__ == "__main__":
    # Example 1: Download first week of ETH 1s data
    download_eth_1s_data()

    # Example 2: Download last month of BTC 1m data
    # download_btc_1m_recent_month()

    # Example 3: Download in CSV format
    # download_csv_format_example()
