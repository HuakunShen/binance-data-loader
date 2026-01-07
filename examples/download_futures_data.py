"""
Example: Download futures (USDT-Margined) kline data.

This script demonstrates how to download Binance futures data for a specific
date range and time interval.
"""

from datetime import datetime, timedelta, UTC
from binance_data_loader import BinanceDataDownloader, BinanceDataLoader
from pathlib import Path


def download_btc_1h_data():
    """Download BTCUSDT 1h futures data for the last year."""
    print("=== Downloading BTCUSDT 1h Futures Data ===")

    # Calculate date range: last year
    one_week_ago = datetime.now(tz=UTC) - timedelta(days=7)
    today = datetime.now(tz=UTC)

    downloader = BinanceDataDownloader(
        prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
        destination_dir=Path("./data"),
        output_format="csv",
        keep_zip=True,
        max_workers=10,
        max_processors=4,
        start_date=one_week_ago,
        end_date=today,
    )

    download_results, process_results = downloader.download()

    successful_downloads = sum(1 for r in download_results if r["status"] == "success")
    skipped_downloads = sum(1 for r in download_results if r["status"] == "skipped")
    failed_downloads = sum(1 for r in download_results if r["status"] == "error")

    print("Download Summary:")
    print(f"  Successful: {successful_downloads}")
    print(f"  Skipped: {skipped_downloads}")
    print(f"  Failed: {failed_downloads}")

    successful_processes = len(process_results[0])
    skipped_processes = sum(1 for r in process_results[0] if r["status"] == "skipped")
    failed_processes = len(process_results[1])

    print("Processing Summary:")
    print(f"  Successful: {successful_processes}")
    print(f"  Skipped: {skipped_processes}")
    print(f"  Failed: {failed_processes}")

    # Load and display the downloaded data
    print("\n=== Loading and displaying data ===")
    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="futures",
        output_format="csv",
    )

    df = loader.load(
        symbol="BTCUSDT",
        interval="1h",
        start_time=one_week_ago,
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


def download_eth_5m_data_2024():
    """Download ETHUSDT 5m futures data for 2024."""
    print("=== Downloading ETHUSDT 5m Futures Data for 2024 ===")

    downloader = BinanceDataDownloader(
        prefix="data/futures/um/daily/klines/ETHUSDT/5m/",
        destination_dir=Path("./data"),
        output_format="parquet",
        keep_zip=True,  # Keep ZIP files
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
        data_type="futures",
        output_format="parquet",
    )

    df = loader.load(
        symbol="ETHUSDT",
        interval="5m",
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


if __name__ == "__main__":
    # Example 1: Download last year of BTC 1h data
    download_btc_1h_data()

    # Example 2: Download 2024 ETH 5m data
    # download_eth_5m_data_2024()
