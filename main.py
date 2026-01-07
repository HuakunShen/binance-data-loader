"""
Example usage of binance-data library.

This script demonstrates how to download Binance Vision data for both
spot and futures markets using the binance-data library, and how to load
and resample the processed data.
"""

from datetime import datetime, timedelta, UTC
from binance_data_loader import (
    BinanceDataDownloader,
    BinanceDataLoader,
    load_kline_data,
    get_date_range,
)
from pathlib import Path


def download_futures_kline():
    """
    Download futures (USDT-Margined) kline data.

    Example: Download BTCUSDT 1h klines as Parquet files.
    """
    downloader = BinanceDataDownloader(
        prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
        destination_dir=Path("./data"),
        output_format="parquet",
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        end_date=datetime(2024, 1, 31, tzinfo=UTC),
        keep_zip=True,
        max_workers=10,
        max_processors=4,
    )

    download_results, process_results = downloader.download()

    print(f"\nDownload Results: {len(download_results)} files")
    print(
        f"Process Results: {len(process_results[0])} successful, {len(process_results[1])} failed"
    )


def download_futures_kline_with_dates():
    """
    Download futures kline data for a specific date range using datetime objects.

    Example: Download 2024 BTCUSDT 1h klines.
    """
    start_date = datetime(2024, 1, 1, tzinfo=UTC)
    end_date = datetime(2024, 12, 31, tzinfo=UTC)

    downloader = BinanceDataDownloader(
        prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
        destination_dir=Path("./data"),
        output_format="parquet",
        keep_zip=False,
        max_workers=10,
        max_processors=4,
        start_date=start_date,
        end_date=end_date,
    )

    download_results, process_results = downloader.download()

    print(f"\nDownload Results: {len(download_results)} files")
    print(
        f"Process Results: {len(process_results[0])} successful, {len(process_results[1])} failed"
    )


def download_futures_kline_recent_year():
    """
    Download futures (USDT-Margined) kline data for the last year.

    Example: Download BTCUSDT 1h klines as Parquet files for the last year.
    """
    one_year_ago = datetime.now(tz=UTC) - timedelta(days=365)
    today = datetime.now(tz=UTC)

    downloader = BinanceDataDownloader(
        prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
        destination_dir=Path("./data"),
        output_format="parquet",
        keep_zip=True,
        max_workers=10,
        max_processors=4,
        start_date=one_year_ago,
        end_date=today,
    )

    download_results, process_results = downloader.download()

    print(f"\nDownload Results: {len(download_results)} files")
    print(
        f"Process Results: {len(process_results[0])} successful, {len(process_results[1])} failed"
    )


def download_spot_kline():
    """
    Download spot kline data.

    Example: Download ETHUSDT 5m klines as CSV files.
    """
    downloader = BinanceDataDownloader(
        prefix="data/spot/daily/klines/ETHUSDT/1s/",
        destination_dir=Path("./data"),
        output_format="parquet",
        start_date=datetime(2024, 1, 1, tzinfo=UTC),
        end_date=datetime(2024, 1, 7, tzinfo=UTC),
        keep_zip=True,  # Keep raw ZIP files
        max_workers=10,
        max_processors=4,
    )

    download_results, process_results = downloader.download()

    print(f"\nDownload Results: {len(download_results)} files")
    print(
        f"Process Results: {len(process_results[0])} successful, {len(process_results[1])} failed"
    )


def download_spot_kline_recent_month():
    """
    Download spot kline data for the last month using datetime objects.

    Example: Download ETHUSDT 1d klines as Parquet files for the last month.
    """
    one_month_ago = datetime.now(tz=UTC) - timedelta(days=30)
    today = datetime.now(tz=UTC)

    downloader = BinanceDataDownloader(
        prefix="data/spot/daily/klines/ETHUSDT/1d/",
        destination_dir=Path("./data"),
        output_format="parquet",
        keep_zip=False,
        max_workers=10,
        max_processors=4,
        start_date=one_month_ago,
        end_date=today,
    )

    download_results, process_results = downloader.download()

    print(f"\nDownload Results: {len(download_results)} files")
    print(
        f"Process Results: {len(process_results[0])} successful, {len(process_results[1])} failed"
    )


def example_load_spot_data():
    """
    Example: Load spot kline data using BinanceDataLoader.

    Demonstrates loading data with and without resampling.
    """
    print("=== Loading Spot Data Example ===")

    # Create loader
    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="parquet",
    )

    # Get available date range
    try:
        start, end = loader.get_date_range("ETHUSDT", "1s")
        print(f"Available date range for ETHUSDT 1s: {start} to {end}")
    except Exception as e:
        print(f"Could not get date range: {e}")
        return

    # Load last 3 days of 1s data
    end_time = datetime.now(tz=UTC)
    start_time = end_time - timedelta(days=3)

    df = loader.load(
        symbol="ETHUSDT",
        interval="1s",
        start_time=start_time,
        end_time=end_time,
    )
    print(f"\nLoaded {len(df)} rows of 1s data")
    print(df.head())


def example_load_resampled_data():
    """
    Example: Load and resample kline data.

    Demonstrates resampling from 1s to 5m and 1h intervals.
    """
    print("\n=== Resampling Example ===")

    # Load 1s data and resample to 5m
    end_time = datetime.now(tz=UTC)
    start_time = end_time - timedelta(days=1)

    df_5m = load_kline_data(
        data_dir=Path("./data"),
        symbol="ETHUSDT",
        data_type="spot",
        interval="1s",
        resample_to="5m",
        start_time=start_time,
        end_time=end_time,
        output_format="parquet",
    )
    print(f"Resampled to 5m: {len(df_5m)} rows")
    print(df_5m.head())

    # Load 1s data and resample to 1h
    df_1h = load_kline_data(
        data_dir=Path("./data"),
        symbol="ETHUSDT",
        data_type="spot",
        interval="1s",
        resample_to="1h",
        start_time=start_time,
        end_time=end_time,
        output_format="parquet",
    )
    print(f"\nResampled to 1h: {len(df_1h)} rows")
    print(df_1h.head())


def example_load_futures_data():
    """
    Example: Load futures kline data.

    Demonstrates loading futures data with optional resampling.
    """
    print("\n=== Loading Futures Data Example ===")

    # Create loader for futures data
    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="futures",
        output_format="parquet",
    )

    # Get available date range
    try:
        start, end = loader.get_date_range("BTCUSDT", "1h")
        print(f"Available date range for BTCUSDT 1h: {start} to {end}")
    except Exception as e:
        print(f"Could not get date range: {e}")
        return

    # Load last week of 1h data
    end_time = datetime.now(tz=UTC)
    start_time = end_time - timedelta(days=7)

    df = loader.load(
        symbol="BTCUSDT",
        interval="1h",
        start_time=start_time,
        end_time=end_time,
    )
    print(f"\nLoaded {len(df)} rows of 1h futures data")
    print(df.head())


if __name__ == "__main__":
    pass
    # Example: Download futures kline data for the last year
    # download_futures_kline_recent_year()

    # Uncomment to download other examples:
    download_futures_kline()
    # download_futures_kline_with_dates()
    # Loader examples (run after downloading data):
    # example_load_spot_data()
    # example_load_resampled_data()
    # example_load_futures_data()
    # download_spot_kline_recent_month()
