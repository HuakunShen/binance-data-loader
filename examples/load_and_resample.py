"""
Example: Load and resample kline data.

This script demonstrates how to load downloaded Binance data,
get date ranges, and resample to different timeframes.
"""

from datetime import datetime, timedelta, UTC
from binance_data_loader import BinanceDataLoader, load_kline_data
from pathlib import Path


def example_load_spot_data():
    """Example: Load spot kline data without resampling."""
    print("=== Loading Spot Data Example ===")

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
    print(f"\nData shape: {df.shape}")


def example_resample_to_higher_timeframes():
    """Example: Resample 1s data to 5m, 15m, and 1h intervals."""
    print("\n=== Resampling Example ===")

    end_time = datetime.now(tz=UTC)
    start_time = end_time - timedelta(days=1)

    # Resample to 5m
    print("\n--- Resampling to 5m ---")
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

    # Resample to 15m
    print("\n--- Resampling to 15m ---")
    df_15m = load_kline_data(
        data_dir=Path("./data"),
        symbol="ETHUSDT",
        data_type="spot",
        interval="1s",
        resample_to="15m",
        start_time=start_time,
        end_time=end_time,
        output_format="parquet",
    )
    print(f"Resampled to 15m: {len(df_15m)} rows")
    print(df_15m.head())

    # Resample to 1h
    print("\n--- Resampling to 1h ---")
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
    print(f"Resampled to 1h: {len(df_1h)} rows")
    print(df_1h.head())


def example_load_futures_data():
    """Example: Load futures kline data."""
    print("\n=== Loading Futures Data Example ===")

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
    print(f"\nData shape: {df.shape}")


def example_combined_workflow():
    """Example: Complete workflow - load, resample, and compare."""
    print("\n=== Combined Workflow Example ===")

    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="parquet",
    )

    # Get available date range
    start, end = loader.get_date_range("ETHUSDT", "1m")
    print(f"Available data range: {start} to {end}")

    # Load 1 week of 1m data
    end_time = datetime.now(tz=UTC)
    start_time = end_time - timedelta(days=7)

    df_1m = loader.load("ETHUSDT", "1m", start_time=start_time, end_time=end_time)
    df_5m = loader.load("ETHUSDT", "1m", "5m", start_time, end_time)
    df_1h = loader.load("ETHUSDT", "1m", "1h", start_time, end_time)

    print(f"\n1m data: {len(df_1m)} rows")
    print(f"5m resampled: {len(df_5m)} rows")
    print(f"1h resampled: {len(df_1h)} rows")

    print("\nReduction ratio:")
    print(f"  1m -> 5m: {len(df_1m) / len(df_5m):.1f}x reduction")
    print(f"  1m -> 1h: {len(df_1m) / len(df_1h):.1f}x reduction")


def example_specific_date_range():
    """Example: Load data for specific date range."""
    print("\n=== Specific Date Range Example ===")

    df = load_kline_data(
        data_dir=Path("./data"),
        symbol="ETHUSDT",
        data_type="spot",
        interval="1s",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 7, tzinfo=UTC),
        output_format="parquet",
    )

    print("Loaded data from 2024-01-01 to 2024-01-07")
    print(f"Shape: {df.shape}")
    print(df.head())


def example_shifted_resampling():
    """Example: Resample with shifted interval boundaries."""
    print("\n=== Shifted Resampling Example ===")

    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="parquet",
    )

    # Load with no shift - intervals end at 0, 15, 30, 45 minutes
    print("\n--- Standard 15m intervals (no shift) ---")
    df_standard = loader.load(
        symbol="ETHUSDT",
        interval="1m",
        resample_to="15m",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
    )
    print(
        f"Standard intervals: {df_standard.select('open_time').to_series().to_list()[:3]}"
    )
    print(f"Shape: {df_standard.shape}")

    # Load with 1 minute shift - intervals end at 1, 16, 31, 46 minutes
    print("\n--- 15m intervals shifted by 1 minute ---")
    df_shifted_1m = loader.load(
        symbol="ETHUSDT",
        interval="1m",
        resample_to="15m",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        shift="1m",
    )
    print(
        f"Shifted intervals: {df_shifted_1m.select('open_time').to_series().to_list()[:3]}"
    )
    print(f"Shape: {df_shifted_1m.shape}")

    # Load with 30 seconds shift - intervals end at 00:30, 15:30, 30:30, 45:30
    print("\n--- 15m intervals shifted by 30 seconds ---")
    df_shifted_30s = loader.load(
        symbol="ETHUSDT",
        interval="1m",
        resample_to="15m",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        shift="30s",
    )
    print(
        f"Shifted intervals: {df_shifted_30s.select('open_time').to_series().to_list()[:3]}"
    )
    print(f"Shape: {df_shifted_30s.shape}")


def example_skip_partial_intervals():
    """Example: Skip partial intervals when shifting."""
    print("\n=== Skip Partial Intervals Example ===")

    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="parquet",
    )

    # Load with shift and skip_first=True (default)
    # This removes the first row if it contains less than 80% of the interval
    print("\n--- With shift='1m' and skip_first=True (default) ---")
    df_with_skip = loader.load(
        symbol="ETHUSDT",
        interval="1m",
        resample_to="4h",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        shift="1m",
    )
    print(f"Shape: {df_with_skip.shape}")
    print(
        f"First 3 open times: {df_with_skip.select('open_time').to_series().to_list()[:3]}"
    )
    print(df_with_skip.head(3))

    # Load with shift and skip_first=False
    # This keeps all rows including partial first interval
    print("\n--- With shift='1m' and skip_first=False ---")
    df_without_skip = loader.load(
        symbol="ETHUSDT",
        interval="1m",
        resample_to="4h",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        shift="1m",
        skip_first=False,
    )
    print(f"Shape: {df_without_skip.shape}")
    print(
        f"First 3 open times: {df_without_skip.select('open_time').to_series().to_list()[:3]}"
    )
    print(df_without_skip.head(3))

    print("\nNote: skip_first=True removes the first row if it contains")
    print("less than 80% of the full resample interval (partial interval).")


def example_training_data_generation():
    """Example: Generate multiple shifted datasets for training."""
    print("\n=== Training Data Generation Example ===")

    loader = BinanceDataLoader(
        data_dir=Path("./data"),
        data_type="spot",
        output_format="parquet",
    )

    start_time = datetime(2024, 1, 1, tzinfo=UTC)
    end_time = datetime(2024, 1, 2, tzinfo=UTC)

    print("Generating multiple shifted 15m datasets for data augmentation...")

    # Generate 4 different shifted versions for training
    shifts = ["0m", "1m", "2m", "3m"]
    datasets = {}

    for shift in shifts:
        df = loader.load(
            symbol="ETHUSDT",
            interval="1m",
            resample_to="15m",
            start_time=start_time,
            end_time=end_time,
            shift=shift,
        )
        datasets[shift] = df
        print(
            f"  Shift {shift}: {len(df)} rows, first at {df.select('open_time').item()}"
        )

    print(f"\nGenerated {len(datasets)} datasets for training data augmentation")
    print("Each dataset represents the same underlying data with different")
    print("interval alignments, useful for model robustness testing.")


if __name__ == "__main__":
    # Example 1: Load spot data without resampling
    # example_load_spot_data()

    # Example 2: Resample to higher timeframes
    # example_resample_to_higher_timeframes()

    # Example 3: Load futures data
    # example_load_futures_data()

    # Example 4: Complete workflow with comparison
    # example_combined_workflow()

    # Example 5: Load specific date range
    # example_specific_date_range()

    # Run all examples
    example_load_spot_data()
    example_resample_to_higher_timeframes()
    example_load_futures_data()
    example_combined_workflow()
    example_specific_date_range()

    # Example 6: Shifted resampling
    # example_shifted_resampling()

    # Example 7: Skip partial intervals
    # example_skip_partial_intervals()

    # Example 8: Generate training data
    # example_training_data_generation()

    # Run all examples
    example_load_spot_data()
    example_resample_to_higher_timeframes()
    example_load_futures_data()
    example_combined_workflow()
    example_specific_date_range()
    example_shifted_resampling()
    example_skip_partial_intervals()
    example_training_data_generation()
