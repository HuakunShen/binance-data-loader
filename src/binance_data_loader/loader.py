"""Data loader and resampler for Binance data."""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional, Tuple
import polars as pl


class BinanceDataLoader:
    """Loader for Binance kline data with optional resampling."""

    def __init__(
        self,
        data_dir: Path,
        data_type: Literal["futures", "spot"] = "spot",
        output_format: Literal["parquet", "csv"] = "parquet",
    ):
        """
        Initialize Binance data loader.

        Args:
            data_dir: Root directory containing processed Binance data
            data_type: Type of data - "futures" or "spot"
            output_format: Format of processed files - "parquet" or "csv"
        """
        self.data_dir = Path(data_dir)
        self.data_type = data_type
        self.output_format = output_format

    def _build_path(self, symbol: str, interval: str) -> Path:
        """
        Build path to data directory for given symbol and interval.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Data interval (e.g., "1s", "1m", "1h", "1d")

        Returns:
            Path to data directory
        """
        if self.data_type == "futures":
            # Path: data_dir/futures/um/daily/klines/symbol/interval
            path = (
                self.data_dir
                / "futures"
                / "um"
                / "daily"
                / "klines"
                / symbol
                / interval
            )
        else:  # spot
            # Path: data_dir/spot/daily/klines/symbol/interval
            path = self.data_dir / "spot" / "daily" / "klines" / symbol / interval

        return path

    def get_date_range(
        self, symbol: str, interval: str = "1m"
    ) -> Tuple[datetime, datetime]:
        """
        Get the date range available in processed data files.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Base interval of the data files (e.g., "1s", "1m", "1h")

        Returns:
            Tuple of (start_date, end_date) as datetime objects

        Raises:
            FileNotFoundError: If no data files are found
            ValueError: If data directory is empty
        """
        data_path = self._build_path(symbol, interval)

        if self.output_format == "parquet":
            # Use glob pattern to only read parquet files
            pattern = str(data_path / "*.parquet")
            # Use lazy scan to find min/max dates efficiently
            # Polars pushes down aggregation to parquet level
            df = (
                pl.scan_parquet(pattern)
                .select(
                    [
                        pl.col("open_time").min().alias("min_time"),
                        pl.col("open_time").max().alias("max_time"),
                    ]
                )
                .collect()
            )

            if df.is_empty():
                raise ValueError(f"No data found for {symbol} at {interval}")

            start_date: datetime = df["min_time"].item()
            end_date: datetime = df["max_time"].item()

            return start_date, end_date
        else:  # CSV format
            # For CSV, use glob pattern to read only CSV files
            csv_files = sorted(data_path.glob("*.csv"))
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found at {data_path}")

            # Read all CSV files and concatenate
            dfs = []
            for csv_file in csv_files:
                df_csv = pl.read_csv(csv_file, try_parse_dates=True)
                dfs.append(df_csv)

            if not dfs:
                raise ValueError(f"No data found for {symbol} at {interval}")

            df = pl.concat(dfs)
            start_date = df["open_time"].min()
            end_date = df["open_time"].max()

            return start_date, end_date

    def load(
        self,
        symbol: str,
        interval: str = "1m",
        resample_to: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pl.DataFrame:
        """
        Load kline data with optional resampling and time filtering.

        Uses lazy loading for efficient processing of large datasets.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Base interval of the data files (e.g., "1s", "1m", "1h", "1d")
            resample_to: Optional resampling interval (e.g., "5s", "15s", "5m", "15m", "1h", "1d")
            start_time: Optional start datetime. If None, uses 1 year before end_time.
            end_time: Optional end datetime. If None, uses latest available data.

        Returns:
            Polars DataFrame with kline data filtered to specified time range

        Raises:
            FileNotFoundError: If no data files are found
            ValueError: If data directory is empty
        """
        data_path = self._build_path(symbol, interval)

        # Get the full date range if start_time or end_time is not specified
        if start_time is None or end_time is None:
            full_start, full_end = self.get_date_range(symbol, interval)
            if end_time is None:
                end_time = full_end
            if start_time is None:
                # Default to 1 year before end_time
                start_time = end_time - timedelta(days=365)

        # Strip timezone from filter parameters to match data (parquet data is timezone-naive)
        # This avoids schema mismatch errors when comparing timezone-aware with timezone-naive datetimes
        start_time_naive = start_time.replace(tzinfo=None)
        end_time_naive = end_time.replace(tzinfo=None)

        # Load data using lazy scan for parquet or read_csv for CSV
        if self.output_format == "parquet":
            pattern = str(data_path / "*.parquet")
            df_lazy = pl.scan_parquet(pattern)
            # Apply time filter to lazy parquet and collect to DataFrame
            df = (
                df_lazy.filter(
                    (pl.col("open_time") >= start_time_naive)
                    & (pl.col("open_time") <= end_time_naive)
                )
                .sort("open_time")
                .unique(subset=["open_time"], keep="first")
                .collect()
            )
        else:  # CSV - already loaded as DataFrame
            csv_files = sorted(data_path.glob("*.csv"))
            if not csv_files:
                raise FileNotFoundError(f"No CSV files found at {data_path}")

            # Read all CSVs and concatenate
            dfs = [pl.read_csv(f, try_parse_dates=True) for f in csv_files]
            df = pl.concat(dfs)

            # Apply time filter directly to DataFrame (no lazy operations needed)
            df = (
                df.filter(
                    (pl.col("open_time") >= start_time_naive)
                    & (pl.col("open_time") <= end_time_naive)
                )
                .sort("open_time")
                .unique(subset=["open_time"], keep="first")
            )

        # Resample if requested and interval != resample_to
        if resample_to and interval != resample_to:
            df = self.resample(df, resample_to)

        return df

    @staticmethod
    def resample(df: pl.DataFrame, interval: str) -> pl.DataFrame:
        """
        Resample klines to a different interval.

        Args:
            df: Input kline DataFrame
            interval: Target interval (e.g., "5s", "15s", "5m", "15m", "1h", "1d")

        Returns:
            Resampled DataFrame with aggregated OHLCV data
        """
        # Handle both old (count, taker_buy_volume) and new (trades, taker_buy_base_volume) column names
        count_col = pl.col("trades") if "trades" in df.columns else pl.col("count")
        taker_buy_vol_col = (
            pl.col("taker_buy_base_volume")
            if "taker_buy_base_volume" in df.columns
            else pl.col("taker_buy_volume")
        )

        return df.group_by_dynamic("open_time", every=interval, closed="left").agg(
            [
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("volume").sum().alias("volume"),
                pl.col("close_time").last().alias("close_time"),
                pl.col("quote_volume").sum().alias("quote_volume"),
                count_col.sum().alias("trades"),
                taker_buy_vol_col.sum().alias("taker_buy_base_volume"),
                pl.col("taker_buy_quote_volume").sum().alias("taker_buy_quote_volume"),
                pl.col("ignore").first().alias("ignore"),
            ]
        )


# Convenience functions for quick loading without class instantiation
def load_kline_data(
    data_dir: Path,
    symbol: str,
    data_type: Literal["futures", "spot"] = "spot",
    interval: str = "1m",
    resample_to: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    output_format: Literal["parquet", "csv"] = "parquet",
) -> pl.DataFrame:
    """
    Convenience function to load kline data.

    Args:
        data_dir: Root directory containing processed Binance data
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        data_type: Type of data - "futures" or "spot"
        interval: Base interval of the data files (e.g., "1s", "1m", "1h")
        resample_to: Optional resampling interval (e.g., "5s", "5m", "1h", "1d")
        start_time: Optional start datetime
        end_time: Optional end datetime
        output_format: Format of processed files - "parquet" or "csv"

    Returns:
        Polars DataFrame with kline data
    """
    loader = BinanceDataLoader(data_dir, data_type, output_format)
    return loader.load(symbol, interval, resample_to, start_time, end_time)


def get_date_range(
    data_dir: Path,
    symbol: str,
    data_type: Literal["futures", "spot"] = "spot",
    interval: str = "1m",
    output_format: Literal["parquet", "csv"] = "parquet",
) -> Tuple[datetime, datetime]:
    """
    Get date range available in processed data files.

    Args:
        data_dir: Root directory containing processed Binance data
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        data_type: Type of data - "futures" or "spot"
        interval: Base interval of the data files (e.g., "1s", "1m", "1h")
        output_format: Format of processed files - "parquet" or "csv"

    Returns:
        Tuple of (start_date, end_date) as datetime objects
    """
    loader = BinanceDataLoader(data_dir, data_type, output_format)
    return loader.get_date_range(symbol, interval)
