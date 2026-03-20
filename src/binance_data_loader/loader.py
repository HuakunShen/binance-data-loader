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
        shift: Optional[str] = None,
        skip_first: bool = True,
    ):
        """
        Initialize Binance data loader.

        Args:
            data_dir: Root directory containing processed Binance data
            data_type: Type of data - "futures" or "spot"
            output_format: Format of processed files - "parquet" or "csv"
            shift: Optional time offset to shift resampling interval boundaries.
                For example, with shift="1m" and resample_to="15m",
                intervals will end at 1, 16, 31, 46 minutes instead of 0, 15, 30, 45.
                Supports units: "1s", "5m", "1h", "1d", etc.
                Default is None (no shift).
            skip_first: Whether to skip the first row if it contains less data than
                the full resample interval (useful when shifting causes partial intervals).
                Default is True.
        """
        self.data_dir = Path(data_dir)
        self.data_type = data_type
        self.output_format = output_format
        self.shift = shift
        self.skip_first = skip_first

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
        shift: Optional[str] = None,
        skip_first: Optional[bool] = None,
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
            shift: Optional override for shift. If None, uses value from __init__.
            skip_first: Optional override for skip_first. If None, uses value from __init__.
                Whether to skip the first row if it contains less data than
                full resample interval (useful when shifting causes partial intervals).

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
            shift_param = shift if shift is not None else self.shift
            skip_first_param = skip_first if skip_first is not None else self.skip_first
            df = self.resample(
                df, resample_to, shift=shift_param, skip_first=skip_first_param
            )

        return df

    def load_aggtrades(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        source: Optional[Literal["spot", "futures"]] = None,
    ) -> pl.DataFrame:
        """Load aggTrades OFI data (buy_vol, sell_vol, n_trades) resampled to 1s.

        Files are at: {data_dir}/{market}/daily/aggTrades/{symbol}/...parquet
        Schema: timestamp_ms (Int64), buy_vol (Float64), sell_vol (Float64), n_trades (UInt32)

        Args:
            symbol: Trading pair (e.g. "BTCUSDT").
            start_time: Optional start filter.
            end_time: Optional end filter.
            source: "spot" or "futures". Defaults to self.data_type.

        Returns a DataFrame with columns:
            timestamp (Datetime ms UTC) — 1s bar open time, matches kline open_time
            buy_vol   (Float64)
            sell_vol  (Float64)
            n_trades  (UInt32)

        Raises:
            FileNotFoundError: if the aggTrades directory does not exist.
        """
        market = source or self.data_type
        if market == "futures":
            aggtrades_dir = (
                self.data_dir / "futures" / "um" / "daily" / "aggTrades" / symbol
            )
        else:
            aggtrades_dir = self.data_dir / "spot" / "daily" / "aggTrades" / symbol
        if not aggtrades_dir.exists():
            raise FileNotFoundError(
                f"aggTrades directory not found: {aggtrades_dir}\n"
                "Run packages/data/download-binance.py first."
            )

        pattern = str(aggtrades_dir / "*.parquet")
        df = pl.scan_parquet(pattern)

        # Convert Int64 ms → Datetime for consistent filtering/joining with klines
        df = df.with_columns(
            pl.from_epoch("timestamp_ms", time_unit="ms").alias("timestamp")
        )

        # Date range filter (strip tz to match naive parquet timestamps)
        if start_time is not None:
            st = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
            df = df.filter(pl.col("timestamp") >= st)
        if end_time is not None:
            et = end_time.replace(tzinfo=None) if end_time.tzinfo else end_time
            df = df.filter(pl.col("timestamp") <= et)

        return (
            df.select(["timestamp", "buy_vol", "sell_vol", "n_trades"])
            .sort("timestamp")
            .collect()
        )

    def load_bookdepth(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        source: Optional[Literal["spot", "futures"]] = None,
    ) -> pl.DataFrame:
        """Load raw bookDepth snapshots.

        Files are at: {data_dir}/futures/um/daily/bookDepth/{symbol}/...parquet
        Schema: timestamp_ms, percentage, depth, notional

        Spot Binance Vision does not provide the same processed bookDepth layout in
        this repo, so this loader is currently futures-only.
        """
        market = source or self.data_type
        if market != "futures":
            raise ValueError("load_bookdepth currently supports futures data only")

        depth_dir = self.data_dir / "futures" / "um" / "daily" / "bookDepth" / symbol
        if not depth_dir.exists():
            raise FileNotFoundError(
                f"bookDepth directory not found: {depth_dir}\n"
                "Run packages/data/download-binance.py first."
            )

        pattern = str(depth_dir / "*.parquet")
        df = pl.scan_parquet(pattern).with_columns(
            pl.from_epoch("timestamp_ms", time_unit="ms").alias("timestamp")
        )

        if start_time is not None:
            st = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
            df = df.filter(pl.col("timestamp") >= st)
        if end_time is not None:
            et = end_time.replace(tzinfo=None) if end_time.tzinfo else end_time
            df = df.filter(pl.col("timestamp") <= et)

        return (
            df.select(["timestamp", "percentage", "depth", "notional"])
            .sort(["timestamp", "percentage"])
            .collect()
        )

    @staticmethod
    def resample(
        df: pl.DataFrame,
        interval: str,
        shift: Optional[str] = None,
        skip_first: bool = True,
    ) -> pl.DataFrame:
        """
        Resample klines to a different interval.

        Args:
            df: Input kline DataFrame
            interval: Target interval (e.g., "5s", "15s", "5m", "15m", "1h", "1d")
            shift: Optional time offset to shift interval boundaries.
                For example, with shift="1m" and interval="15m",
                intervals will end at 1, 16, 31, 46 minutes instead of 0, 15, 30, 45.
                Supports units: "1s", "5m", "1h", "1d", etc.
                Default is None (no shift).
            skip_first: Whether to skip the first row if it contains less data than
                the full resample interval (useful when shifting causes partial intervals).
                Default is True.

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

        # Parse shift string to timedelta if provided
        shift_delta = None
        if shift:
            shift_delta = BinanceDataLoader._parse_shift_to_timedelta(shift)

        # Parse interval to timedelta for duration comparison
        interval_delta = BinanceDataLoader._parse_interval_to_timedelta(interval)

        # If shift is specified, create a shifted timestamp for grouping
        if shift_delta:
            df_shifted = df.with_columns(
                (pl.col("open_time") - shift_delta).alias("_shifted_time")
            )
            result = df_shifted.group_by_dynamic(
                "_shifted_time", every=interval, closed="left"
            ).agg(
                [
                    pl.col("_shifted_time").first().alias("open_time"),
                    pl.col("open").first().alias("open"),
                    pl.col("high").max().alias("high"),
                    pl.col("low").min().alias("low"),
                    pl.col("close").last().alias("close"),
                    pl.col("volume").sum().alias("volume"),
                    pl.col("close_time").last().alias("close_time"),
                    pl.col("quote_volume").sum().alias("quote_volume"),
                    count_col.sum().alias("trades"),
                    taker_buy_vol_col.sum().alias("taker_buy_base_volume"),
                    pl.col("taker_buy_quote_volume")
                    .sum()
                    .alias("taker_buy_quote_volume"),
                    pl.col("ignore").first().alias("ignore"),
                ]
            )
            # Add the shift back to get the actual open_time
            result = result.with_columns(
                (pl.col("open_time") + shift_delta).alias("open_time"),
                (pl.col("close_time") + shift_delta).alias("close_time"),
            )

            # Skip first row if it's a partial interval
            if skip_first and len(result) > 0:
                # Calculate duration of first interval
                first_duration = result.select(
                    (pl.col("close_time") - pl.col("open_time"))
                ).row(0)[0]
                # If first interval duration is less than 80% of target interval, skip it
                if first_duration < interval_delta * 0.8:
                    result = result.slice(1)

            return result
        else:
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
                    pl.col("taker_buy_quote_volume")
                    .sum()
                    .alias("taker_buy_quote_volume"),
                    pl.col("ignore").first().alias("ignore"),
                ]
            )

    @staticmethod
    def _parse_shift_to_timedelta(shift: str) -> timedelta:
        """
        Parse a shift string (e.g., "1m", "30s", "2h", "1d") to a timedelta.

        Args:
            shift: Shift string with value and unit (e.g., "1m", "30s", "2h", "1d")

        Returns:
            timedelta representing the shift

        Raises:
            ValueError: If shift format is invalid
        """
        if not shift or len(shift) < 2:
            raise ValueError(
                f"Invalid shift format: '{shift}'. Expected format: '1m', '30s', '2h', '1d', etc."
            )

        unit = shift[-1].lower()
        try:
            value = int(shift[:-1])
        except ValueError:
            raise ValueError(f"Invalid shift value in '{shift}'. Expected integer.")

        unit_map = {
            "s": ("seconds", value),
            "m": ("minutes", value),
            "h": ("hours", value),
            "d": ("days", value),
        }

        if unit not in unit_map:
            raise ValueError(
                f"Invalid shift unit in '{shift}'. Expected 's', 'm', 'h', or 'd'."
            )

        unit_name, unit_value = unit_map[unit]
        return timedelta(**{unit_name: unit_value})

    @staticmethod
    def _parse_interval_to_timedelta(interval: str) -> timedelta:
        """
        Parse an interval string (e.g., "15m", "1h", "1d") to a timedelta.

        Args:
            interval: Interval string with value and unit (e.g., "1m", "30s", "2h", "1d")

        Returns:
            timedelta representing the interval

        Raises:
            ValueError: If interval format is invalid
        """
        if not interval or len(interval) < 2:
            raise ValueError(
                f"Invalid interval format: '{interval}'. Expected format: '1m', '30s', '2h', '1d', etc."
            )

        unit = interval[-1].lower()
        try:
            value = int(interval[:-1])
        except ValueError:
            raise ValueError(
                f"Invalid interval value in '{interval}'. Expected integer."
            )

        unit_map = {
            "s": ("seconds", value),
            "m": ("minutes", value),
            "h": ("hours", value),
            "d": ("days", value),
            "w": ("weeks", value),
        }

        if unit not in unit_map:
            raise ValueError(
                f"Invalid interval unit in '{interval}'. Expected 's', 'm', 'h', 'd', or 'w'."
            )

        unit_name, unit_value = unit_map[unit]
        return timedelta(**{unit_name: unit_value})


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
    shift: Optional[str] = None,
    skip_first: bool = True,
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
        shift: Optional time offset to shift resampling interval boundaries.
            For example, with shift="1m" and resample_to="15m",
            intervals will end at 1, 16, 31, 46 minutes instead of 0, 15, 30, 45.
            Supports units: "1s", "5m", "1h", "1d", etc.
            Default is None (no shift).
        skip_first: Whether to skip the first row if it contains less data than
            the full resample interval (useful when shifting causes partial intervals).
            Default is True.

    Returns:
        Polars DataFrame with kline data
    """
    loader = BinanceDataLoader(data_dir, data_type, output_format, shift, skip_first)
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
