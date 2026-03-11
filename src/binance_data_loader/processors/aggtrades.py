"""Processor for Binance aggTrades data with volume resampling."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import polars as pl

from binance_data_loader.processors.base import BaseProcessor
from binance_data_loader.types import AssetType, DataType
from binance_data_loader.utils import parse_interval_ms

# aggTrades CSV columns (spot has no header; futures has header)
# 8 columns verified across all dates 2023-present.
# Column 8 (best_price_match) was always present but undocumented in Binance Vision.
_AGG_TRADE_COLUMNS = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
    "best_price_match",
]

# transact_time format changes over time:
#   <= 2024-12-31: milliseconds (ms, 13 digits), e.g. 1672563331123
#   >= 2025-01-01: microseconds (us, 16 digits), e.g. 1672563331123456
# threshold: first_ts > 10^13 → μs
_MICROSECOND_THRESHOLD = 10_000_000_000_000

# Normally, there should be about 70k rows per day; < 1000 indicates a corrupted file
_DEFAULT_MIN_VALID_ROWS = 1_000


class AggTradeProcessor(BaseProcessor):
    """Processor for Binance aggTrades data.

    Downloads daily aggTrades ZIP files and resamples them to OHLV-style
    buy/sell volume bars at a configurable resolution.

    Output schema:
        timestamp_ms (Int64)  — bar open time in epoch milliseconds
        buy_vol      (Float64) — taker-buy quantity in that bar
        sell_vol     (Float64) — taker-sell quantity in that bar
        n_trades     (UInt32)  — total trade count in that bar

    Timestamp handling:
        - Up to 2024-12-31: transact_time is milliseconds (13 digits)
        - From 2025-01-01: transact_time is microseconds (16 digits)
        Detection: first_ts > 10^13 → microseconds, else milliseconds

    is_buyer_maker interpretation:
        - False → taker is buyer → buy_vol
        - True  → taker is seller → sell_vol

    Validity check:
        Parquet file is considered valid only if row count >= min_valid_rows.
        This catches previously-downloaded files corrupted by the ms/μs bug.
    """

    def __init__(
        self,
        resample_to: str = "1s",
        min_valid_rows: int = _DEFAULT_MIN_VALID_ROWS,
    ) -> None:
        """Initialize AggTradeProcessor.

        Args:
            resample_to: Output bar resolution. Supports Xms / Xs / Xm / Xh
                         e.g. "1s", "500ms", "5s", "1m". Default "1s".
            min_valid_rows: Minimum row count for a parquet file to be considered
                            valid. Files below this threshold are re-downloaded.
        """
        self.resample_to = resample_to
        self.min_valid_rows = min_valid_rows
        # Validate format early so we get a clear error at construction time
        self._interval_ms = parse_interval_ms(resample_to)

    @property
    def data_type(self) -> DataType:
        return DataType.AGG_TRADES

    def s3_prefix(self, symbol: str, asset_type: AssetType) -> str:
        """Return S3 prefix for aggTrades files.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            asset_type: Market type

        Returns:
            e.g. "data/spot/daily/aggTrades/BTCUSDT/"
                 "data/futures/um/daily/aggTrades/BTCUSDT/"
        """
        from binance_data_loader.processors.klines import _ASSET_TYPE_S3_PATH

        market_path = _ASSET_TYPE_S3_PATH.get(asset_type.value, asset_type.value)
        return f"data/{market_path}/daily/aggTrades/{symbol}/"

    def output_path(self, s3_key: str, destination_dir: Path) -> Path:
        """Map an S3 aggTrades key to a local parquet path.

        The market path (spot / futures/um / ...) and symbol are extracted from
        the s3_key itself so this method is self-contained.

        Example:
            s3_key = "data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-01-01.zip"
            resample_to = "1s"
            → destination_dir / "spot/daily/aggTrades/BTCUSDT/1s/BTCUSDT-1s-2023-01-01.parquet"

        Args:
            s3_key: Full S3 key from bucket listing
            destination_dir: Root data directory

        Returns:
            Local parquet path
        """
        # Extract market path: everything between "data/" and "daily"
        # e.g. "data/spot/daily/..." → asset_value = "spot"
        # e.g. "data/futures/um/daily/..." → asset_value = "futures/um"
        key_body = s3_key.removeprefix("data/")
        parts = key_body.split("/")
        daily_idx = parts.index("daily")
        asset_value = "/".join(parts[:daily_idx])  # "spot" or "futures/um"

        # Mirror S3 structure exactly: just swap .zip → .parquet
        # e.g. "data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-01-01.zip"
        #    → "{dest}/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2023-01-01.parquet"
        key = s3_key.removeprefix("data/")
        return destination_dir / Path(key).with_suffix(".parquet")

    def is_output_valid(self, path: Path) -> bool:
        """Check existence AND minimum row count.

        A file is considered invalid (needs re-download) if:
        - It doesn't exist, OR
        - It has fewer than min_valid_rows rows (indicates corruption from ms/μs bug)

        Args:
            path: Path to check

        Returns:
            True if file is valid and can be skipped
        """
        if not path.exists():
            return False
        try:
            n = pl.read_parquet(path, columns=["timestamp_ms"]).height
            return n >= self.min_valid_rows
        except Exception:
            return False

    def process(self, zip_content: bytes) -> pl.DataFrame:
        """Parse aggTrades ZIP bytes and resample to volume bars.

        Steps:
        1. Extract CSV from zip
        2. Detect header (futures have header, spot don't)
        3. Read CSV, cast types
        4. Detect timestamp unit (ms vs μs) and convert to ms
        5. Floor to bar boundaries using resample_to interval
        6. Group by bar → sum buy_vol, sell_vol, count trades

        Args:
            zip_content: Raw ZIP file bytes

        Returns:
            Resampled DataFrame with columns: timestamp_ms, buy_vol, sell_vol, n_trades

        Raises:
            ValueError: If CSV is empty or malformed
            zipfile.BadZipFile: If bytes are not a valid ZIP
        """
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            csv_bytes = zf.read(csv_name)

        if not csv_bytes:
            raise ValueError("Empty CSV file inside aggTrades ZIP")

        first_line = (
            csv_bytes.split(b"\n", 1)[0].decode("utf-8", errors="ignore").strip()
        )
        has_header = first_line.startswith("agg_trade_id")

        # Try using schema_overrides first, fallback to cast if it fails
        try:
            df = pl.read_csv(
                csv_bytes,
                has_header=has_header,
                new_columns=None if has_header else _AGG_TRADE_COLUMNS,
                schema_overrides={
                    "transact_time": pl.Int64,
                    "is_buyer_maker": pl.Boolean,
                    "best_price_match": pl.Boolean,
                },
            )
        except Exception:
            df = pl.read_csv(
                csv_bytes,
                has_header=has_header,
                new_columns=None if has_header else _AGG_TRADE_COLUMNS,
            )
            df = df.with_columns(
                [
                    pl.col("transact_time").cast(pl.Int64),
                    pl.col("is_buyer_maker").cast(pl.Boolean),
                ]
            )

        if df.is_empty():
            raise ValueError("Empty DataFrame after parsing aggTrades CSV")

        df = df.with_columns(
            [
                pl.col("price").cast(pl.Float64),
                pl.col("quantity").cast(pl.Float64),
            ]
        )

        # Normalize transact_time to milliseconds
        first_ts = int(df["transact_time"][0])
        if first_ts > _MICROSECOND_THRESHOLD:
            # microseconds → milliseconds
            transact_ms = pl.col("transact_time") // 1_000
        else:
            # already milliseconds
            transact_ms = pl.col("transact_time")

        # Floor to bar boundaries: (ts_ms // interval_ms) * interval_ms
        interval_ms = self._interval_ms
        floor_expr = (transact_ms // interval_ms) * interval_ms

        df = df.with_columns(floor_expr.alias("timestamp_ms"))

        resampled = (
            df.group_by("timestamp_ms")
            .agg(
                [
                    pl.col("quantity")
                    .filter(~pl.col("is_buyer_maker"))
                    .sum()
                    .alias("buy_vol"),
                    pl.col("quantity")
                    .filter(pl.col("is_buyer_maker"))
                    .sum()
                    .alias("sell_vol"),
                    pl.len().cast(pl.UInt32).alias("n_trades"),
                ]
            )
            .sort("timestamp_ms")
            .with_columns(
                [
                    pl.col("buy_vol").cast(pl.Float64),
                    pl.col("sell_vol").cast(pl.Float64),
                ]
            )
        )

        return resampled
