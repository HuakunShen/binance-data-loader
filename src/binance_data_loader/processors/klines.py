"""Processor for Binance kline (OHLCV candlestick) data."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import polars as pl

from binance_data_loader.processors.base import BaseProcessor
from binance_data_loader.schema import BINANCE_KLINE_COLUMNS, BinanceKlineDataSchema
from binance_data_loader.types import AssetType, DataType
from binance_data_loader.utils import detect_timestamp_unit

# AssetType.value → S3 market path segment mapping.
# FUTURES_UM = "um" → S3 uses "futures/um"
# FUTURES_CM = "cm" → S3 uses "futures/cm"
_ASSET_TYPE_S3_PATH: dict[str, str] = {
    "spot": "spot",
    "um": "futures/um",
    "cm": "futures/cm",
    "option": "option",
}


class KlineProcessor(BaseProcessor):
    """Processor for Binance kline (OHLCV) data.

    Supports both spot and futures klines. Handles:
    - Auto-detection of header presence (futures CSVs have headers, spot don't)
    - Auto-detection of timestamp unit (ms vs ns)
    - Schema validation via BinanceKlineDataSchema (pandera)

    Output schema: open_time, open, high, low, close, volume, close_time,
                   quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore
    """

    def __init__(self, interval: str) -> None:
        """Initialize KlineProcessor.

        Args:
            interval: Kline interval string (e.g., "1s", "1m", "5m", "1h", "1d")
        """
        self.interval = interval

    @property
    def data_type(self) -> DataType:
        return DataType.KLINE

    def s3_prefix(self, symbol: str, asset_type: AssetType) -> str:
        """Return S3 prefix for kline files.

        Args:
            symbol: Trading pair (e.g., "BTCUSDT")
            asset_type: Market type

        Returns:
            e.g. "data/spot/daily/klines/BTCUSDT/1m/"
                 "data/futures/um/daily/klines/BTCUSDT/1m/"
        """
        market_path = _ASSET_TYPE_S3_PATH.get(asset_type.value, asset_type.value)
        return f"data/{market_path}/daily/klines/{symbol}/{self.interval}/"

    def output_path(self, s3_key: str, destination_dir: Path) -> Path:
        """Map an S3 kline key to a local parquet path.

        Example:
            s3_key = "data/spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2023-01-01.zip"
            → destination_dir / "spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2023-01-01.parquet"

        Args:
            s3_key: Full S3 key
            destination_dir: Root data directory

        Returns:
            Local parquet path
        """
        # Strip leading "data/" and swap .zip → .parquet
        key = s3_key.removeprefix("data/")
        return destination_dir / Path(key).with_suffix(".parquet")

    def process(self, zip_content: bytes) -> pl.DataFrame:
        """Parse kline ZIP bytes into a validated polars DataFrame.

        Steps:
        1. Extract CSV from zip
        2. Detect header (futures have header starting with "open_time", spot don't)
        3. Read CSV with polars
        4. Detect timestamp unit (ms vs ns) and normalize to ms
        5. Cast columns to correct types
        6. Validate with BinanceKlineDataSchema

        Args:
            zip_content: Raw ZIP file bytes

        Returns:
            Validated kline DataFrame

        Raises:
            ValueError: If CSV is empty or malformed
            zipfile.BadZipFile: If bytes are not a valid ZIP
        """
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            csv_bytes = zf.read(csv_name)

        if not csv_bytes:
            raise ValueError("Empty CSV file inside ZIP")

        first_line = csv_bytes.split(b"\n", 1)[0].decode("utf-8", errors="ignore")
        has_header = first_line.startswith("open_time")

        df = pl.read_csv(
            csv_bytes,
            has_header=has_header,
            new_columns=None if has_header else BINANCE_KLINE_COLUMNS,
            separator=",",
            ignore_errors=False,
        )

        if df.is_empty():
            raise ValueError("Empty DataFrame after parsing kline CSV")

        first_ts = int(df["open_time"][0])
        ts_unit = detect_timestamp_unit(first_ts)
        if ts_unit == "ns":
            df = df.with_columns(
                [
                    (pl.col("open_time").cast(pl.Int64) // 1_000)
                    .cast(pl.Int64)
                    .alias("open_time"),
                    (pl.col("close_time").cast(pl.Int64) // 1_000)
                    .cast(pl.Int64)
                    .alias("close_time"),
                ]
            )

        df = df.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime("ms", "UTC")),
                pl.col("close_time").cast(pl.Datetime("ms", "UTC")),
            ]
        )

        df = df.with_columns(
            [
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
                pl.col("quote_volume").cast(pl.Float64),
                pl.col("count").cast(pl.Int64),
                pl.col("taker_buy_volume").cast(pl.Float64),
                pl.col("taker_buy_quote_volume").cast(pl.Float64),
                pl.col("ignore").cast(pl.Int64),
            ]
        )

        return BinanceKlineDataSchema.validate(df)
