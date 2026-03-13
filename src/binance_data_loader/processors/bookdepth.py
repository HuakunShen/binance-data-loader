"""Processor for Binance bookDepth (orderbook depth snapshots) data."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import polars as pl

from binance_data_loader.processors.base import BaseProcessor
from binance_data_loader.types import AssetType, DataType

# bookDepth CSV columns (futures have header)
# Each snapshot has 12 rows: percentage levels -5, -4, -3, -2, -1, -0.20, 0.20, 1, 2, 3, 4, 5
# representing cumulative depth at that percentage distance from midprice.
_BOOK_DEPTH_COLUMNS = ["timestamp", "percentage", "depth", "notional"]


class BookDepthProcessor(BaseProcessor):
    """Processor for Binance bookDepth data.

    Downloads daily bookDepth ZIP files and converts to parquet.

    bookDepth 是按 percentage 分层的订单簿深度快照。每个时间戳有 12 行，
    表示距离中间价不同百分比处的累计深度和名义价值。
    负数 = bid 侧, 正数 = ask 侧。

    Output schema:
        timestamp_ms (Int64)     — snapshot time in epoch milliseconds
        percentage   (Float64)   — distance from midprice (e.g., -5.0, -0.20, 0.20, 5.0)
        depth        (Float64)   — cumulative quantity at this level
        notional     (Float64)   — cumulative notional (USD) at this level
    """

    @property
    def data_type(self) -> DataType:
        return DataType.BOOK_DEPTH

    def s3_prefix(self, symbol: str, asset_type: AssetType) -> str:
        from binance_data_loader.processors.klines import _ASSET_TYPE_S3_PATH

        market_path = _ASSET_TYPE_S3_PATH.get(asset_type.value, asset_type.value)
        return f"data/{market_path}/daily/bookDepth/{symbol}/"

    def output_path(self, s3_key: str, destination_dir: Path) -> Path:
        key = s3_key.removeprefix("data/")
        return destination_dir / Path(key).with_suffix(".parquet")

    def process(self, zip_content: bytes) -> pl.DataFrame:
        """Parse bookDepth ZIP bytes into a polars DataFrame.

        Timestamp format: "2026-03-08 00:00:08" (string) → converted to epoch ms.
        """
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            csv_bytes = zf.read(csv_name)

        if not csv_bytes:
            raise ValueError("Empty CSV file inside bookDepth ZIP")

        first_line = (
            csv_bytes.split(b"\n", 1)[0].decode("utf-8", errors="ignore").strip()
        )
        has_header = first_line.startswith("timestamp")

        df = pl.read_csv(
            csv_bytes,
            has_header=has_header,
            new_columns=None if has_header else _BOOK_DEPTH_COLUMNS,
            schema_overrides={
                "percentage": pl.Float64,
                "depth": pl.Float64,
                "notional": pl.Float64,
            },
        )

        if df.is_empty():
            raise ValueError("Empty DataFrame after parsing bookDepth CSV")

        # Convert timestamp string → epoch ms
        # Format: "2026-03-08 00:00:08"
        df = df.with_columns(
            pl.col("timestamp")
            .str.to_datetime("%Y-%m-%d %H:%M:%S")
            .dt.epoch("ms")
            .alias("timestamp_ms")
        ).drop("timestamp")

        # Reorder columns
        return df.select(["timestamp_ms", "percentage", "depth", "notional"])
