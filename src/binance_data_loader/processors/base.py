"""Abstract base class for all Binance data processors."""

from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from binance_data_loader.types import AssetType, DataType


class BaseProcessor(ABC):
    """Abstract base class for all Binance data processors.

    A processor encapsulates all knowledge about a specific data type:
    - How to build the S3 prefix for listing/downloading
    - How to map an S3 key to a local output path
    - Whether an existing output file is valid (not corrupted/incomplete)
    - How to process raw ZIP bytes into a clean DataFrame

    BinanceDataDownloader is a pure infrastructure layer (S3 listing, HTTP downloading, progress tracking),
    and all data-type specific knowledge is encapsulated in the processor that inherits from this class.
    """

    @property
    @abstractmethod
    def data_type(self) -> DataType:
        """Identifies which Binance data type this processor handles."""
        ...

    @abstractmethod
    def s3_prefix(self, symbol: str, asset_type: AssetType) -> str:
        """Return S3 prefix to list available files for this symbol/market.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            asset_type: Market type (SPOT, FUTURES_UM, etc.)

        Returns:
            S3 prefix string, e.g. "data/spot/daily/klines/BTCUSDT/1s/"
        """
        ...

    @abstractmethod
    def output_path(self, s3_key: str, destination_dir: Path) -> Path:
        """Map an S3 key to the local output file path.

        The S3 key includes the full path, e.g.:
        "data/spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2023-01-01.zip"

        Returns the corresponding local path under destination_dir.
        The output format is always parquet (no zip).

        Args:
            s3_key: Full S3 key from bucket listing
            destination_dir: Root directory for downloaded data

        Returns:
            Local path where the processed parquet file should be written
        """
        ...

    def is_output_valid(self, path: Path) -> bool:
        """Return True if the local output file exists and is not corrupted.

        Default implementation: just checks existence. Override to add data
        integrity checks (e.g. row count thresholds).

        Args:
            path: Path returned by output_path()

        Returns:
            True if file should be skipped (already processed), False to re-download
        """
        return path.exists()

    @abstractmethod
    def process(self, zip_content: bytes) -> pl.DataFrame:
        """Parse raw ZIP bytes and return a clean polars DataFrame.

        Called after each file is downloaded. The result is written directly
        to the output path — no intermediate files needed.

        Args:
            zip_content: Raw bytes of the downloaded ZIP file

        Returns:
            Clean, validated polars DataFrame ready for writing to parquet
        """
        ...
