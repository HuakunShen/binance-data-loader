"""Processor-first BinanceDataDownloader: pure download infrastructure."""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import polars as pl
import requests
from tqdm import tqdm

from binance_data_loader.metadata import BinanceDataMetadata
from binance_data_loader.processors.base import BaseProcessor
from binance_data_loader.types import AssetType

# Base URL for downloading (different from S3 metadata URL)
DEFAULT_DOWNLOAD_BASE_URL = "https://data.binance.vision"


@dataclass
class DownloadSummary:
    """Summary of a download run.

    Attributes:
        ok:           Files newly downloaded and processed successfully
        skipped:      Files already on disk (valid), skipped
        not_found:    Files that returned HTTP 404 (not yet available on S3)
        errors:       Files that failed due to network or processing errors
        error_details: Human-readable error messages for each failed file
    """

    ok: int = 0
    skipped: int = 0
    not_found: int = 0
    errors: int = 0
    error_details: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.ok + self.skipped + self.not_found + self.errors

    def __str__(self) -> str:
        return (
            f"ok={self.ok} skipped={self.skipped} "
            f"not_found={self.not_found} errors={self.errors}"
        )


def _download_one(
    symbol: str,
    s3_key: str,
    processor: BaseProcessor,
    destination_dir: Path,
    base_url: str,
    keep_zip: bool,
) -> tuple[str, str]:
    """Download a single file, process it, and write parquet.

    Args:
        symbol:          Trading pair symbol (used only for error messages)
        s3_key:          Full S3 key, e.g. "data/spot/daily/klines/BTCUSDT/1m/..."
        processor:       Processor that owns parsing and path logic
        destination_dir: Root output directory
        base_url:        Download base URL (not S3 listing URL)
        keep_zip:        If True, also write the raw ZIP bytes alongside the parquet

    Returns:
        Tuple (status, detail) where status ∈ {"ok", "skipped", "not_found", "error"}
        and detail is an empty string on success or an error message.
    """
    out = processor.output_path(s3_key, destination_dir)
    if processor.is_output_valid(out):
        return "skipped", ""

    url = f"{base_url}/{s3_key}"
    zip_bytes: Optional[bytes] = None
    retry_delay = 1.0

    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 404:
                return "not_found", ""
            resp.raise_for_status()
            zip_bytes = resp.content
            break
        except requests.RequestException as exc:
            if attempt < 2:
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                # Extract date from the last filename in s3_key (if possible)
                fname = s3_key.rsplit("/", 1)[-1]
                return "error", f"{symbol} {fname}: {exc}"

    if zip_bytes is None:
        return "error", f"{symbol} {s3_key}: no bytes downloaded"

    try:
        df = processor.process(zip_bytes)
    except Exception as exc:
        fname = s3_key.rsplit("/", 1)[-1]
        return "error", f"{symbol} {fname} [process]: {exc}"

    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out, compression="snappy")
    except Exception as exc:
        fname = s3_key.rsplit("/", 1)[-1]
        return "error", f"{symbol} {fname} [write]: {exc}"

    if keep_zip:
        # Write ZIP to destination_dir / s3_key-without-"data/"-prefix
        zip_path = destination_dir / s3_key.removeprefix("data/")
        try:
            zip_path.parent.mkdir(parents=True, exist_ok=True)
            zip_path.write_bytes(zip_bytes)
        except Exception:
            pass  # Ignore ZIP preservation failure as it is not fatal

    return "ok", ""


class BinanceDataDownloader:
    """Download and process Binance Vision historical data.

    Processor-first design: all data-type-specific logic (S3 path, output path,
    CSV parsing, validation) lives in the BaseProcessor subclass. This class is
    pure infrastructure — S3 listing, concurrent HTTP download, progress bars.

    Usage::

        from binance_data_loader import BinanceDataDownloader, KlineProcessor, AssetType

        summary = BinanceDataDownloader(
            symbol="BTCUSDT",
            processor=KlineProcessor(interval="1m"),
            asset_type=AssetType.FUTURES_UM,
            destination_dir=Path("/data/binance"),
        ).download()
        print(summary)  # ok=1234 skipped=56 not_found=0 errors=0
    """

    def __init__(
        self,
        symbol: str,
        processor: BaseProcessor,
        asset_type: AssetType = AssetType.SPOT,
        destination_dir: Path = Path("./data"),
        keep_zip: bool = False,
        max_workers: int = 10,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        base_url: str = DEFAULT_DOWNLOAD_BASE_URL,
    ) -> None:
        """Initialize BinanceDataDownloader.

        Args:
            symbol:          Trading pair symbol (e.g., "BTCUSDT")
            processor:       Processor instance that owns data-type logic
            asset_type:      Market type (SPOT, FUTURES_UM, etc.)
            destination_dir: Root directory for output files
            keep_zip:        Whether to keep raw ZIP bytes on disk (default False)
            max_workers:     Number of concurrent download threads (default 10)
            start_date:      Optional start datetime filter (inclusive)
            end_date:        Optional end datetime filter (inclusive; default: yesterday)
            base_url:        Download base URL (default https://data.binance.vision)
        """
        self.symbol = symbol
        self.processor = processor
        self.asset_type = asset_type
        self.destination_dir = (
            destination_dir
            if isinstance(destination_dir, Path)
            else Path(destination_dir)
        )
        self.keep_zip = keep_zip
        self.max_workers = max_workers
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = base_url

        self._metadata = BinanceDataMetadata()

    def download(self) -> DownloadSummary:
        """Fetch the file list and download/process all files.

        Files that are already valid on disk are skipped automatically.
        Ctrl+C is handled cleanly — in-flight workers are cancelled and
        already-written files remain intact.

        Returns:
            DownloadSummary with counts per status and error details
        """
        prefix = self.processor.s3_prefix(self.symbol, self.asset_type)
        print(f"\n=== {self.symbol} ({self.asset_type.value}) ===")
        print(f"Prefix      : {prefix}")
        print(f"Destination : {self.destination_dir}")

        file_list_df = self._metadata.fetch_file_list(prefix, end_date=self.end_date)

        if file_list_df.is_empty():
            print("No files found.")
            return DownloadSummary()

        # Date range filtering
        filtered_df = self._filter_by_date(file_list_df)
        if filtered_df.is_empty():
            print("No files in specified date range.")
            return DownloadSummary()

        print(f"Files to process: {len(filtered_df)}")

        summary = DownloadSummary()
        rows = filtered_df.to_dicts()

        pool = ThreadPoolExecutor(max_workers=self.max_workers)
        try:
            future_to_key = {
                pool.submit(
                    _download_one,
                    self.symbol,
                    row["Key"],
                    self.processor,
                    self.destination_dir,
                    self.base_url,
                    self.keep_zip,
                ): row["Key"]
                for row in rows
            }

            with tqdm(total=len(rows), desc="Downloading", unit="file") as pbar:
                for future in as_completed(future_to_key):
                    try:
                        status, detail = future.result()
                    except Exception as exc:
                        key = future_to_key[future]
                        status, detail = "error", f"{self.symbol} {key}: {exc}"

                    if status == "ok":
                        summary.ok += 1
                    elif status == "skipped":
                        summary.skipped += 1
                    elif status == "not_found":
                        summary.not_found += 1
                    else:
                        summary.errors += 1
                        if detail:
                            summary.error_details.append(detail)
                        tqdm.write(f"ERROR: {detail}")

                    pbar.update(1)
                    pbar.set_postfix(
                        {
                            "ok": summary.ok,
                            "skip": summary.skipped,
                            "err": summary.errors,
                        }
                    )

        except KeyboardInterrupt:
            print("\n\nInterrupted — cancelling pending tasks...")
            pool.shutdown(wait=False, cancel_futures=True)
            print("Done. Re-run anytime; completed files will be skipped.")
            os._exit(0)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        print(f"Result: {summary}")
        return summary

    def _filter_by_date(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply start_date / end_date filters to the file list DataFrame.

        Args:
            df: DataFrame with at least a "Date" column (string "YYYY-MM-DD")

        Returns:
            Filtered DataFrame
        """
        result = df
        if self.start_date is not None:
            start_str = self.start_date.strftime("%Y-%m-%d")
            result = result.filter(pl.col("Date") >= start_str)
        if self.end_date is not None:
            end_str = self.end_date.strftime("%Y-%m-%d")
            result = result.filter(pl.col("Date") <= end_str)
        return result
