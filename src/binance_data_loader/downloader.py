"""Main downloader class for Binance Vision data."""

import glob
import time
import requests
import polars as pl
from datetime import datetime
from pathlib import Path
from typing import List, Literal, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from binance_data_loader.metadata import BinanceDataMetadata
from binance_data_loader.processor import DataProcessor
from binance_data_loader.types import (
    DownloadResult,
    ProcessResult,
    DownloadResultSuccess,
    DownloadResultSkipped,
    DownloadResultError,
)


class BinanceDataDownloader:
    """
    Download and process Binance Vision historical data.

    This is the main API for binance-data library. It handles:
    1. Fetching file metadata from Binance S3 bucket
    2. Downloading ZIP files concurrently
    3. Converting to Parquet or CSV format with validation
    4. Optional cleanup of raw ZIP files
    """

    def __init__(
        self,
        prefix: str,
        destination_dir: Path = Path("./data"),
        output_format: Literal["parquet", "csv"] = "parquet",
        keep_zip: bool = True,
        max_workers: int = 10,
        max_processors: int = 4,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        skip_download: bool = False,
        base_url: str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
    ):
        """
        Initialize Binance data downloader.

        Args:
            prefix: Required prefix for data download
                   (e.g., "data/futures/um/daily/klines/BTCUSDT/1h/")
            destination_dir: Output directory for processed files (default: "./data")
            output_format: Output format, either "parquet" or "csv" (default: "parquet")
            keep_zip: Whether to keep raw ZIP files after processing (default: True)
            max_workers: Number of concurrent download workers (default: 10)
            max_processors: Number of parallel processing workers (default: 4)
            start_date: Optional start datetime for filtering. Only download files from this date onwards.
            end_date: Optional end datetime for filtering. Only download files up to this date.
            skip_download: If True, skip downloading and only process existing ZIP files (default: False)
            base_url: Base URL for Binance data S3 bucket
        """
        self.prefix = prefix
        self.destination_dir = (
            destination_dir
            if isinstance(destination_dir, Path)
            else Path(destination_dir)
        )
        self.output_format = output_format.lower()
        self.keep_zip = keep_zip
        self.max_workers = max_workers
        self.max_processors = max_processors
        self.base_url = base_url
        self.start_date = start_date
        self.end_date = end_date
        self.skip_download = skip_download

        # Validate output format
        if self.output_format not in ["parquet", "csv"]:
            raise ValueError("output_format must be 'parquet' or 'csv'")

        # Initialize components
        self.metadata_fetcher = BinanceDataMetadata()
        self.data_processor = DataProcessor(output_format=self.output_format)

    def download(
        self,
    ) -> Tuple[List[DownloadResult], Tuple[List[ProcessResult], List[ProcessResult]]]:
        """
        Download and process Binance data.

        Returns:
            Tuple of (download_results, (process_successful, process_failed))
            - download_results: List of download success/failure info
            - process_successful: List of successfully processed files
            - process_failed: List of failed process attempts
        """
        print(f"Starting download for prefix: {self.prefix}")
        print(f"Output format: {self.output_format.upper()}")
        print(f"Destination: {self.destination_dir}")

        # Step 1: Fetch file list
        print("\n=== Step 1: Fetching file list ===")
        date_range = ""
        if self.start_date or self.end_date:
            date_range = f" ({self.start_date.strftime('%Y-%m-%d') if self.start_date else self.start_date} to {self.end_date.strftime('%Y-%m-%d') if self.end_date else self.end_date or 'yesterday'})"
        print(f"Time range{date_range}")
        file_list_df = self.metadata_fetcher.fetch_file_list(
            self.prefix, end_date=self.end_date
        )

        if file_list_df.is_empty():
            print("No files found for given prefix.")
            return [], ([], [])

        print(f"\nFound {len(file_list_df)} files in metadata")

        # Step 2: Download ZIP files (only if not skipping and files don't exist)
        if self.skip_download:
            print("\n=== Step 2: Skipping download ===")
            print("Skip download mode enabled. Using existing ZIP files.")
            download_results = []
        else:
            print("\n=== Step 2: Downloading ZIP files ===")
            download_results = self._download_files(file_list_df)

        # Count download results
        successful_downloads = sum(
            1 for r in download_results if r["status"] == "success"
        )
        skipped_downloads = sum(1 for r in download_results if r["status"] == "skipped")
        failed_downloads = (
            len(download_results) - successful_downloads - skipped_downloads
        )

        if not self.skip_download:
            print(
                f"\nDownload complete: {successful_downloads} successful, {skipped_downloads} skipped, {failed_downloads} failed"
            )

        # Step 3: Process all ZIP files (not just newly downloaded ones)
        print("\n=== Step 3: Processing files ===")

        # Get all existing ZIP files in the destination directory
        all_zip_files = self._get_existing_zip_files()

        if not all_zip_files:
            print("No ZIP files found to process.")
            return download_results, ([], [])

        print(f"Found {len(all_zip_files)} ZIP files to process")

        process_results = self.data_processor.process_zip_files(
            zip_files=all_zip_files,
            output_dir=self.destination_dir,
            base_data_dir=self.destination_dir,
            max_workers=self.max_processors,
        )

        # Count process results
        successful_processes = sum(
            1 for r in process_results[0] if r["status"] == "success"
        )
        skipped_processes = sum(
            1 for r in process_results[0] if r["status"] == "skipped"
        )
        failed_processes = len(process_results[1])

        print(
            f"\nProcessing complete: {successful_processes} successful, {skipped_processes} skipped, {failed_processes} failed"
        )

        # Step 4: Clean up ZIP files if requested
        if not self.keep_zip and not self.skip_download:
            print("\n=== Step 4: Cleaning up ZIP files ===")
            # Only clean up newly downloaded files
            newly_downloaded = [
                Path(r["zip_path"])
                for r in download_results
                if r["status"] == "success"
            ]
            cleanup_count = self._cleanup_zip_files(newly_downloaded)
            print(f"Deleted {cleanup_count} ZIP files")

        return download_results, process_results

    def _download_files(self, file_list_df: pl.DataFrame) -> List[DownloadResult]:
        """
        Download files concurrently.

        Args:
            file_list_df: Polars DataFrame with file metadata

        Returns:
            List of download results
        """
        # Filter files by date range before downloading
        filtered_df = file_list_df
        if self.start_date or self.end_date:
            if self.start_date:
                start_str = self.start_date.strftime("%Y-%m-%d")
                filtered_df = filtered_df.filter(pl.col("Date") >= start_str)

            if self.end_date:
                end_str = self.end_date.strftime("%Y-%m-%d")
                filtered_df = filtered_df.filter(pl.col("Date") <= end_str)

        # Update file list after filtering
        file_list_df = filtered_df

        # If no files after filtering, return early
        if file_list_df.is_empty():
            print("No files found in specified date range.")
            return []

        results = []
        total_size = file_list_df["Size"].sum()
        downloaded_bytes = 0

        # Prepare download tasks
        download_tasks = []
        for row in file_list_df.iter_rows(named=True):
            key = row["Key"]
            size = row["Size"]
            # Remove 'data/' prefix from key if present to avoid double prefix
            key_without_prefix = key.lstrip("data/")
            output_path = self.destination_dir / key_without_prefix
            download_tasks.append((key, output_path, size))

        # Download with progress tracking
        with tqdm(
            total=len(download_tasks), desc="Downloading files", unit="file"
        ) as file_pbar:
            with tqdm(
                total=total_size, desc="Download progress", unit="B", unit_scale=True
            ) as bytes_pbar:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all download tasks
                    future_to_task = {
                        executor.submit(self._download_single_file, task): task
                        for task in download_tasks
                    }

                    # Process results as they complete
                    for future in as_completed(future_to_task):
                        task = future_to_task[future]
                        key, output_path, size = task

                        try:
                            result = future.result()
                            results.append(result)

                            if result["status"] == "success":
                                downloaded_bytes += size
                                bytes_pbar.update(size)

                            file_pbar.update(1)

                            # Update progress
                            percentage = (downloaded_bytes / total_size) * 100
                            file_pbar.set_postfix(
                                {
                                    "Size": f"{percentage:.1f}%",
                                }
                            )

                        except Exception as e:
                            results.append(
                                {
                                    "status": "error",
                                    "key": key,
                                    "error": f"Task error: {str(e)}",
                                }
                            )
                            file_pbar.update(1)

        print(f"\nDownloaded {downloaded_bytes / (1024 * 1024):.2f} MB of data")

        return results

    def _download_single_file(self, task: Tuple[str, Path, int]) -> DownloadResult:
        """
        Download a single file.

        Args:
            task: Tuple of (key, output_path, size)

        Returns:
            Download result dictionary
        """
        key, output_path, size = task

        # Skip if file already exists
        if output_path.exists():
            result: DownloadResultSkipped = {
                "status": "skipped",
                "key": key,
                "zip_path": str(output_path),
                "reason": "File already exists",
            }
            return result

        # Create directory if it doesn't exist
        output_dir = output_path.parent
        if output_dir != Path("."):
            output_dir.mkdir(parents=True, exist_ok=True)

        # Download with retry logic
        max_retries = 3
        retry_delay = 1  # seconds

        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}/{key}"
                response = requests.get(url, timeout=60)
                response.raise_for_status()

                with open(output_path, "wb") as f:
                    f.write(response.content)

                success_result: DownloadResultSuccess = {
                    "status": "success",
                    "key": key,
                    "zip_path": output_path,
                    "size": size,
                }
                return success_result

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    error_result: DownloadResultError = {
                        "status": "error",
                        "key": key,
                        "error": str(e),
                    }
                    return error_result

        max_retry_error: DownloadResultError = {
            "status": "error",
            "key": key,
            "error": "Max retries exceeded",
        }
        return max_retry_error

    def _cleanup_zip_files(self, zip_files: List[Path]) -> int:
        """
        Delete downloaded ZIP files.

        Args:
            zip_files: List of ZIP file paths to delete

        Returns:
            Number of files deleted
        """
        deleted_count = 0

        for zip_path in zip_files:
            try:
                if zip_path.exists():
                    zip_path.unlink()
                    deleted_count += 1
            except Exception as e:
                print(f"Warning: Failed to delete {zip_path}: {e}")

        return deleted_count

    def _get_existing_zip_files(self) -> List[Path]:
        """
        Get list of existing ZIP files in the destination directory that match the prefix.

        Returns:
            List of ZIP file paths
        """
        # Try to find ZIP files at both possible locations:
        # 1. New location: destination_dir / key_without_prefix (e.g., ./data/futures/...)
        # 2. Old location: destination_dir / full_key (e.g., ./data/data/futures/...)

        # New location pattern (remove 'data/' from prefix)
        prefix_without_data = self.prefix.lstrip("data/")
        pattern_new = str(self.destination_dir / prefix_without_data / "*.zip")

        # Old location pattern (keep full prefix)
        pattern_old = str(self.destination_dir / self.prefix / "*.zip")

        # Find all matching ZIP files from both locations
        zip_files = []
        zip_files.extend([Path(f) for f in glob.glob(pattern_new, recursive=True)])
        zip_files.extend([Path(f) for f in glob.glob(pattern_old, recursive=True)])

        # Deduplicate while preserving order
        seen = set()
        unique_zip_files = []
        for f in zip_files:
            if str(f) not in seen:
                seen.add(str(f))
                unique_zip_files.append(f)

        return unique_zip_files
