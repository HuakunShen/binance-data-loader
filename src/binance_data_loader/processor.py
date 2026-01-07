"""Process downloaded ZIP files into Parquet or CSV format."""

import zipfile
from pathlib import Path
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
from tqdm import tqdm

from binance_data_loader.schema import BinanceKlineDataSchema, BINANCE_KLINE_COLUMNS
from binance_data_loader.utils import (
    detect_timestamp_unit,
    get_relative_path,
    remove_prefix_from_path,
)
from binance_data_loader.types import (
    ProcessResult,
    ProcessResultSuccess,
    ProcessResultSkipped,
    ProcessResultError,
)


class DataProcessor:
    """
    Process downloaded ZIP files into Parquet or CSV format with validation.
    """

    def __init__(self, output_format: str = "parquet"):
        """
        Initialize data processor.

        Args:
            output_format: Output format, either "parquet" or "csv" (default: "parquet")
        """
        self.output_format = output_format.lower()

        if self.output_format not in ["parquet", "csv"]:
            raise ValueError("output_format must be 'parquet' or 'csv'")

    def process_zip_file(
        self, zip_path: Path, output_dir: Path, base_data_dir: Path
    ) -> ProcessResult:
        """
        Process a single ZIP file, validate CSV data, and save as Parquet or CSV.

        Args:
            zip_path: Path to the ZIP file
            output_dir: Output directory for processed files
            base_data_dir: Base data directory to extract path structure

        Returns:
            Dictionary with processing results
        """
        try:
            with zipfile.ZipFile(str(zip_path), "r") as zip_ref:
                # Get the CSV file name (should be only one file)
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if not csv_files:
                    error_result: ProcessResultError = {
                        "status": "error",
                        "file_path": zip_path,
                        "error": "No CSV file found in ZIP",
                    }
                    return error_result

                csv_file = csv_files[0]

                # Read the CSV data as bytes
                with zip_ref.open(csv_file) as f:
                    csv_content = f.read()

            # Detect if CSV has a header (futures data has headers, spot doesn't)
            first_line = csv_content.split(b"\n", 1)[0].decode("utf-8", errors="ignore")
            has_header = first_line.startswith("open_time")

            # Read CSV with Polars
            try:
                if has_header:
                    # Futures data: has header, use column names from CSV
                    df = pl.read_csv(
                        csv_content,
                        has_header=True,
                        separator=",",
                        ignore_errors=False,
                    )
                else:
                    # Spot data: no header, manually specify columns
                    df = pl.read_csv(
                        csv_content,
                        has_header=False,
                        new_columns=BINANCE_KLINE_COLUMNS,
                        separator=",",
                        ignore_errors=False,
                    )
            except Exception as e:
                parse_error: ProcessResultError = {
                    "status": "error",
                    "file_path": zip_path,
                    "error": f"Failed to parse CSV: {str(e)}",
                }
                return parse_error

            if df.is_empty():
                empty_error: ProcessResultError = {
                    "status": "error",
                    "file_path": zip_path,
                    "error": "Empty DataFrame",
                }
                return empty_error

            # Detect timestamp unit from first row
            first_timestamp = df["open_time"][0]
            timestamp_unit = detect_timestamp_unit(first_timestamp)

            # Convert all timestamps to milliseconds (to avoid overflow issues)
            try:
                if timestamp_unit == "ns":
                    # Convert nanoseconds to milliseconds (divide by 1,000)
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

                # Convert integer timestamps to Datetime (in milliseconds, UTC)
                df = df.with_columns(
                    [
                        pl.col("open_time").cast(pl.Datetime("ms", "UTC")),
                        pl.col("close_time").cast(pl.Datetime("ms", "UTC")),
                    ]
                )
            except Exception as e:
                timestamp_error: ProcessResultError = {
                    "status": "error",
                    "file_path": zip_path,
                    "error": f"Failed to convert timestamps: {str(e)}",
                }
                return timestamp_error

            # Cast numeric columns to proper types
            try:
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
            except Exception as e:
                cast_error: ProcessResultError = {
                    "status": "error",
                    "file_path": zip_path,
                    "error": f"Failed to cast numeric columns: {str(e)}",
                }
                return cast_error

            # Validate against Pandera schema
            try:
                validated_df = BinanceKlineDataSchema.validate(df)
                df = validated_df
            except Exception as e:
                validation_error: ProcessResultError = {
                    "status": "error",
                    "file_path": zip_path,
                    "error": f"Validation failed: {str(e)}",
                }
                return validation_error

            # Determine output path
            output_path = Path(output_dir)

            zip_path_obj = Path(zip_path)
            rel_path = get_relative_path(zip_path, base_data_dir)

            if rel_path is None:
                # Fallback: extract symbol and interval from filename
                filename = zip_path_obj.stem  # e.g., BTCUSDT-1h-2024-12-31
                parts = filename.split("-")
                if len(parts) >= 2:
                    symbol = parts[0]
                    interval = parts[1]
                    rel_path = (
                        Path("spot")
                        / "daily"
                        / "klines"
                        / symbol
                        / interval
                        / f"{filename}.{self.output_format}"
                    )
                else:
                    path_error: ProcessResultError = {
                        "status": "error",
                        "file_path": zip_path,
                        "error": "Could not determine output path from filename",
                    }
                    return path_error

            # Remove 'data/' prefix if present
            if rel_path.parts[0] == "data":
                rel_path = remove_prefix_from_path(rel_path, "data")

            # Change extension to .parquet or .csv
            output_rel_path = rel_path.with_suffix(f".{self.output_format}")

            # Create the full output path
            final_path = output_path / output_rel_path

            # Skip if output file already exists
            if final_path.exists():
                skip_result: ProcessResultSkipped = {
                    "status": "skipped",
                    "file_path": zip_path,
                    "output_path": str(final_path),
                    "reason": "Output file already exists",
                }
                return skip_result

            # Create output directory
            final_path.parent.mkdir(parents=True, exist_ok=True)

            # Save file based on output format
            try:
                if self.output_format == "parquet":
                    df.write_parquet(final_path, compression="snappy")
                else:  # csv
                    df.write_csv(final_path)
            except Exception as e:
                save_error: ProcessResultError = {
                    "status": "error",
                    "file_path": zip_path,
                    "error": f"Failed to save {self.output_format}: {str(e)}",
                }
                return save_error

            success_result: ProcessResultSuccess = {
                "status": "success",
                "file_path": zip_path,
                "output_path": str(final_path),
                "rows": len(df),
                "timestamp_unit": timestamp_unit,
            }
            return success_result

        except zipfile.BadZipFile as e:
            bad_zip_error: ProcessResultError = {
                "status": "error",
                "file_path": zip_path,
                "error": f"Bad ZIP file: {str(e)}",
            }
            return bad_zip_error
        except Exception as e:
            unexpected_error: ProcessResultError = {
                "status": "error",
                "file_path": zip_path,
                "error": f"Unexpected error: {str(e)}",
            }
            return unexpected_error

    def process_zip_files(
        self,
        zip_files: List[Path],
        output_dir: Path,
        base_data_dir: Path,
        max_workers: int = 4,
    ) -> Tuple[List[ProcessResult], List[ProcessResult]]:
        """
        Process multiple ZIP files in parallel.

        Args:
            zip_files: List of ZIP file paths
            output_dir: Output directory for processed files
            base_data_dir: Base data directory to extract path structure
            max_workers: Number of parallel workers

        Returns:
            Tuple of (successful_results, failed_results)
        """
        successful = []
        failed = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_file = {
                executor.submit(
                    self.process_zip_file, zip_path, output_dir, base_data_dir
                ): zip_path
                for zip_path in zip_files
            }

            # Collect results as they complete
            with tqdm(
                total=len(zip_files),
                desc=f"Processing to {self.output_format.upper()}",
                unit="file",
            ) as pbar:
                for future in as_completed(future_to_file):
                    zip_path = future_to_file[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        result = {
                            "status": "error",
                            "file_path": zip_path,
                            "error": f"Worker process error: {str(e)}",
                        }
                    pbar.update(1)

                    # Ensure result is always a dict
                    assert isinstance(result, dict), "Result should always be a dict"

                    if result["status"] == "success":
                        successful.append(result)
                        pbar.set_postfix(
                            {"Rows": result.get("rows", 0), "Errors": len(failed)}
                        )
                    elif result["status"] == "skipped":
                        successful.append(result)
                    else:
                        failed.append(result)
                        pbar.set_postfix({"Errors": len(failed)})

        return successful, failed
