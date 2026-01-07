"""Fetch metadata about available Binance data files."""

import re
import requests
import xmltodict
import polars as pl
from datetime import datetime, timedelta, UTC
from typing import List, Dict, Optional
from tqdm import tqdm


class BinanceDataMetadata:
    """
    Fetch metadata about available Binance data files from S3 bucket.
    """

    _BASE_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
    _MAX_KEYS_PER_REQUEST = 1000

    def __init__(self):
        """Initialize metadata fetcher."""
        self._files: List[Dict] = []

    def fetch_file_list(
        self, prefix: str, end_date: Optional[datetime] = None
    ) -> pl.DataFrame:
        """
        Fetch list of available files for a given prefix.

        Args:
            prefix: S3 prefix to fetch files for (e.g., "data/futures/um/daily/klines/BTCUSDT/1h/")
            end_date: Optional stop datetime. If not provided,
                      defaults to yesterday.

        Returns:
            Polars DataFrame with file metadata (Key, LastModified, Size, ETag, Date)
        """
        url = f"{self._BASE_URL}?delimiter=/&prefix={prefix}"

        all_files = []
        marker = None
        request_count = 0

        # Calculate stop date string
        if end_date is None:
            stop_date_obj = datetime.now(tz=UTC) - timedelta(days=1)
            stop_date_str = stop_date_obj.strftime("%Y-%m-%d")
        else:
            # Handle both string and datetime inputs
            if isinstance(end_date, datetime):
                stop_date_str = end_date.strftime("%Y-%m-%d")
            else:
                stop_date_str = end_date

        today = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        latest_date_found = None
        filtered_count = 0

        print(f"Fetching metadata for prefix: {prefix}")

        with tqdm(desc="Retrieving file metadata", unit="page") as pbar:
            while True:
                # Construct URL with marker if needed
                request_url = url
                if marker:
                    request_url += f"&marker={marker}"

                # Make request
                try:
                    response = requests.get(request_url, timeout=30)
                    response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching data: {e}")
                    break

                request_count += 1

                # Parse XML response
                data = xmltodict.parse(response.content)

                # Get contents list
                contents = data.get("ListBucketResult", {}).get("Contents", [])

                # Handle case where there's only one item
                if isinstance(contents, dict):
                    contents = [contents]

                if not contents:
                    break

                # Process each file
                for content in contents:
                    key = content.get("Key")
                    if not key:
                        continue

                    # Only process zip files
                    if not key.endswith(".zip"):
                        continue

                    last_modified = content.get("LastModified")
                    if not last_modified:
                        continue

                    size = content.get("Size")
                    if not size:
                        continue

                    etag = content.get("ETag")
                    if not etag:
                        continue

                    # Extract date from filename (format: BTCUSDT-1h-2021-05-13.zip)
                    filename = key.split("/")[-1]

                    # Use regex to safely extract date parts
                    match = re.search(r"(\d{4})-(\d{2})-(\d{2})\.zip$", filename)
                    if match:
                        year, month, day = match.groups()
                        try:
                            # Validate date
                            date_str = f"{year}-{month}-{day}"

                            # Skip future dates
                            if date_str > today:
                                filtered_count += 1
                                continue

                            all_files.append(
                                {
                                    "Key": key,
                                    "LastModified": last_modified,
                                    "Size": int(size),
                                    "ETag": etag.strip('"'),
                                    "Date": date_str,
                                }
                            )

                            latest_date_found = date_str
                        except ValueError:
                            # Skip files with invalid dates
                            print(
                                f"Warning: Skipping file with invalid date: {filename}"
                            )
                            filtered_count += 1
                    else:
                        # Unrecognized filename format
                        print(f"Warning: Unrecognized filename format: {filename}")

                # Update marker for next request
                if contents:
                    marker = contents[-1].get("Key")
                    if not marker:
                        break

                # Update progress bar
                pbar.update(1)
                pbar.set_postfix(
                    {"Files": len(all_files), "Latest": latest_date_found or "None"}
                )

                # Stop if we've reached the stop date
                if latest_date_found and latest_date_found >= stop_date_str:
                    break

        print(
            f"Retrieved metadata for {len(all_files)} files in {request_count} requests"
        )
        if filtered_count > 0:
            print(f"Filtered out {filtered_count} files with future or invalid dates")

        # Convert to Polars DataFrame and sort by date
        df = pl.DataFrame(all_files)
        if not df.is_empty() and "Date" in df.columns:
            df = df.sort("Date")

        return df
