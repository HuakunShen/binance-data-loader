# binance-data

A Python library for downloading and processing historical data from Binance Vision.

## Features

- Download historical data from Binance Vision S3 bucket
- Support for multiple asset types (spot, futures)
- Flexible prefix-based approach for any data type
- Output formats: Parquet (default) or CSV
- Pandera schema validation for data integrity
- Timestamp auto-detection (milliseconds vs nanoseconds)
- Concurrent downloads for better performance
- Optional retention of raw ZIP files
- Preserve original directory structure

## Installation

```bash
pip install binance-data
```

Or install from source:

```bash
uv pip install -e .
```

## Quick Start

```python
from binance_data_loader import BinanceDataDownloader

# Download BTCUSDT 1h futures data as Parquet
downloader = BinanceDataDownloader(
    prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
    destination_dir="./data",
    output_format="parquet",
    keep_zip=False,
)
downloader.download()
```

## Usage Examples

### Download Futures Data

```python
from binance_data_loader import BinanceDataDownloader

# Download USDT-Margined futures data
downloader = BinanceDataDownloader(
    prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
    destination_dir="./data",
    output_format="parquet",
)
downloader.download()

# Download COIN-Margined futures data
downloader = BinanceDataDownloader(
    prefix="data/futures/cm/daily/klines/BTCUSD_PERP/1h/",
    destination_dir="./data",
    output_format="parquet",
)
downloader.download()
```

### Download Spot Data

```python
# Download spot data
downloader = BinanceDataDownloader(
    prefix="data/spot/daily/klines/ETHUSDT/5m/",
    destination_dir="./data",
    output_format="csv",  # Save as CSV instead of Parquet
    keep_zip=True,  # Keep raw ZIP files
)
downloader.download()
```

### Process Existing ZIP Files

If you already have ZIP files downloaded and only want to convert them to Parquet/CSV:

```python
from binance_data_loader import BinanceDataDownloader
from datetime import datetime, UTC

# Process existing ZIP files, skip downloading
downloader = BinanceDataDownloader(
    prefix="data/spot/daily/klines/ETHUSDT/5m/",
    destination_dir="./data",
    output_format="parquet",
    skip_download=True,  # Skip downloading, only process existing ZIP files
)
downloader.download()
```

### Filter by Date Range

Download only files within a specific date range:

```python
from binance_data_loader import BinanceDataDownloader
from datetime import datetime, UTC, timedelta

# Download data for the last 6 months
six_months_ago = datetime.now(tz=UTC) - timedelta(days=180)

downloader = BinanceDataDownloader(
    prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
    destination_dir="./data",
    output_format="parquet",
    start_date=six_months_ago,
)
downloader.download()
```

### Available Intervals

Binance supports the following intervals:

- Seconds: `1s`
- Minutes: `1m`, `3m`, `5m`, `15m`, `30m`
- Hours: `1h`, `2h`, `4h`, `6h`, `8h`, `12h`
- Days: `1d`, `3d`
- Weeks: `1w`
- Months: `1M`

### Prefix Structure

The library uses a prefix-based approach where you specify the exact path to the data you want:

```
data/{asset_type}/{time_period}/{data_type}/{symbol}/{interval}/
```

Examples:

- `data/futures/um/daily/klines/BTCUSDT/1h/` - BTCUSDT futures 1h klines
- `data/spot/daily/klines/ETHUSDT/5m/` - ETHUSDT spot 5m klines
- `data/futures/um/monthly/klines/BTCUSDT/1m/` - BTCUSDT futures monthly 1m klines

### Configuration Options

```python
downloader = BinanceDataDownloader(
    prefix="data/futures/um/daily/klines/BTCUSDT/1h/",  # Required: Data prefix
    destination_dir="./data",                              # Optional: Output directory (default: "./data")
    output_format="parquet",                                # Optional: "parquet" or "csv" (default: "parquet")
    keep_zip=True,                                         # Optional: Keep raw ZIP files (default: True)
    max_workers=10,                                        # Optional: Concurrent download workers (default: 10)
    max_processors=4,                                      # Optional: Parallel processing workers (default: 4)
    start_date=datetime(2024, 1, 1, tzinfo=UTC),        # Optional: Start datetime filter (default: None)
    end_date=datetime(2024, 12, 31, tzinfo=UTC),          # Optional: End datetime filter (default: None)
    skip_download=False,                                     # Optional: Skip download, only process existing ZIP files (default: False)
    base_url="https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",  # Optional: Custom base URL
)
```

## API Reference

### BinanceDataDownloader

Main downloader class for fetching Binance Vision data.

#### Constructor

```python
BinanceDataDownloader(
    prefix: str,
    destination_dir: str = "./data",
    output_format: str = "parquet",
    keep_zip: bool = True,
    max_workers: int = 10,
    max_processors: int = 4,
    start_date: datetime = None,
    end_date: datetime = None,
    skip_download: bool = False,
    base_url: str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision",
)
```

**Parameters:**

- `prefix` (str, required): Binance S3 bucket prefix for the data you want to download
- `destination_dir` (str, optional): Directory where processed files will be saved. Default: `"./data"`
- `output_format` (str, optional): Output format, either `"parquet"` or `"csv"`. Default: `"parquet"`
- `keep_zip` (bool, optional): Whether to keep raw ZIP files after processing. Default: `True`
- `max_workers` (int, optional): Number of concurrent download workers. Default: `10`
- `max_processors` (int, optional): Number of parallel processing workers. Default: `4`
- `start_date` (datetime, optional): Start datetime for filtering files. Only downloads/converts files from this date onwards. Default: `None`
- `end_date` (datetime, optional): End datetime for filtering files. Only downloads/converts files up to this date. Default: `None`
- `skip_download` (bool, optional): If `True`, skip downloading and only process existing ZIP files. Default: `False`
- `base_url` (str, optional): Base URL for Binance data S3 bucket

#### Methods

##### download()

```python
download() -> Tuple[List[dict], List[dict]]
```

Execute the download and processing pipeline.

**Returns:**

- `Tuple[List[dict], List[dict]]`:
  - First element: List of download results (success/failure)
  - Second element: List of processing results (successful, failed)

**Example:**

```python
download_results, process_results = downloader.download()

# Download results
print(f"Downloaded {len([r for r in download_results if r['status'] == 'success'])} files")

# Process results: (successful, failed)
successful, failed = process_results
print(f"Processed {len(successful)} files successfully, {len(failed)} failed")
```

### DataProcessor

Process downloaded ZIP files into Parquet or CSV format.

```python
from binance_data_loader.processor import DataProcessor

processor = DataProcessor(output_format="parquet")
result = processor.process_zip_file(
    zip_path="data/futures/um/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip",
    output_dir="./output",
    base_data_dir="./data",
)

# Process multiple files in parallel
successful, failed = processor.process_zip_files(
    zip_files=["path1.zip", "path2.zip"],
    output_dir="./output",
    base_data_dir="./data",
    max_workers=4,
)
```

### BinanceDataMetadata

Fetch metadata about available Binance data files.

```python
from binance_data_loader.metadata import BinanceDataMetadata

metadata = BinanceDataMetadata()
df = metadata.fetch_file_list(
    prefix="data/futures/um/daily/klines/BTCUSDT/1h/",
    stop_date="2024-01-31",  # Optional: stop at this date
)

print(f"Found {len(df)} files")
print(df.head())
```

### BinanceDataLoader

Loader for accessing and resampling downloaded Binance kline data.

#### Constructor

```python
BinanceDataLoader(
    data_dir: Path,
    data_type: Literal["futures", "spot"] = "spot",
    output_format: Literal["parquet", "csv"] = "parquet",
    shift: Optional[str] = None,
    skip_first: bool = True,
)
```

**Parameters:**

- `data_dir` (Path, required): Root directory containing processed Binance data
- `data_type` (str, optional): Type of data - `"futures"` or `"spot"`. Default: `"spot"`
- `output_format` (str, optional): Format of processed files - `"parquet"` or `"csv"`. Default: `"parquet"`
- `shift` (str, optional): Time offset to shift resampling interval boundaries. For example, with `shift="1m"` and `resample_to="15m"`, intervals will end at 1, 16, 31, 46 minutes instead of 0, 15, 30, 45. Supports units: `"1s"`, `"5m"`, `"1h"`, `"1d"`, etc. Default: `None` (no shift)
- `skip_first` (bool, optional): Whether to skip the first row if it contains less data than full resample interval. Useful when shifting causes partial intervals. Default: `True`

#### Methods

##### load()

```python
load(
    symbol: str,
    interval: str = "1m",
    resample_to: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    shift: Optional[str] = None,
    skip_first: Optional[bool] = None,
) -> pl.DataFrame
```

Load kline data with optional resampling and time filtering.

**Parameters:**

- `symbol` (str, required): Trading pair symbol (e.g., "BTCUSDT")
- `interval` (str, optional): Base interval of the data files (e.g., `"1s"`, `"1m"`, `"1h"`, `"1d"`). Default: `"1m"`
- `resample_to` (str, optional): Resampling interval (e.g., `"5s"`, `"15s"`, `"5m"`, `"15m"`, `"1h"`, `"1d"`). Default: `None`
- `start_time` (datetime, optional): Start datetime. If `None`, uses 1 year before end_time. Default: `None`
- `end_time` (datetime, optional): End datetime. If `None`, uses latest available data. Default: `None`
- `shift` (str, optional): Override for shift. If `None`, uses value from `__init__`. Default: `None`
- `skip_first` (bool, optional): Override for skip_first. If `None`, uses value from `__init__`. Default: `None`

**Returns:**

- `pl.DataFrame`: Polars DataFrame with kline data filtered to specified time range

**Example:**

```python
loader = BinanceDataLoader(
    data_dir=Path("./data"),
    data_type="spot",
    shift="1m",  # Default shift for all loads
)

# Load with default shift
df = loader.load("BTCUSDT", "1m", "15m")

# Override shift and skip_first for this specific load
df_custom = loader.load(
    "BTCUSDT", "1m", "15m",
    shift="30s",  # Custom shift
    skip_first=False,  # Keep partial intervals
)
```

##### get_date_range()

```python
get_date_range(symbol: str, interval: str = "1m") -> Tuple[datetime, datetime]
```

Get the date range available in processed data files.

**Parameters:**

- `symbol` (str, required): Trading pair symbol (e.g., "BTCUSDT")
- `interval` (str, optional): Base interval of the data files (e.g., `"1s"`, `"1m"`, `"1h"`). Default: `"1m"`

**Returns:**

- `Tuple[datetime, datetime]`: (start_date, end_date) as datetime objects

**Raises:**

- `FileNotFoundError`: If no data files are found
- `ValueError`: If data directory is empty

**Example:**

```python
loader = BinanceDataLoader(data_dir="./data", data_type="spot")
start, end = loader.get_date_range("ETHUSDT", "1s")
print(f"Available data from {start} to {end}")
```

## Data Loading and Resampling

### Loading Data

After downloading and processing data, you can easily load it for analysis:

```python
from binance_data_loader import BinanceDataLoader
from datetime import datetime, timedelta, UTC

loader = BinanceDataLoader(data_dir="./data", data_type="spot")

# Get available date range
start, end = loader.get_date_range("ETHUSDT", "1s")
print(f"Available data from {start} to {end}")

# Load last week of data
end_time = datetime.now(tz=UTC)
start_time = end_time - timedelta(days=7)

df = loader.load(
    symbol="ETHUSDT",
    interval="1s",
    start_time=start_time,
    end_time=end_time,
)
```

### Resampling Data

The loader supports on-the-fly resampling to higher timeframes:

```python
# Load 1s data and resample to 5m
df_5m = loader.load(
    symbol="ETHUSDT",
    interval="1s",
    resample_to="5m",
    start_time=start_time,
    end_time=end_time,
)

# Resample to 1h
df_1h = loader.load(
    symbol="ETHUSDT",
    interval="1s",
    resample_to="1h",
    start_time=start_time,
    end_time=end_time,
)
```

Supported resampling intervals:

- Seconds: `1s`, `5s`, `15s`, `30s`
- Minutes: `1m`, `3m`, `5m`, `15m`, `30m`
- Hours: `1h`, `2h`, `4h`, `6h`, `12h`
- Days: `1d`
- Weeks: `1w`

### Shifted Resampling

Shift interval boundaries to create different data augmentations for training:

```python
# Default 15m intervals end at 0, 15, 30, 45 minutes
df_standard = loader.load(
    symbol="ETHUSDT",
    interval="1m",
    resample_to="15m",
    start_time=start_time,
    end_time=end_time,
)

# Shifted by 1m - intervals end at 1, 16, 31, 46 minutes
df_shifted = loader.load(
    symbol="ETHUSDT",
    interval="1m",
    resample_to="15m",
    start_time=start_time,
    end_time=end_time,
    shift="1m",
)
```

Shift supports multiple time units:
- Seconds: `30s`, `45s`
- Minutes: `1m`, `5m`, `10m`
- Hours: `2h`, `6h`
- Days: `1d`, `3d`

**Use case**: Generate multiple shifted versions of your data (e.g., `0m`, `1m`, `2m`, `3m`) for data augmentation during model training. This helps models learn patterns regardless of interval alignment.

### Skip Partial Intervals

When shifting intervals, the first interval may contain partial data. Use `skip_first` to remove it:

```python
# Skip partial intervals (default behavior)
df = loader.load(
    symbol="ETHUSDT",
    interval="1m",
    resample_to="4h",
    start_time=start_time,
    end_time=end_time,
    shift="1m",
    # skip_first=True  # Default, removes partial first interval
)

# Keep all intervals including partial
df_all = loader.load(
    symbol="ETHUSDT",
    interval="1m",
    resample_to="4h",
    start_time=start_time,
    end_time=end_time,
    shift="1m",
    skip_first=False,  # Keep partial intervals
)
```

A partial interval is removed if its duration is less than 80% of the target resample interval. This prevents biased data at the start when shifting causes incomplete first intervals.

### Convenience Functions

Quick loading without class instantiation:

```python
from binance_data_loader import load_kline_data, get_date_range

# Get date range
start, end = get_date_range(
    data_dir="./data",
    symbol="BTCUSDT",
    data_type="spot",
    interval="1h",
)

# Load with resampling
df = load_kline_data(
    data_dir="./data",
    symbol="BTCUSDT",
    data_type="spot",
    interval="1h",
    resample_to="1d",
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 12, 31),
    shift="1h",  # Optional shift for interval boundaries
    skip_first=True,  # Optional: skip partial first interval
)
```

### Working with Both Spot and Futures

```python
# Load spot data
spot_loader = BinanceDataLoader(data_dir="./data", data_type="spot")
df_spot = spot_loader.load("BTCUSDT", "1h")

# Load futures data
futures_loader = BinanceDataLoader(data_dir="./data", data_type="futures")
df_futures = futures_loader.load("BTCUSDT", "1h")
```

## Data Schema

### Kline Data

When downloading kline data, the output will contain the following columns:

| Column                 | Type     | Description                  |
| ---------------------- | -------- | ---------------------------- |
| open_time              | Datetime | Open time (UTC)              |
| open                   | Float    | Open price                   |
| high                   | Float    | High price                   |
| low                    | Float    | Low price                    |
| close                  | Float    | Close price                  |
| volume                 | Float    | Volume in base asset         |
| close_time             | Datetime | Close time (UTC)             |
| quote_volume           | Float    | Volume in quote asset        |
| count                  | Int      | Number of trades             |
| taker_buy_volume       | Float    | Taker buy base asset volume  |
| taker_buy_quote_volume | Float    | Taker buy quote asset volume |
| ignore                 | Int      | Ignore                       |

The library automatically:

- Validates data structure using Pandera schemas
- Detects and converts timestamp units (milliseconds/nanoseconds)
- Ensures proper type casting
- Validates UTC timezone

## Performance Tips

1. **Adjust Workers**: Increase `max_workers` for faster downloads, but be mindful of your network bandwidth.
2. **Process in Parallel**: Increase `max_processors` for faster conversion, but consider CPU resources.
3. **Use Parquet**: Parquet is more efficient than CSV for large datasets and subsequent analysis.
4. **Keep ZIP**: Set `keep_zip=True` if you need to re-process data with different settings.

## Examples

The library includes several example scripts in the `examples/` folder to help you get started quickly:

### Download Examples

- **`examples/download_futures_data.py`** - Download futures (USDT-Margined) kline data

  - Download last year of BTCUSDT 1h data
  - Download 2024 ETHUSDT 5m data
  - Demonstrates date range filtering and keep_zip options

- **`examples/download_spot_data.py`** - Download spot kline data
  - Download first week of ETHUSDT 1s data (Jan 1-7, 2024)
  - Download last month of BTCUSDT 1m data
  - Download in CSV format instead of Parquet

### Loading and Resampling Examples

- **`examples/load_and_resample.py`** - Load and resample downloaded data
  - Load spot data without resampling
  - Resample 1s data to 5m, 15m, and 1h intervals
  - Load futures data
  - Complete workflow showing load, resample, and compare different timeframes
  - Load data for specific date ranges
  - **Shift resampling with different time offsets** (`shift` parameter)
  - **Skip partial intervals when shifting** (`skip_first` parameter)
  - **Generate multiple shifted datasets for training data augmentation**

### Running the Examples

Each example can be run directly:

```bash
# Download futures data
python examples/download_futures_data.py

# Download spot data
python examples/download_spot_data.py

# Load and resample data
python examples/load_and_resample.py
```

You can also modify the examples to suit your needs - change symbols, intervals, date ranges, or output formats.

## Roadmap

- [x] Kline data download and processing
- [x] Parquet and CSV output formats
- [x] Concurrent downloads and parallel processing
- [x] Data loader utilities for easy reading of downloaded data
- [x] Resampling utilities
- [ ] Support for other data types (aggTrades, trades, bookDepth, etc.)
- [ ] CLI interface

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## License

MIT License

## Acknowledgments

This library is inspired by and borrows ideas from:

- [binance-bulk-downloader](https://github.com/binance/binance-public-data)
- Binance Vision S3 bucket structure

## Support

For issues and questions, please open an issue on GitHub.

## Publish

```bash
.venv/bin/python -m build
twine upload dist/*
```
