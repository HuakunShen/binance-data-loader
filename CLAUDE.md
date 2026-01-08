# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**binance-data-loader** is a Python library for downloading and processing historical cryptocurrency data from Binance Vision (Binance's public S3 bucket). It supports both spot and futures markets with concurrent downloading, data validation, resampling, and flexible output formats (Parquet/CSV).

## Development Commands

### Installation
```bash
# Install from source (recommended: use uv)
uv pip install -e .

# Or with pip
pip install -e .
```

### Build & Publish
```bash
# Build package
.venv/bin/python -m build

# Publish to PyPI
twine upload dist/*
```

### Running Examples
```bash
python examples/download_futures_data.py
python examples/download_spot_data.py
python examples/load_and_resample.py
python main.py
```

**Note:** This project does not have a test suite yet.

## Architecture

### Core Components

```
src/binance_data_loader/
├── __init__.py          # Public API exports
├── types.py             # TypedDict result types, Enums
├── schema.py            # Pandera validation schemas
├── utils.py             # Timestamp detection, path utilities
├── metadata.py          # S3 bucket metadata fetcher
├── downloader.py        # Main orchestrator for downloads
├── processor.py         # ZIP file processing (CSV -> Parquet/CSV)
└── loader.py            # Data loading and resampling utilities
```

### Component Relationships

1. **BinanceDataDownloader** (`downloader.py`) - Main orchestrator
   - Uses `BinanceDataMetadata` to fetch file lists from S3
   - Downloads ZIP files concurrently via `ThreadPoolExecutor`
   - Uses `DataProcessor` to convert files
   - Returns typed `DownloadResult` (success/skipped/error)

2. **BinanceDataMetadata** (`metadata.py`)
   - Queries Binance S3 bucket via XML API
   - Returns Polars DataFrame with file metadata
   - Handles pagination with marker-based iteration

3. **DataProcessor** (`processor.py`)
   - Extracts CSV from ZIP files
   - Validates with `BinanceKlineDataSchema` (Pandera)
   - Auto-detects timestamp units (ms vs ns) to avoid overflow
   - Outputs Parquet (default) or CSV
   - Uses `ThreadPoolExecutor` for parallel processing

4. **BinanceDataLoader** (`loader.py`)
   - Lazy loads Parquet files with Polars `scan_parquet()`
   - Time-range filtering
   - On-the-fly resampling to different intervals
   - Convenience functions: `load_kline_data()`, `get_date_range()`

### Key Patterns

- **Type Safety:** Extensive use of `TypedDict` for result types with union types (success/skipped/error)
- **Polars over Pandas:** Uses Polars for all DataFrame operations (lazy loading, efficient I/O)
- **Concurrency:** Uses `ThreadPoolExecutor` (I/O-bound), NOT `ProcessPoolExecutor` (changed in recent commit)
- **Data Validation:** Pandera schemas enforced before saving processed data
- **Timestamp Handling:** Auto-detects milliseconds vs nanoseconds to prevent overflow errors
- **Date Filtering:** Applied at both download time (metadata fetch) and load time (DataFrame filtering)

### Prefix Structure

The library uses a flexible prefix-based approach:

```
data/{asset_type}/{time_period}/{data_type}/{symbol}/{interval}/
```

Examples:
- `data/futures/um/daily/klines/BTCUSDT/1h/` - USDT-Margined futures
- `data/futures/cm/daily/klines/BTCUSD_PERP/1h/` - COIN-Margined futures
- `data/spot/daily/klines/ETHUSDT/5m/` - Spot data

### Data Type Detection

The library handles paths with and without `data/` prefix. The `data_type` parameter ("spot" or "futures") in `BinanceDataLoader` determines which directory to use:
- `spot` → `data/spot/`
- `futures` → `data/futures/`

## Dependencies

Core runtime dependencies:
- `polars>=1.36.1` - DataFrame library (lazy loading, efficient I/O)
- `pandera>=0.28.0` - Data validation schemas
- `requests>=2.32.5` - HTTP client for S3 API
- `tqdm>=4.66.0` - Progress bars
- `xmltodict>=1.0.2` - XML parsing for S3 bucket listing

Python 3.12+ required (targets 3.12, 3.13).

## Recent Refactoring

- Package renamed from `binance-data` to `binance-data-loader`
- Added typed result types (`DownloadResult`, `ProcessResult`)
- Switched from `ProcessPoolExecutor` to `ThreadPoolExecutor` for processing
- Added output existence check before processing to skip already-converted files
