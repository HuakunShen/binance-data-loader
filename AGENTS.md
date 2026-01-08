# AGENTS.md

## Build and Development Commands

This project uses Python 3.12+ and Hatchling for building.

### Installation
```bash
# Install in development mode
uv pip install -e .

# Or with pip
pip install -e .
```

### Code Quality
```bash
ruff check .    # Lint code
ruff format .   # Format code
```

### Build Package
```bash
.venv/bin/python -m build
twine upload dist/*
```

### Running Examples
```bash
python examples/download_futures_data.py
python examples/download_spot_data.py
python examples/load_and_resample.py
```

### Testing
No test infrastructure configured yet. To add tests, create `tests/` directory and set up pytest.

## Code Style Guidelines

### Imports
- Standard library first, then third-party, then local (alphabetical within sections)
- Import from `typing`: `Optional`, `Literal`, `Union`, `TypedDict`, `Tuple`, `List`
- Always import `Path` from `pathlib` for file paths

```python
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Literal, Optional, Tuple
import polars as pl
```

### Type Hints
- Always include type hints for parameters and returns
- Use `Literal` for string choices: `Literal["parquet", "csv"]`
- Use `Optional` for nullable, `TypedDict` for structured returns, `Union` for multiple types

```python
def process_zip_file(
    self, zip_path: Path, output_dir: Path, base_data_dir: Path
) -> ProcessResult:
```

### Naming Conventions
- Classes: PascalCase (`BinanceDataDownloader`)
- Functions/Methods: snake_case (`download_file`)
- Constants: UPPER_SNAKE_CASE (`BINANCE_KLINE_COLUMNS`)
- Private methods: prefix with underscore (`_download_single_file`)
- Parameters: snake_case (`output_format`, `max_workers`)

### Docstrings
- Triple-quoted docstrings for modules, classes, and public methods
- Format: Description → Args → Returns → Raises (if applicable)
- Use UTC timezone for datetime examples

```python
def download(self) -> Tuple[List[DownloadResult], Tuple[List[ProcessResult], List[ProcessResult]]]:
    """
    Download and process Binance data.

    Returns:
        Tuple of (download_results, (process_successful, process_failed))
    """
```

### Error Handling
- Use try/except for expected failures
- Return result dictionaries with `status` field: "success", "skipped", or "error"
- Use TypedDict variants for each status type
- Catch specific exceptions when possible
- Include error messages in result dicts

```python
try:
    result = future.result()
    return success_result
except Exception as e:
    error_result: ProcessResultError = {
        "status": "error",
        "file_path": zip_path,
        "error": str(e),
    }
    return error_result
```

### Data Processing
- Use Polars for all DataFrame operations
- Prefer lazy operations (`pl.scan_parquet`) for efficiency
- Use explicit type casting: `pl.col("open").cast(pl.Float64)`
- Use `group_by_dynamic` for time-based aggregation
- Use Pandera schemas for validation

### File Paths
- Always use `Path` objects from `pathlib`
- Use `Path("directory")` instead of string concatenation
- Check existence with `path.exists()`
- Create directories with `path.mkdir(parents=True, exist_ok=True)`

### Progress Tracking
- Use `tqdm` for progress bars
- Update progress bars inside loops/futures
- Show relevant metrics in `set_postfix`

```python
with tqdm(total=len(files), desc="Processing", unit="file") as pbar:
    for result in results:
        pbar.update(1)
        pbar.set_postfix({"Rows": result.get("rows", 0)})
```

### Concurrency
- Use `ThreadPoolExecutor` for I/O-bound tasks
- Use `as_completed` to process results as they arrive
- Set max_workers appropriately (default 10 for downloads, 4 for processing)

### Validation
- Validate input parameters early in methods
- Raise `ValueError` for invalid arguments
- Check required fields before expensive operations

```python
if self.output_format not in ["parquet", "csv"]:
    raise ValueError("output_format must be 'parquet' or 'csv'")
```

### Return Types
- Return early for error conditions
- Use specific TypedDict types for result objects
- Include all relevant metadata in success results

### Dependencies
- Primary dependencies: polars, pandera, requests, tqdm, xmltodict
- Avoid adding new dependencies without necessity
- Use existing utility functions from `binance_data_loader.utils`

### Project Structure
- Main source: `src/binance_data_loader/`
- Examples: `examples/`
- Main entry: `main.py` (demonstrations)
- Types: `types.py` (TypedDict definitions)
- Schema: `schema.py` (Pandera validation)
- Utils: `utils.py` (helper functions)
