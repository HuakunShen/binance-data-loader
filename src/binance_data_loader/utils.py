"""Utility functions for binance-data library."""

from pathlib import Path
from typing import Optional, Union


def detect_timestamp_unit(timestamp: int) -> str:
    """
    Detect whether timestamp is in milliseconds or nanoseconds.

    Args:
        timestamp: Integer timestamp value

    Returns:
        'ms' or 'ns'
    """
    # Milliseconds: ~13 digits for 2025 (e.g., 1767225600000)
    # Nanoseconds: ~19 digits for 2025 (e.g., 1767225600000000000)
    if timestamp > 1e15:  # Greater than quadrillion (15 zeros) = nanoseconds
        return "ns"
    else:  # Less than that = milliseconds
        return "ms"


def get_relative_path(
    full_path: Union[str, Path], base_dir: Union[str, Path]
) -> Optional[Path]:
    """
    Get relative path from full path and base directory.

    Args:
        full_path: Full path to file
        base_dir: Base directory

    Returns:
        Relative path or None if path is not relative to base_dir
    """
    try:
        full_path_obj = Path(full_path)
        base_path_obj = Path(base_dir)
        return full_path_obj.relative_to(base_path_obj)
    except ValueError:
        return None


def remove_prefix_from_path(rel_path: Path, prefix: str) -> Path:
    """
    Remove a prefix from a path.

    Args:
        rel_path: Relative path
        prefix: Prefix to remove (e.g., "data/")

    Returns:
        Path without prefix
    """
    parts = list(rel_path.parts)
    if parts and parts[0] == prefix.strip("/"):
        return Path(*parts[1:])
    return rel_path


def parse_interval_ms(interval: str) -> int:
    """Parse a duration string to milliseconds.

    Supports: Xms (milliseconds), Xs (seconds), Xm (minutes), Xh (hours).

    Args:
        interval: Duration string, e.g. "500ms", "1s", "5s", "1m", "1h"

    Returns:
        Duration in milliseconds

    Raises:
        ValueError: If the format is not supported

    Examples:
        >>> parse_interval_ms("500ms")
        500
        >>> parse_interval_ms("1s")
        1000
        >>> parse_interval_ms("1m")
        60000
        >>> parse_interval_ms("1h")
        3600000
    """
    if interval.endswith("ms"):
        return int(interval[:-2])
    elif interval.endswith("s"):
        return int(interval[:-1]) * 1_000
    elif interval.endswith("m"):
        return int(interval[:-1]) * 60_000
    elif interval.endswith("h"):
        return int(interval[:-1]) * 3_600_000
    raise ValueError(
        f"Unsupported interval format: {interval!r}. Use Xms / Xs / Xm / Xh."
    )
