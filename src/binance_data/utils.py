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
