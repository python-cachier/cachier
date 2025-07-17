"""Utility helpers for Cachier."""

import re
from typing import Optional, Union


def parse_bytes(size: Union[int, str, None]) -> Optional[int]:
    """Convert a human friendly size string to bytes."""
    if size is None:
        return None
    if isinstance(size, int):
        return size
    match = re.fullmatch(r"(?i)\s*(\d+(?:\.\d+)?)\s*([kmgt]?b)?\s*", str(size))
    if not match:
        raise ValueError(f"Invalid size value: {size}")
    number = float(match.group(1))
    unit = (match.group(2) or "b").upper()
    factor = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }[unit]
    return int(number * factor)
