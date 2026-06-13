"""Benchmark default Cachier hashing against xxhash for large NumPy arrays."""

from __future__ import annotations

import argparse
import pickle
import statistics
import time
from typing import Any, Callable, Dict, List

import numpy as np

from cachier.config import _default_hash_func


def _xxhash_numpy_hash(args: tuple[Any, ...], kwds: dict[str, Any]) -> str:
    """Hash call arguments with xxhash, optimized for NumPy arrays.

    Parameters
    ----------
    args : tuple[Any, ...]
        Positional arguments.
    kwds : dict[str, Any]
        Keyword arguments.

    Returns
    -------
    str
        xxhash hex digest.

    """
    import xxhash

    hasher = xxhash.xxh64()
    hasher.update(b"args")
    for value in args:
        if isinstance(value, np.ndarray):
            hasher.update(value.dtype.str.encode("utf-8"))
            hasher.update(str(value.shape).encode("utf-8"))
            hasher.update(value.tobytes(order="C"))
        else:
            hasher.update(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))

    hasher.update(b"kwds")
    for key, value in sorted(kwds.items()):
        hasher.update(pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL))
        if isinstance(value, np.ndarray):
            hasher.update(value.dtype.str.encode("utf-8"))
            hasher.update(str(value.shape).encode("utf-8"))
            hasher.update(value.tobytes(order="C"))
        else:
            hasher.update(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))

    return hasher.hexdigest()


def _benchmark(hash_func: Callable[[tuple[Any, ...], dict[str, Any]], str], args: tuple[Any, ...], runs: int) -> float:
    durations: List[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        hash_func(args, {})
        durations.append(time.perf_counter() - start)
    return statistics.median(durations)


def main() -> None:
    """Run benchmark comparing cachier default hashing with xxhash."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--elements",
        type=int,
        default=10_000_000,
        help="Number of float64 elements in the benchmark array",
    )
    parser.add_argument("--runs", type=int, default=7, help="Number of benchmark runs")
    parsed = parser.parse_args()

    try:
        import xxhash  # noqa: F401
    except ImportError as error:
        raise SystemExit("Missing dependency: xxhash. Install with `pip install xxhash`.") from error

    array = np.arange(parsed.elements, dtype=np.float64)
    args = (array,)

    results: Dict[str, float] = {
        "cachier_default": _benchmark(_default_hash_func, args, parsed.runs),
        "xxhash_reference": _benchmark(_xxhash_numpy_hash, args, parsed.runs),
    }

    ratio = results["cachier_default"] / results["xxhash_reference"]

    print(f"Array elements: {parsed.elements:,}")
    print(f"Array bytes: {array.nbytes:,}")
    print(f"Runs: {parsed.runs}")
    print(f"cachier_default median: {results['cachier_default']:.6f}s")
    print(f"xxhash_reference median: {results['xxhash_reference']:.6f}s")
    print(f"ratio (cachier_default / xxhash_reference): {ratio:.2f}x")


if __name__ == "__main__":
    main()
