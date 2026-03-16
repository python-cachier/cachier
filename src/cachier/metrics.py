"""Cache metrics and observability framework for cachier."""

# This file is part of Cachier.
# https://github.com/python-cachier/cachier

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license

import random
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from typing import Deque, Optional


@dataclass
class MetricSnapshot:
    """Snapshot of cache metrics at a point in time.

    Attributes
    ----------
    hits : int
        Number of cache hits
    misses : int
        Number of cache misses. Note: stale cache hits are also counted
        as misses, so stale_hits and misses may overlap.
    hit_rate : float
        Cache hit rate as percentage (0-100). Note: stale cache hits are
        counted as misses when computing the hit rate, so the rate may
        appear lower than expected when stale entries are served.
    total_calls : int
        Total number of cache accesses
    avg_latency_ms : float
        Average operation latency in milliseconds
    stale_hits : int
        Number of times stale cache entries were accessed
    recalculations : int
        Number of cache recalculations performed
    wait_timeouts : int
        Number of wait timeouts that occurred
    entry_count : int
        Current number of entries in cache
    total_size_bytes : int
        Total size of cache in bytes. Only populated for the memory
        backend; all other backends report 0.
    size_limit_rejections : int
        Number of entries rejected due to size limit

    """

    hits: int = 0
    misses: int = 0
    hit_rate: float = 0.0
    total_calls: int = 0
    avg_latency_ms: float = 0.0
    stale_hits: int = 0
    recalculations: int = 0
    wait_timeouts: int = 0
    entry_count: int = 0
    total_size_bytes: int = 0
    size_limit_rejections: int = 0


@dataclass
class _TimestampedMetric:
    """Internal metric with monotonic timestamp for time-windowed aggregation.

    Uses time.perf_counter() for monotonic timestamps that are immune to
    system clock adjustments.

    Parameters
    ----------
    timestamp : float
        Monotonic timestamp when the metric was recorded (from time.perf_counter())
    value : float
        The metric value

    """

    timestamp: float
    value: float


class CacheMetrics:
    """Thread-safe metrics collector for cache operations.

    This class collects and aggregates cache performance metrics including
    hit/miss rates, latencies, and size information. Metrics are collected
    in a thread-safe manner and can be aggregated over time windows.

    Parameters
    ----------
    sampling_rate : float, optional
        Sampling rate for metrics collection (0.0-1.0), by default 1.0
        Lower values reduce overhead at the cost of accuracy
    window_sizes : list of timedelta, optional
        Time windows to track for aggregated metrics, by default [1 minute, 1 hour, 1 day]

    Examples
    --------
    >>> metrics = CacheMetrics(sampling_rate=0.1)
    >>> metrics.record_hit()
    >>> metrics.record_miss()
    >>> stats = metrics.get_stats()
    >>> print(f"Hit rate: {stats.hit_rate}%")

    """

    def __init__(self, sampling_rate: float = 1.0, window_sizes: Optional[list[timedelta]] = None):
        """Initialize cache metrics collector.

        Parameters
        ----------
        sampling_rate : float
            Sampling rate between 0.0 and 1.0
        window_sizes : list of timedelta, optional
            Time windows for aggregated metrics

        """
        if not 0.0 <= sampling_rate <= 1.0:
            raise ValueError("sampling_rate must be between 0.0 and 1.0")

        self._lock = threading.RLock()
        self._sampling_rate = sampling_rate

        # Core counters
        self._hits = 0
        self._misses = 0
        self._stale_hits = 0
        self._recalculations = 0
        self._wait_timeouts = 0
        self._size_limit_rejections = 0

        # Latency tracking - time-windowed
        if window_sizes is None:
            window_sizes = [
                timedelta(minutes=1),
                timedelta(hours=1),
                timedelta(days=1),
            ]
        self._window_sizes = window_sizes
        self._max_window = max(window_sizes) if window_sizes else timedelta(0)

        # Use deque with fixed size based on expected frequency
        # Assuming ~1000 ops/sec max, keep 1 day of data = 86.4M points
        # Limit to 100K points for memory efficiency
        max_latency_points = 100000
        self._latencies: Deque[_TimestampedMetric] = deque(maxlen=max_latency_points)

        # Size tracking
        self._entry_count = 0
        self._total_size_bytes = 0

        self._random = random.Random()  # noqa: S311

    def _should_sample(self) -> bool:
        """Determine if this metric should be sampled.

        Returns
        -------
        bool
            True if metric should be recorded

        """
        if self._sampling_rate >= 1.0:
            return True
        return self._random.random() < self._sampling_rate

    def _record_counter(self, attr: str) -> None:
        """Increment a named counter if sampling allows it.

        Parameters
        ----------
        attr : str
            Name of the instance attribute to increment (e.g. ``"_hits"``)

        """
        if self._should_sample():
            with self._lock:
                self.__dict__[attr] += 1

    def record_hit(self) -> None:
        """Record a cache hit."""
        self._record_counter("_hits")

    def record_miss(self) -> None:
        """Record a cache miss."""
        self._record_counter("_misses")

    def record_stale_hit(self) -> None:
        """Record a stale cache hit."""
        self._record_counter("_stale_hits")

    def record_recalculation(self) -> None:
        """Record a cache recalculation."""
        self._record_counter("_recalculations")

    def record_wait_timeout(self) -> None:
        """Record a wait timeout."""
        self._record_counter("_wait_timeouts")

    def record_size_limit_rejection(self) -> None:
        """Record an entry rejection due to size limit."""
        self._record_counter("_size_limit_rejections")

    def record_latency(self, latency_seconds: float) -> None:
        """Record an operation latency.

        Parameters
        ----------
        latency_seconds : float
            Operation latency in seconds

        """
        if not self._should_sample():
            return
        with self._lock:
            # Use monotonic timestamp for immune-to-clock-adjustment windowing
            timestamp = time.perf_counter()
            self._latencies.append(_TimestampedMetric(timestamp=timestamp, value=latency_seconds))

    def update_size_metrics(self, entry_count: int, total_size_bytes: int) -> None:
        """Update cache size metrics.

        Parameters
        ----------
        entry_count : int
            Current number of entries in cache
        total_size_bytes : int
            Total size of cache in bytes

        """
        with self._lock:
            self._entry_count = entry_count
            self._total_size_bytes = total_size_bytes

    def _calculate_avg_latency(self, window: Optional[timedelta] = None) -> float:
        """Calculate average latency within a time window.

        Parameters
        ----------
        window : timedelta, optional
            Time window to consider. If None, uses all data.

        Returns
        -------
        float
            Average latency in milliseconds

        """
        # Use monotonic clock for cutoff calculation
        now = time.perf_counter()
        cutoff = now - window.total_seconds() if window else 0

        latencies = [metric.value for metric in self._latencies if metric.timestamp >= cutoff]

        if not latencies:
            return 0.0

        return (sum(latencies) / len(latencies)) * 1000  # Convert to ms

    def get_stats(self, window: Optional[timedelta] = None) -> MetricSnapshot:
        """Get current cache statistics.

        Parameters
        ----------
        window : timedelta, optional
            Time window for windowed metrics (latency).
            If None, returns all-time statistics.

        Returns
        -------
        MetricSnapshot
            Snapshot of current cache metrics

        """
        with self._lock:
            total_calls = self._hits + self._misses
            hit_rate = (self._hits / total_calls * 100) if total_calls > 0 else 0.0
            avg_latency = self._calculate_avg_latency(window)

            return MetricSnapshot(
                hits=self._hits,
                misses=self._misses,
                hit_rate=hit_rate,
                total_calls=total_calls,
                avg_latency_ms=avg_latency,
                stale_hits=self._stale_hits,
                recalculations=self._recalculations,
                wait_timeouts=self._wait_timeouts,
                entry_count=self._entry_count,
                total_size_bytes=self._total_size_bytes,
                size_limit_rejections=self._size_limit_rejections,
            )

    def reset(self) -> None:
        """Reset all metrics to zero.

        Thread-safe method to clear all collected metrics.

        """
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._stale_hits = 0
            self._recalculations = 0
            self._wait_timeouts = 0
            self._size_limit_rejections = 0
            self._latencies.clear()
            self._entry_count = 0
            self._total_size_bytes = 0


class MetricsContext:
    """Null-object context manager for cache operation instrumentation.

    Wraps an optional ``CacheMetrics`` instance so call-path code can invoke
    ``record_*`` methods unconditionally without ``if metrics:`` guards.
    Starts the latency timer on ``__enter__`` and records it automatically on
    ``__exit__``, covering every return path including exceptions.

    Parameters
    ----------
    metrics : CacheMetrics, optional
        Metrics object to record to. When ``None`` all operations are no-ops.

    Examples
    --------
    >>> metrics = CacheMetrics()
    >>> with MetricsContext(metrics) as m:
    ...     m.record_miss()
    ...     # Do cache operation
    ...     m.record_recalculation()

    """

    __slots__ = ("_m", "_start")

    def __init__(self, metrics: Optional[CacheMetrics]) -> None:
        self._m = metrics
        self._start: float = 0.0

    def __enter__(self) -> "MetricsContext":
        """Start timing the operation."""
        if self._m is not None:
            self._start = time.perf_counter()
        return self

    def __exit__(self, *_: object) -> None:
        """Record the operation latency."""
        if self._m is not None:
            self._m.record_latency(time.perf_counter() - self._start)

    def record_hit(self) -> None:
        """Record a cache hit."""
        if self._m:
            self._m.record_hit()

    def record_miss(self) -> None:
        """Record a cache miss."""
        if self._m:
            self._m.record_miss()

    def record_stale_hit(self) -> None:
        """Record a stale cache hit."""
        if self._m:
            self._m.record_stale_hit()

    def record_recalculation(self) -> None:
        """Record a cache recalculation."""
        if self._m:
            self._m.record_recalculation()

    def record_wait_timeout(self) -> None:
        """Record a wait timeout."""
        if self._m:
            self._m.record_wait_timeout()
